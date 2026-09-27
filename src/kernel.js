import { randomUUID } from 'node:crypto';
import { asBytes, isDigest, sha256Hex, verifyDigest } from './digest.js';
import { EVT, emptyState, fold, replay } from './fold.js';
import { stateHash } from './state-hash.js';
import { nodeKind, reach, retainers } from './graph.js';
import { compareSnapshots, gcCandidates, resolvePath, reviewReport } from './analysis.js';
import { ConflictError, NotFoundError, ValidationError } from './errors.js';
import { paginate } from './paging.js';

/**
 * Content-addressable snapshot review kernel.
 *
 * Durable truth lives in the storage event log; the in-memory projection is a
 * cache rebuilt by fold(). All mutations are serialized through a promise
 * chain so concurrent callers get linearizable commits, and every mutating
 * call returns the post-commit revision — enabling optimistic concurrency
 * with explicit conflict reporting.
 */
export class Kernel {
  constructor(storage, { now = () => new Date() } = {}) {
    this.storage = storage;
    this._now = now;
    this.state = emptyState();
    this._commitChain = Promise.resolve();
  }

  static async recover(storage, opts = {}) {
    const k = new Kernel(storage, opts);
    const events = await storage.listEvents();
    k.state = replay(events);
    return k;
  }

  get revision() {
    return this.state.revision;
  }

  hash() {
    return stateHash(this.state);
  }

  nowIso() {
    return this._now().toISOString();
  }

  // -------------------------------------------------------------------------
  // internal event plumbing
  // -------------------------------------------------------------------------

  /** Serialize a mutation; body receives nothing, must return its result. */
  _serialize(body) {
    const run = this._commitChain.then(() => body());
    // keep the chain alive even when the caller's promise rejects
    this._commitChain = run.then(
      () => undefined,
      () => undefined,
    );
    return run;
  }

  async _emit(type, payload) {
    const seq = this.state.revision + 1;
    const evt = {
      id: 'evt_' + seq.toString(10).padStart(8, '0') + '_' + randomUUID().slice(0, 8),
      seq,
      type,
      recordedAt: this.nowIso(),
      ...payload,
    };
    await this.storage.appendEvent(evt);
    fold(this.state, evt);
    this._invalidateHistory();
    return evt;
  }

  async _stateAt(revision) {
    if (revision == null || revision >= this.state.revision) return this.state;
    if (!this._historyCache) this._historyCache = new Map();
    const key = Math.max(0, Math.min(revision, this.state.revision));
    if (this._historyCache.has(key)) return this._historyCache.get(key);
    const events = await this.storage.listEvents();
    const st = replay(events.slice(0, key));
    this._historyCache.set(key, st);
    return st;
  }

  /** Drop memoized historical projections after a commit. */
  _invalidateHistory() {
    this._historyCache = null;
  }

  // -------------------------------------------------------------------------
  // batch import — objects may arrive before the objects they reference,
  // content may arrive later than an announcement, bad items never propagate
  // -------------------------------------------------------------------------

  importBatch(batch) {
    return this._serialize(() => this._importBatch(batch));
  }

  async _importBatch(batch) {
    if (!batch || typeof batch !== 'object') throw new ValidationError('batch must be an object');
    const rawItems = Array.isArray(batch.items)
      ? batch.items
      : batch.items && typeof batch.items === 'object'
        ? [batch.items]
        : [];
    const items = rawItems;
    const edges = Array.isArray(batch.edges)
      ? batch.edges
      : batch.edges && typeof batch.edges === 'object'
        ? [batch.edges]
        : [];
    const batchId = batch.batchId || 'bat_' + randomUUID();
    if (this.state.batches.has(batchId)) {
      throw new ConflictError('batch already imported', { kind: 'duplicate-batch', batchId });
    }

    const itemResults = [];
    // Two phases within the batch: object outcomes first, then edges — so a
    // reference satisfied in the *same* batch is immediately resolvable.
    for (const raw of items) {
      itemResults.push(await this._ingestItem(raw, batchId));
    }
    const edgeResults = [];
    for (const rawEdge of edges) {
      edgeResults.push(await this._declareEdge(rawEdge, batchId));
    }

    await this._emit(EVT.BATCH_RECORDED, {
      batchId,
      note: batch.note ?? null,
      items: itemResults.map((r) => ({
        digest: r.digest,
        status: r.status,
        evidenceId: r.evidenceId ?? null,
        reason: r.reason ?? null,
      })),
      edges: edgeResults.map((r) => ({ from: r.from, to: r.to, status: r.status })),
    });

    await this._reconcileDecisions();

    return {
      batchId,
      revision: this.state.revision,
      stateHash: this.hash(),
      items: itemResults,
      edges: edgeResults,
    };
  }

  async _ingestItem(raw, batchId) {
    if (!raw || typeof raw !== 'object') throw new ValidationError('batch item must be an object');
    const declared = typeof raw.digest === 'string' ? raw.digest.toLowerCase() : null;
    if (!declared || !isDigest(declared)) {
      throw new ValidationError('item.digest must be a sha256: content address', { item: raw });
    }

    // Already a valid object: idempotent re-import is a no-op record.
    if (this.state.objects.has(declared)) {
      return { digest: declared, status: 'already-present' };
    }
    // A previously rejected digest may be retried; new evidence appends.
    if (raw.content === undefined || raw.content === null) {
      // Announcement only — a late object may fill this in later.
      await this._emit(EVT.OBJECT_OFFERED, { digest: declared, batchId });
      return { digest: declared, status: 'offered', note: 'content not yet received' };
    }

    let bytes;
    try {
      bytes = asBytes(raw.content);
    } catch (e) {
      throw new ValidationError(e.message, { digest: declared });
    }
    if (raw.size != null && Number(raw.size) !== bytes.length) {
      return this._reject(declared, bytes, {
        batchId,
        reason: 'size-mismatch',
        declaredSize: Number(raw.size),
        size: bytes.length,
        contentType: raw.contentType ?? null,
      });
    }

    const check = verifyDigest(declared, bytes);
    if (!check.ok) {
      return this._reject(declared, bytes, {
        batchId,
        reason: check.reason,
        actualDigest: check.actual,
        expected: check.expected,
        size: bytes.length,
        contentType: raw.contentType ?? null,
      });
    }

    await this.storage.saveBlob(check.digest, bytes);
    await this._emit(EVT.OBJECT_IMPORTED, {
      digest: check.digest,
      size: bytes.length,
      contentType: raw.contentType ?? null,
      batchId,
    });
    return { digest: check.digest, status: 'imported', size: bytes.length };
  }

  async _reject(declared, bytes, info) {
    const actual = info.actualDigest ?? this._quickHash(bytes);
    const evidenceId =
      'ev_' + (actual && actual.startsWith('sha256:') ? actual.slice(7, 21) : randomUUID().slice(0, 12));
    await this.storage.saveQuarantine(
      evidenceId,
      {
        receivedAt: this.nowIso(),
        context: info.batchId ? 'batch-import' : 'inline',
        batchId: info.batchId ?? null,
        declaredDigest: declared,
        actualDigest: actual,
        reason: info.reason,
        size: bytes.length,
        declaredSize: info.declaredSize ?? null,
        contentType: info.contentType ?? null,
      },
      bytes,
    );
    await this._emit(EVT.OBJECT_REJECTED, {
      digest: declared,
      actualDigest: actual,
      reason: info.reason,
      evidenceId,
      batchId: info.batchId ?? null,
      size: bytes.length,
      declaredSize: info.declaredSize ?? null,
    });
    return {
      digest: declared,
      status: 'rejected',
      reason: info.reason,
      expected: declared,
      actual,
      evidenceId,
    };
  }

  _quickHash(bytes) {
    return sha256Hex(bytes);
  }

  async _declareEdge(rawEdge, batchId) {
    const from = typeof rawEdge?.from === 'string' ? rawEdge.from.toLowerCase() : null;
    const to = typeof rawEdge?.to === 'string' ? rawEdge.to.toLowerCase() : null;
    if (!from || !to || !isDigest(from) || !isDigest(to)) {
      throw new ValidationError('edge requires valid sha256 from/to', { edge: rawEdge });
    }
    const key = from + ' ' + to;
    if (this.state.edgesByKey.has(key)) {
      return { from, to, status: 'already-declared' };
    }
    await this._emit(EVT.EDGE_DECLARED, { from, to, batchId });
    const fromKind = nodeKind(this.state, from);
    return {
      from,
      to,
      status: 'declared',
      note:
        fromKind !== 'present'
          ? `source is ${fromKind}; edge indexed but cannot propagate through it`
          : null,
    };
  }

  // -------------------------------------------------------------------------
  // chunked upload path — used for large objects; storage owns streaming
  // -------------------------------------------------------------------------

  createUpload(meta) {
    return this._serialize(async () => {
      if (meta?.declaredDigest && !isDigest(meta.declaredDigest)) {
        throw new ValidationError('declaredDigest must be a sha256 content address');
      }
      const up = await this.storage.createUpload({
        declaredDigest: meta?.declaredDigest ?? null,
        declaredSize: meta?.declaredSize ?? null,
        contentType: meta?.contentType ?? null,
      });
      return up;
    });
  }

  writeUploadPart(uploadId, chunk, opts) {
    return this._serialize(() => this.storage.writeUploadPart(uploadId, chunk, opts));
  }

  commitUpload(uploadId, { batchId = null, digest = null, note = null, edges = [] } = {}) {
    return this._serialize(() => this._commitUpload(uploadId, { batchId, digest, note, edges }));
  }

  async _commitUpload(uploadId, opts) {
    const before = await this.storage.getUpload(uploadId);
    if (!before) throw new NotFoundError('unknown upload', { uploadId });
    const finalized = await this.storage.finalizeUpload(uploadId, { claimedDigest: opts.digest });
    const importBatchId = opts.batchId || 'bat_' + randomUUID();

    if (finalized.status !== 'committed') {
      // The storage layer preserved the bad bytes in quarantine. Record the
      // rejection in the chain so it can never silently disappear. When the
      // claimed address is invalid/absent, index by the actual content digest
      // (what was received) rather than inventing a fake all-zero address.
      const declaredRaw = (opts.digest || finalized.declaredDigest || '').toLowerCase();
      const declared = isDigest(declaredRaw) ? declaredRaw : null;
      const actual = finalized.actualDigest && isDigest(finalized.actualDigest)
        ? finalized.actualDigest
        : null;
      const indexedDigest = declared ?? actual;
      if (!indexedDigest) {
        throw new ValidationError('rejected upload has no digestable identity', {
          uploadId,
          rejection: finalized.rejection ?? null,
          evidenceId: finalized.quarantineEvidenceId ?? null,
        });
      }
      const rejectionEvent = {
        digest: indexedDigest,
        declaredDigest: declared,
        actualDigest: actual,
        reason: finalized.rejection?.reason ?? 'upload-rejected',
        evidenceId: finalized.quarantineEvidenceId ?? null,
        batchId: importBatchId,
        size: finalized.size ?? null,
        declaredSize: finalized.declaredSize ?? null,
      };
      await this._emit(EVT.OBJECT_REJECTED, rejectionEvent);
      await this._emit(EVT.BATCH_RECORDED, {
        batchId: importBatchId,
        note: opts.note ?? 'staged upload rejected',
        items: [{ digest: indexedDigest, status: 'rejected', evidenceId: rejectionEvent.evidenceId, reason: rejectionEvent.reason }],
        edges: [],
      });
      return {
        batchId: importBatchId,
        revision: this.state.revision,
        upload: finalized,
        items: [{ digest: indexedDigest, status: 'rejected', reason: rejectionEvent.reason }],
        edges: [],
      };
    }

    const item = { digest: finalized.digest, status: 'imported', size: finalized.size };
    await this._emit(EVT.OBJECT_IMPORTED, {
      digest: finalized.digest,
      size: finalized.size,
      contentType: finalized.contentType ?? null,
      batchId: importBatchId,
    });
    const edgeResults = [];
    for (const e of opts.edges ?? []) edgeResults.push(await this._declareEdge(e, importBatchId));
    await this._emit(EVT.BATCH_RECORDED, {
      batchId: importBatchId,
      note: opts.note ?? 'staged upload',
      items: [{ digest: finalized.digest, status: 'imported', evidenceId: null, reason: null }],
      edges: edgeResults.map((r) => ({ from: r.from, to: r.to, status: r.status })),
    });
    await this._reconcileDecisions();
    return {
      batchId: importBatchId,
      revision: this.state.revision,
      stateHash: this.hash(),
      upload: finalized,
      items: [item],
      edges: edgeResults,
    };
  }

  abortUpload(uploadId) {
    return this._serialize(() => this.storage.abortUpload(uploadId));
  }

  // -------------------------------------------------------------------------
  // snapshots
  // -------------------------------------------------------------------------

  defineSnapshot({ name, digest, active = true }) {
    return this._serialize(async () => {
      if (typeof name !== 'string' || !name.trim()) throw new ValidationError('snapshot name required');
      if (!isDigest(digest)) throw new ValidationError('snapshot target must be a sha256 digest');
      const d = digest.toLowerCase();
      await this._emit(EVT.SNAPSHOT_POINTED, { name, target: d, active: active !== false });
      await this._reconcileDecisions();
      const snap = this.state.snapshots.get(name);
      return { name, target: d, active: snap.active, revision: this.state.revision, pointerSeq: snap.seq };
    });
  }

  listSnapshots() {
    return [...this.state.snapshots.values()].map((s) => ({ ...s }));
  }

  snapshotHistory(name) {
    const tl = (this.state.snapshotTimeline ?? []).filter((t) => t.name === name);
    return tl.map((t) => ({ ...t }));
  }

  // -------------------------------------------------------------------------
  // GC review: confirm candidate, bind decision to root set + object versions
  // -------------------------------------------------------------------------

  gcCandidates() {
    return gcCandidates(this.state);
  }

  confirmGcCandidate(candidateDigest, { expectedRevision, note = null } = {}) {
    return this._serialize(() => this._confirm(candidateDigest, { expectedRevision, note }));
  }

  async _confirm(candidateDigest, { expectedRevision, note }) {
    const digest = typeof candidateDigest === 'string' ? candidateDigest.toLowerCase() : null;
    if (!digest || !isDigest(digest)) throw new ValidationError('candidateDigest must be a sha256 digest');

    // optimistic concurrency: callers must acknowledge the revision they saw
    if (expectedRevision !== undefined && expectedRevision !== null && Number(expectedRevision) !== this.state.revision) {
      const candidates = gcCandidates(this.state);
      const present =
        candidates.collectable.find((c) => c.digest === digest) ||
        candidates.inactiveRetained.find((c) => c.digest === digest);
      throw new ConflictError('state changed since candidate list was read', {
        kind: 'revision-stale',
        expectedRevision: Number(expectedRevision),
        currentRevision: this.state.revision,
        candidate: digest,
        currentStateHash: this.hash(),
        candidateNow: present
          ? { digest, status: present.status, category: candidates.collectable.some((c) => c.digest === digest) ? 'collectable' : 'inactive-retained' }
          : { digest, status: 'not-a-candidate' },
      });
    }

    if (!this.state.objects.has(digest)) {
      // unknown vs known-bad distinction for clear 4xx mapping
      const kind = nodeKind(this.state, digest);
      throw new NotFoundError(`candidate ${digest} is not a present object`, { kind, candidate: digest });
    }
    const candidates = gcCandidates(this.state);
    const inCollectable = candidates.collectable.some((c) => c.digest === digest);
    const inInactive = candidates.inactiveRetained.find((c) => c.digest === digest);
    if (!inCollectable && !inInactive) {
      throw new ConflictError('object is retained by an active root', {
        kind: 'not-a-candidate',
        candidate: digest,
        activeRetainers: this._activeRetainerNames(digest),
      });
    }

    const rootBasis = {
      roots: [...this.state.snapshots.values()]
        .sort((a, b) => a.name.localeCompare(b.name))
        .map((s) => ({ snapshot: s.name, digest: s.target, active: s.active, pointerSeq: s.seq })),
    };
    // Bind the decision to object versions reachable at that moment: the
    // candidate plus every active-root-reachable digest.
    const reachNow = reach(
      this.state,
      rootBasis.roots.filter((r) => r.active).map((r) => r.snapshot),
    );
    const objectVersions = {};
    objectVersions[digest] = digest;
    for (const d of reachNow._nodeIndex.keys()) objectVersions[d] = d;

    const basisRevision = this.state.revision;
    const basisStateHash = this.hash();
    const decisionId = 'dec_' + randomUUID();
    await this._emit(EVT.GC_CONFIRMED, {
      decisionId,
      candidate: digest,
      atRevision: basisRevision,
      basisRevision,
      basisStateHash,
      rootBasis,
      objectVersions,
      note,
      category: inCollectable ? 'collectable' : 'inactive-retained',
    });
    return {
      decisionId,
      candidate: digest,
      status: 'open',
      atRevision: basisRevision,
      basisRevision,
      basisStateHash,
      rootBasis,
      objectVersions,
      currentRevision: this.state.revision,
    };
  }

  _activeRetainerNames(digest) {
    const r = retainers(this.state, digest, {
      rootSelectors: [...this.state.snapshots.values()].map((s) => s.name),
    });
    return r.activeRetainers;
  }

  /**
   * Emit GC_RESOLVED for open decisions whose candidate stopped being a
   * candidate — e.g. a late object arrived and re-attached it to an active
   * root. The decision record is never deleted; this is the explicit audit
   * trail that "the candidate vanished" must produce.
   */
  async _reconcileDecisions() {
    const candidates = gcCandidates(this.state);
    const byDigest = new Map();
    for (const c of [...candidates.collectable, ...candidates.inactiveRetained]) {
      byDigest.set(c.digest, c);
    }
    for (const [decisionId, rec] of [...this.state.decisions]) {
      if (this.state.resolved.has(decisionId)) continue;
      if (byDigest.has(rec.candidate)) continue;
      // status changed: re-retained, or object became invalid somehow
      const retainerInfo = this.state.objects.has(rec.candidate)
        ? this._activeRetainerNames(rec.candidate)
        : [];
      const reason = !this.state.objects.has(rec.candidate)
        ? 'object-no-longer-present'
        : retainerInfo.length
          ? 're-retained'
          : 'state-changed';
      await this._emit(EVT.GC_RESOLVED, {
        decisionId,
        candidate: rec.candidate,
        atRevision: this.state.revision,
        reason,
        retainedBy: retainerInfo,
      });
    }
  }

  decisionStatus(decisionId) {
    const rec = this.state.decisions.get(decisionId);
    if (!rec) throw new NotFoundError('unknown decision', { decisionId });
    const resolution = this.state.resolved.get(decisionId) ?? null;
    return {
      decisionId,
      candidate: rec.candidate,
      status: resolution ? 'resolved' : 'open',
      confirmedAt: rec.confirmedAt,
      atRevision: rec.atRevision,
      basisRevision: rec.basisRevision,
      basisStateHash: rec.basisStateHash,
      rootBasis: rec.rootBasis,
      objectVersions: Object.fromEntries(rec.objectVersions),
      resolution,
    };
  }

  listDecisions({ candidate } = {}) {
    const out = [];
    for (const rec of this.state.decisions.values()) {
      if (candidate && rec.candidate !== String(candidate).toLowerCase()) continue;
      const resolution = this.state.resolved.get(rec.decisionId) ?? null;
      out.push({
        decisionId: rec.decisionId,
        candidate: rec.candidate,
        status: resolution ? 'resolved' : 'open',
        resolutionReason: resolution?.reason ?? null,
        confirmedAt: rec.confirmedAt,
        atRevision: rec.atRevision,
        basisRevision: rec.basisRevision,
      });
    }
    out.sort((a, b) => a.confirmedAt.localeCompare(b.confirmedAt) || a.decisionId.localeCompare(b.decisionId));
    return out;
  }

  // -------------------------------------------------------------------------
  // read model — all partial, contents never travel with metadata
  // -------------------------------------------------------------------------

  node(digest) {
    const d = String(digest).toLowerCase();
    const kind = nodeKind(this.state, d);
    const base = { digest: d, kind };
    if (kind === 'present') {
      const o = this.state.objects.get(d);
      return {
        ...base,
        size: o.size,
        contentType: o.contentType,
        firstBatchId: o.firstBatchId,
        importedAt: o.importedAt,
        rejectionAttempts: this.state.rejected.get(d) ?? [],
        incoming: this.incomingEdges(d).length,
        outgoing: this.state.edges.get(d)?.size ?? 0,
      };
    }
    if (kind === 'digest-mismatch') {
      return { ...base, attempts: this.state.rejected.get(d) };
    }
    return { ...base };
  }

  outgoingEdges(digest, paging = {}) {
    const d = String(digest).toLowerCase();
    const m = this.state.edges.get(d);
    const list = m
      ? [...m.entries()]
          .map(([to, meta]) => ({ from: d, to, kind: nodeKind(this.state, to), ...meta }))
          .sort((a, b) => a.seq - b.seq || a.to.localeCompare(b.to))
      : [];
    return paginate(list, paging);
  }

  incomingEdges(digest, paging = {}) {
    const d = String(digest).toLowerCase();
    const list = [];
    for (const [from, targets] of this.state.edges) {
      const meta = targets.get(d);
      if (meta) list.push({ from, to: d, sourceKind: nodeKind(this.state, from), ...meta });
    }
    list.sort((a, b) => a.seq - b.seq || a.from.localeCompare(b.from));
    return paginate(list, paging);
  }

  async readContentRange(digest, { start = 0, end = null } = {}) {
    const d = String(digest).toLowerCase();
    const kind = nodeKind(this.state, d);
    if (kind !== 'present') {
      const e = new NotFoundError(`object ${d} is not present (${kind})`, { kind, digest: d });
      e.kind = kind;
      throw e;
    }
    const res = await this.storage.readBlobRange(d, { start, end });
    if (!res) throw new NotFoundError('blob missing from storage', { digest: d });
    return res;
  }

  expand(roots, opts = {}) {
    return this._withHistoryState(opts.atRevision, (st) => reach(st, roots, opts));
  }

  retainersOf(digest, opts = {}) {
    return this._withHistoryState(opts?.atRevision, (st) => retainers(st, digest, opts));
  }

  compare(left, right, opts = {}) {
    const leftRev = revisionOfSpec(left);
    const rightRev = revisionOfSpec(right);
    const leftHistorical = leftRev != null && leftRev < this.state.revision;
    const rightHistorical = rightRev != null && rightRev < this.state.revision;
    if (!leftHistorical && !rightHistorical) {
      return Promise.resolve(compareSnapshots(this.state, left, right, opts));
    }
    const jobs = [];
    if (leftHistorical) jobs.push(['left', this._stateAt(leftRev)]);
    if (rightHistorical) jobs.push(['right', this._stateAt(rightRev)]);
    return Promise.all(jobs.map(([, p]) => p)).then((resolved) => {
      const historicalStates = {};
      jobs.forEach(([side], i) => {
        historicalStates[side] = resolved[i];
      });
      return compareSnapshots(this.state, left, right, { ...opts, historicalStates });
    });
  }

  review(roots, opts = {}) {
    return this._withHistoryState(opts.atRevision, (st) => reviewReport(st, roots, opts));
  }

  resolve(rootSpec, pathDigests) {
    const rev = revisionOfSpec(rootSpec);
    return this._withHistoryState(rev, (st) => resolvePath(st, rootSpec, pathDigests));
  }

  _withHistoryState(revision, fn) {
    if (revision == null || revision >= this.state.revision) return fn(this.state);
    return this._stateAt(revision).then(fn);
  }

  batch(batchId) {
    const b = this.state.batches.get(batchId);
    if (!b) throw new NotFoundError('unknown batch', { batchId });
    return { ...b, items: b.items.map((i) => ({ ...i })), edges: b.edges.map((e) => ({ ...e })) };
  }

  listBatches(paging = {}) {
    const list = [...this.state.batches.values()]
      .map((b) => ({ batchId: b.batchId, receivedAt: b.receivedAt, items: b.items.length, edges: b.edges.length }))
      .sort((a, b) => a.receivedAt.localeCompare(b.receivedAt) || a.batchId.localeCompare(b.batchId));
    return paginate(list, paging);
  }

  objectStatus(digest) {
    const d = String(digest).toLowerCase();
    return { digest: d, status: nodeKind(this.state, d) };
  }

  async quarantineEvidence(evidenceId) {
    const rec = await this.storage.getQuarantine(evidenceId);
    if (!rec) throw new NotFoundError('unknown quarantine evidence', { evidenceId });
    return rec;
  }

  listQuarantine() {
    return this.storage.listQuarantine();
  }

  events() {
    return this.storage.listEvents();
  }
}

function revisionOfSpec(spec) {
  if (!spec || typeof spec === 'string' || Array.isArray(spec)) return null;
  return spec.atRevision ?? null;
}
