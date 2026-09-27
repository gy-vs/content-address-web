// 审阅内核：命令 -> 追加哈希链事件 -> 折叠进内存状态的唯一汇聚点。
// 写命令经 #chain 串行化（并发确认因此有确定顺序，冲突以事件显式记录）；
// 查询命令只读状态/块存储，从不整体序列化对象图。
import path from 'node:path';
import { rm } from 'node:fs/promises';
import { BlobStore } from './blobstore.js';
import { Log } from './log.js';
import { emptyState, applyRecord, replayRecords } from './state.js';
import {
  closure,
  diffSnapshots,
  retainedBy,
  classifyObject,
  loadPath,
  resolveRoots,
  findSnapshot,
  pageDigests,
  closureFingerprint,
  DEFAULT_PAGE_SIZE,
} from './graph.js';
import { deriveRefs } from './refs.js';
import { normalizeDigest, assertDigest, sha256, digestFromHex, newId } from './digest.js';

export class KernelError extends Error {
  constructor(code, message, details = {}) {
    super(message);
    this.code = code;
    this.details = details;
  }
}

const bad = (code, message, details = {}) => { throw new KernelError(code, message, details); };

export class Kernel {
  constructor(store, log, opts = {}) {
    this.store = store;
    this.log = log;
    this.dataDir = opts.dataDir;
    this.clock = opts.clock ?? (() => Date.now());
    this.state = replayRecords(log.slice());
    this.#chain = Promise.resolve();
  }

  #chain;

  /** 创建/打开内核目录并恢复（重放日志 + 盘点崩溃残留的导入会话） */
  static async create(dataDir, opts = {}) {
    const clock = opts.clock ?? (() => Date.now());
    const store = await BlobStore.open(path.join(dataDir, 'objects'));
    const log = await Log.open(path.join(dataDir, 'log', 'events.jsonl'), clock);
    const kernel = new Kernel(store, log, { dataDir, clock });
    await kernel.#reconcileSessions();
    return kernel;
  }

  /** 磁盘会话目录与事件链对齐：清理崩溃残留（无事件的分块/孤儿目录） */
  async #reconcileSessions() {
    const onDisk = await this.store.listPendingSessions();
    const live = new Set(this.state.pending.keys());
    for (const { importId, chunks } of onDisk) {
      const p = this.state.pending.get(importId);
      if (!p) {
        await this.store.removeSession(importId); // 事件已终结但目录残留
        continue;
      }
      const known = new Set(p.chunks.map((c) => c.index));
      for (const idx of chunks) {
        if (!known.has(idx)) {
          // 分块已落盘但事件未追加（崩溃点）：删除，等待调用方重发该分块
          await rm(this.store.chunkPath(importId, idx), { force: true });
        }
      }
    }
  }

  // ---- 写串行化 ------------------------------------------------------

  async #mutate(fn) {
    const run = this.#chain.then(() => fn());
    this.#chain = run.then(() => {}, () => {});
    return run;
  }

  async #emit(type, payload, meta) {
    const { record } = await this.log.append(type, payload, meta);
    applyRecord(this.state, record);
    return record;
  }

  #evidence(rec, extra = {}) {
    return {
      seq: rec.seq,
      recordId: rec.id,
      ts: rec.ts,
      chainHead: this.log.headHash,
      ...extra,
    };
  }

  // ==================================================================
  // 导入（分批 / 迟到对象 / 错误对象隔离）
  // ==================================================================

  /** 开始一次分批导入。声明摘要（及可选引用意图），随后可分块上传。 */
  startImport(input = {}) {
    return this.#mutate(() => this.#startImport(input));
  }

  async #startImport(input) {
    const declaredDigest = normalizeDigest(input.declaredDigest);
    if (!declaredDigest) bad('invalid-digest', 'declaredDigest 不是合法 sha256 摘要');
    const parseJson = !!input.parseJson;
    const declaredRefs = this.#validateDeclaredRefs(input.refs ?? []);
    const importId = input.importId ?? newId();
    if (this.state.pending.has(importId)) {
      bad('import-exists', `导入会话已存在: ${importId}`, { importId });
    }
    const chunkCount = input.chunkCount ?? null;
    if (chunkCount != null && (!(Number.isInteger(chunkCount)) || chunkCount < 1)) {
      bad('invalid-chunk-count', 'chunkCount 必须为正整数');
    }
    const rec = await this.#emit('import.started', {
      importId,
      declaredDigest,
      parseJson,
      declaredRefs,
      chunkCount,
      clientRef: input.clientRef ?? null,
    });
    return {
      status: 'started',
      importId,
      declaredDigest,
      parseJson,
      chunkCount,
      evidence: this.#evidence(rec),
    };
  }

  /** 上传一个分块（可乱序到达；同一序号重发走幂等校验，支持断点重传） */
  uploadChunk(input) {
    return this.#mutate(() => this.#uploadChunk(input));
  }

  async #uploadChunk({ importId, index, data, chunkDigest: clientChunkDigest } = {}) {
    const p = this.state.pending.get(importId);
    if (!p) bad('import-not-found', `导入会话不存在或已终结: ${importId}`, { importId });
    if (!Number.isInteger(index) || index < 0) bad('invalid-chunk-index', '分块序号必须为非负整数');
    const prior = p.chunks.find((c) => c.index === index);
    if (prior) {
      if (clientChunkDigest && normalizeDigest(clientChunkDigest) !== prior.chunkDigest) {
        bad('chunk-conflict', `分块 #${index} 已存在且摘要与重发内容不一致`, {
          importId, index, stored: prior.chunkDigest, received: clientChunkDigest,
        });
      }
      return { status: 'duplicate', importId, index, ...prior, evidence: this.#evidence(this.log.at(prior.seq)) };
    }
    if (p.expectedChunkCount != null && index >= p.expectedChunkCount) {
      bad('invalid-chunk-index', `分块序号超出声明总数 ${p.expectedChunkCount}`, { importId, index });
    }
    const { bytes, chunkDigest } = await this.store.writeChunk(importId, index, data);
    const rec = await this.#emit('import.chunk', { importId, index, bytes, chunkDigest });
    return { status: 'uploaded', importId, index, bytes, chunkDigest, evidence: this.#evidence(rec) };
  }

  /** 结束导入：校验摘要、推导引用、提交为有效节点或隔离拒收。 */
  endImport(input) {
    return this.#mutate(() => this.#endImport(input));
  }

  async #endImport({ importId, chunkCount: explicitCount } = {}) {
    const p = this.state.pending.get(importId);
    if (!p) bad('import-not-found', `导入会话不存在或已终结: ${importId}`, { importId });
    const chunkCount = explicitCount ?? p.expectedChunkCount ?? p.chunks.length;
    if (!Number.isInteger(chunkCount) || chunkCount < 1) bad('invalid-chunk-count', '缺少分块总数');
    const received = p.chunks.map((c) => c.index);
    const missing = [];
    for (let i = 0; i < chunkCount; i++) if (!received.includes(i)) missing.push(i);
    if (missing.length) {
      bad('chunks-incomplete', `尚缺分块: ${missing.join(',')}`, { importId, received, missing });
    }

    let committed;
    try {
      committed = await this.store.commitSession(importId, chunkCount, p.declaredDigest);
    } catch (err) {
      if (err.code === 'chunk-missing') {
        bad('chunks-incomplete', err.message, { importId });
      }
      throw err;
    }

    if (!committed.ok) {
      return await this.#reject({
        pending: p,
        reason: 'digest-mismatch',
        detail: { declared: p.declaredDigest, actual: committed.actualDigest },
        actualDigest: committed.actualDigest,
        size: committed.size,
        commitPath: committed.commit,
      });
    }

    // 内容寻址身份：若同摘要已有存活对象，则字节即同一对象——幂等接受，
    // 本次 parseJson/refs 意图不能改变或删除既有节点（否则会误删合法 blob）。
    const existing = this.state.objects.get(committed.actualDigest);
    if (existing && !existing.tombstoned) {
      const rec = await this.#emit('import.object-accepted', {
        importId: p.importId,
        digest: committed.actualDigest,
        declaredDigest: p.declaredDigest,
        size: committed.size,
        parseJson: existing.parseJson,
        refs: existing.refs,
        chunks: p.chunks.map((c) => ({ index: c.index, bytes: c.bytes, chunkDigest: c.chunkDigest })),
        clientRef: p.clientRef,
        idempotent: true,
      });
      await this.store.removeSession(p.importId);
      return {
        status: 'accepted',
        digest: committed.actualDigest,
        revived: false,
        idempotent: true,
        object: this.#summary(committed.actualDigest),
        evidence: this.#evidence(rec),
      };
    }

    // 摘要匹配：按 manifest 意图解析引用（只有 parseJson 才需要读字节）
    const priorTombstoned = new Set(
      [...this.state.objects.values()].filter((o) => o.tombstoned).map((o) => o.digest),
    );
    let refs;
    if (p.parseJson) {
      const bytes = await this.store.readAll(committed.actualDigest);
      const derived = deriveRefs(bytes, { parseJson: true, declaredRefs: p.declaredRefs });
      if (derived.error) {
        // 字节摘要是对的，但调用方声明它是 manifest 却不是合法 JSON：
        // 不能作为会传播引用的有效节点——隔离字节并拒收。
        return await this.#reject({
          pending: p,
          reason: 'malformed-content',
          detail: { declared: p.declaredDigest, note: '声明为 manifest 但内容不是合法 JSON' },
          actualDigest: committed.actualDigest,
          size: committed.size,
          blobDigest: committed.actualDigest,
        });
      }
      refs = derived.refs;
    } else {
      refs = deriveRefs(Buffer.alloc(0), { declaredRefs: p.declaredRefs }).refs;
    }

    const rec = await this.#emit('import.object-accepted', {
      importId: p.importId,
      digest: committed.actualDigest,
      declaredDigest: p.declaredDigest,
      size: committed.size,
      parseJson: p.parseJson,
      refs,
      chunks: p.chunks.map((c) => ({ index: c.index, bytes: c.bytes, chunkDigest: c.chunkDigest })),
      clientRef: p.clientRef,
    });
    await this.store.removeSession(p.importId);
    const revived = priorTombstoned.has(committed.actualDigest);
    return {
      status: 'accepted',
      digest: committed.actualDigest,
      revived,
      object: this.#summary(committed.actualDigest),
      evidence: this.#evidence(rec),
    };
  }

  /** 拒收通用流程：隔离原始字节、追加拒收事件、清理会话。错误对象永不进对象表。 */
  async #reject({ pending: p, reason, detail, actualDigest, size, commitPath, blobDigest }) {
    const evidenceId = newId();
    if (blobDigest) {
      await this.store.quarantineBlob(blobDigest, evidenceId);
      await this.store.deleteBlob(blobDigest);
    } else {
      await this.store.quarantineCommit(commitPath, evidenceId);
    }
    const rec = await this.#emit('import.object-rejected', {
      evidenceId,
      importId: p.importId,
      reason,
      declaredDigest: p.declaredDigest,
      actualDigest: actualDigest ?? null,
      size: size ?? null,
      detail: detail ?? null,
      parseJson: p.parseJson,
      declaredRefs: p.declaredRefs,
      chunks: p.chunks.map((c) => ({ index: c.index, bytes: c.bytes, chunkDigest: c.chunkDigest })),
      clientRef: p.clientRef,
      quarantinePath: this.store.quarantinePath(evidenceId),
    });
    await this.store.removeSession(p.importId);
    return {
      status: 'rejected',
      reason,
      evidenceId,
      declaredDigest: p.declaredDigest,
      actualDigest: actualDigest ?? null,
      detail: detail ?? null,
      quarantine: { evidenceId, path: this.store.quarantinePath(evidenceId) },
      evidence: this.#evidence(rec),
    };
  }

  /** 放弃未完成会话（显式取消；分块记录保留在 abandoned 台账） */
  abandonImport(input) {
    return this.#mutate(() => this.#abandonImport(input));
  }

  async #abandonImport({ importId, reason = 'abandoned' } = {}) {
    const p = this.state.pending.get(importId);
    if (!p) bad('import-not-found', `导入会话不存在或已终结: ${importId}`, { importId });
    const rec = await this.#emit('import.abandoned', { importId, reason });
    await this.store.removeSession(importId);
    return { status: 'abandoned', importId, reason, evidence: this.#evidence(rec) };
  }

  /**
   * 小对象便捷导入（一次给出字节）。同样走“分块 -> 校验 -> 事件”链，
   * declaredDigest 省略时按实际计算；给错摘要即可复现 digest-mismatch 拒收。
   */
  importObject(input) {
    return this.#mutate(() => this.#importObject(input));
  }

  async #importObject({ data, declaredDigest, parseJson, refs, importId, clientRef } = {}) {
    if (!Buffer.isBuffer(data) && !(data instanceof Uint8Array)) bad('invalid-data', 'data 必须是 Buffer/Uint8Array');
    const buf = Buffer.from(data);
    const declared = declaredDigest ? normalizeDigest(declaredDigest) : digestFromHex(sha256(buf));
    if (!declared) bad('invalid-digest', 'declaredDigest 不是合法 sha256 摘要');
    const id = importId ?? newId();
    await this.#startImport({ declaredDigest: declared, parseJson, refs, chunkCount: 1, importId: id, clientRef });
    await this.#uploadChunk({ importId: id, index: 0, data: buf });
    return this.#endImport({ importId: id, chunkCount: 1 });
  }

  /** 列出进行中的分批导入（恢复后调用方可续传缺失分块） */
  listPendingImports() {
    return [...this.state.pending.values()].map((p) => ({
      importId: p.importId,
      declaredDigest: p.declaredDigest,
      parseJson: p.parseJson,
      expectedChunkCount: p.expectedChunkCount,
      receivedChunks: p.chunks.map((c) => ({ index: c.index, bytes: c.bytes, chunkDigest: c.chunkDigest })),
      startedSeq: p.startedSeq,
    }));
  }

  /** 拒收台账（错误输入证据，可定位隔离字节与分块链） */
  listRejections() {
    return [...this.state.rejected.values()].map((e) => ({
      evidenceId: e.evidenceId,
      reason: e.reason,
      declaredDigest: e.declaredDigest,
      actualDigest: e.actualDigest,
      size: e.size,
      detail: e.detail,
      chunks: e.chunks,
      quarantinePath: e.quarantinePath,
      importId: e.importId,
      clientRef: e.clientRef,
      seq: e.seq,
      ts: e.ts,
    }));
  }

  #validateDeclaredRefs(refs) {
    if (!Array.isArray(refs)) bad('invalid-reference', 'refs 必须是数组');
    // 复用 deriveRefs 的声明校验（不读字节）
    try {
      deriveRefs(Buffer.alloc(0), { declaredRefs: refs });
    } catch (err) {
      if (err.code === 'invalid-reference') throw new KernelError('invalid-reference', err.message);
      throw err;
    }
    return refs.map((r) => ({ digest: normalizeDigest(r.digest), label: r.label ?? null }));
  }

  // ==================================================================
  // 快照与活动根
  // ==================================================================

  createSnapshot(input) {
    return this.#mutate(() => this.#createSnapshot(input));
  }

  async #createSnapshot({ name, roots } = {}) {
    if (!name || typeof name !== 'string') bad('invalid-snapshot-name', '快照名必须是非空字符串');
    if (!Array.isArray(roots)) bad('invalid-roots', 'roots 必须是摘要数组');
    const normalized = [...new Set(roots.map((r) => assertDigest(r, 'roots[]')))];
    const version = this.state.snapshotHistory.filter((s) => s.name === name).length + 1;
    const rec = await this.#emit('snapshot.created', { name, version, roots: normalized });
    return {
      status: 'created',
      snapshot: { name, version, roots: normalized },
      evidence: this.#evidence(rec),
    };
  }

  setActiveRoots(input) {
    return this.#mutate(() => this.#setActiveRoots(input));
  }

  async #setActiveRoots({ roots, source = { kind: 'explicit' } } = {}) {
    if (!Array.isArray(roots)) bad('invalid-roots', 'roots 必须是摘要数组');
    const normalized = [...new Set(roots.map((r) => assertDigest(r, 'roots[]')))];
    const version = this.state.activeRootsHistory.length;
    const rec = await this.#emit('snapshot.activeroots-set', { version, roots: normalized, source });
    return {
      status: 'active-set',
      activeRootsVersion: version,
      roots: normalized,
      source,
      evidence: this.#evidence(rec),
    };
  }

  listSnapshots() {
    return this.state.snapshotHistory.map((s) => ({
      name: s.name, version: s.version, roots: s.roots, seq: s.seq, ts: s.ts,
      latest: this.state.snapshots.get(s.name).version === s.version,
    }));
  }

  getSnapshot(name, version) {
    const s = findSnapshot(this.state, name, version);
    if (!s) bad('snapshot-not-found', `快照不存在: ${name}${version != null ? `@v${version}` : ''}`);
    return { name: s.name, version: s.version, roots: s.roots, seq: s.seq, ts: s.ts,
      latest: this.state.snapshots.get(name).version === s.version };
  }

  getActiveRoots(version) {
    const v = version ?? this.state.activeRootsHistory.length - 1;
    const e = this.state.activeRootsHistory[v];
    if (!e) bad('active-version-not-found', `活动根集合版本不存在: ${v}`);
    return { version: e.version, roots: e.roots, source: e.source ?? { kind: 'initial' }, seq: e.seq, ts: e.ts };
  }

  // ==================================================================
  // 图关系查看（局部、分页、不带字节）
  // ==================================================================

  #selectorFrom(input = {}) {
    const sel = {};
    if (input.roots) sel.roots = input.roots;
    if (input.snapshot) sel.snapshot = input.snapshot;
    if (input.snapshotVersion != null) sel.snapshotVersion = input.snapshotVersion;
    if (input.activeVersion != null) sel.activeVersion = input.activeVersion;
    return sel;
  }

  expand(input = {}) {
    const sel = this.#selectorFrom(input);
    const c = closure(this.state, sel);
    const limit = input.limit ?? DEFAULT_PAGE_SIZE;
    const page = (map, cursor) => pageDigests([...map.keys()], { cursor, limit }).items;
    const withMeta = (map, keys) => keys.map((k) => this.#expandEntry(map, k, c));
    return {
      rootSelection: {
        source: c.roots.source,
        version: c.roots.version,
        snapshot: c.roots.snapshot ?? null,
        roots: c.roots.roots,
      },
      rootsReport: c.rootsReport,
      counts: {
        reachable: c.reachable.size,
        missing: c.missing.size,
        digestMismatch: c.mismatched.size,
        tombstoned: c.tombstoned.size,
      },
      reachable: withMeta(c.reachable, page(c.reachable, input.cursorReachable)),
      missing: withMeta(c.missing, page(c.missing, input.cursorMissing)),
      digestMismatch: withMeta(c.mismatched, page(c.mismatched, input.cursorMismatch)),
      tombstoned: withMeta(c.tombstoned, page(c.tombstoned, input.cursorTombstoned)),
      cursors: {
        reachable: nextCursor(c.reachable, input.cursorReachable, limit),
        missing: nextCursor(c.missing, input.cursorMissing, limit),
        digestMismatch: nextCursor(c.mismatched, input.cursorMismatch, limit),
        tombstoned: nextCursor(c.tombstoned, input.cursorTombstoned, limit),
      },
    };
  }

  #expandEntry(map, digest, c) {
    const e = map.get(digest);
    const base = { digest };
    if (e.size != null) base.size = e.size;
    if (e.reasons) base.rejections = e.reasons;
    if (e.via?.length) base.via = e.via;
    if (e.edges) base.refCount = e.edges.length;
    if (e.enactedSeq != null) base.enactedSeq = e.enactedSeq;
    return base;
  }

  diff(a, b, input = {}) {
    const d = diffSnapshots(this.state, a, b, { aVersion: input.aVersion, bVersion: input.bVersion });
    if (d.kind === 'error') bad(d.error.code, d.error.message);
    const limit = input.limit ?? DEFAULT_PAGE_SIZE;
    const page = (arr, cursor) => pageDigests(arr, { cursor, limit });
    return {
      a: d.a, b: d.b, counts: d.counts, problems: d.problems,
      shared: page(d.shared, input.cursorShared),
      onlyA: page(d.onlyA, input.cursorOnlyA),
      onlyB: page(d.onlyB, input.cursorOnlyB),
    };
  }

  retained(digest, input = {}) {
    return retainedBy(this.state, digest, { snapshot: input.snapshot });
  }

  objectStatus(digest) {
    return classifyObject(this.state, digest);
  }

  loadPath(rootDigest, labels = [], input = {}) {
    return loadPath(this.state, rootDigest, labels, this.#selectorFrom(input));
  }

  listObjects(input = {}) {
    const live = [...this.state.objects.values()].filter((o) => !o.tombstoned).map((o) => o.digest);
    const tombs = [...this.state.objects.values()].filter((o) => o.tombstoned).map((o) => o.digest);
    const p = pageDigests(live, { cursor: input.cursor, limit: input.limit });
    return {
      objects: p.items.map((d) => this.#summary(d)),
      nextCursor: p.nextCursor,
      remaining: p.remaining,
      counts: { live: live.length, tombstoned: tombs.length },
    };
  }

  /** 对象摘要：元数据/引用/生命周期与证据指针；永不包含对象字节 */
  #summary(digest) {
    const s = classifyObject(this.state, digest);
    const obj = this.state.objects.get(digest);
    if (!obj) return s;
    return {
      ...s,
      importId: obj.importId,
      clientRef: obj.clientRef,
      acceptedAt: obj.acceptedAt,
      acceptedSeq: obj.acceptedSeq,
      refs: obj.refs,
      chunks: obj.chunks.length,
    };
  }

  getSummary(digest) {
    const d = normalizeDigest(digest);
    if (!d) bad('invalid-digest', '非法摘要');
    if (!this.state.objects.has(d) && !this.state.rejectedByDigest.has(d)) {
      // 区分“被引用但缺失”与“完全未知”
      return { digest: d, status: 'missing', referenced: this.#isReferenced(d) };
    }
    return this.#summary(d);
  }

  #isReferenced(digest) {
    for (const obj of this.state.objects.values()) {
      if (obj.tombstoned) continue;
      if (obj.refs.some((r) => r.target === digest)) return true;
    }
    return false;
  }

  /** 内容边界：摘要走 JSON；字节只能通过这里取。返回 Node 流（可接 HTTP Range） */
  openContent(digest, { start = 0, end } = {}) {
    const d = assertDigest(digest, 'digest');
    const obj = this.state.objects.get(d);
    if (!obj) {
      if (this.state.rejectedByDigest.has(d)) bad('digest-mismatch', '该摘要下只有被拒收的对象', { digest: d });
      bad('object-not-found', '对象缺失或从未导入', { digest: d });
    }
    if (obj.tombstoned) bad('object-tombstoned', '对象已作为回收候选被删除（决定记录保留）', { digest: d, enactedSeq: obj.enactedSeq });
    if (start < 0 || (end != null && end <= start)) bad('invalid-range', '非法字节区间');
    if (end != null && end > obj.size) bad('invalid-range', `区间上界超过对象大小 ${obj.size}`);
    const stream = this.store.openRead(d, { start, end });
    return {
      stream,
      meta: {
        digest: d,
        size: obj.size,
        start,
        end: end ?? obj.size,
        contentType: obj.parseJson ? 'application/json' : 'application/octet-stream',
      },
    };
  }

  /** 小对象一次性读取（测试/便捷路径；大对象请用 openContent） */
  async readContent(digest) {
    const d = assertDigest(digest, 'digest');
    const { stream, meta } = this.openContent(d);
    const chunks = [];
    for await (const c of stream) chunks.push(c);
    return { bytes: Buffer.concat(chunks), meta };
  }

  // ==================================================================
  // 回收审阅：候选 -> 决定（绑定版本基）-> 显式冲突 -> 执行删除
  // ==================================================================

  openReview(input = {}) {
    return this.#mutate(() => this.#openReview(input));
  }

  async #openReview({ note = null, ...selectorInput } = {}) {
    const sel = this.#selectorFrom(selectorInput);
    const c = closure(this.state, sel);
    const reach = new Set(c.reachable.keys());
    const activeNow = resolveRoots(this.state, {});
    const candidates = [];
    for (const [digest, obj] of [...this.state.objects.entries()].sort((a, b) => a[0].localeCompare(b[0]))) {
      if (obj.tombstoned || reach.has(digest)) continue;
      const retention = retainedBy(this.state, digest);
      candidates.push({
        digest,
        size: obj.size,
        objectVersion: { acceptedSeq: obj.acceptedSeq, revivedSeq: obj.revivedSeq },
        reason: retention.inactiveOnly ? 'inactive-only' : 'unreachable',
        retainedBy: retention.inactiveOnly
          ? retention.retainedBy.map((r) => ({ snapshot: r.snapshot, version: r.version }))
          : [],
      });
    }
    const basis = closureFingerprint(c.roots, [...c.reachable.keys()]);
    const reviewId = newId();
    const rec = await this.#emit('review.opened', {
      reviewId,
      basis: {
        fingerprint: basis.fingerprint,
        rootSource: basis.rootSource,
        rootVersion: basis.rootVersion,
        snapshot: basis.snapshot,
        seq: basis.seq,
        roots: basis.roots,
        reachableCount: basis.reachable.length,
      },
      candidates,
      note,
    });
    return {
      status: 'opened',
      reviewId,
      basis: rec.payload.basis,
      activeRootsVersion: activeNow.version,
      candidates,
      problems: {
        missing: [...c.missing.keys()],
        digestMismatch: [...c.mismatched.keys()],
        tombstoned: [...c.tombstoned.keys()],
      },
      evidence: this.#evidence(rec),
    };
  }

  getReview(reviewId) {
    const r = this.state.reviews.get(reviewId);
    if (!r) bad('review-not-found', `审阅不存在: ${reviewId}`);
    return this.#hydrateReview(r);
  }

  #hydrateReview(r) {
    const decisions = this.state.decisions.filter((d) => d.reviewId === r.reviewId);
    const conflicts = this.state.conflicts.filter((d) => d.reviewId === r.reviewId);
    return {
      reviewId: r.reviewId,
      basis: r.basis,
      note: r.note,
      openedSeq: r.openedSeq,
      candidates: r.candidates.map((c) => {
        const obj = this.state.objects.get(c.digest);
        return {
          ...c,
          decision: latestDecision(this.state, c.digest)?.decision ?? null,
          deleted: !!obj?.tombstoned,
        };
      }),
      decisions: decisions.map(serializeDecision),
      conflicts: conflicts.map((c) => ({ ...c })),
    };
  }

  confirmReview(input) {
    return this.#mutate(() => this.#confirmReview(input));
  }

  async #confirmReview({ reviewId, digest, decision, caller, basis: clientBasis, note = null } = {}) {
    const r = this.state.reviews.get(reviewId);
    if (!r) bad('review-not-found', `审阅不存在: ${reviewId}`);
    const d = normalizeDigest(digest);
    if (!d) bad('invalid-digest', '非法候选摘要');
    if (!['confirm-delete', 'keep'].includes(decision)) bad('invalid-decision', "decision 必须是 confirm-delete 或 keep");
    if (!caller || typeof caller !== 'string') bad('invalid-caller', 'caller 必须是非空字符串');
    const candidate = r.candidates.find((c) => c.digest === d);

    // 1) 计划基校验（决定必须绑定当时的根集合与对象版本）
    const basisToken = clientBasis ?? r.basis.fingerprint;
    if (basisToken !== r.basis.fingerprint) {
      return this.#conflict(r, d, caller, 'stale-basis', basisToken, {
        note: '携带的审阅基与打开计划时不一致',
        planBasis: r.basis.fingerprint,
        supplied: basisToken,
      });
    }

    // 2) 对象存在性与候选资格（即便基已漂移，not-candidate 仍是更准确的解释）
    const obj = this.state.objects.get(d);
    if (!obj) {
      return this.#conflict(r, d, caller, 'not-candidate', basisToken, {
        note: '候选对象当前不存在（可能已被删除）',
      });
    }
    if (obj.tombstoned) {
      return this.#conflict(r, d, caller, 'already-deleted', basisToken, {
        note: '对象已被前序删除批次移除（决定记录保留）',
        enactedSeq: obj.enactedSeq,
      });
    }
    if (!candidate) {
      return this.#conflict(r, d, caller, 'not-candidate', basisToken, {
        note: '该摘要不在本计划候选集中',
      });
    }

    // 3) 按计划基重算当前活性：迟到对象/根变更必须显式呈现
    const live = this.#recomputeBasis(r.basis);
    if (live.reachable.has(d) || live.fingerprint !== r.basis.fingerprint) {
      return this.#conflict(r, d, caller, 'stale-basis', basisToken, {
        note: live.reachable.has(d)
          ? '候选已重新被根闭包保留（迟到引用/根集合变化）'
          : '根闭包自计划打开后已变化',
        nowReachable: live.reachable.has(d),
        currentFingerprint: live.fingerprint,
      });
    }

    // 3) 同候选既有决定：相同决定幂等 ack；相反决定显式冲突
    const prior = latestDecision(this.state, d);
    if (prior) {
      if (prior.decision === decision) {
        return {
          status: 'ack',
          reviewId,
          digest: d,
          decision,
          caller,
          existing: serializeDecision(prior),
          evidence: { seq: prior.seq, recordId: prior.recordId, chainHead: this.log.headHash },
        };
      }
      return this.#conflict(r, d, caller, 'opposite-decision', basisToken, {
        note: `已有决定 ${prior.decision}（来自 ${prior.caller}），与 ${decision} 冲突`,
      }, prior);
    }

    const objectVersion = { acceptedSeq: obj.acceptedSeq, revivedSeq: obj.revivedSeq };
    const rec = await this.#emit('review.decided', {
      reviewId: r.reviewId,
      digest: d,
      decision,
      caller,
      note,
      basis: { fingerprint: r.basis.fingerprint, rootVersion: r.basis.rootVersion },
      objectVersion,
    });
    return {
      status: 'decided',
      reviewId,
      digest: d,
      decision,
      caller,
      objectVersion,
      evidence: this.#evidence(rec),
    };
  }

  /** 冲突也落事件链：冲突不会因为对象迟到/消失而“没有记录” */
  async #conflict(review, digest, caller, kind, basisToken, detail, existing = null) {
    const rec = await this.#emit('review.conflicted', {
      reviewId: review.reviewId,
      digest,
      caller,
      kind,
      detail,
      basis: basisToken ? { fingerprint: basisToken } : null,
      existing: existing ? serializeDecision(existing) : null,
    });
    return {
      status: 'conflict',
      conflict: kind,
      reviewId: review.reviewId,
      digest,
      caller,
      detail,
      existing: existing ? serializeDecision(existing) : null,
      evidence: this.#evidence(rec),
    };
  }

  /**
   * 依据计划基在“当下”重新求闭包：
   *  - snapshot 基：快照@版本不可变，闭包只会因迟到对象到达而变化；
   *  - active 基：活动根是浮动指针，确认时按当前活动根重算，
   *    基里保存的 rootVersion 仅作为打开时的历史证据。
   */
  #recomputeBasis(basis) {
    const sel = basis.rootSource === 'snapshot'
      ? { snapshot: basis.snapshot, snapshotVersion: basis.rootVersion }
      : basis.rootSource === 'active'
        ? { activeVersion: this.state.activeRootsHistory.length - 1 }
        : { roots: basis.roots };
    const c = closure(this.state, sel);
    const fp = closureFingerprint(c.roots, [...c.reachable.keys()]);
    return { reachable: c.reachable, fingerprint: fp.fingerprint, rootsSelection: c.roots };
  }

  /** 执行删除：把仍确认删除且仍不可达的对象从块存储移除，并记录墓碑批次 */
  enactDeletions(input) {
    return this.#mutate(() => this.#enactDeletions(input));
  }

  async #enactDeletions({ reviewId, caller = 'system' } = {}) {
    const r = this.state.reviews.get(reviewId);
    if (!r) bad('review-not-found', `审阅不存在: ${reviewId}`);
    const live = this.#recomputeBasis(r.basis);
    const deleted = [];
    const skipped = [];
    const decisionSeqs = [];
    for (const c of r.candidates) {
      const dec = latestDecision(this.state, c.digest);
      if (!dec || dec.decision !== 'confirm-delete') continue;
      decisionSeqs.push(dec.seq);
      const obj = this.state.objects.get(c.digest);
      if (!obj || obj.tombstoned) {
        skipped.push({ digest: c.digest, reason: 'not-present' });
        continue;
      }
      if (live.reachable.has(c.digest)) {
        // 迟到引用让对象复活：跳过删除，并把变化记入冲突台账
        await this.#conflict(r, c.digest, caller, 'stale-basis', r.basis.fingerprint, {
          note: '执行删除时对象已重新可达，跳过（决定记录保留）',
          nowReachable: true,
        });
        skipped.push({ digest: c.digest, reason: 'now-reachable' });
        continue;
      }
      await this.store.deleteBlob(c.digest);
      deleted.push(c.digest);
    }
    let rec = null;
    if (deleted.length) {
      rec = await this.#emit('review.delete-enacted', {
        reviewId,
        digests: deleted,
        caller,
        decisionSeqs,
        basis: { fingerprint: r.basis.fingerprint },
      });
    }
    return {
      status: 'enacted',
      reviewId,
      deleted,
      skipped,
      evidence: rec ? this.#evidence(rec) : { seq: this.log.headSeq, chainHead: this.log.headHash, empty: true },
    };
  }

  listReviews() {
    return [...this.state.reviews.keys()].map((id) => {
      const r = this.state.reviews.get(id);
      return {
        reviewId: id,
        basis: r.basis.fingerprint,
        candidateCount: r.candidates.length,
        openedSeq: r.openedSeq,
        note: r.note,
      };
    });
  }

  listDecisions() {
    return this.state.decisions.map(serializeDecision);
  }

  listConflicts() {
    return this.state.conflicts.map((c) => ({ ...c }));
  }

  // ==================================================================
  // 数据链：校验/盘点
  // ==================================================================

  verifyChain() {
    return this.log.verify();
  }

  chainInfo() {
    return {
      headSeq: this.log.headSeq,
      headHash: this.log.headHash,
      records: this.log.records.length,
      logFile: this.log.file,
    };
  }

  stateInfo() {
    return {
      objects: this.state.objects.size,
      liveObjects: [...this.state.objects.values()].filter((o) => !o.tombstoned).length,
      tombstoned: [...this.state.objects.values()].filter((o) => o.tombstoned).length,
      pendingImports: this.state.pending.size,
      rejections: this.state.rejected.size,
      snapshots: new Set(this.state.snapshotHistory.map((s) => s.name)).size,
      snapshotVersions: this.state.snapshotHistory.length,
      activeRootsVersion: this.state.activeRootsHistory.length - 1,
      reviews: this.state.reviews.size,
      decisions: this.state.decisions.length,
      conflicts: this.state.conflicts.length,
      enactments: this.state.enactments.length,
    };
  }
}

function latestDecision(state, digest) {
  let found = null;
  for (const d of state.decisions) if (d.digest === digest) found = d;
  return found;
}

function serializeDecision(d) {
  return {
    reviewId: d.reviewId,
    digest: d.digest,
    decision: d.decision,
    caller: d.caller,
    note: d.note,
    basis: d.basis,
    objectVersion: d.objectVersion,
    seq: d.seq,
    ts: d.ts,
    recordId: d.recordId,
  };
}

function nextCursor(map, cursor, limit) {
  const p = pageDigests([...map.keys()], { cursor, limit });
  return p.nextCursor;
}

export { replayRecords, emptyState };
