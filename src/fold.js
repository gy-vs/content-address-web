import { isDigest } from './digest.js';

/**
 * Event types — the only durable state of the kernel is this append-only
 * chain. Every projection is rebuilt by folding events; decisions are
 * immutable records, never deleted (see GC_CONFIRMED / GC_RESOLVED).
 */
export const EVT = Object.freeze({
  OBJECT_OFFERED: 'object-offered', // announced without content yet (late object)
  OBJECT_IMPORTED: 'object-imported', // digest verified, content stored
  OBJECT_REJECTED: 'object-rejected', // digest mismatch / unsupported / size mismatch
  EDGE_DECLARED: 'edge-declared',
  SNAPSHOT_POINTED: 'snapshot-pointed',
  BATCH_RECORDED: 'batch-recorded',
  GC_CONFIRMED: 'gc-confirmed',
  GC_RESOLVED: 'gc-resolved', // a previously confirmed candidate changed state
});

export function emptyState() {
  return {
    revision: 0,
    objects: new Map(), // digest -> {digest, size, contentType, firstBatchId, importedAt}
    rejected: new Map(), // digest -> [{digest(expected), actual, reason, evidenceId, batchId, receivedAt}]
    offered: new Set(), // digests announced but not present as valid nor rejected
    edges: new Map(), // from -> Map(to -> {seq, batchId, declaredAt})
    edgesByKey: new Set(), // "from\\u0000to" dedupe
    edgeSeq: 0,
    snapshots: new Map(), // name -> {name, target, active, pointedAt, seq}
    snapshotSeq: 0,
    batches: new Map(), // batchId -> {batchId, receivedAt, items:[{kind,digest,status}], edges}
    decisions: new Map(), // decisionId -> full decision record (immutable)
    decisionsByCandidate: new Map(), // candidateDigest -> latest decisionId
    resolved: new Map(), // decisionId -> {atRevision, reason, retainedBy}
    snapshotTimeline: [], // {revision,name,target,active} rebuilt during fold for history views
  };
}

function validDigest(s) {
  return typeof s === 'string' && isDigest(s) ? s.toLowerCase() : null;
}

/**
 * Apply one event to a state. This is intentionally synchronous and
 * side-effect free: storage writes happen in the kernel before/alongside the
 * event, so replay only mutates the projection.
 */
export function fold(state, evt) {
  const s = state;
  switch (evt.type) {
    case EVT.OBJECT_OFFERED: {
      const d = validDigest(evt.digest);
      if (d && !s.objects.has(d) && !s.rejected.has(d)) s.offered.add(d);
      break;
    }
    case EVT.OBJECT_IMPORTED: {
      const d = validDigest(evt.digest);
      if (!d) break;
      if (!s.objects.has(d)) {
        s.objects.set(d, {
          digest: d,
          size: Number(evt.size) || 0,
          contentType: evt.contentType ?? null,
          firstBatchId: evt.batchId ?? null,
          importedAt: evt.recordedAt,
        });
      }
      s.offered.delete(d);
      // rejection attempts are retained as audit; nodeKind() now treats a
      // verified object as present regardless of earlier failures
      break;
    }
    case EVT.OBJECT_REJECTED: {
      const d = validDigest(evt.digest);
      if (!d) break;
      s.offered.delete(d);
      const list = s.rejected.get(d) ?? [];
      list.push({
        digest: d,
        declaredDigest: evt.declaredDigest ?? d,
        actualDigest: evt.actualDigest ?? null,
        reason: evt.reason ?? 'unknown',
        evidenceId: evt.evidenceId ?? null,
        batchId: evt.batchId ?? null,
        size: evt.size ?? null,
        declaredSize: evt.declaredSize ?? null,
        receivedAt: evt.recordedAt,
      });
      s.rejected.set(d, list);
      break;
    }
    case EVT.EDGE_DECLARED: {
      const from = validDigest(evt.from);
      const to = validDigest(evt.to);
      if (!from || !to) break;
      const key = from + ' ' + to;
      if (!s.edgesByKey.has(key)) {
        s.edgesByKey.add(key);
        let m = s.edges.get(from);
        if (!m) {
          m = new Map();
          s.edges.set(from, m);
        }
        s.edgeSeq += 1;
        m.set(to, { seq: s.edgeSeq, batchId: evt.batchId ?? null, declaredAt: evt.recordedAt });
      }
      break;
    }
    case EVT.SNAPSHOT_POINTED: {
      const name = typeof evt.name === 'string' && evt.name ? evt.name : null;
      const target = validDigest(evt.target);
      if (!name || !target) break;
      const active = evt.active !== false;
      const prev = s.snapshots.get(name);
      if (prev && prev.target === target && !!prev.active === active) break;
      s.snapshotSeq += 1;
      s.snapshots.set(name, {
        name,
        target,
        active,
        pointedAt: evt.recordedAt,
        seq: s.snapshotSeq,
      });
      // revision isn't incremented until end of fold; capture after via +1 below
      s.snapshotTimeline.push({ revision: s.revision + 1, name, target, active });
      break;
    }
    case EVT.BATCH_RECORDED: {
      if (!evt.batchId || s.batches.has(evt.batchId)) break;
      s.batches.set(evt.batchId, {
        batchId: evt.batchId,
        receivedAt: evt.recordedAt,
        items: Array.isArray(evt.items) ? evt.items.map((i) => ({ ...i })) : [],
        edges: Array.isArray(evt.edges) ? evt.edges.map((e) => ({ ...e })) : [],
        note: evt.note ?? null,
      });
      break;
    }
    case EVT.GC_CONFIRMED: {
      if (!evt.decisionId || s.decisions.has(evt.decisionId)) break;
      const rec = {
        decisionId: evt.decisionId,
        candidate: validDigest(evt.candidate) || evt.candidate,
        confirmedAt: evt.recordedAt,
        atRevision: Number(evt.atRevision) || 0,
        basisRevision: Number(evt.basisRevision) || 0,
        basisStateHash: evt.basisStateHash ?? null,
        rootBasis: evt.rootBasis ? { ...evt.rootBasis } : { roots: [] },
        objectVersions: new Map(Object.entries(evt.objectVersions ?? {})),
        note: evt.note ?? null,
      };
      s.decisions.set(evt.decisionId, rec);
      s.decisionsByCandidate.set(rec.candidate, evt.decisionId);
      break;
    }
    case EVT.GC_RESOLVED: {
      if (!evt.decisionId || s.resolved.has(evt.decisionId)) break;
      s.resolved.set(evt.decisionId, {
        atRevision: Number(evt.atRevision) || 0,
        reason: evt.reason ?? 'unknown',
        retainedBy: Array.isArray(evt.retainedBy) ? [...evt.retainedBy] : [],
        resolvedAt: evt.recordedAt,
      });
      break;
    }
    default:
      break;
  }
  s.revision += 1;
  return s;
}

/** Rebuild a projection from an iterable of events. */
export function replay(events) {
  let state = emptyState();
  for (const evt of events) state = fold(state, evt);
  return state;
}
