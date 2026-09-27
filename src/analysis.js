import { isDigest } from './digest.js';
import { nodeKind, reach, resolveRoots } from './graph.js';
import { paginate } from './paging.js';

// ---------------------------------------------------------------------------
// Snapshot comparison
// ---------------------------------------------------------------------------

function rootSelectorsOf(spec) {
  if (!spec) throw new TypeError('snapshot spec required');
  if (Array.isArray(spec)) return spec;
  if (typeof spec === 'string') return [spec];
  if (Array.isArray(spec.roots)) return spec.roots;
  if (typeof spec.snapshot === 'string') return [spec.snapshot];
  if (isDigest(spec.digest)) return [spec.digest];
  throw new TypeError('invalid snapshot spec');
}

/**
 * Compare two root sets (snapshots or explicit digests), each optionally at a
 * historical revision. Returns the shared reachable core, each side's
 * exclusively reachable objects and the union of broken-chain problems —
 * keyed by digests only (no contents are serialized).
 */
export function compareSnapshots(state, leftSpec, rightSpec, opts = {}) {
  const leftRoots = rootSelectorsOf(leftSpec);
  const rightRoots = rootSelectorsOf(rightSpec);
  const leftRev = revisionOf(leftSpec);
  const rightRev = revisionOf(rightSpec);

  // When comparing across history, each side is computed on the projection
  // rebuilt at that revision (objects/edges/snapshots as they were then).
  const leftState = leftRev != null && leftRev < state.revision
    ? (opts.historicalStates?.left ?? state)
    : state;
  const rightState = rightRev != null && rightRev < state.revision
    ? (opts.historicalStates?.right ?? state)
    : state;

  const left = reach(leftState, leftRoots, { atRevision: leftRev });
  const right = reach(rightState, rightRoots, { atRevision: rightRev });

  const leftSet = left._nodeIndex;
  const rightSet = right._nodeIndex;

  const shared = [];
  const leftOnly = [];
  const rightOnly = [];
  for (const d of new Set([...leftSet.keys(), ...rightSet.keys()])) {
    if (leftSet.has(d) && rightSet.has(d)) shared.push(d);
    else if (leftSet.has(d)) leftOnly.push(d);
    else rightOnly.push(d);
  }
  shared.sort();
  leftOnly.sort();
  rightOnly.sort();

  const classify = (d, whichState) => ({ digest: d, kind: nodeKind(whichState, d) });
  const page = (list, whichState) =>
    paginate(
      list.map((d) => classify(d, whichState)),
      { cursor: opts.cursor, pageSize: opts.pageSize, maxPageSize: opts.maxPageSize },
    );

  return {
    revision: state.revision,
    left: {
      roots: left.roots,
      revision: leftRev ?? state.revision,
      counts: left.counts,
    },
    right: {
      roots: right.roots,
      revision: rightRev ?? state.revision,
      counts: right.counts,
    },
    shared: page(shared, state).items,
    leftOnly: page(leftOnly, leftState).items,
    rightOnly: page(rightOnly, rightState).items,
    problems: mergeProblems(left.problems, right.problems),
  };
}

function revisionOf(spec) {
  if (Array.isArray(spec) || typeof spec === 'string' || spec == null) return null;
  return spec.atRevision ?? null;
}

function mergeProblems(a, b) {
  const map = new Map();
  for (const p of [...a, ...b]) map.set(`${p.kind} ${p.digest} ${p.rootSnapshot ?? ''}`, p);
  return [...map.values()].sort((x, y) => (x.id < y.id ? -1 : 1));
}

// ---------------------------------------------------------------------------
// Path loading
// ---------------------------------------------------------------------------

/**
 * Walk a digest path from a root, node by node. Each hop is returned with its
 * status so the caller can render where a chain breaks (missing edge, missing
 * object, digest mismatch). Cycles are flagged, not looped.
 */
export function resolvePath(state, rootSpec, pathDigests) {
  const { roots, problems } = resolveRoots(state, rootSpec == null ? [] : rootSelectorsOf(rootSpec));
  const hops = [];

  if (!roots.length) {
    return {
      ok: false,
      revision: state.revision,
      root: null,
      hops,
      problems,
      break: { kind: 'invalid-root', problems },
    };
  }
  // Path expansion is done relative to one root; use first resolved root.
  const root = roots[0];
  const seen = new Set([root.digest]);
  let current = root.digest;
  hops.push({ from: null, to: root.digest, index: -1, nodeKind: nodeKind(state, root.digest), root: true });

  for (let i = 0; i < pathDigests.length; i++) {
    const to = String(pathDigests[i]).toLowerCase();
    if (!isDigest(to)) {
      return finish(state.revision, root, hops, { kind: 'invalid-digest', at: i, value: pathDigests[i] });
    }
    const targets = state.edges.get(current);
    const edge = targets?.get(to);
    const hop = {
      from: current,
      to,
      index: i,
      edgePresent: !!edge,
      edgeSeq: edge?.seq ?? null,
      nodeKind: nodeKind(state, to),
      cycle: seen.has(to),
    };
    hops.push(hop);
    if (!edge) return finish(state.revision, root, hops, { kind: 'missing-edge', at: i, from: current, to });
    if (hop.nodeKind !== 'present') {
      return finish(state.revision, root, hops, { kind: hop.nodeKind, at: i, digest: to });
    }
    if (seen.has(to)) return finish(state.revision, root, hops, { kind: 'cycle', at: i, digest: to });
    seen.add(to);
    current = to;
  }
  return {
    ok: true,
    revision: state.revision,
    root,
    hops,
    problems: [],
    break: null,
  };
}

function finish(revision, root, hops, breakInfo) {
  return {
    ok: false,
    revision,
    root,
    hops,
    problems: [breakInfo],
    break: breakInfo,
  };
}

// ---------------------------------------------------------------------------
// GC review
// ---------------------------------------------------------------------------

/**
 * Garbage-collection candidates relative to the active roots.
 *
 * collectable       present objects unreachable from every snapshot root
 * inactive-retained present objects reachable only by inactive snapshots
 * Both carry the latest decision (if any) and its derived status.
 */
export function gcCandidates(state, { decisions, statusOf } = {}) {
  const activeSelectors = [...state.snapshots.values()]
    .filter((s) => s.active)
    .map((s) => s.name);
  const allSelectors = [...state.snapshots.keys()].sort();

  const activeReach = reach(state, activeSelectors);
  const allReach = reach(state, allSelectors);

  const activeSet = activeReach._nodeIndex;
  const allSet = allReach._nodeIndex;

  const collectable = [];
  const inactiveRetained = [];

  for (const d of [...state.objects.keys()].sort()) {
    const inAll = allSet.get(d);
    const inActive = activeSet.has(d);
    if (inActive) continue;
    const candidate = {
      digest: d,
      size: state.objects.get(d).size,
      decision: latestDecision(state, d, decisions),
      status: statusOf ? statusOf(d) : deriveStatus(state, d),
    };
    if (inAll && inAll.reachability === 'inactive-only') {
      candidate.inactiveRetainers = inAll.reachedByInactive;
      candidate.pathEvidence = inAll.pathEvidence;
      inactiveRetained.push(candidate);
    } else if (!inAll) {
      collectable.push(candidate);
    }
  }

  return {
    revision: state.revision,
    activeRoots: activeReach.roots,
    counts: {
      collectable: collectable.length,
      inactiveRetained: inactiveRetained.length,
      activeReachable: activeSet.size,
      problems: activeReach.problems.length,
    },
    collectable,
    inactiveRetained,
    problems: activeReach.problems,
  };
}

function latestDecision(state, digest, decisionsOverride) {
  const id = state.decisionsByCandidate.get(digest);
  if (!id) return null;
  const rec = state.decisions.get(id);
  const resolved = state.resolved.get(id);
  return {
    decisionId: rec.decisionId,
    confirmedAt: rec.confirmedAt,
    atRevision: rec.atRevision,
    basisRevision: rec.basisRevision,
    basisStateHash: rec.basisStateHash,
    resolved: !!resolved,
    resolution: resolved ?? null,
  };
}

function deriveStatus(state, digest) {
  const id = state.decisionsByCandidate.get(digest);
  if (!id) return 'unconfirmed';
  return state.resolved.has(id) ? 'resolved' : 'open';
}

// ---------------------------------------------------------------------------
// Global review report — the three required public distinctions
// ---------------------------------------------------------------------------

/**
 * Whole-workbench review from a chosen root set:
 *  - missingObjects: referenced but never seen
 *  - digestMismatches: reached from a root but failed verification
 *  - unreachableMismatches: failed verification AND not reachable from any
 *    selected root (bad uploads that nothing points at still must surface)
 *  - inactiveOnlyObjects: present, but reachable solely under inactive roots
 */
export function reviewReport(state, rootSelectors, opts = {}) {
  const r = reach(state, rootSelectors, opts);
  const missingObjects = [];
  const digestMismatches = [];
  const inactiveOnlyObjects = [];
  const awaitingContent = [];

  for (const node of r._nodeIndex.values()) {
    if (node.kind === 'missing') missingObjects.push(node);
    if (node.kind === 'digest-mismatch') {
      digestMismatches.push({
        ...node,
        attempts: state.rejected.get(node.digest) ?? [],
      });
    }
    if (node.kind === 'offered') awaitingContent.push(node);
    if (node.kind === 'present' && node.reachability === 'inactive-only') {
      inactiveOnlyObjects.push(node);
    }
  }

  // bad objects that never got attached to a chosen root still get reported
  const reached = r._nodeIndex;
  const unreachableMismatches = [];
  for (const d of [...state.rejected.keys()].sort()) {
    if (reached.has(d)) continue;
    unreachableMismatches.push({
      digest: d,
      kind: 'digest-mismatch',
      reachability: 'none',
      reachedByActive: [],
      reachedByInactive: [],
      pathEvidence: [],
      attempts: state.rejected.get(d) ?? [],
    });
  }

  missingObjects.sort((a, b) => a.digest.localeCompare(b.digest));
  digestMismatches.sort((a, b) => a.digest.localeCompare(b.digest));
  inactiveOnlyObjects.sort((a, b) => a.digest.localeCompare(b.digest));
  awaitingContent.sort((a, b) => a.digest.localeCompare(b.digest));

  return {
    revision: state.revision,
    roots: r.roots,
    counts: {
      ...r.counts,
      missingObjects: missingObjects.length,
      digestMismatches: digestMismatches.length,
      unreachableMismatches: unreachableMismatches.length,
      inactiveOnlyObjects: inactiveOnlyObjects.length,
      awaitingContent: awaitingContent.length,
    },
    missingObjects: maybePage(missingObjects, opts, 'missing'),
    digestMismatches: maybePage(digestMismatches, opts, 'mismatch'),
    unreachableMismatches: maybePage(unreachableMismatches, opts, 'unreachableMismatch'),
    inactiveOnlyObjects: maybePage(inactiveOnlyObjects, opts, 'inactive'),
    awaitingContent: maybePage(awaitingContent, opts, 'offered'),
    problems: r.problems,
  };
}

function maybePage(list, opts, key) {
  const cursor = opts.cursors?.[key] ?? opts.cursor ?? null;
  return paginate(list, { cursor, pageSize: opts.pageSize, maxPageSize: opts.maxPageSize });
}

export { reach };
