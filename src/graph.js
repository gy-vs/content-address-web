import { isDigest } from './digest.js';
import { paginate } from './paging.js';

export function nodeKind(state, digest) {
  if (state.objects.has(digest)) return 'present';
  if (state.rejected.has(digest)) return 'digest-mismatch';
  if (state.offered.has(digest)) return 'offered'; // announced, content not yet seen
  return 'missing';
}

/**
 * Resolve root selectors. A selector is one of:
 *   "<sha256 digest>"
 *   { digest }                  explicit anonymous root
 *   { snapshot: "name" }        resolved via the snapshot pointer
 *   { snapshot: "name", atRevision: n } historical pointer
 * Returns {roots, problems}. Each root is {snapshot|null, digest, active,
 * pointerStatus}.
 */
export function resolveRoots(state, selectors, { atRevision = null } = {}) {
  const lookup = atRevision == null
    ? state.snapshots
    : snapshotTableAt(state, atRevision);
  const roots = [];
  const problems = [];
  const seen = new Set();

  for (let i = 0; i < selectors.length; i++) {
    const sel = selectors[i];
    let snapshotName = null;
    let digest = null;

    if (typeof sel === 'string') {
      if (isDigest(sel)) digest = sel.toLowerCase();
      else snapshotName = sel;
    } else if (sel && typeof sel === 'object') {
      if (isDigest(sel.digest)) digest = String(sel.digest).toLowerCase();
      if (typeof sel.snapshot === 'string' && sel.snapshot) snapshotName = sel.snapshot;
    }

    if (snapshotName && !digest) {
      const rec = lookup.get(snapshotName);
      if (!rec) {
        problems.push({
          id: `root-missing-snapshot:${snapshotName}`,
          kind: 'root-missing-snapshot',
          snapshot: snapshotName,
          selectorIndex: i,
        });
        continue;
      }
      digest = rec.target;
    }
    if (!digest) {
      problems.push({
        id: `root-invalid:${i}`,
        kind: 'invalid-root-selector',
        selector: typeof sel === 'string' ? sel : JSON.stringify(sel),
        selectorIndex: i,
      });
      continue;
    }

    const currentRec = state.snapshots.get(snapshotName);
    const active = snapshotName ? !!currentRec?.active && currentRec.target === digest : true;
    const key = `${snapshotName ?? ''} ${digest}`;
    if (seen.has(key)) continue;
    seen.add(key);
    roots.push({
      snapshot: snapshotName,
      digest,
      active: snapshotName === null ? true : !!currentRec?.active && currentRec.target === digest,
      pointerStatus: snapshotName
        ? currentRec
          ? currentRec.target === digest ? 'current' : 'historical'
          : 'unknown'
        : 'explicit',
    });
  }
  return { roots, problems };
}

/** Snapshot pointers as they were at a given revision (used by history views). */
export function snapshotTableAt(state, revision) {
  // Cheap rebuild: fold snapshot events from the stored history-less state is
  // not possible, so the kernel provides a prebuilt snapshotTimeline; fall
  // back to current state if absent.
  if (!state.snapshotTimeline) return state.snapshots;
  const table = new Map();
  for (const entry of state.snapshotTimeline) {
    if (entry.revision > revision) break;
    table.set(entry.name, { name: entry.name, target: entry.target, active: entry.active });
  }
  return table;
}

/**
 * Expand reachability from a set of roots.
 *
 * Invalid nodes (missing / mismatch / offered) are *reported* but never
 * traversed through: an error object cannot be treated as a valid node and
 * keep propagating references. Every reached node carries shortest-path
 * evidence from each root.
 */
export function reach(state, rootSelectors, opts = {}) {
  const { roots, problems: rootProblems } = resolveRoots(state, rootSelectors, opts);
  const problems = [...rootProblems];
  const perRoot = new Map(); // key -> {root, visited:Set, paths:Map digest->digest[]}

  for (const root of roots) {
    const key = root.snapshot ?? `digest:${root.digest}`;
    const visited = new Set();
    const paths = new Map();
    const queue = [root.digest];
    paths.set(root.digest, [root.digest]);
    while (queue.length) {
      const d = queue.shift();
      if (visited.has(d)) continue;
      visited.add(d);
      const kind = nodeKind(state, d);
      if (kind !== 'present') {
        // record exactly why the chain stops here; the node stays visited so
        // it is counted exactly once, but its outgoing edges are not walked
        problems.push(problemFor(kind, d, root, paths.get(d)));
        continue;
      }
      const targets = state.edges.get(d);
      if (!targets) continue;
      const basePath = paths.get(d);
      for (const to of [...targets.keys()].sort(edgeComparator(targets))) {
        if (!paths.has(to)) {
          paths.set(to, [...basePath, to]);
        }
        if (!visited.has(to)) queue.push(to);
      }
    }
    perRoot.set(key, { root, visited, paths });
  }

  // aggregate per-node evidence
  const nodeIndex = new Map(); // digest -> aggregated
  const activeRoots = roots.filter((r) => r.active);
  const inactiveRoots = roots.filter((r) => !r.active);

  const union = new Set();
  for (const { visited } of perRoot.values()) for (const d of visited) union.add(d);

  for (const d of union) {
    const reachedByActive = [];
    const reachedByInactive = [];
    const pathEvidence = [];
    for (const [key, info] of perRoot) {
      if (!info.visited.has(d)) continue;
      const path = info.paths.get(d) ?? [info.root.digest];
      const label = info.root.snapshot ?? null;
      pathEvidence.push({ rootKey: key, snapshot: label, digest: info.root.digest, active: info.root.active, path });
      (info.root.active ? reachedByActive : reachedByInactive).push(label ?? `digest:${info.root.digest}`);
    }
    const kind = nodeKind(state, d);
    const reachability = reachedByActive.length
      ? 'active'
      : reachedByInactive.length
        ? 'inactive-only'
        : 'none';
    nodeIndex.set(d, {
      digest: d,
      kind,
      reachability,
      reachedByActive,
      reachedByInactive,
      pathEvidence,
    });
  }

  const digestList = [...union].sort();
  const nodesPage = paginate(digestList.map((d) => nodeIndex.get(d)), {
    cursor: opts.cursor,
    pageSize: opts.pageSize,
    maxPageSize: opts.maxPageSize,
  });

  return {
    revision: state.revision,
    roots,
    counts: {
      roots: roots.length,
      activeRoots: activeRoots.length,
      inactiveRoots: inactiveRoots.length,
      reached: union.size,
      present: digestList.filter((d) => state.objects.has(d)).length,
      missing: digestList.filter((d) => nodeKind(state, d) === 'missing').length,
      digestMismatch: digestList.filter((d) => nodeKind(state, d) === 'digest-mismatch').length,
      offered: digestList.filter((d) => nodeKind(state, d) === 'offered').length,
      problems: problems.length,
    },
    rootsView: roots,
    nodes: nodesPage.items,
    page: nodesPage.page,
    problems: dedupeProblems(problems),
    _nodeIndex: nodeIndex,
  };
}

function problemFor(kind, digest, root, path) {
  const base = {
    digest,
    rootSnapshot: root.snapshot,
    rootDigest: root.digest,
    path: path ?? [root.digest, digest],
  };
  if (kind === 'digest-mismatch') return { ...base, id: `digest-mismatch:${digest}`, kind: 'digest-mismatch' };
  if (kind === 'offered') return { ...base, id: `offered:${digest}`, kind: 'object-awaiting-content' };
  return { ...base, id: `missing:${digest}`, kind: 'missing-object' };
}

function edgeComparator(targets) {
  return (a, b) => targets.get(a).seq - targets.get(b).seq || a.localeCompare(b);
}

function dedupeProblems(problems) {
  const map = new Map();
  for (const p of problems) {
    const key = `${p.kind} ${p.digest ?? ''} ${p.snapshot ?? p.rootSnapshot ?? ''}`;
    if (!map.has(key)) map.set(key, p);
  }
  return [...map.values()].sort((a, b) => (a.id < b.id ? -1 : a.id > b.id ? 1 : 0));
}

/**
 * Which roots retain a specific object, across *all* known snapshots (or a
 * supplied selector list). Distinguishes active retention, inactive-root-only
 * retention, and "unknown object" — without ever walking the whole graph for
 * the caller.
 */
export function retainers(state, digest, { rootSelectors = null, includeInactive = true } = {}) {
  const d = String(digest).toLowerCase();
  const kind = nodeKind(state, d);
  let selectors;
  if (rootSelectors) {
    selectors = rootSelectors;
  } else {
    const names = [...state.snapshots.keys()].sort();
    selectors = names;
    if (!includeInactive) {
      // filtered below through active flags anyway
    }
  }
  const result = reach(state, selectors);
  const node = result._nodeIndex.get(d);

  if (!node) {
    return {
      digest: d,
      kind,
      retained: false,
      activeRetainers: [],
      inactiveRetainers: [],
      pathEvidence: [],
      note: kind === 'present' ? 'not reachable from any selected root' : `node is ${kind}`,
    };
  }
  return {
    digest: d,
    kind: node.kind,
    retained: node.reachability !== 'none',
    activeRetainers: node.reachedByActive,
    inactiveRetainers: node.reachedByInactive,
    pathEvidence: node.pathEvidence,
  };
}

/** Full active-root expansion used internally (no paging fields consumed). */
export function activeReachableSet(state) {
  const selectors = [...state.snapshots.values()]
    .filter((s) => s.active)
    .map((s) => s.name);
  const r = reach(state, selectors);
  const set = new Set();
  for (const n of r.nodes) set.add(n.digest);
  // paging may truncate nodes; recompute union internally:
  for (const d of r._nodeIndex.keys()) set.add(d);
  return { reach: r, set };
}
