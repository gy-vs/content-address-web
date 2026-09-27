// 图关系引擎：所有查询只读折叠状态，绝不把整图序列化给调用方。
// 列表一律游标分页；“按路径加载”只返回路径上的节点；内容字节由 BlobStore 单独提供。
import { normalizeDigest, assertDigest, sha256 } from './digest.js';
import { canonicalJSON } from './canonical.js';

export const DEFAULT_PAGE_SIZE = 100;
export const MAX_PAGE_SIZE = 1000;

/** 解析根选择：显式 roots / 快照（名称+版本）/ 活动根集合某版本 */
export function resolveRoots(state, { roots, snapshot, snapshotVersion, activeVersion } = {}) {
  if (roots) {
    return {
      source: 'explicit',
      roots: roots.map((d) => assertDigest(d, 'roots[]')),
      version: null,
    };
  }
  if (snapshot) {
    const entry = findSnapshot(state, snapshot, snapshotVersion);
    if (!entry) {
      const err = new Error(`快照不存在: ${snapshot}${snapshotVersion != null ? `@v${snapshotVersion}` : ''}`);
      err.code = 'snapshot-not-found';
      throw err;
    }
    return { source: 'snapshot', snapshot: entry.name, version: entry.version, roots: entry.roots, seq: entry.seq };
  }
  const v = activeVersion ?? state.activeRootsHistory.length - 1;
  const entry = state.activeRootsHistory[v];
  if (!entry) {
    const err = new Error(`活动根集合版本不存在: ${v}`);
    err.code = 'active-version-not-found';
    throw err;
  }
  return { source: 'active', version: entry.version, roots: entry.roots, seq: entry.seq };
}

export function findSnapshot(state, name, version) {
  if (version != null) {
    return state.snapshotHistory.find((s) => s.name === name && s.version === version) ?? null;
  }
  return state.snapshots.get(name) ?? null;
}

/**
 * 从根沿引用展开闭包（BFS）。
 * 返回四类结果（恰好对应公开可见性区分）：
 *  - reachable：活动闭包内的有效对象（+ 来自哪个根、最短路径、边）
 *  - missing：被引用/作为根，但从未导入
 *  - digest-mismatch：该摘要下有被拒收记录（摘要不匹配/畸形内容/非法引用）
 *  - inactive-only 由调用方用 retainedBy 在“全部历史根”上另行计算
 *  - tombstoned：有效但已被已确认的回收删除（迟到引用会显式落到这里）
 */
export function closure(state, opts = {}) {
  const rootSel = resolveRoots(state, opts);
  const reachable = new Map();
  const missing = new Map();
  const mismatched = new Map();
  const tombstoned = new Map();
  const rootsReport = [];
  const parents = new Map(); // digest -> { from, label }

  const classifyTarget = (digest, { from = null, label = null } = {}) => {
    const obj = state.objects.get(digest);
    if (obj) {
      if (obj.tombstoned) {
        if (!tombstoned.has(digest)) {
          tombstoned.set(digest, { digest, via: [], enactedSeq: obj.enactedSeq, size: obj.size });
        }
        if (from) tombstoned.get(digest).via.push({ from, label });
        return;
      }
      if (!reachable.has(digest)) {
        reachable.set(digest, {
          digest,
          size: obj.size,
          acceptedSeq: obj.acceptedSeq,
          revivedSeq: obj.revivedSeq,
          via: [],
          edges: obj.refs,
        });
        parents.set(digest, { from, label });
      }
      if (from) {
        const rec = reachable.get(digest);
        if (!rec.via.some((v) => v.from === from && v.label === label)) rec.via.push({ from, label });
      }
      return;
    }
    const rej = state.rejectedByDigest.get(digest);
    if (rej && rej.length) {
      if (!mismatched.has(digest)) {
        mismatched.set(digest, {
          digest,
          reasons: rej.map((rid) => ({ evidenceId: rid, reason: state.rejected.get(rid).reason })),
          via: [],
        });
      }
      if (from) mismatched.get(digest).via.push({ from, label });
      return;
    }
    if (!missing.has(digest)) missing.set(digest, { digest, via: [] });
    if (from) missing.get(digest).via.push({ from, label });
  };

  const queue = rootSel.roots.map((digest) => ({ digest, root: digest }));
  const enqueued = new Set();
  for (const r of rootSel.roots) {
    rootsReport.push({ digest: r, status: rootStatus(state, r) });
  }
  while (queue.length) {
    const { digest, root } = queue.shift();
    if (enqueued.has(digest)) continue;
    enqueued.add(digest);
    classifyTarget(digest, { from: null, label: '@root' });
    const obj = state.objects.get(digest);
    if (!obj || obj.tombstoned) continue;
    for (const edge of obj.refs) {
      classifyTarget(edge.target, { from: digest, label: edge.label });
      const child = state.objects.get(edge.target);
      if (child && !child.tombstoned && !enqueued.has(edge.target)) {
        queue.push({ digest: edge.target, root });
      }
    }
  }

  const pathOf = (digest) => shortestPath(parents, digest);
  return {
    roots: rootSel,
    rootsReport,
    reachable,
    missing,
    mismatched,
    tombstoned,
    parents,
    pathOf,
  };
}

function rootStatus(state, digest) {
  const obj = state.objects.get(digest);
  if (obj) return obj.tombstoned ? 'tombstoned' : 'present';
  if (state.rejectedByDigest.get(digest)?.length) return 'digest-mismatch';
  return 'missing';
}

function shortestPath(parents, digest) {
  if (!parents.has(digest)) return digest ? [{ digest, label: '@root' }] : [];
  const path = [];
  let cur = digest;
  while (cur) {
    const p = parents.get(cur);
    if (!p) {
      path.unshift({ digest: cur, label: '@root' });
      break;
    }
    path.unshift({ digest: cur, label: p.label, from: p.from ?? undefined });
    cur = p.from;
  }
  return path;
}

/** 比较两个快照（默认最新版本，可指定 @vN）的闭包：共享/各自独有/异常 */
export function diffSnapshots(state, a, b, { aVersion, bVersion } = {}) {
  const sa = findSnapshot(state, a, aVersion);
  const sb = findSnapshot(state, b, bVersion);
  if (!sa) return diffError(`快照不存在: ${a}`);
  if (!sb) return diffError(`快照不存在: ${b}`);
  const ca = closure(state, { snapshot: a, snapshotVersion: sa.version });
  const cb = closure(state, { snapshot: b, snapshotVersion: sb.version });
  const setA = new Set(ca.reachable.keys());
  const setB = new Set(cb.reachable.keys());
  const shared = [...setA].filter((d) => setB.has(d)).sort();
  const onlyA = [...setA].filter((d) => !setB.has(d)).sort();
  const onlyB = [...setB].filter((d) => !setA.has(d)).sort();
  return {
    kind: 'ok',
    a: { name: sa.name, version: sa.version, roots: sa.roots, seq: sa.seq },
    b: { name: sb.name, version: sb.version, roots: sb.roots, seq: sb.seq },
    counts: { shared: shared.length, onlyA: onlyA.length, onlyB: onlyB.length },
    shared,
    onlyA,
    onlyB,
    problems: {
      a: summarizeProblems(ca),
      b: summarizeProblems(cb),
    },
  };
}

function diffError(message) {
  const err = new Error(message);
  err.code = 'snapshot-not-found';
  return { kind: 'error', error: { code: err.code, message } };
}

function summarizeProblems(c) {
  return {
    missing: [...c.missing.keys()].sort(),
    digestMismatch: [...c.mismatched.keys()].sort(),
    tombstoned: [...c.tombstoned.keys()].sort(),
  };
}

/**
 * 查看某个对象被哪些根保留：在所有历史快照（或指定集合）上求闭包。
 * 这是“仅存在于非活动根下”判定的数据来源。
 */
export function retainedBy(state, digest, opts = {}) {
  const d = normalizeDigest(digest);
  if (!d) {
    const err = new Error('非法摘要');
    err.code = 'invalid-digest';
    throw err;
  }
  const active = resolveRoots(state, {});
  const activeClosure = closure(state, { activeVersion: active.version });
  const inActive = activeClosure.reachable.has(d);

  const snapshots = state.snapshotHistory.filter((s) =>
    opts.snapshot ? s.name === opts.snapshot : true,
  );
  const retained = [];
  for (const snap of snapshots) {
    const rootsReaching = snap.roots.filter((r) => reachableFromRoot(state, r, d));
    if (rootsReaching.length === 0) continue;
    retained.push({
      snapshot: snap.name,
      version: snap.version,
      isLatestOfName: state.snapshots.get(snap.name).version === snap.version,
      roots: rootsReaching,
      seq: snap.seq,
    });
  }

  return {
    digest: d,
    status: classifyObject(state, d).status,
    retainedByActiveRoots: inActive,
    activeRootsVersion: active.version,
    activeReachableRoots: active.roots.filter((r) => reachableFromRoot(state, r, d)),
    retainedBy: retained,
    // 仅存在于非活动根下：当前活动根闭包不可达，但被某个历史/其他快照保留
    inactiveOnly: !inActive && retained.length > 0,
  };
}

/** 判断从单个根出发能否到达目标（沿有效非墓碑对象） */
function reachableFromRoot(state, rootDigest, target) {
  if (rootDigest === target) return state.objects.has(target) && !state.objects.get(target).tombstoned;
  const c = closure(state, { roots: [rootDigest] });
  return c.reachable.has(target);
}

/** 对象的公开分类（七态），带证据指针 */
export function classifyObject(state, digest) {
  const d = normalizeDigest(digest);
  if (!d) return { digest, status: 'unknown' };
  const obj = state.objects.get(d);
  if (obj) {
    if (obj.tombstoned) {
      return {
        digest: d,
        status: 'tombstoned',
        size: obj.size,
        enactedSeq: obj.enactedSeq,
        revived: false,
        evidence: { acceptedSeq: obj.acceptedSeq, acceptedRecordId: obj.acceptedRecordId },
      };
    }
    return {
      digest: d,
      status: 'present',
      size: obj.size,
      parseJson: obj.parseJson,
      revived: obj.revivedSeq != null,
      revivedSeq: obj.revivedSeq,
      enactedSeq: obj.enactedSeq,
      refs: obj.refs.map((r) => ({ target: r.target, label: r.label, source: r.source })),
      evidence: { acceptedSeq: obj.acceptedSeq, acceptedRecordId: obj.acceptedRecordId },
    };
  }
  const rejIds = state.rejectedByDigest.get(d);
  if (rejIds?.length) {
    const ev = rejIds.map((rid) => state.rejected.get(rid));
    return {
      digest: d,
      status: 'digest-mismatch',
      rejections: ev.map((e) => ({
        evidenceId: e.evidenceId,
        reason: e.reason,
        actualDigest: e.actualDigest,
        detail: e.detail,
        quarantinePath: e.quarantinePath,
        seq: e.seq,
      })),
    };
  }
  // 可能作为引用目标出现过但从未导入
  return { digest: d, status: 'missing' };
}

/**
 * 按路径加载详情：从（解析得到的）根沿 label 序列下行。
 * 只返回路径上的节点摘要，绝不返回整图/内容字节。
 * @param {string[]} path label 序列，如 ['$.layers', '[0]']
 */
export function loadPath(state, digest, pathLabels = [], opts = {}) {
  const rootSel = resolveRoots(state, opts);
  const d = assertDigest(digest, 'root digest');
  if (!rootSel.roots.includes(d)) {
    const err = new Error('起点不在所选根集合中');
    err.code = 'not-a-root';
    throw err;
  }
  const trail = [];
  let current = d;
  for (const label of pathLabels) {
    const obj = state.objects.get(current);
    if (!obj) {
      return { kind: 'broken', at: trail.length, current: classifyObject(state, current), missingEdge: label, rootSelection: rootSel };
    }
    const choices = obj.refs.filter((r) => r.label === label);
    if (choices.length === 0) {
      const err = new Error(`对象 ${current} 不存在标签为 ${label} 的出边`);
      err.code = 'edge-not-found';
      throw err;
    }
    if (choices.length > 1) {
      const err = new Error(`标签 ${label} 在 ${current} 上不唯一（${choices.length} 条），请改用摘要直取`);
      err.code = 'ambiguous-label';
      throw err;
    }
    current = choices[0].target;
    trail.push({ to: current, label });
    // 跟随边后立即检查：目标缺失/拒收/被删除都在此处中断
    if (!state.objects.get(current) || state.objects.get(current)?.tombstoned) {
      return { kind: 'broken', at: trail.length, current: classifyObject(state, current), missingEdge: null, rootSelection: rootSel };
    }
  }
  return {
    kind: 'ok',
    rootSelection: rootSel,
    path: buildPath(state, d, pathLabels, current),
    target: classifyObject(state, current),
  };
}

function buildPath(state, root, labels, target) {
  const nodes = [{ digest: root, label: '@root', ...compact(state, root) }];
  let cur = root;
  for (const label of labels) {
    const edge = state.objects.get(cur).refs.find((r) => r.label === label);
    cur = edge.target;
    nodes.push({ digest: cur, label, ...compact(state, cur) });
  }
  return nodes;
}

function compact(state, digest) {
  const obj = state.objects.get(digest);
  if (!obj) return { status: classifyObject(state, digest).status };
  return { status: obj.tombstoned ? 'tombstoned' : 'present', size: obj.size, parseJson: obj.parseJson };
}

/** 列表分页：对排序后的摘要数组做 keyset 切片，游标是不透明 base64url */
export function pageDigests(list, { cursor, limit = DEFAULT_PAGE_SIZE } = {}) {
  const sorted = [...list].sort();
  const size = Math.min(MAX_PAGE_SIZE, Math.max(1, Number(limit) || DEFAULT_PAGE_SIZE));
  let start = 0;
  if (cursor) {
    const after = decodeCursor(cursor);
    start = sorted.findIndex((d) => d > after);
    if (start < 0) start = sorted.length;
  }
  const slice = sorted.slice(start, start + size);
  return {
    items: slice,
    nextCursor: start + size < sorted.length ? encodeCursor(slice[slice.length - 1]) : null,
    remaining: Math.max(0, sorted.length - start - size),
  };
}

/** 闭包指纹：决定绑定的“当时对象版本”基。根集合版本 + 闭包有序摘要 */
export function closureFingerprint(rootsSelection, reachableIterable) {
  const reachable = [...reachableIterable].sort();
  const basis = {
    rootSource: rootsSelection.source,
    rootVersion: rootsSelection.version ?? null,
    snapshot: rootsSelection.snapshot ?? null,
    seq: rootsSelection.seq ?? null,
    roots: rootsSelection.roots,
    reachable,
  };
  return {
    ...basis,
    fingerprint: `sha256:${sha256(Buffer.from(canonicalJSON(basis), 'utf8'))}`,
  };
}

function encodeCursor(value) {
  return Buffer.from(value, 'utf8').toString('base64url');
}

function decodeCursor(cursor) {
  return Buffer.from(cursor, 'base64url').toString('utf8');
}
