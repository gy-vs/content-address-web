// 图关系查看：根展开、缺失/拒收分类、快照版本、比较共享部分、保留分析、仅非活动根。
import { test } from 'node:test';
import assert from 'node:assert/strict';
import { freshKernel, dg, putBlob, putManifestRef } from './helpers.js';

test('沿引用闭包展开：可达对象带路径/入边，缺失与拒收各自分类', async () => {
  const { kernel, cleanup } = await freshKernel();
  try {
    const leaf = await putBlob(kernel, 'leaf-bytes');
    const shared = await putBlob(kernel, 'shared-bytes');
    const mid = await putManifestRef(kernel, [leaf, shared]);
    const root = await putManifestRef(kernel, [mid, shared]);
    await kernel.setActiveRoots({ roots: [root] });

    const e = kernel.expand({});
    assert.equal(e.counts.reachable, 4); // root, mid, leaf, shared（去重）
    const digests = e.reachable.map((x) => x.digest).sort();
    assert.deepEqual(digests, [root, mid, leaf, shared].sort());

    const sharedEntry = e.reachable.find((x) => x.digest === shared);
    // shared 有两个来源
    const froms = sharedEntry.via.map((v) => v.from).sort();
    assert.ok(froms.includes(mid) && froms.includes(root));
  } finally {
    await cleanup();
  }
});

test('闭包显式区分：缺失对象 / 摘要不匹配 / 墓碑对象', async () => {
  const { kernel, cleanup } = await freshKernel();
  try {
    const missing = 'sha256:' + '1'.repeat(64);
    const badBytes = Buffer.from('corrupt');
    const rejected = await kernel.importObject({ data: badBytes, declaredDigest: dg('x') });
    const r2 = await putManifestRef(kernel, [missing, rejected.declaredDigest]);
    const e = kernel.expand({ roots: [r2] });
    assert.deepEqual(e.missing.map((x) => x.digest), [missing]);
    assert.deepEqual(e.digestMismatch.map((x) => x.digest), [rejected.declaredDigest]);
    assert.equal(e.digestMismatch[0].rejections[0].evidenceId, rejected.evidenceId);
    assert.equal(e.counts.reachable, 1); // 只有根本身
  } finally {
    await cleanup();
  }
});

test('同名快照保留全部历史版本，比较两个版本的共享/独有部分', async () => {
  const { kernel, cleanup } = await freshKernel();
  try {
    const a = await putBlob(kernel, 'a');
    const b = await putBlob(kernel, 'b');
    const c = await putBlob(kernel, 'c');

    const v1 = await kernel.createSnapshot({ name: 'build', roots: [a, b] });
    const v2 = await kernel.createSnapshot({ name: 'build', roots: [b, c] });
    assert.equal(v1.snapshot.version, 1);
    assert.equal(v2.snapshot.version, 2);

    const d = kernel.diff('build', 'build', { aVersion: 1, bVersion: 2 });
    assert.deepEqual(d.shared.items, [b]);
    assert.deepEqual(d.onlyA.items, [a]);
    assert.deepEqual(d.onlyB.items, [c]);
    assert.equal(d.a.version, 1);
    assert.equal(d.b.version, 2);

    const list = kernel.listSnapshots().filter((s) => s.name === 'build');
    assert.equal(list.length, 2);
    assert.equal(list.find((s) => s.version === 2).latest, true);
    assert.equal(list.find((s) => s.version === 1).latest, false);
  } finally {
    await cleanup();
  }
});

test('比较两个不同快照，并报告各自闭包中的问题对象', async () => {
  const { kernel, cleanup } = await freshKernel();
  try {
    const a = await putBlob(kernel, 'obj-a');
    const ghost = 'sha256:' + '2'.repeat(64);
    const rOld = await putManifestRef(kernel, [a, ghost]);
    const rNew = await putBlob(kernel, 'new-only');
    await kernel.createSnapshot({ name: 'old', roots: [rOld] });
    await kernel.createSnapshot({ name: 'new', roots: [rNew] });

    const d = kernel.diff('old', 'new');
    assert.ok(d.onlyA.items.includes(a));
    assert.ok(d.onlyA.items.includes(rOld));
    assert.deepEqual(d.onlyB.items, [rNew]);
    assert.deepEqual(d.problems.a.missing, [ghost]);
  } finally {
    await cleanup();
  }
});

test('保留分析：对象被哪些根/快照保留；仅历史快照保留时 inactiveOnly', async () => {
  const { kernel, cleanup } = await freshKernel();
  try {
    const legacy = await putBlob(kernel, 'legacy-only');
    const current = await putBlob(kernel, 'current');

    await kernel.createSnapshot({ name: 'build', roots: [legacy] }); // v1：历史
    await kernel.createSnapshot({ name: 'build', roots: [current] }); // v2：最新
    await kernel.setActiveRoots({ roots: [current] });

    const rLegacy = kernel.retained(legacy);
    assert.equal(rLegacy.retainedByActiveRoots, false);
    assert.equal(rLegacy.inactiveOnly, true);
    assert.equal(rLegacy.retainedBy.length, 1);
    assert.equal(rLegacy.retainedBy[0].snapshot, 'build');
    assert.equal(rLegacy.retainedBy[0].version, 1);
    assert.equal(rLegacy.retainedBy[0].isLatestOfName, false);

    const rCurrent = kernel.retained(current);
    assert.equal(rCurrent.retainedByActiveRoots, true);
    assert.equal(rCurrent.inactiveOnly, false);

    const rNobody = kernel.retained('sha256:' + '9'.repeat(64));
    assert.equal(rNobody.status, 'missing');
    assert.equal(rNobody.retainedBy.length, 0);
  } finally {
    await cleanup();
  }
});

test('展开支持游标分页，不要求一次性取完整图', async () => {
  const { kernel, cleanup } = await freshKernel();
  try {
    const digests = [];
    for (let i = 0; i < 5; i++) digests.push(await putBlob(kernel, `blob-${i}`));
    await kernel.setActiveRoots({ roots: digests });

    const page1 = kernel.expand({ limit: 2 });
    assert.equal(page1.reachable.length, 2);
    assert.ok(page1.cursors.reachable);
    const page2 = kernel.expand({ limit: 2, cursorReachable: page1.cursors.reachable });
    assert.equal(page2.reachable.length, 2);
    const page3 = kernel.expand({ limit: 2, cursorReachable: page2.cursors.reachable });
    assert.equal(page3.reachable.length, 1);
    assert.equal(page3.cursors.reachable, null);

    const all = new Set([...page1.reachable, ...page2.reachable, ...page3.reachable].map((x) => x.digest));
    assert.equal(all.size, 5);
  } finally {
    await cleanup();
  }
});

test('比较不存在的快照返回结构化错误而非抛穿', async () => {
  const { kernel, cleanup } = await freshKernel();
  try {
    assert.throws(() => kernel.diff('nope', 'nada'), (e) => e.code === 'snapshot-not-found');
  } finally {
    await cleanup();
  }
});
