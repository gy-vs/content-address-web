// 历史与版本绑定：快照历史、活动根版本、审阅决定记录当时的根集合闭包指纹与对象版本。
import { test } from 'node:test';
import assert from 'node:assert/strict';
import { freshKernel, dg, putBlob, putManifestRef } from './helpers.js';

test('活动根集合每次变更都产生新版本，历史版本可回溯', async () => {
  const { kernel, cleanup } = await freshKernel();
  try {
    const a = await putBlob(kernel, 'a');
    const b = await putBlob(kernel, 'b');
    const v1 = await kernel.setActiveRoots({ roots: [a] });
    const v2 = await kernel.setActiveRoots({ roots: [a, b] });
    assert.equal(v1.activeRootsVersion, 1);
    assert.equal(v2.activeRootsVersion, 2);
    assert.deepEqual(kernel.getActiveRoots(1).roots, [a]);
    assert.deepEqual(kernel.getActiveRoots(2).roots, [a, b]);
    // 默认取最新
    assert.deepEqual(kernel.getActiveRoots().roots, [a, b]);
  } finally {
    await cleanup();
  }
});

test('审阅计划的基绑定根选择与闭包；候选标注 unreachable/inactive-only', async () => {
  const { kernel, cleanup } = await freshKernel();
  try {
    const garbage = await putBlob(kernel, 'nobody-points-here');
    const legacy = await putBlob(kernel, 'old-build-artifact');
    const current = await putBlob(kernel, 'current-artifact');

    await kernel.createSnapshot({ name: 'build', roots: [legacy] }); // v1 历史
    await kernel.createSnapshot({ name: 'build', roots: [current] }); // v2
    const active = await kernel.setActiveRoots({ roots: [current] });

    const review = await kernel.openReview({});
    assert.equal(review.basis.rootVersion, active.activeRootsVersion);
    assert.ok(review.basis.fingerprint.startsWith('sha256:'));
    const digests = review.candidates.map((c) => c.digest).sort();
    assert.deepEqual(digests, [garbage, legacy].sort());

    const legacyCand = review.candidates.find((c) => c.digest === legacy);
    assert.equal(legacyCand.reason, 'inactive-only');
    assert.deepEqual(legacyCand.retainedBy, [{ snapshot: 'build', version: 1 }]);
    const garbageCand = review.candidates.find((c) => c.digest === garbage);
    assert.equal(garbageCand.reason, 'unreachable');

    // 对象版本快照（acceptedSeq）随候选保存
    assert.ok(legacyCand.objectVersion.acceptedSeq >= 0);
  } finally {
    await cleanup();
  }
});

test('决定携带对象版本与基指纹；可从决定记录回溯到链上事件', async () => {
  const { kernel, cleanup } = await freshKernel();
  try {
    const garbage = await putBlob(kernel, 'gc-me');
    const kept = await putBlob(kernel, 'live');
    await kernel.setActiveRoots({ roots: [kept] });

    const review = await kernel.openReview({});
    const basis = review.basis.fingerprint;
    const cand = review.candidates.find((c) => c.digest === garbage);

    const dec = await kernel.confirmReview({
      reviewId: review.reviewId,
      digest: garbage,
      decision: 'confirm-delete',
      caller: 'alice',
    });
    assert.equal(dec.status, 'decided');
    assert.deepEqual(dec.objectVersion, cand.objectVersion);

    // 决定台账里的记录绑定基、对象版本、事件序号/记录 id
    const logged = kernel.listDecisions().find((d) => d.digest === garbage);
    assert.equal(logged.basis.fingerprint, basis);
    assert.deepEqual(logged.objectVersion, cand.objectVersion);
    assert.equal(logged.caller, 'alice');
    assert.ok(logged.seq > review.evidence.seq);
    // 事件记录可直接在链上按 seq 取到
    const rec = kernel.log.at(logged.seq);
    assert.equal(rec.type, 'review.decided');
    assert.equal(rec.payload.digest, garbage);
  } finally {
    await cleanup();
  }
});

test('根集合在计划打开后变化：携带旧基确认会得到 stale-basis 显式冲突', async () => {
  const { kernel, cleanup } = await freshKernel();
  try {
    const garbage = await putBlob(kernel, 'gc-me-2');
    const anchor = await putBlob(kernel, 'anchor');
    await kernel.setActiveRoots({ roots: [anchor] });
    const review = await kernel.openReview({});

    // 调用方持有的计划基令牌（打开时返回）
    const staleBasis = review.basis.fingerprint;
    // 活动根随后变化：garbage 成为新根
    await kernel.setActiveRoots({ roots: [anchor, garbage] });

    // 客户端用默认（当前）基确认：系统按计划基重算发现对象已重新可达
    const res = await kernel.confirmReview({
      reviewId: review.reviewId,
      digest: garbage,
      decision: 'confirm-delete',
      caller: 'bob',
    });
    assert.equal(res.status, 'conflict');
    assert.equal(res.conflict, 'stale-basis');
    assert.equal(res.detail.nowReachable, true);

    // 显式携带旧令牌同样冲突，并回报当前指纹
    const res2 = await kernel.confirmReview({
      reviewId: review.reviewId,
      digest: garbage,
      decision: 'confirm-delete',
      caller: 'carol',
      basis: staleBasis,
    });
    assert.equal(res2.status, 'conflict');
    assert.equal(res2.conflict, 'stale-basis');

    // 冲突已落链，不会因为状态继续变化而消失
    const conflicts = kernel.listConflicts();
    assert.ok(conflicts.length >= 1);
    assert.ok(conflicts.every((c) => c.kind === 'stale-basis'));
  } finally {
    await cleanup();
  }
});

test('历史快照版本不受后续同名快照影响，旧基审阅仍按旧闭包解释', async () => {
  const { kernel, cleanup } = await freshKernel();
  try {
    const drop = await putBlob(kernel, 'dropped-in-v2');
    const keep = await putBlob(kernel, 'kept');
    await kernel.createSnapshot({ name: 'build', roots: [keep, drop] });
    await kernel.createSnapshot({ name: 'build', roots: [keep] });
    await kernel.setActiveRoots({ roots: [keep] });

    // 用旧版本快照作为审阅基：drop 在旧基下是保留对象，不是候选
    const reviewOld = await kernel.openReview({ snapshot: 'build', snapshotVersion: 1 });
    assert.equal(reviewOld.basis.snapshot, 'build');
    assert.equal(reviewOld.basis.rootVersion, 1);
    assert.equal(reviewOld.candidates.find((c) => c.digest === drop), undefined);

    // 用最新版本作为基：drop 是 inactive-only 候选
    const reviewNew = await kernel.openReview({ snapshot: 'build' });
    assert.ok(reviewNew.candidates.find((c) => c.digest === drop));
  } finally {
    await cleanup();
  }
});
