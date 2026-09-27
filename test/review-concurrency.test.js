// 并发回收审阅：多个调用方同时确认不同候选/同一候选，
// 冲突必须显式呈现并落链；幂等确认得到 ack；删除执行后对象成墓碑且决定记录保留。
import { test } from 'node:test';
import assert from 'node:assert/strict';
import { freshKernel, dg, putBlob } from './helpers.js';

async function setup() {
  const ctx = await freshKernel();
  const { kernel } = ctx;
  const live = await putBlob(kernel, 'live-root');
  const g1 = await putBlob(kernel, 'garbage-1');
  const g2 = await putBlob(kernel, 'garbage-2');
  const g3 = await putBlob(kernel, 'garbage-3');
  await kernel.setActiveRoots({ roots: [live] });
  const review = await kernel.openReview({});
  return { ...ctx, review, live, g1, g2, g3 };
}

test('多个调用方并发确认不同候选：全部串行落链、互不丢失', async () => {
  const ctx = await setup();
  try {
    const { kernel, review, g1, g2, g3 } = ctx;
    const results = await Promise.all([
      kernel.confirmReview({ reviewId: review.reviewId, digest: g1, decision: 'confirm-delete', caller: 'worker-1' }),
      kernel.confirmReview({ reviewId: review.reviewId, digest: g2, decision: 'confirm-delete', caller: 'worker-2' }),
      kernel.confirmReview({ reviewId: review.reviewId, digest: g3, decision: 'keep', caller: 'worker-3' }),
    ]);
    assert.deepEqual(results.map((r) => r.status).sort(), ['decided', 'decided', 'decided']);
    const decisions = kernel.listDecisions();
    assert.equal(decisions.length, 3);
    // 链序号严格连续递增
    const seqs = decisions.map((d) => d.seq);
    assert.deepEqual(seqs, [...seqs].sort((a, b) => a - b));
    assert.equal(new Set(seqs).size, 3);
  } finally {
    await ctx.cleanup();
  }
});

test('并发对同一候选做相反决定：一个成功，另一个得到 opposite-decision 冲突', async () => {
  const ctx = await setup();
  try {
    const { kernel, review, g1 } = ctx;
    const [r1, r2] = await Promise.all([
      kernel.confirmReview({ reviewId: review.reviewId, digest: g1, decision: 'confirm-delete', caller: 'alice' }),
      kernel.confirmReview({ reviewId: review.reviewId, digest: g1, decision: 'keep', caller: 'bob' }),
    ]);
    const statuses = [r1.status, r2.status].sort();
    assert.deepEqual(statuses, ['conflict', 'decided']);
    const conflict = [r1, r2].find((r) => r.status === 'conflict');
    assert.equal(conflict.conflict, 'opposite-decision');
    assert.ok(conflict.existing);
    assert.ok(['alice', 'bob'].includes(conflict.existing.caller));

    // 冲突与决定都落链
    assert.equal(kernel.listDecisions().length, 1);
    assert.equal(kernel.listConflicts().length, 1);
    const hydrated = kernel.getReview(review.reviewId);
    assert.equal(hydrated.conflicts[0].kind, 'opposite-decision');
  } finally {
    await ctx.cleanup();
  }
});

test('相同决定重复确认（含并发）：幂等 ack，不产生第二条决定', async () => {
  const ctx = await setup();
  try {
    const { kernel, review, g2 } = ctx;
    const first = await kernel.confirmReview({ reviewId: review.reviewId, digest: g2, decision: 'keep', caller: 'alice' });
    assert.equal(first.status, 'decided');
    const again = await kernel.confirmReview({ reviewId: review.reviewId, digest: g2, decision: 'keep', caller: 'alice' });
    assert.equal(again.status, 'ack');
    const [c1, c2] = await Promise.all([
      kernel.confirmReview({ reviewId: review.reviewId, digest: g2, decision: 'keep', caller: 'x' }),
      kernel.confirmReview({ reviewId: review.reviewId, digest: g2, decision: 'keep', caller: 'y' }),
    ]);
    assert.equal(c1.status, 'ack');
    assert.equal(c2.status, 'ack');
    assert.equal(kernel.listDecisions().length, 1);
    assert.equal(kernel.listConflicts().length, 0);
  } finally {
    await ctx.cleanup();
  }
});

test('对不存在的审阅/候选/非法决定给出结构化错误', async () => {
  const ctx = await setup();
  try {
    const { kernel, review, g1 } = ctx;
    await assert.rejects(() => kernel.confirmReview({ reviewId: 'nope', digest: g1, decision: 'keep', caller: 'x' }), (e) => e.code === 'review-not-found');
    await assert.rejects(() => kernel.confirmReview({ reviewId: review.reviewId, digest: g1, decision: 'nuke', caller: 'x' }), (e) => e.code === 'invalid-decision');
    // 活对象不是候选
    const res = await kernel.confirmReview({ reviewId: review.reviewId, digest: ctx.live, decision: 'confirm-delete', caller: 'x' });
    assert.equal(res.status, 'conflict');
    assert.equal(res.conflict, 'not-candidate');
  } finally {
    await ctx.cleanup();
  }
});

test('执行删除：确认删除的对象字节被移除并成墓碑；keep 的保留；记录不消失', async () => {
  const ctx = await setup();
  try {
    const { kernel, review, g1, g2, g3 } = ctx;
    await kernel.confirmReview({ reviewId: review.reviewId, digest: g1, decision: 'confirm-delete', caller: 'a' });
    await kernel.confirmReview({ reviewId: review.reviewId, digest: g2, decision: 'confirm-delete', caller: 'b' });
    await kernel.confirmReview({ reviewId: review.reviewId, digest: g3, decision: 'keep', caller: 'c' });

    const enacted = await kernel.enactDeletions({ reviewId: review.reviewId, caller: 'gc-job' });
    assert.deepEqual(enacted.deleted.sort(), [g1, g2].sort());
    assert.equal(enacted.skipped.length, 0);

    // 字节确实没了
    assert.throws(() => kernel.openContent(g1), (e) => e.code === 'object-tombstoned');
    // keep 的还在
    const kept = await kernel.readContent(g3);
    assert.equal(kept.bytes.toString(), 'garbage-3');

    // 状态分类为墓碑，且仍能追溯到删除批次与决定
    const st = kernel.objectStatus(g1);
    assert.equal(st.status, 'tombstoned');
    assert.ok(st.enactedSeq > 0);
    const hydrated = kernel.getReview(review.reviewId);
    assert.deepEqual(hydrated.candidates.find((c) => c.digest === g1).deleted, true);
    assert.equal(hydrated.candidates.find((c) => c.digest === g3).deleted, false);
    // 决定记录依然完整
    assert.equal(kernel.listDecisions().length, 3);
    assert.ok(kernel.stateInfo().tombstoned >= 2);
  } finally {
    await ctx.cleanup();
  }
});

test('迟到引用让已确认候选复活：执行时跳过并记录 stale-basis 冲突', async () => {
  const ctx = await setup();
  try {
    const { kernel, review, g1 } = ctx;
    await kernel.confirmReview({ reviewId: review.reviewId, digest: g1, decision: 'confirm-delete', caller: 'a' });

    // 在执行前，一个新的根 manifest 引用 g1（迟到对象/新结构），并成为活动根
    const bridge = await kernel.importObject({
      data: Buffer.from(JSON.stringify({ rescue: g1 })),
      parseJson: false,
      refs: [{ digest: g1, label: 'rescue' }],
    });
    await kernel.setActiveRoots({ roots: [bridge.digest, ctx.live] });

    const enacted = await kernel.enactDeletions({ reviewId: review.reviewId, caller: 'gc-job' });
    assert.deepEqual(enacted.deleted, []);
    assert.equal(enacted.skipped[0].reason, 'now-reachable');
    assert.equal(enacted.skipped[0].digest, g1);

    // 字节仍在
    const bytes = await kernel.readContent(g1);
    assert.equal(bytes.bytes.toString(), 'garbage-1');
    // 变化有记录
    assert.ok(kernel.listConflicts().some((c) => c.kind === 'stale-basis'));
  } finally {
    await ctx.cleanup();
  }
});

test('墓碑对象被同内容重新导入时复活，并保留 revived 证据', async () => {
  const ctx = await setup();
  try {
    const { kernel, review, g1 } = ctx;
    await kernel.confirmReview({ reviewId: review.reviewId, digest: g1, decision: 'confirm-delete', caller: 'a' });
    await kernel.enactDeletions({ reviewId: review.reviewId });
    assert.equal(kernel.objectStatus(g1).status, 'tombstoned');

    // 同内容（同摘要）重新到达
    const re = await kernel.importObject({ data: Buffer.from('garbage-1') });
    assert.equal(re.status, 'accepted');
    assert.equal(re.revived, true);
    const st = kernel.objectStatus(g1);
    assert.equal(st.status, 'present');
    assert.equal(st.revived, true);
    assert.ok(st.revivedSeq > st.enactedSeq);
  } finally {
    await ctx.cleanup();
  }
});
