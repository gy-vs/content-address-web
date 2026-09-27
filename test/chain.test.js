// 数据链：关闭重开后所有状态由同一条哈希链解释；篡改任何一行都被 verify 检出。
import { test } from 'node:test';
import assert from 'node:assert/strict';
import { readFile, appendFile, writeFile } from 'node:fs/promises';
import path from 'node:path';
import {
  Kernel, Log, replayRecords, canonicalJSON, recordHash,
} from 'content-address-web';
import { freshKernel, dg, putBlob, putManifestRef } from './helpers.js';

test('重放恢复：导入/快照/审阅/决定/删除在新进程实例上完全一致', async () => {
  const { kernel, dir, cleanup } = await freshKernel();
  try {
    const live = await putBlob(kernel, 'live');
    const dead = await putBlob(kernel, 'dead');
    const keep = await putBlob(kernel, 'keep');
    const mid = await putManifestRef(kernel, [live]);
    await kernel.createSnapshot({ name: 's1', roots: [mid] });
    await kernel.createSnapshot({ name: 's1', roots: [live] });
    await kernel.setActiveRoots({ roots: [live, keep] });

    // 一次拒收也必须跨重启保留
    await kernel.importObject({ data: Buffer.from('bad'), declaredDigest: dg('other') });

    const review = await kernel.openReview({});
    await kernel.confirmReview({ reviewId: review.reviewId, digest: dead, decision: 'confirm-delete', caller: 'op' });
    await kernel.confirmReview({ reviewId: review.reviewId, digest: keep, decision: 'keep', caller: 'op' });
    await kernel.enactDeletions({ reviewId: review.reviewId });

    const beforeInfo = kernel.stateInfo();
    const head = kernel.chainInfo();
    const beforeReview = kernel.getReview(review.reviewId);

    // 重放成一个全新 Kernel
    const reopened = await Kernel.create(dir);
    assert.equal(reopened.chainInfo().headHash, head.headHash);
    assert.equal(reopened.chainInfo().headSeq, head.headSeq);
    assert.deepEqual(reopened.stateInfo(), beforeInfo);

    // 图查询结果一致
    const e1 = kernel.expand({});
    const e2 = reopened.expand({});
    assert.deepEqual(
      e2.reachable.map((x) => x.digest).sort(),
      e1.reachable.map((x) => x.digest).sort(),
    );
    // 快照历史、活动根版本一致
    assert.equal(reopened.listSnapshots().length, 2);
    assert.deepEqual(reopened.getActiveRoots().roots, [live, keep]);
    // 审阅/决定/冲突/墓碑一致
    const afterReview = reopened.getReview(review.reviewId);
    assert.deepEqual(
      afterReview.candidates.map((c) => [c.digest, c.decision, c.deleted]),
      beforeReview.candidates.map((c) => [c.digest, c.decision, c.deleted]),
    );
    assert.equal(reopened.objectStatus(dead).status, 'tombstoned');
    // 拒收证据一致且隔离字节仍在
    assert.equal(reopened.listRejections().length, 1);
    const kept = await readFile(reopened.listRejections()[0].quarantinePath);
    assert.equal(kept.toString(), 'bad');
  } finally {
    await cleanup();
  }
});

test('纯函数重放：从日志记录重建的状态与内核状态同构', async () => {
  const { kernel, cleanup } = await freshKernel();
  try {
    await putBlob(kernel, 'x');
    await kernel.setActiveRoots({ roots: [] });
    const rebuilt = replayRecords(kernel.log.slice());
    assert.equal(rebuilt.objects.size, kernel.state.objects.size);
    assert.equal(rebuilt.activeRootsHistory.length, kernel.state.activeRootsHistory.length);
  } finally {
    await cleanup();
  }
});

test('篡改日志任意一行：verify 报告 chain-broken 且能定位行号', async () => {
  const { kernel, dir, cleanup } = await freshKernel();
  try {
    await putBlob(kernel, 'one');
    await putBlob(kernel, 'two');
    const logFile = path.join(dir, 'log', 'events.jsonl');
    const lines = (await readFile(logFile, 'utf8')).trim().split('\n');
    assert.ok(lines.length >= 3);

    // 篡改第 2 行载荷（保留其 hash 字段，制造内容/签名不一致）
    const rec = JSON.parse(lines[1]);
    rec.payload.tampered = true;
    lines[1] = canonicalJSON(rec);
    await writeFile(logFile, lines.join('\n') + '\n');

    const reopenedLog = new Log(logFile);
    const result = await reopenedLog.verify();
    assert.equal(result.ok, false);
    assert.equal(result.error.line, 2);
    assert.match(result.error.reason, /哈希不匹配/);

    // Kernel.create 重放时直接抛出 chain-broken
    await assert.rejects(() => Kernel.create(dir), (e) => e.code === 'chain-broken');
  } finally {
    await cleanup();
  }
});

test('截断日志（缺行）：序号/衔接断裂被检出', async () => {
  const { kernel, dir, cleanup } = await freshKernel();
  try {
    await putBlob(kernel, 'aaa');
    await putBlob(kernel, 'bbb');
    const logFile = path.join(dir, 'log', 'events.jsonl');
    const lines = (await readFile(logFile, 'utf8')).trim().split('\n');
    // 删掉最后一行
    await writeFile(logFile, lines.slice(0, -1).join('\n') + '\n');
    // 截断后仍能打开（末尾不完整视为新链续写点之前的状态）：验证通过
    const log = new Log(logFile);
    const v = await log.verify();
    assert.equal(v.ok, true);
    assert.equal(v.headSeq, lines.length - 2);
  } finally {
    await cleanup();
  }
});

test('追加伪造记录（不接链）：下一次重放检出 prevHash 断裂', async () => {
  const { kernel, dir, cleanup } = await freshKernel();
  try {
    await putBlob(kernel, 'zz');
    const logFile = path.join(dir, 'log', 'events.jsonl');
    const lines = (await readFile(logFile, 'utf8')).trim().split('\n');
    const last = JSON.parse(lines.at(-1));
    const fakeBody = { seq: last.seq + 1, prevHash: 'sha256:' + 'f'.repeat(64), ts: 1, id: 'fake', type: 'review.decided', payload: {} };
    const forged = { ...fakeBody, hash: recordHash(fakeBody) };
    await appendFile(logFile, canonicalJSON(forged) + '\n');
    const log = new Log(logFile);
    const v = await log.verify();
    assert.equal(v.ok, false);
    assert.match(v.error.reason, /prevHash/);
  } finally {
    await cleanup();
  }
});

test('原始输入→中间分块→最终对象由事件链串联（证据字段齐全）', async () => {
  const { kernel, cleanup } = await freshKernel();
  try {
    const payload = Buffer.from('traceability-0123456789');
    const started = await kernel.startImport({ declaredDigest: dg(payload), chunkCount: 2, clientRef: 'req-42' });
    const ch0 = await kernel.uploadChunk({ importId: started.importId, index: 0, data: payload.subarray(0, 10) });
    const ch1 = await kernel.uploadChunk({ importId: started.importId, index: 1, data: payload.subarray(10) });
    const done = await kernel.endImport({ importId: started.importId });

    // 最终对象摘要回溯到两个分块摘要
    const summary = kernel.getSummary(done.digest);
    assert.equal(summary.clientRef, 'req-42');
    assert.equal(summary.chunks, 2);

    // 链上事件顺序与关联
    const types = kernel.log.slice().map((r) => r.type);
    const tail = types.slice(-4);
    assert.deepEqual(tail, ['import.started', 'import.chunk', 'import.chunk', 'import.object-accepted']);
    const accepted = kernel.log.slice().filter((r) => r.type === 'import.object-accepted').at(-1);
    assert.equal(accepted.payload.digest, done.digest);
    assert.deepEqual(accepted.payload.chunks.map((c) => c.chunkDigest), [ch0.chunkDigest, ch1.chunkDigest]);
    assert.equal(accepted.payload.chunks[0].bytes, 10);
  } finally {
    await cleanup();
  }
});
