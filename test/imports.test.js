// 分批导入：乱序分块、断点重传、迟到引用对象、恢复后续传。
import { test } from 'node:test';
import assert from 'node:assert/strict';
import { writeFile } from 'node:fs/promises';
import { freshKernel, dg, putBlob } from './helpers.js';

test('分块乱序到达后仍能提交，且幂等重发', async () => {
  const { kernel, cleanup } = await freshKernel();
  try {
    const payload = Buffer.from('0123456789-分批-abcdef');
    const declared = dg(payload);
    const started = await kernel.startImport({ declaredDigest: declared, chunkCount: 3 });
    const parts = [payload.subarray(0, 6), payload.subarray(6, 14), payload.subarray(14)];

    // 乱序：先 2，再 0，再 1
    const c2 = await kernel.uploadChunk({ importId: started.importId, index: 2, data: parts[2] });
    const c0 = await kernel.uploadChunk({ importId: started.importId, index: 0, data: parts[0] });
    const c1 = await kernel.uploadChunk({ importId: started.importId, index: 1, data: parts[1] });
    assert.equal(c2.status, 'uploaded');

    // 重复重发同序号同内容：ack 幂等，不产生第二条分块事件
    const dup = await kernel.uploadChunk({ importId: started.importId, index: 1, data: parts[1], chunkDigest: c1.chunkDigest });
    assert.equal(dup.status, 'duplicate');

    const pending = kernel.listPendingImports()[0];
    assert.deepEqual(pending.receivedChunks.map((c) => c.index), [0, 1, 2]);

    const done = await kernel.endImport({ importId: started.importId });
    assert.equal(done.status, 'accepted');
    assert.equal(done.digest, declared);
    assert.equal(kernel.listPendingImports.length, 0);
  } finally {
    await cleanup();
  }
});

test('分块未到齐时结束导入报 chunks-incomplete，会话仍可续传', async () => {
  const { kernel, cleanup } = await freshKernel();
  try {
    const payload = Buffer.from('xyz-content');
    const started = await kernel.startImport({ declaredDigest: dg(payload), chunkCount: 2 });
    await kernel.uploadChunk({ importId: started.importId, index: 0, data: payload.subarray(0, 4) });
    await assert.rejects(
      () => kernel.endImport({ importId: started.importId }),
      (e) => e.code === 'chunks-incomplete' && e.details.missing.includes(1),
    );
    // 补齐后成功——迟到的分块不应强制重来
    await kernel.uploadChunk({ importId: started.importId, index: 1, data: payload.subarray(4) });
    const done = await kernel.endImport({ importId: started.importId });
    assert.equal(done.digest, dg(payload));
  } finally {
    await cleanup();
  }
});

test('重发分块内容不一致显式报 chunk-conflict', async () => {
  const { kernel, cleanup } = await freshKernel();
  try {
    const started = await kernel.startImport({ declaredDigest: dg('abcdef'), chunkCount: 2 });
    await kernel.uploadChunk({ importId: started.importId, index: 0, data: Buffer.from('ab') });
    await assert.rejects(
      () => kernel.uploadChunk({ importId: started.importId, index: 0, data: Buffer.from('zz'), chunkDigest: dg('zz') }),
      (e) => e.code === 'chunk-conflict',
    );
  } finally {
    await cleanup();
  }
});

test('引用边指向的对象晚到：闭包从 missing 变为 reachable', async () => {
  const { kernel, cleanup } = await freshKernel();
  try {
    const lateBytes = Buffer.from('=== late payload ===');
    const lateDigest = dg(lateBytes);

    // 1) 先导入根（声明引用一个尚不存在的摘要）
    const rootBytes = Buffer.from(JSON.stringify({ name: 'root' }));
    const rootImport = await kernel.startImport({
      declaredDigest: dg(rootBytes),
      parseJson: false,
      refs: [{ digest: lateDigest, label: 'dep' }],
    });
    await kernel.uploadChunk({ importId: rootImport.importId, index: 0, data: rootBytes });
    const rootRes = await kernel.endImport({ importId: rootImport.importId });
    const rootDigest = rootRes.digest;
    await kernel.setActiveRoots({ roots: [rootDigest] });

    // 2) 此刻展开：late 是缺失对象
    const before = kernel.expand({});
    assert.deepEqual(before.missing.map((x) => x.digest), [lateDigest]);
    assert.equal(before.counts.missing, 1);

    // 3) 迟到对象到达
    await putBlob(kernel, lateBytes);

    // 4) 同一根集合重新展开：缺失消失，late 可达
    const after = kernel.expand({});
    assert.equal(after.counts.missing, 0);
    assert.ok(after.reachable.some((x) => x.digest === lateDigest));
  } finally {
    await cleanup();
  }
});

test('显式放弃导入会记录 abandoned 并释放会话', async () => {
  const { kernel, cleanup } = await freshKernel();
  try {
    const started = await kernel.startImport({ declaredDigest: dg('qq'), chunkCount: 2 });
    await kernel.uploadChunk({ importId: started.importId, index: 0, data: Buffer.from('q') });
    const r = await kernel.abandonImport({ importId: started.importId, reason: 'client-cancel' });
    assert.equal(r.status, 'abandoned');
    assert.equal(kernel.listPendingImports().length, 0);
  } finally {
    await cleanup();
  }
});

test('相同内容重复导入幂等：同一摘要、同一对象记录，不产生重复对象', async () => {
  const { kernel, cleanup } = await freshKernel();
  try {
    const d1 = await putBlob(kernel, 'identical-bytes');
    const d2 = await putBlob(kernel, 'identical-bytes');
    assert.equal(d1, d2);
    assert.equal(kernel.stateInfo().liveObjects, 1);
    const r = await kernel.importObject({ data: Buffer.from('identical-bytes') });
    assert.equal(r.status, 'accepted');
    assert.equal(r.revived, false);
    assert.equal(kernel.stateInfo().liveObjects, 1);
  } finally {
    await cleanup();
  }
});

test('进程重启（重开同一数据目录）后未完成会话可继续上传并提交', async () => {
  const { kernel, dir, cleanup } = await freshKernel();
  const { Kernel } = await import('content-address-web');
  try {
    const payload = Buffer.from('recovery-session-payload-0123456789');
    const started = await kernel.startImport({ declaredDigest: dg(payload), chunkCount: 3 });
    await kernel.uploadChunk({ importId: started.importId, index: 0, data: payload.subarray(0, 10) });
    await kernel.uploadChunk({ importId: started.importId, index: 1, data: payload.subarray(10, 20) });

    // 模拟崩溃后重开：新 Kernel 实例重放日志
    const reopened = await Kernel.create(dir);
    const pending = reopened.listPendingImports();
    assert.equal(pending.length, 1);
    assert.deepEqual(pending[0].receivedChunks.map((c) => c.index), [0, 1]);

    // 磁盘上有一个事件链不知道的孤儿分块（崩溃点残留），重开时应被清掉
    await writeFile(
      `${dir}/objects/tmp/import-${started.importId}/chunk-99`,
      Buffer.from('orphan'),
    );
    const reopened2 = await Kernel.create(dir);

    await reopened2.uploadChunk({ importId: started.importId, index: 2, data: payload.subarray(20) });
    const done = await reopened2.endImport({ importId: started.importId });
    assert.equal(done.status, 'accepted');
    assert.equal(done.digest, dg(payload));
  } finally {
    await cleanup();
  }
});
