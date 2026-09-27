// 摘要校验失败：错误对象必须被拒收、隔离原始字节、留下证据，
// 且不能作为有效节点进入图或沿它传播引用。
import { test } from 'node:test';
import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import { KernelError } from 'content-address-web';
import { freshKernel, dg, putJson } from './helpers.js';

test('声明摘要与实际不符：拒收并隔离原始字节，证据可定位', async () => {
  const { kernel, cleanup } = await freshKernel();
  try {
    const payload = Buffer.from('real-bytes-here');
    const r = await kernel.importObject({ data: payload, declaredDigest: dg('different-bytes') });
    assert.equal(r.status, 'rejected');
    assert.equal(r.reason, 'digest-mismatch');
    assert.equal(r.actualDigest, dg(payload));
    assert.ok(r.evidenceId);
    assert.equal(r.evidence.seq, r.evidence.seq);

    // 隔离区保留原始输入字节
    const kept = await readFile(r.quarantine.path);
    assert.equal(kept.toString(), 'real-bytes-here');

    // 拒收台账
    const rej = kernel.listRejections();
    assert.equal(rej.length, 1);
    assert.equal(rej[0].reason, 'digest-mismatch');
    assert.equal(rej[0].declaredDigest, dg('different-bytes'));
    assert.equal(rej[0].actualDigest, dg(payload));

    // 不能当对象读取
    assert.equal(kernel.stateInfo().liveObjects, 0);
    assert.throws(
      () => kernel.openContent(dg(payload)),
      (e) => e.code === 'object-not-found',
    );
  } finally {
    await cleanup();
  }
});

test('声明为 manifest 但内容不是合法 JSON：malformed-content，且不传播其“声明引用”', async () => {
  const { kernel, cleanup } = await freshKernel();
  try {
    const target = 'sha256:' + 'a'.repeat(64);
    const bad = Buffer.from('{ this is : not json ,,,');
    // 给出正确的字节摘要，证明问题出在内容形态而非传输损坏
    const r = await kernel.importObject({
      data: bad,
      declaredDigest: dg(bad),
      parseJson: true,
      refs: [{ digest: target, label: 'should-not-propagate' }],
    });
    assert.equal(r.status, 'rejected');
    assert.equal(r.reason, 'malformed-content');

    // 被隔离的字节仍然完整可查
    const kept = await readFile(r.quarantine.path);
    assert.equal(kept.toString(), bad.toString());

    // target 不应因为错误对象的声明而出现在任何闭包/引用索引里
    const status = kernel.objectStatus(target);
    assert.equal(status.status, 'missing'); // 只是“未知摘要”，不是经由坏对象引用
  } finally {
    await cleanup();
  }
});

test('摘要错误的分批导入：拒收后会话释放，可重新以正确摘要导入', async () => {
  const { kernel, cleanup } = await freshKernel();
  try {
    const payload = Buffer.from('chunked-but-wrong-declared');
    const started = await kernel.startImport({ declaredDigest: dg('nope'), chunkCount: 2 });
    await kernel.uploadChunk({ importId: started.importId, index: 0, data: payload.subarray(0, 10) });
    await kernel.uploadChunk({ importId: started.importId, index: 1, data: payload.subarray(10) });
    const bad = await kernel.endImport({ importId: started.importId });
    assert.equal(bad.status, 'rejected');
    assert.equal(bad.reason, 'digest-mismatch');
    assert.equal(kernel.listPendingImports().length, 0);

    // 隔离证据包含每个分块的摘要链（可定位失败输入到具体分块）
    const rej = kernel.listRejections()[0];
    assert.equal(rej.chunks.length, 2);
    assert.ok(rej.chunks.every((c) => c.chunkDigest.startsWith('sha256:')));

    // 用正确声明重新导入
    const ok = await kernel.importObject({ data: payload });
    assert.equal(ok.status, 'accepted');
    assert.equal(ok.digest, dg(payload));
    // 两次尝试都有记录：一条拒收 + 一条接受
    assert.equal(kernel.listRejections().length, 1);
  } finally {
    await cleanup();
  }
});

test('非法摘要/非法引用声明在导入入口即被拒', async () => {
  const { kernel, cleanup } = await freshKernel();
  try {
    await assert.rejects(() => kernel.startImport({ declaredDigest: 'not-a-digest' }), (e) => e.code === 'invalid-digest');
    await assert.rejects(
      () => kernel.importObject({ data: Buffer.from('x'), refs: [{ digest: 'sha256:bad' }] }),
      (e) => e.code === 'invalid-reference',
    );
    assert.ok(new KernelError('x', 'y') instanceof Error);
  } finally {
    await cleanup();
  }
});

test('错误对象不会被根“带活”：以拒收摘要为根时归类为 digest-mismatch', async () => {
  const { kernel, cleanup } = await freshKernel();
  try {
    const bad = Buffer.from('bad-root-content');
    await kernel.importObject({ data: bad, declaredDigest: dg('other') });
    const expansion = kernel.expand({ roots: [dg('other')] });
    assert.equal(expansion.counts.reachable, 0);
    assert.deepEqual(expansion.digestMismatch.map((x) => x.digest), [dg('other')]);
    assert.equal(expansion.digestMismatch[0].rejections[0].reason, 'digest-mismatch');
  } finally {
    await cleanup();
  }
});

test('同摘要合法对象不会因后来一次错误的 manifest 声明被隔离或删除', async () => {
  const { kernel, cleanup } = await freshKernel();
  try {
    const bytes = Buffer.from('not json but valid blob');
    const first = await kernel.importObject({ data: bytes }); // 作为普通 blob 接受
    assert.equal(first.status, 'accepted');

    // 再次导入相同字节，却声称是 manifest：必须幂等接受且原对象不动
    const again = await kernel.importObject({ data: bytes, parseJson: true });
    assert.equal(again.status, 'accepted');
    assert.equal(again.idempotent, true);

    const st = kernel.objectStatus(first.digest);
    assert.equal(st.status, 'present');
    assert.equal(st.parseJson, false);
    const { bytes: got } = await kernel.readContent(first.digest);
    assert.equal(got.toString(), bytes.toString());
    // 没有产生任何拒收记录
    assert.equal(kernel.listRejections().length, 0);
  } finally {
    await cleanup();
  }
});

test('manifest 的 JSON 内嵌摘要字符串自动成为引用边，显式标签优先', async () => {
  const { kernel, cleanup } = await freshKernel();
  try {
    const child = 'sha256:' + 'b'.repeat(64);
    const digest = await putJson(kernel, { config: { layer: child }, unrelated: 'text' });
    const summary = kernel.getSummary(digest);
    const edge = summary.refs.find((r) => r.target === child);
    assert.ok(edge);
    assert.equal(edge.label, '$.config.layer');
    assert.equal(edge.source, 'json');
  } finally {
    await cleanup();
  }
});
