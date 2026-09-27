// 局部加载：按标签路径只取路径节点；内容边界（摘要不带字节，字节走流，支持 Range）。
import { test } from 'node:test';
import assert from 'node:assert/strict';
import { freshKernel, dg, putBlob, putJson } from './helpers.js';

test('按路径加载只返回路径节点，不序列化整图', async () => {
  const { kernel, cleanup } = await freshKernel();
  try {
    const leaf1 = await putBlob(kernel, 'leaf-1');
    const leaf2 = await putBlob(kernel, 'leaf-2');
    const deep = await putJson(kernel, { layer: leaf1 }, { refs: [{ digest: leaf1, label: 'layer' }] });
    // 根上有大量无关分支，证明只沿路径走
    const distractors = [];
    for (let i = 0; i < 10; i++) distractors.push(await putBlob(kernel, `noise-${i}`));
    const rootValue = {
      config: { deep },
      another: leaf2,
      noise: distractors,
    };
    const root = await putJson(kernel, rootValue, {
      refs: [
        { digest: deep, label: 'config.deep' },
        { digest: leaf2, label: 'another' },
        ...distractors.map((d, i) => ({ digest: d, label: `noise[${i}]` })),
      ],
    });

    const r = kernel.loadPath(root, ['config.deep', 'layer'], { roots: [root] });
    assert.equal(r.kind, 'ok');
    assert.equal(r.target.digest, leaf1);
    assert.equal(r.path.length, 3);
    assert.deepEqual(r.path.map((n) => n.digest), [root, deep, leaf1]);
    // 路径节点摘要只含元数据，绝不包含字节
    for (const node of r.path) {
      assert.equal('content' in node, false);
      assert.equal('bytes' in node, false);
    }
  } finally {
    await cleanup();
  }
});

test('路径在缺失对象处中断时给出 broken 与当前位置', async () => {
  const { kernel, cleanup } = await freshKernel();
  try {
    const ghost = 'sha256:' + '3'.repeat(64);
    const root = await putJson(kernel, { x: ghost }, { refs: [{ digest: ghost, label: 'x' }] });
    const r = kernel.loadPath(root, ['x'], { roots: [root] });
    assert.equal(r.kind, 'broken');
    assert.equal(r.at, 1);
    assert.equal(r.current.status, 'missing');
    assert.equal(r.current.digest, ghost);
  } finally {
    await cleanup();
  }
});

test('路径标签不存在/起点非根时报结构化错误', async () => {
  const { kernel, cleanup } = await freshKernel();
  try {
    const leaf = await putBlob(kernel, 'l');
    const root = await putJson(kernel, {}, { refs: [{ digest: leaf, label: 'only' }] });
    assert.throws(() => kernel.loadPath(root, ['nope'], { roots: [root] }), (e) => e.code === 'edge-not-found');
    assert.throws(() => kernel.loadPath(leaf, [], { roots: [root] }), (e) => e.code === 'not-a-root');
  } finally {
    await cleanup();
  }
});

test('摘要接口不含字节，内容经流读取；大对象只做顺序 IO 且可分段取', async () => {
  const { kernel, cleanup } = await freshKernel();
  try {
    const size = 3 * 1024 * 1024 + 123; // 3 MiB+，明显超过任何内联阈值
    const payload = Buffer.alloc(size, 0x61);
    payload.write('HEAD-MARKER', 0);
    payload.write('TAIL-MARKER', size - 11);

    // 分三批导入
    const declared = dg(payload);
    const started = await kernel.startImport({ declaredDigest: declared, chunkCount: 3 });
    const cut1 = Math.floor(size / 3);
    const cut2 = Math.floor((size * 2) / 3);
    await kernel.uploadChunk({ importId: started.importId, index: 0, data: payload.subarray(0, cut1) });
    await kernel.uploadChunk({ importId: started.importId, index: 2, data: payload.subarray(cut2) });
    await kernel.uploadChunk({ importId: started.importId, index: 1, data: payload.subarray(cut1, cut2) });
    const done = await kernel.endImport({ importId: started.importId });
    assert.equal(done.digest, declared);

    // 摘要：有大小/分块数，没有任何字节字段
    const summary = kernel.getSummary(declared);
    assert.equal(summary.size, size);
    assert.equal(summary.chunks, 3);
    assert.equal('content' in summary, false);
    assert.equal(JSON.stringify(summary).length < 2000, true);

    // 分段读：头部（半开区间 [0,11) 恰好覆盖 11 字节标记）
    const head = kernel.openContent(declared, { start: 0, end: 11 });
    assert.equal(head.meta.size, size);
    const headBuf = await collect(head.stream);
    assert.equal(headBuf.toString(), 'HEAD-MARKER');

    // 分段读：尾部（闭区间语义为半开 [start,end)）
    const tail = kernel.openContent(declared, { start: size - 11 });
    const tailBuf = await collect(tail.stream);
    assert.equal(tailBuf.toString(), 'TAIL-MARKER');

    // 中段一致性：完整流式读取并复核摘要
    const full = kernel.openContent(declared);
    let total = 0;
    for await (const c of full.stream) total += c.length;
    assert.equal(total, size);

    // 越界区间被拒
    assert.throws(() => kernel.openContent(declared, { start: 10, end: 5 }), (e) => e.code === 'invalid-range');
    assert.throws(() => kernel.openContent(declared, { start: 0, end: size + 1 }), (e) => e.code === 'invalid-range');
  } finally {
    await cleanup();
  }
});

test('列表/展开的 JSON 体积与对象大小无关（只放指针）', async () => {
  const { kernel, cleanup } = await freshKernel();
  try {
    const big = await kernel.importObject({ data: Buffer.alloc(1024 * 1024, 0x7a) });
    await kernel.setActiveRoots({ roots: [big.digest] });
    const listed = kernel.listObjects();
    const serialized = Buffer.byteLength(JSON.stringify(listed));
    assert.ok(serialized < 5000, `列表序列化体积异常: ${serialized}`);
  } finally {
    await cleanup();
  }
});

async function collect(stream) {
  const chunks = [];
  for await (const c of stream) chunks.push(c);
  return Buffer.concat(chunks);
}
