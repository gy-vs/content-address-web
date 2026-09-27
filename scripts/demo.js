#!/usr/bin/env node
// 端到端演示：同一脚本可重复运行，每次从空目录构建、审阅、重放验证。
// 运行：node scripts/demo.js [数据目录]
import { mkdtemp, rm } from 'node:fs/promises';
import os from 'node:os';
import path from 'node:path';
import { Kernel, sha256, digestFromHex } from '../src/index.js';

const D = (s) => digestFromHex(sha256(Buffer.from(s)));
const line = (t) => console.log(`\n=== ${t} ===`);

const dir = process.argv[2] ?? await mkdtemp(path.join(os.tmpdir(), 'caw-demo-'));
console.log('数据目录:', dir);
const k = await Kernel.create(dir);

// 1) 分批导入：引用先于对象到达
line('1. 分批导入（引用先到，对象迟到）');
const leafLate = D('leaf-layer');
const rootBytes = Buffer.from(JSON.stringify({ name: 'build-root', layer: leafLate }));
const imp = await k.startImport({ declaredDigest: D(rootBytes), parseJson: true, chunkCount: 2 });
await k.uploadChunk({ importId: imp.importId, index: 1, data: rootBytes.subarray(20) });
await k.uploadChunk({ importId: imp.importId, index: 0, data: rootBytes.subarray(0, 20) });
const root = (await k.endImport({ importId: imp.importId })).digest;
await k.setActiveRoots({ roots: [root] });
console.log('根已导入，被引用对象缺失：', k.expand({}).counts);
await k.importObject({ data: Buffer.from('leaf-layer') }); // 迟到
console.log('迟到对象到达后：', k.expand({}).counts);

// 2) 摘要错误被拒收并隔离
line('2. 摘要错误对象拒收');
const rej = await k.importObject({ data: Buffer.from('corrupt!'), declaredDigest: D('different') });
console.log(`拒收原因=${rej.reason} 证据=${rej.evidenceId}\n隔离文件=${rej.quarantine.path}`);

// 3) 历史快照 + 比较
line('3. 快照历史与比较');
const oldOnly = await k.importObject({ data: Buffer.from('v1-only') });
const shared = await k.importObject({ data: Buffer.from('shared') });
await k.createSnapshot({ name: 'build', roots: [root, oldOnly.digest, shared.digest] });
await k.createSnapshot({ name: 'build', roots: [root, shared.digest] });
const diff = k.diff('build', 'build', { aVersion: 1, bVersion: 2 });
console.log('v1/v2 共享:', diff.counts.shared, '仅 v1:', diff.counts.onlyA, '仅 v2:', diff.counts.onlyB);
console.log('oldOnly 是否仅历史根保留:', k.retained(oldOnly.digest).inactiveOnly);

// 4) 回收审阅 + 并发相反决定
line('4. 回收审阅与并发冲突');
const garbage = (await k.importObject({ data: Buffer.from('garbage') })).digest;
const review = await k.openReview({});
console.log('候选数:', review.candidates.length, '基指纹:', review.basis.fingerprint.slice(0, 24) + '…');
const [a, b] = await Promise.all([
  k.confirmReview({ reviewId: review.reviewId, digest: garbage, decision: 'confirm-delete', caller: 'alice' }),
  k.confirmReview({ reviewId: review.reviewId, digest: garbage, decision: 'keep', caller: 'bob' }),
]);
console.log('并发相反决定 ->', a.status, '/', b.status, '(冲突已落链)');
for (const c of review.candidates) {
  if (c.reason === 'unreachable' && c.digest !== garbage) {
    await k.confirmReview({ reviewId: review.reviewId, digest: c.digest, decision: 'confirm-delete', caller: 'gc' });
  }
}

// 5) 数据链验证 + 重放
line('5. 哈希链验证');
const headBefore = k.chainInfo().headHash;
console.log(await k.verifyChain());
const k2 = await Kernel.create(dir);
console.log('重放后链头一致:', k2.chainInfo().headHash === headBefore);
console.log('重放后状态计数:', k2.stateInfo());

if (!process.argv[2]) await rm(dir, { recursive: true, force: true });
console.log('\n完成。');
