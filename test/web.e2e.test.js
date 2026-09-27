// 通过公开模块入口 content-address-web/web 走 HTTP 端到端：
// 分批导入、图查询、快照比较、局部 Range、回收审阅冲突、重启恢复。
import { test, after } from 'node:test';
import assert from 'node:assert/strict';
import { mkdtemp, rm } from 'node:fs/promises';
import os from 'node:os';
import path from 'node:path';
import { createServer } from 'content-address-web/web';
import { Kernel } from 'content-address-web';
import { sha256, digestFromHex } from 'content-address-web';

const digestOf = (s) => digestFromHex(sha256(Buffer.from(s)));

async function harness() {
  const dir = await mkdtemp(path.join(os.tmpdir(), 'caw-web-'));
  const server = await createServer({ dataDir: dir });
  await new Promise((res) => server.listen(0, '127.0.0.1', res));
  const port = server.address().port;
  const base = `http://127.0.0.1:${port}`;
  const api = async (route, init = {}) => {
    const res = await fetch(base + route, init);
    const ct = res.headers.get('content-type') ?? '';
    // 206 分片响应或非 JSON 类型一律按字节返回（内容边界）
    const isJson = res.status !== 206 && ct.includes('application/json');
    const body = isJson ? await res.json() : Buffer.from(await res.arrayBuffer());
    return { status: res.status, headers: res.headers, body };
  };
  const close = () => new Promise((res) => server.close(res));
  return { dir, base, api, close, kernel: server.kernel };
}

test('HTTP 全链路：分批导入→展开→快照→比较→审阅冲突→Range→重启重放', async () => {
  const h = await harness();
  after(h.close);
  const { api, dir } = h;

  // ---- 分批导入一个引用 manifest（乱序分块） ----
  const leafA = digestOf('leaf-A-bytes');
  const manifest = Buffer.from(JSON.stringify({ a: leafA, note: 'root' }));
  const rootDigest = digestFromHex(sha256(manifest));
  let r = await api('/v1/imports', {
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify({
      declaredDigest: rootDigest, parseJson: true, chunkCount: 2,
      refs: [{ digest: leafA, label: 'a' }],
    }),
  });
  assert.equal(r.status, 201);
  const importId = r.body.importId;
  const cut = Math.floor(manifest.length / 2);
  // 先传 1 再传 0
  r = await api(`/v1/imports/${importId}/chunks/1`, { method: 'PUT', body: manifest.subarray(cut) });
  assert.equal(r.status, 201);
  r = await api(`/v1/imports/${importId}/chunks/0`, { method: 'PUT', body: manifest.subarray(0, cut) });
  assert.equal(r.status, 201);
  r = await api(`/v1/imports/${importId}/end`, { method: 'POST', body: '{}' });
  assert.equal(r.status, 200);
  assert.equal(r.body.digest, rootDigest);

  // ---- 摘要不匹配经 HTTP 返回 422 + 证据 ----
  r = await api('/v1/objects', { method: 'POST', headers: { 'x-declared-digest': digestOf('nope') }, body: Buffer.from('bad-bytes') });
  assert.equal(r.status, 422);
  assert.equal(r.body.reason, 'digest-mismatch');
  assert.ok(r.body.quarantine.path);

  // ---- 直接导入 leaf，缺失转可达 ----
  r = await api('/v1/objects', { method: 'POST', body: Buffer.from('leaf-A-bytes') });
  assert.equal(r.status, 201);
  assert.equal(r.body.digest, leafA);

  // ---- 快照两版 + 活动根 ----
  const other = digestOf('only-in-v2');
  await api('/v1/objects', { method: 'POST', body: Buffer.from('only-in-v2') });
  await api('/v1/snapshots', { method: 'POST', body: JSON.stringify({ name: 'build', roots: [rootDigest] }) });
  await api('/v1/snapshots', { method: 'POST', body: JSON.stringify({ name: 'build', roots: [rootDigest, other] }) });
  await api('/v1/active-roots', { method: 'PUT', body: JSON.stringify({ roots: [rootDigest, other] }) });

  // ---- 展开：分页元数据齐全 ----
  r = await api('/v1/graph/expand?limit=1');
  assert.equal(r.status, 200);
  assert.ok(r.body.counts.reachable >= 3);

  // ---- 快照版本比较 ----
  r = await api('/v1/snapshots/build/diff/build?aVersion=1&bVersion=2');
  assert.equal(r.status, 200);
  assert.deepEqual(r.body.onlyB.items, [other]);
  assert.ok(r.body.shared.items.includes(rootDigest));

  // ---- 按路径局部加载 ----
  r = await api(`/v1/graph/path?root=${rootDigest}&label=a`);
  assert.equal(r.status, 200);
  assert.equal(r.body.target.digest, leafA);
  assert.equal(r.body.path.length, 2);

  // ---- 内容边界：摘要无字节，Range 206 ----
  r = await api(`/v1/objects/${rootDigest}/summary`);
  assert.equal(r.status, 200);
  assert.equal('content' in r.body, false);

  r = await api(`/v1/objects/${rootDigest}/content`, { headers: { range: 'bytes=0-3' } });
  assert.equal(r.status, 206);
  assert.equal(r.headers.get('content-range'), `bytes 0-3/${manifest.length}`);
  assert.equal(r.body.toString(), manifest.subarray(0, 4).toString());

  // ---- 回收审阅：制造一个垃圾对象，两个调用方并发相反决定 ----
  await api('/v1/objects', { method: 'POST', body: Buffer.from('garbage-web') });
  const garbage = digestOf('garbage-web');
  r = await api('/v1/reviews', { method: 'POST', body: '{}' });
  assert.equal(r.status, 201);
  const reviewId = r.body.reviewId;
  assert.ok(r.body.candidates.some((c) => c.digest === garbage));

  const [d1, d2] = await Promise.all([
    api(`/v1/reviews/${reviewId}/decisions`, {
      method: 'POST', body: JSON.stringify({ digest: garbage, decision: 'confirm-delete', caller: 'web-a' }),
    }),
    api(`/v1/reviews/${reviewId}/decisions`, {
      method: 'POST', body: JSON.stringify({ digest: garbage, decision: 'keep', caller: 'web-b' }),
    }),
  ]);
  const statuses = [d1.status, d2.status].sort();
  assert.deepEqual(statuses, [200, 409]);
  const conflict = [d1, d2].find((x) => x.status === 409);
  assert.equal(conflict.body.conflict, 'opposite-decision');

  // ---- 执行删除（决定方若获胜则删除；keep 获胜则跳过），两种走向都自洽 ----
  r = await api(`/v1/reviews/${reviewId}/enact`, { method: 'POST', body: JSON.stringify({ caller: 'gc' }) });
  assert.equal(r.status, 200);
  r = await api(`/v1/reviews/${reviewId}`);
  const cand = r.body.candidates.find((c) => c.digest === garbage);
  if (cand.decision === 'confirm-delete') {
    assert.equal(cand.deleted, true);
  } else {
    assert.equal(cand.deleted, false);
  }
  assert.equal(r.body.conflicts.length, 1);

  // ---- 数据链可验证 ----
  r = await api('/v1/chain');
  assert.equal(r.status, 200);
  assert.equal(r.body.ok, true);
  const headBefore = r.body.headHash;

  // ---- 重启：新 server 打开同一目录，重复调用得到同一状态解释 ----
  await h.close();
  const server2 = await createServer({ dataDir: dir });
  await new Promise((res) => server2.listen(0, '127.0.0.1', res));
  const base2 = `http://127.0.0.1:${server2.address().port}`;
  after(() => new Promise((res) => server2.close(res)));

  const health = await (await fetch(`${base2}/v1/health`)).json();
  assert.equal(health.headHash, headBefore);
  const review2 = await (await fetch(`${base2}/v1/reviews/${reviewId}`)).json();
  assert.equal(review2.candidates.find((c) => c.digest === garbage).decision, cand.decision);
  assert.equal(review2.conflicts[0].kind, 'opposite-decision');
  // 历史快照仍在
  const snaps = await (await fetch(`${base2}/v1/snapshots`)).json();
  assert.equal(snaps.snapshots.filter((s) => s.name === 'build').length, 2);
});

test('直接用 Kernel 恢复后 web 层继续服务（同一数据链）', async () => {
  const h = await harness();
  after(h.close);
  const { api, dir } = h;
  const digest = digestOf('persisted-object');
  let r = await api('/v1/objects', { method: 'POST', body: Buffer.from('persisted-object') });
  assert.equal(r.status, 201);

  await h.close();
  const kernel = await Kernel.create(dir);
  assert.equal(kernel.objectStatus(digest).status, 'present');
  const server = await createServer({ kernel });
  await new Promise((res) => server.listen(0, '127.0.0.1', res));
  after(() => new Promise((res) => server.close(res)));
  const base2 = `http://127.0.0.1:${server.address().port}`;
  const got = await fetch(`${base2}/v1/objects/${digest}/content`);
  assert.equal(got.status, 200);
  assert.equal((Buffer.from(await got.arrayBuffer())).toString(), 'persisted-object');
  await rm(dir, { recursive: true, force: true });
});
