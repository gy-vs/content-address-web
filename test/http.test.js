import { test } from 'node:test';
import assert from 'node:assert/strict';
import { Kernel, MemoryStorage, sha256Hex } from '../src/index.js';
import { createHttpServer } from '../src/http/server.js';

let server;
let base;
let k;

async function request(method, pathname, body, { headers = {}, raw = false } = {}) {
  const url = new URL(pathname, base);
  const opts = { method, headers: { ...headers } };
  let payload;
  if (body !== undefined) {
    if (raw || Buffer.isBuffer(body)) {
      payload = body;
      opts.headers['content-type'] = opts.headers['content-type'] ?? 'application/octet-stream';
    } else {
      payload = Buffer.from(JSON.stringify(body));
      opts.headers['content-type'] = 'application/json';
    }
    opts.headers['content-length'] = Buffer.byteLength(payload);
  }
  const res = await fetch(url, { ...opts, body: payload, duplex: 'half' });
  const type = res.headers.get('content-type') ?? '';
  const data = type.includes('application/json') ? await res.json() : Buffer.from(await res.arrayBuffer());
  return { status: res.status, headers: res.headers, data };
}

function obj(s) {
  const bytes = Buffer.from(s);
  return {
    digest: sha256Hex(bytes),
    content: { encoding: 'base64', data: bytes.toString('base64') },
    _bytes: bytes,
  };
}

test.beforeEach(async () => {
  k = await Kernel.recover(new MemoryStorage());
  const s = await createHttpServer(k);
  server = s.server;
  base = s.url;
});

test.afterEach(async () => {
  await new Promise((resolve) => server.close(resolve));
});

test('HTTP end-to-end: import, snapshot, expand and review distinguish all three cases', async () => {
  const root = obj('http-root');
  const child = obj('http-child');
  const absent = obj('http-absent'); // never uploaded
  const inactiveOnly = obj('http-inactive-only');
  const oldRoot = obj('http-old-root');

  // batch with good objects + edges to a missing one
  const imp = await request('POST', '/batches', {
    items: [
      { digest: root.digest, content: root.content },
      { digest: child.digest, content: child.content },
      { digest: oldRoot.digest, content: oldRoot.content },
      { digest: inactiveOnly.digest, content: inactiveOnly.content },
    ],
    edges: [
      { from: root.digest, to: child.digest },
      { from: child.digest, to: absent.digest },
      { from: oldRoot.digest, to: inactiveOnly.digest },
    ],
  });
  assert.equal(imp.status, 201, JSON.stringify(imp.data));

  // bad object -> 422 semantics in review + quarantine evidence
  const evil = Buffer.from('evil');
  const claimed = sha256Hex(Buffer.from('other'));
  const bad = await request('POST', '/batches', {
    items: [{ digest: claimed, content: { encoding: 'utf8', data: evil.toString('utf8') } }],
  });
  assert.equal(bad.data.items[0].status, 'rejected');

  await request('PUT', '/snapshots/current', { digest: root.digest, active: true });
  await request('PUT', '/snapshots/archived', { digest: oldRoot.digest, active: false });

  const review = await request('POST', '/review', { roots: ['current', 'archived'] });
  assert.equal(review.status, 200);
  assert.deepEqual(review.data.missingObjects.items.map((x) => x.digest), [absent.digest]);
  // the bad upload has no incoming edge from these roots, but still surfaces
  assert.deepEqual(review.data.unreachableMismatches.items.map((x) => x.digest), [claimed]);
  assert.deepEqual(
    review.data.inactiveOnlyObjects.items.map((x) => x.digest).sort(),
    [inactiveOnly.digest, oldRoot.digest].sort(),
  );

  // metadata endpoint never includes content
  const node = await request('GET', `/objects/${root.digest}`);
  assert.equal(node.status, 200);
  assert.equal(node.data.content, undefined);
  assert.equal(node.data.size, root._bytes.length);
  // bad object metadata yields 422
  const badNode = await request('GET', `/objects/${claimed}`);
  assert.equal(badNode.status, 422);
  assert.equal(badNode.data.kind, 'digest-mismatch');

  // missing object metadata yields 404
  const missingNode = await request('GET', `/objects/${absent.digest}`);
  assert.equal(missingNode.status, 404);
  assert.equal(missingNode.data.kind, 'missing');
});

test('HTTP: Range requests return 206 with clear metadata boundary', async () => {
  const bytes = Buffer.alloc(5000, 7);
  const digest = sha256Hex(bytes);
  const up = await request('POST', '/uploads', { declaredDigest: digest, declaredSize: 5000 });
  assert.equal(up.status, 201);
  const id = up.data.uploadId;

  const part = await request('PUT', `/uploads/${id}/parts?offset=0`, bytes, { raw: true });
  assert.equal(part.status, 200);
  assert.equal(part.data.size, 5000);

  const commit = await request('POST', `/uploads/${id}/commit`, {});
  assert.equal(commit.status, 201, JSON.stringify(commit.data));
  assert.equal(commit.data.upload.digest, digest);

  // query-param range
  const r1 = await request('GET', `/objects/${digest}/content?start=100&end=199`);
  assert.equal(r1.status, 206);
  assert.equal(r1.headers.get('content-range'), 'bytes 100-199/5000');
  assert.equal(r1.headers.get('x-content-digest'), digest);
  assert.equal(r1.data.length, 100);

  // HTTP Range header
  const r2 = await fetch(new URL(`/objects/${digest}/content`, base), {
    headers: { Range: 'bytes=0-99' },
  });
  assert.equal(r2.status, 206);
  assert.equal((await r2.arrayBuffer()).byteLength, 100);

  // suffix range
  const r3 = await fetch(new URL(`/objects/${digest}/content`, base), {
    headers: { Range: 'bytes=-50' },
  });
  assert.equal(r3.status, 206);
  assert.equal(r3.headers.get('content-range'), 'bytes 4950-4999/5000');
});

test('HTTP: concurrent GC confirms surface explicit conflicts with rebasing detail', async () => {
  const root = obj('c-root');
  const g1 = obj('c-g1');
  const g2 = obj('c-g2');
  await request('POST', '/batches', {
    items: [root, g1, g2].map((o) => ({ digest: o.digest, content: o.content })),
  });
  await request('PUT', '/snapshots/r', { digest: root.digest, active: true });

  const state = await request('GET', '/state');
  const rev = state.data.revision;

  // fire two confirms that both observed the same revision
  const [a, b] = await Promise.all([
    request('POST', '/gc/confirm', { digest: g1.digest, expectedRevision: rev }),
    request('POST', '/gc/confirm', { digest: g2.digest, expectedRevision: rev }),
  ]);
  const ok = a.status === 201 ? a : b;
  const fail = a.status === 409 ? a : b;
  assert.equal(ok.status, 201);
  assert.equal(fail.status, 409);
  assert.equal(fail.data.error, 'conflict');
  assert.equal(fail.data.details.kind, 'revision-stale');
  assert.equal(fail.data.details.currentRevision, rev + 1);
  assert.ok(fail.data.details.currentStateHash);
  assert.equal(fail.data.details.candidateNow.status, 'unconfirmed');

  // rebase: list candidates then confirm at the new revision
  const cands = await request('GET', '/gc/candidates');
  const newRev = cands.data.revision;
  const retry = await request('POST', '/gc/confirm', {
    digest: fail.data.details.candidateNow.digest,
    expectedRevision: newRev,
  });
  assert.equal(retry.status, 201, JSON.stringify(retry.data));

  // decision is retrievable with bound basis
  const dec = await request('GET', `/decisions/${ok.data.decisionId}`);
  assert.equal(dec.status, 200);
  assert.deepEqual(dec.data.rootBasis.roots.map((r) => r.snapshot), ['r']);
  assert.ok(dec.data.objectVersions[g1.digest] || dec.data.objectVersions[g2.digest]);
});

test('HTTP: events and batches provide raw -> intermediate -> final traceability', async () => {
  const x = obj('trace');
  const r = await request('POST', '/batches', {
    batchId: 'bat_http_trace',
    items: [{ digest: x.digest, content: x.content }],
  });
  assert.equal(r.status, 201);
  assert.ok(r.data.stateHash);

  const batch = await request('GET', '/batches/bat_http_trace');
  assert.equal(batch.data.items[0].status, 'imported');

  const events = await request('GET', '/events');
  assert.ok(events.data.events.some((e) => e.type === 'object-imported'));
  assert.ok(events.data.events.some((e) => e.type === 'batch-recorded'));
});
