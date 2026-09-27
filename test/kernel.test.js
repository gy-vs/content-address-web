import { test } from 'node:test';
import assert from 'node:assert/strict';
import { Kernel, MemoryStorage, sha256Hex } from '../src/index.js';

// Deterministic clock so recordedAt values are reproducible in assertions.
function clock() {
  let t = 1_700_000_000_000;
  return () => new Date(t++);
}

async function fresh() {
  return Kernel.recover(new MemoryStorage(), { now: clock() });
}

function obj(content, extra = {}) {
  const bytes = Buffer.from(content);
  return { digest: sha256Hex(bytes), content: bytes, ...extra };
}

test('batched imports: references arrive before the objects they point to', async () => {
  const k = await fresh();
  const root = obj('root-object');
  const child = obj('child-late');
  const leaf = obj('leaf-even-later');

  // batch 1: root + an edge to a child that does not exist yet
  const b1 = await k.importBatch({
    items: [root],
    edges: [{ from: root.digest, to: child.digest }],
  });
  assert.equal(b1.items[0].status, 'imported');
  assert.equal(b1.edges[0].status, 'declared');
  // source is present here, so no propagation warning is needed
  assert.equal(b1.edges[0].note, null);

  let rep = await k.review([{ digest: root.digest }]);
  assert.equal(rep.counts.missingObjects, 1, 'child is referenced but absent');
  assert.ok(rep.missingObjects.items.some((n) => n.digest === child.digest));

  // batch 2: child arrives, points at still-absent leaf
  const b2 = await k.importBatch({
    items: [child],
    edges: [{ from: child.digest, to: leaf.digest }],
  });
  assert.equal(b2.items[0].status, 'imported');
  rep = await k.review([{ digest: root.digest }]);
  // an edge from the still-missing child is also indexed early in batch 2;
  // once the child arrives in the same batch, its note is clean
  assert.equal(b2.edges[0].note, null);
  assert.equal(rep.counts.missingObjects, 1);
  assert.equal(rep.counts.present, 2);
  assert.deepEqual(
    rep.missingObjects.items.map((x) => x.digest),
    [leaf.digest],
  );

  // batch 3: late leaf completes the chain
  await k.importBatch({ items: [leaf] });
  rep = await k.review([{ digest: root.digest }]);
  assert.equal(rep.counts.missingObjects, 0);
  assert.equal(rep.counts.present, 3);

  // the expansion carries shortest-path evidence on every node
  const exp = await k.expand([{ digest: root.digest }]);
  assert.ok(exp.nodes.find((n) => n.digest === leaf.digest));
  const leafInfo = exp.nodes.find((n) => n.digest === leaf.digest);
  assert.deepEqual(leafInfo.pathEvidence[0].path, [root.digest, child.digest, leaf.digest]);
});

test('content can be announced (offered) and filled in later; status moves offered -> present', async () => {
  const k = await fresh();
  const later = obj('payload-later');

  await k.importBatch({ items: [{ digest: later.digest }] });
  assert.equal(k.objectStatus(later.digest).status, 'offered');
  const rep = await k.review([{ digest: later.digest }]);
  assert.equal(rep.counts.awaitingContent, 1);

  await k.importBatch({ items: [later] });
  assert.equal(k.objectStatus(later.digest).status, 'present');
  const rep2 = await k.review([{ digest: later.digest }]);
  assert.equal(rep2.counts.awaitingContent, 0);
  assert.equal(rep2.counts.present, 1);
});

test('digest mismatch is quarantined, rejected, and never propagates as a valid node', async () => {
  const k = await fresh();
  const good = obj('good');
  const evil = Buffer.from('evil-content');
  const evilDeclared = sha256Hex(Buffer.from('something-else')); // does NOT match evil bytes

  const res = await k.importBatch({
    items: [good, { digest: evilDeclared, content: evil }],
    edges: [
      { from: good.digest, to: evilDeclared },
      { from: evilDeclared, to: good.digest },
    ],
  });
  const bad = res.items.find((i) => i.status === 'rejected');
  assert.ok(bad, 'bad item reported within batch result');
  assert.equal(bad.reason, 'digest-mismatch');
  assert.ok(bad.evidenceId, 'evidence id returned');
  assert.notEqual(bad.actual, evilDeclared);

  // evidence chain: raw bytes preserved in quarantine
  const evidence = await k.quarantineEvidence(bad.evidenceId);
  assert.deepEqual(evidence.data, evil);
  assert.equal(evidence.declaredDigest, evilDeclared);
  assert.equal(evidence.actualDigest, sha256Hex(evil));

  // node view says digest-mismatch (not missing, not present)
  const node = k.node(evilDeclared);
  assert.equal(node.kind, 'digest-mismatch');
  assert.equal(node.attempts.length, 1);

  // expansion stops at the bad node; the edge from it to good is not traversed
  const rep = await k.review([{ digest: good.digest }]);
  assert.equal(rep.counts.digestMismatches, 1);
  assert.deepEqual(
    rep.digestMismatches.items.map((x) => x.digest),
    [evilDeclared],
  );
  const exp = await k.expand([{ digest: good.digest }]);
  assert.equal(exp.nodes.length, 2); // good + the broken child
  assert.ok(exp.problems.find((p) => p.kind === 'digest-mismatch'));

  // metadata never returns contents; raw range read of rejected object fails 422-style
  const meta = k.node(evilDeclared);
  assert.equal(meta.size, undefined);
  await assert.rejects(
    () => k.readContentRange(evilDeclared),
    (e) => e.kind === 'digest-mismatch',
  );

  // a later valid upload under the same address clears rejection
  await k.importBatch({ items: { digest: evilDeclared, content: Buffer.from('something-else') } });
  assert.equal(k.objectStatus(evilDeclared).status, 'present');
});

test('snapshots have history; comparisons bind to revisions and share/diff correctly', async () => {
  const k = await fresh();
  const a = obj('a');
  const b = obj('b');
  const c = obj('c');
  await k.importBatch({
    items: [a, b, c],
    edges: [
      { from: a.digest, to: b.digest },
      { from: c.digest, to: b.digest },
    ],
  });

  await k.defineSnapshot({ name: 'rel', digest: a.digest, active: true });
  const rev1 = k.revision;
  await k.defineSnapshot({ name: 'rel', digest: c.digest, active: true });
  const rev2 = k.revision;

  const hist = k.snapshotHistory('rel');
  assert.equal(hist.length, 2);
  assert.equal(hist[0].target, a.digest);
  assert.equal(hist[1].target, c.digest);

  // compare historical rev1 root (a) against current (c): b is shared
  const cmp = await k.compare({ snapshot: 'rel', atRevision: rev1 }, { snapshot: 'rel' });
  assert.deepEqual(cmp.shared.map((x) => x.digest).sort(), [b.digest].sort());
  assert.deepEqual(cmp.leftOnly.map((x) => x.digest), [a.digest]);
  assert.deepEqual(cmp.rightOnly.map((x) => x.digest), [c.digest]);
  assert.equal(cmp.left.revision, rev1);
  assert.equal(cmp.right.revision, rev2);

  // resolving a path hop by hop
  const path = await k.resolve({ snapshot: 'rel', atRevision: rev1 }, [b.digest]);
  assert.equal(path.ok, true);
  assert.deepEqual(path.hops.map((h) => h.to), [a.digest, b.digest]);
  const broken = await k.resolve({ snapshot: 'rel', atRevision: rev1 }, [c.digest]);
  assert.equal(broken.ok, false);
  assert.equal(broken.break.kind, 'missing-edge');
});

test('inactive roots: objects retained only under inactive roots are reported separately', async () => {
  const k = await fresh();
  const activeRoot = obj('active-root');
  const inactiveRoot = obj('inactive-root');
  const shared = obj('shared');
  const onlyInactive = obj('only-inactive');

  await k.importBatch({
    items: [activeRoot, inactiveRoot, shared, onlyInactive],
    edges: [
      { from: activeRoot.digest, to: shared.digest },
      { from: inactiveRoot.digest, to: shared.digest },
      { from: inactiveRoot.digest, to: onlyInactive.digest },
    ],
  });
  await k.defineSnapshot({ name: 'active-snap', digest: activeRoot.digest, active: true });
  await k.defineSnapshot({ name: 'old-snap', digest: inactiveRoot.digest, active: false });

  const rep = await k.review(['active-snap', 'old-snap']);
  assert.deepEqual(
    rep.inactiveOnlyObjects.items.map((x) => x.digest).sort(),
    [inactiveRoot.digest, onlyInactive.digest].sort(),
  );
  // shared is active-reachable and must not appear in the inactive list
  assert.ok(!rep.inactiveOnlyObjects.items.some((x) => x.digest === shared.digest));

  const r = k.retainersOf(onlyInactive.digest);
  assert.equal(r.retained, true);
  assert.deepEqual(r.activeRetainers, []);
  assert.deepEqual(r.inactiveRetainers, ['old-snap']);
  assert.equal(r.pathEvidence[0].path[0], inactiveRoot.digest);

  // GC view splits collectable vs inactive-retained
  const gc = k.gcCandidates();
  assert.ok(gc.inactiveRetained.some((c) => c.digest === onlyInactive.digest));
  assert.ok(!gc.collectable.some((c) => c.digest === onlyInactive.digest));
});

test('concurrent confirms: stale revision conflicts explicitly; later confirm sees new state', async () => {
  const k = await fresh();
  const root = obj('root');
  const g1 = obj('garbage-one');
  const g2 = obj('garbage-two');
  await k.importBatch({ items: [root, g1, g2] });
  await k.defineSnapshot({ name: 'r', digest: root.digest, active: true });

  const rev = k.revision;
  // interleave two confirmations; both were read at the same revision
  const p1 = k.confirmGcCandidate(g1.digest, { expectedRevision: rev });
  const p2 = k.confirmGcCandidate(g2.digest, { expectedRevision: rev });

  const r1 = await p1;
  assert.equal(r1.status, 'open');
  // p2 must fail with a structured conflict, not be silently applied
  await assert.rejects(p2, (err) => {
    assert.equal(err.code, 'conflict');
    assert.equal(err.details.kind, 'revision-stale');
    assert.equal(err.details.expectedRevision, rev);
    assert.equal(err.details.currentRevision, rev + 1);
    assert.ok(err.details.currentStateHash);
    assert.equal(err.details.candidateNow.status, 'unconfirmed');
    return true;
  });

  // caller re-reads candidates, rebases, confirms successfully
  const retry = await k.confirmGcCandidate(g2.digest, {
    expectedRevision: err_revisionSafe(k, rev + 1),
  });
  assert.equal(retry.status, 'open');

  // confirming an active object is a semantic 409, not 404
  await assert.rejects(
    () => k.confirmGcCandidate(root.digest, {}),
    (e) => e.code === 'conflict' && e.details.kind === 'not-a-candidate',
  );
  // unknown digest is 404 with kind
  await assert.rejects(
    () => k.confirmGcCandidate('sha256:' + 'f'.repeat(64), {}),
    (e) => e.code === 'not-found' && e.details.kind === 'missing',
  );
});

function err_revisionSafe(k, n) {
  return k.revision === n ? n : k.revision;
}

test('GC decision binds root set + object versions and survives a late object that re-retains it', async () => {
  const k = await fresh();
  const root = obj('g-root');
  const garbage = obj('will-be-retained');
  const missingLink = obj('missing-link-then-arrives');

  await k.importBatch({ items: [root, garbage] });
  await k.defineSnapshot({ name: 'r', digest: root.digest, active: true });

  const rev = k.revision;
  const hashBefore = k.hash();
  const dec = await k.confirmGcCandidate(garbage.digest, { expectedRevision: rev });
  assert.equal(dec.basisStateHash, hashBefore);
  assert.ok(dec.objectVersions[garbage.digest]);
  assert.deepEqual(dec.rootBasis.roots.map((r) => r.snapshot), ['r']);

  // late object arrives and creates a path root -> link -> garbage
  await k.importBatch({
    items: [missingLink],
    edges: [
      { from: root.digest, to: missingLink.digest },
      { from: missingLink.digest, to: garbage.digest },
    ],
  });

  // the candidate does NOT vanish silently: an immutable resolution exists
  const status = k.decisionStatus(dec.decisionId);
  assert.equal(status.status, 'resolved');
  assert.equal(status.resolution.reason, 're-retained');
  assert.deepEqual(status.resolution.retainedBy, ['r']);
  // decision record itself remains, bound to its historical basis
  assert.equal(status.basisStateHash, hashBefore);
  const events = await k.events();
  assert.ok(events.some((e) => e.type === 'gc-confirmed'));
  assert.ok(events.some((e) => e.type === 'gc-resolved' && e.decisionId === dec.decisionId));

  // candidate list now reflects retention
  const gc = k.gcCandidates();
  assert.ok(![...gc.collectable, ...gc.inactiveRetained].some((c) => c.digest === garbage.digest));
});

test('partial loading: node metadata and edges are paged; no content travels with metadata', async () => {
  const k = await fresh();
  const root = obj('rootp');
  const kids = Array.from({ length: 5 }, (_, i) => obj('kid-' + i));
  await k.importBatch({
    items: [root, ...kids],
    edges: kids.map((kid) => ({ from: root.digest, to: kid.digest })),
  });

  const page1 = k.outgoingEdges(root.digest, { pageSize: 2 });
  assert.equal(page1.items.length, 2);
  assert.equal(page1.page.total, 5);
  assert.equal(page1.page.remaining, 3);
  assert.ok(page1.page.nextCursor);
  const page2 = k.outgoingEdges(root.digest, { pageSize: 2, cursor: page1.page.nextCursor });
  assert.equal(page2.items.length, 2);
  assert.equal(page2.page.offset, 2);
  assert.deepEqual(
    page2.items[0].to,
    kids[2].digest,
    'stable ordering follows edge declaration',
  );
  const page3 = k.outgoingEdges(root.digest, { pageSize: 2, cursor: page2.page.nextCursor });
  assert.equal(page3.items.length, 1);
  assert.equal(page3.page.nextCursor, null);

  // node metadata has no `content` field at all
  const node = k.node(root.digest);
  assert.equal(node.content, undefined);
  assert.equal(node.size, Buffer.byteLength('rootp'));
});

test('large objects: staged upload + range reads with clear metadata/content boundary', async () => {
  const k = await fresh();
  const payload = Buffer.alloc(1024 * 1024); // 1 MiB deterministic
  for (let i = 0; i < payload.length; i++) payload[i] = i % 251;
  const digest = sha256Hex(payload);

  const up = await k.createUpload({ declaredDigest: digest, declaredSize: payload.length });
  const chunkSize = 256 * 1024;
  for (let off = 0; off < payload.length; off += chunkSize) {
    const part = payload.subarray(off, Math.min(off + chunkSize, payload.length));
    await k.writeUploadPart(up.uploadId, part, { offset: off });
  }
  const done = await k.commitUpload(up.uploadId, {});
  assert.equal(done.upload.status, 'committed');
  assert.equal(done.upload.digest, digest);

  // metadata stays tiny
  const meta = k.node(digest);
  assert.equal(meta.size, payload.length);
  assert.equal(meta.content, undefined);

  // range reads: first 10 bytes, then a middle window
  const head = await k.readContentRange(digest, { start: 0, end: 9 });
  assert.deepEqual(head.bytes, payload.subarray(0, 10));
  assert.equal(head.size, payload.length);
  const mid = await k.readContentRange(digest, { start: 1000, end: 1999 });
  assert.deepEqual(mid.bytes, payload.subarray(1000, 2000));

  // bad offset on a resumable upload is rejected
  const up2 = await k.createUpload({ declaredDigest: digest });
  await k.writeUploadPart(up2.uploadId, payload.subarray(0, 4), { offset: 0 });
  await assert.rejects(
    () => k.writeUploadPart(up2.uploadId, payload.subarray(4, 8), { offset: 99 }),
    (e) => e.code === 'bad-offset',
  );
  await k.abortUpload(up2.uploadId);
});

test('staged upload with wrong digest is quarantined and recorded in the chain', async () => {
  const k = await fresh();
  const claimed = sha256Hex(Buffer.from('expected'));
  const bytes = Buffer.from('actually different bytes');
  const up = await k.createUpload({ declaredDigest: claimed });
  await k.writeUploadPart(up.uploadId, bytes, { offset: 0 });
  const done = await k.commitUpload(up.uploadId, {});
  assert.equal(done.items[0].status, 'rejected');
  assert.equal(k.objectStatus(claimed).status, 'digest-mismatch');
  const q = await k.listQuarantine();
  assert.ok(q.some((e) => e.uploadId === up.uploadId));
});

test('staged upload with wrong declared size is rejected and quarantined', async () => {
  const k = await fresh();
  const bytes = Buffer.from('eleven bytes'); // 12 bytes actually
  assert.equal(bytes.length, 12);
  const digest = sha256Hex(bytes);
  const up = await k.createUpload({ declaredDigest: digest, declaredSize: 999 });
  await k.writeUploadPart(up.uploadId, bytes, { offset: 0 });
  const done = await k.commitUpload(up.uploadId, {});
  assert.equal(done.upload.status, 'rejected');
  assert.equal(done.upload.rejection.reason, 'size-mismatch');
  assert.ok(done.upload.quarantineEvidenceId, 'raw bytes still preserved as evidence');
  const evidence = await k.quarantineEvidence(done.upload.quarantineEvidenceId);
  assert.deepEqual(evidence.data, bytes);
  assert.equal(evidence.declaredSize, 999);
});

test('replay/recovery rebuilds identical projection and state hash', async () => {
  const storage = new MemoryStorage();
  const k = await Kernel.recover(storage, { now: clock() });
  const a = obj('rec-a');
  const b = obj('rec-b');
  await k.importBatch({ items: [a, b], edges: [{ from: a.digest, to: b.digest }] });
  await k.defineSnapshot({ name: 's', digest: a.digest, active: true });
  await k.confirmGcCandidate(
    'sha256:' + '1'.repeat(64),
    {},
  ).catch(() => {}); // noise that rejects
  const hash = k.hash();
  const rev = k.revision;

  const k2 = await Kernel.recover(storage, { now: clock() });
  assert.equal(k2.revision, rev);
  assert.equal(k2.hash(), hash, 'state hash identical after replay');
  assert.equal(k2.snapshotHistory('s').length, 1);
  assert.deepEqual(k2.listSnapshots().map((s) => s.target), [a.digest]);
});

test('batches are traceable: batch id links raw input -> events -> final status', async () => {
  const k = await fresh();
  const x = obj('trace-x');
  const res = await k.importBatch({ batchId: 'bat_trace1', items: [x] });
  assert.equal(res.batchId, 'bat_trace1');
  const b = k.batch('bat_trace1');
  assert.deepEqual(b.items.map((i) => i.digest), [x.digest]);
  assert.equal(b.items[0].status, 'imported');
  const node = k.node(x.digest);
  assert.equal(node.firstBatchId, 'bat_trace1');
  const events = await k.events();
  assert.ok(events.some((e) => e.batchId === 'bat_trace1'));
});
