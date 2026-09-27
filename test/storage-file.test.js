import { test } from 'node:test';
import assert from 'node:assert/strict';
import { mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import path from 'node:path';
import { Kernel, FileStorage, sha256Hex } from '../src/index.js';

async function tempDir() {
  return mkdtemp(path.join(tmpdir(), 'caw-'));
}

test('FileStorage: events, blobs and quarantine survive close + recover', async () => {
  const dir = await tempDir();
  try {
    const storage = await FileStorage.create(dir);
    const k = await Kernel.recover(storage);
    const content = Buffer.from('persist-me');
    const digest = sha256Hex(content);
    const bad = Buffer.from('bad-bytes');
    const badDigest = sha256Hex(Buffer.from('different'));

    await k.importBatch({
      batchId: 'bat_persist',
      items: [
        { digest, content },
        { digest: badDigest, content: bad },
      ],
    });
    const rejected = k.node(badDigest);
    assert.equal(rejected.kind, 'digest-mismatch');
    const evidenceId = rejected.attempts[0].evidenceId;
    const hash = k.hash();
    const rev = k.revision;

    // reopen the same directory with a brand new kernel
    const storage2 = await FileStorage.create(dir);
    const k2 = await Kernel.recover(storage2);
    assert.equal(k2.revision, rev);
    assert.equal(k2.hash(), hash, 'state hash identical after disk recovery');
    assert.equal(k2.objectStatus(digest).status, 'present');
    assert.equal(k2.objectStatus(badDigest).status, 'digest-mismatch');

    // blob bytes readable from disk
    const blob = await k2.readContentRange(digest, { start: 0, end: content.length - 1 });
    assert.deepEqual(blob.bytes, content);

    // quarantine evidence still on disk
    const ev = await k2.quarantineEvidence(evidenceId);
    assert.deepEqual(ev.data, bad);
    assert.equal(ev.declaredDigest, badDigest);
  } finally {
    await rm(dir, { recursive: true, force: true });
  }
});

test('FileStorage: large staged upload hashes via streaming and survives reopen', async () => {
  const dir = await tempDir();
  try {
    const storage = await FileStorage.create(dir);
    const k = await Kernel.recover(storage);
    const payload = Buffer.alloc(300_000);
    for (let i = 0; i < payload.length; i++) payload[i] = (i * 31) % 256;
    const digest = sha256Hex(payload);

    const up = await k.createUpload({ declaredDigest: digest, declaredSize: payload.length });
    await k.writeUploadPart(up.uploadId, payload.subarray(0, 100_000), { offset: 0 });
    await k.writeUploadPart(up.uploadId, payload.subarray(100_000, 200_000), { offset: 100_000 });
    await k.writeUploadPart(up.uploadId, payload.subarray(200_000), { offset: 200_000 });
    const done = await k.commitUpload(up.uploadId, {});
    assert.equal(done.upload.status, 'committed');

    const storage2 = await FileStorage.create(dir);
    const k2 = await Kernel.recover(storage2);
    const tail = await k2.readContentRange(digest, { start: 299_000, end: 299_999 });
    assert.equal(tail.bytes.length, 1000);
    assert.deepEqual(tail.bytes, payload.subarray(299_000));
  } finally {
    await rm(dir, { recursive: true, force: true });
  }
});

test('FileStorage: mismatched staged upload moves bytes to quarantine on disk', async () => {
  const dir = await tempDir();
  try {
    const storage = await FileStorage.create(dir);
    const k = await Kernel.recover(storage);
    const claimed = sha256Hex(Buffer.from('nope'));
    const bytes = Buffer.from('actual payload for quarantine');
    const up = await k.createUpload({ declaredDigest: claimed });
    await k.writeUploadPart(up.uploadId, bytes, { offset: 0 });
    const done = await k.commitUpload(up.uploadId, {});
    assert.equal(done.items[0].status, 'rejected');
    const evidenceId = done.upload.quarantineEvidenceId;
    assert.ok(evidenceId);
    const ev = await k.quarantineEvidence(evidenceId);
    assert.deepEqual(ev.data, bytes);
  } finally {
    await rm(dir, { recursive: true, force: true });
  }
});
