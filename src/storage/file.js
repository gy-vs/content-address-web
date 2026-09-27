import {
  mkdir,
  open,
  readFile,
  writeFile,
  rename,
  stat,
  readdir,
  rm,
} from 'node:fs/promises';
import { mkdirSync } from 'node:fs';
import { createReadStream, createWriteStream } from 'node:fs';
import path from 'node:path';
import { pipeline } from 'node:stream/promises';
import { randomUUID } from 'node:crypto';
import { newHash } from '../digest.js';

/**
 * Durable storage layout under one directory:
 *   events.log              append-only JSONL event chain
 *   blobs/ab/cd/<64hex>     verified object contents (content addressed)
 *   quarantine/<evidenceId> rejected raw payload + .json metadata
 *   uploads/<id>/data + meta.json   resumable chunked uploads
 *
 * Reopening the directory replays nothing itself; the Kernel reads
 * listEvents() and rebuilds its projection.
 */
export class FileStorage {
  constructor(rootDir) {
    if (!rootDir) throw new TypeError('rootDir required');
    this.root = path.resolve(rootDir);
    this.eventPath = path.join(this.root, 'events.log');
    this.blobsDir = path.join(this.root, 'blobs');
    this.quarantineDir = path.join(this.root, 'quarantine');
    this.uploadsDir = path.join(this.root, 'uploads');
  }

  static async create(rootDir) {
    const fs = new FileStorage(rootDir);
    mkdirSync(fs.root, { recursive: true });
    await Promise.all([
      mkdir(fs.blobsDir, { recursive: true }),
      mkdir(fs.quarantineDir, { recursive: true }),
      mkdir(fs.uploadsDir, { recursive: true }),
    ]);
    return fs;
  }

  blobPath(digest) {
    const hex = digest.slice('sha256:'.length);
    return path.join(this.blobsDir, hex.slice(0, 2), hex.slice(2, 4), hex);
  }

  quarantinePath(evidenceId) {
    return {
      data: path.join(this.quarantineDir, evidenceId + '.bin'),
      meta: path.join(this.quarantineDir, evidenceId + '.json'),
    };
  }

  uploadDir(uploadId) {
    return path.join(this.uploadsDir, uploadId);
  }

  // --- event log ---------------------------------------------------------
  async appendEvent(evt) {
    // appendFile with a single string is atomic enough per-write on local FS;
    // the kernel serializes writers, so log order equals commit order.
    const fh = await open(this.eventPath, 'a');
    try {
      await fh.appendFile(JSON.stringify(evt) + '\n');
      await fh.sync?.();
    } finally {
      await fh.close();
    }
  }

  async listEvents() {
    let text;
    try {
      text = await readFile(this.eventPath, 'utf8');
    } catch (e) {
      if (e.code === 'ENOENT') return [];
      throw e;
    }
    if (!text.trim()) return [];
    return text
      .split('\n')
      .filter((l) => l.trim().length > 0)
      .map((l, i) => {
        try {
          return JSON.parse(l);
        } catch (e) {
          throw new Error(`corrupt event log at line ${i + 1}: ${e.message}`);
        }
      });
  }

  // --- blob store --------------------------------------------------------
  async saveBlob(digest, bytes) {
    const dest = this.blobPath(digest);
    try {
      await stat(dest);
      return; // content-addressed: already present
    } catch {
      // fall through
    }
    await mkdir(path.dirname(dest), { recursive: true });
    const tmp = dest + '.tmp-' + randomUUID();
    await writeFile(tmp, bytes);
    await rename(tmp, dest);
  }

  async hasBlob(digest) {
    try {
      await stat(this.blobPath(digest));
      return true;
    } catch {
      return false;
    }
  }

  async blobSize(digest) {
    try {
      return (await stat(this.blobPath(digest))).size;
    } catch {
      return null;
    }
  }

  async readBlobRange(digest, { start = 0, end } = {}) {
    const p = this.blobPath(digest);
    let s;
    try {
      s = await stat(p);
    } catch {
      return null;
    }
    const size = s.size;
    const startByte = Math.max(0, start | 0);
    const endByte = end === undefined || end === null ? size - 1 : Math.min(size - 1, end | 0);
    if (startByte >= size) return { bytes: Buffer.alloc(0), start: startByte, end: startByte - 1, size };
    const fh = await open(p, 'r');
    try {
      const len = endByte - startByte + 1;
      const buf = Buffer.allocUnsafe(len);
      const { bytesRead } = await fh.read(buf, 0, len, startByte);
      return { bytes: buf.subarray(0, bytesRead), start: startByte, end: startByte + bytesRead - 1, size };
    } finally {
      await fh.close();
    }
  }

  // --- quarantine --------------------------------------------------------
  async saveQuarantine(evidenceId, entry, bytes, { moveFrom = null } = {}) {
    const p = this.quarantinePath(evidenceId);
    let already = false;
    try {
      await stat(p.meta);
      already = true;
    } catch {
      // new
    }
    if (!already) {
      if (moveFrom) {
        await rename(moveFrom, p.data).catch(async () => {
          await pipeline(createReadStream(moveFrom), createWriteStream(p.data));
        });
      } else if (bytes !== undefined && bytes !== null) {
        await writeFile(p.data, bytes);
      }
      await writeFile(p.meta, JSON.stringify({ evidenceId, ...entry }, null, 2));
    }
    return { evidenceId, ...entry };
  }

  async getQuarantine(evidenceId) {
    const p = this.quarantinePath(evidenceId);
    let meta;
    try {
      meta = JSON.parse(await readFile(p.meta, 'utf8'));
    } catch {
      return null;
    }
    let data = null;
    try {
      data = await readFile(p.data);
    } catch {
      data = null;
    }
    return { ...meta, data };
  }

  async listQuarantine() {
    let names = [];
    try {
      names = await readdir(this.quarantineDir);
    } catch {
      return [];
    }
    const out = [];
    for (const n of names) {
      if (!n.endsWith('.json')) continue;
      try {
        out.push(JSON.parse(await readFile(path.join(this.quarantineDir, n), 'utf8')));
      } catch {
        // ignore unreadable metadata
      }
    }
    return out;
  }

  // --- chunked uploads ---------------------------------------------------
  async createUpload({ declaredDigest, declaredSize = null, contentType = null } = {}) {
    const uploadId = 'up_' + randomUUID();
    const dir = this.uploadDir(uploadId);
    await mkdir(dir, { recursive: true });
    const meta = {
      uploadId,
      declaredDigest,
      declaredSize: declaredSize == null ? null : Number(declaredSize),
      contentType,
      size: 0,
      status: 'receiving',
      createdAt: new Date().toISOString(),
      hasher: true,
    };
    await writeFile(path.join(dir, 'meta.json'), JSON.stringify(meta));
    // create/truncate data file
    await writeFile(path.join(dir, 'data'), Buffer.alloc(0));
    return { uploadId, declaredDigest, declaredSize: meta.declaredSize, contentType, size: 0, status: 'receiving' };
  }

  async getUpload(uploadId) {
    try {
      const meta = JSON.parse(await readFile(path.join(this.uploadDir(uploadId), 'meta.json'), 'utf8'));
      const { hasher, ...rest } = meta;
      return rest;
    } catch {
      return null;
    }
  }

  async writeUploadPart(uploadId, chunk, { offset } = {}) {
    const dir = this.uploadDir(uploadId);
    const metaPath = path.join(dir, 'meta.json');
    let meta;
    try {
      meta = JSON.parse(await readFile(metaPath, 'utf8'));
    } catch {
      return null;
    }
    if (meta.status !== 'receiving') {
      const { hasher, ...rest } = meta;
      return rest;
    }
    if (offset !== undefined && offset !== null && offset !== meta.size) {
      const e = new RangeError('part offset does not match received bytes');
      e.code = 'bad-offset';
      throw e;
    }
    const dataPath = path.join(dir, 'data');
    const fh = await open(dataPath, 'a');
    try {
      await fh.appendFile(Buffer.from(chunk));
    } finally {
      await fh.close();
    }
    meta.size += Buffer.byteLength(chunk);
    await writeFile(metaPath, JSON.stringify(meta));
    return { uploadId, size: meta.size, status: meta.status };
  }

  async finalizeUpload(uploadId, { claimedDigest } = {}) {
    const dir = this.uploadDir(uploadId);
    const metaPath = path.join(dir, 'meta.json');
    let meta;
    try {
      meta = JSON.parse(await readFile(metaPath, 'utf8'));
    } catch {
      return null;
    }
    if (meta.status === 'committed' || meta.status === 'rejected') {
      const { hasher, ...rest } = meta;
      return rest;
    }
    const dataPath = path.join(dir, 'data');
    let actual;
    try {
      actual = await this._hashFile(dataPath);
    } catch (e) {
      if (e.code !== 'ENOENT') throw e;
      actual = null;
    }
    if (meta.declaredSize != null && meta.declaredSize !== meta.size) {
      meta.status = 'rejected';
      meta.rejection = { reason: 'size-mismatch', expected: meta.declaredSize, actual: meta.size };
      const evidenceId = 'ev_' + (actual ? actual.slice('sha256:'.length, 16) : randomUUID());
      await this.saveQuarantine(
        evidenceId,
        {
          receivedAt: new Date().toISOString(),
          context: 'staged-upload',
          uploadId,
          declaredDigest: meta.declaredDigest,
          actualDigest: actual,
          declaredSize: meta.declaredSize,
          size: meta.size,
          contentType: meta.contentType,
          reason: 'size-mismatch',
        },
        null,
        { moveFrom: dataPath },
      );
      meta.quarantineEvidenceId = evidenceId;
      await writeFile(metaPath, JSON.stringify(meta));
      const { hasher, ...rest } = meta;
      return rest;
    }
    const expected = claimedDigest
      ? claimedDigest.toLowerCase()
      : meta.declaredDigest
        ? String(meta.declaredDigest).toLowerCase()
        : null;
    const ok = expected ? actual === expected : true;
    if (ok) {
      const digest = expected ?? actual;
      await mkdir(path.dirname(this.blobPath(digest)), { recursive: true });
      await rename(dataPath, this.blobPath(digest)).catch(async () => {
        await pipeline(createReadStream(dataPath), createWriteStream(this.blobPath(digest)));
      });
      meta.status = 'committed';
      meta.digest = digest;
      meta.actualDigest = actual;
      await writeFile(metaPath, JSON.stringify(meta));
      const { hasher, ...rest } = meta;
      return rest;
    }
    meta.status = 'rejected';
    meta.actualDigest = actual;
    meta.rejection = { reason: 'digest-mismatch', expected, actual };
    const evidenceId = 'ev_' + (actual ? actual.slice('sha256:'.length, 16) : randomUUID());
    const entry = {
      receivedAt: new Date().toISOString(),
      context: 'staged-upload',
      uploadId,
      declaredDigest: expected,
      actualDigest: actual,
      declaredSize: meta.declaredSize,
      size: meta.size,
      contentType: meta.contentType,
      reason: 'digest-mismatch',
    };
    await this.saveQuarantine(evidenceId, entry, null, { moveFrom: dataPath });
    meta.quarantineEvidenceId = evidenceId;
    await writeFile(metaPath, JSON.stringify(meta));
    const { hasher, ...rest } = meta;
    return rest;
  }

  async _hashFile(p) {
    // streaming hash so large objects are never fully buffered in memory
    const h = newHash();
    await new Promise((resolve, reject) => {
      const stream = createReadStream(p);
      stream.on('data', (chunk) => h.update(chunk));
      stream.on('end', resolve);
      stream.on('error', reject);
    });
    return 'sha256:' + h.digest('hex');
  }

  async abortUpload(uploadId) {
    await rm(this.uploadDir(uploadId), { recursive: true, force: true });
    return true;
  }
}
