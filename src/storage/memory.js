import { randomUUID } from 'node:crypto';
import { sha256Hex, verifyDigest } from '../digest.js';

/**
 * In-memory storage used by tests. It implements the same contract as
 * FileStorage: an append-only event log, content-addressed blobs, a
 * quarantine area for rejected payloads and resumable chunked uploads.
 */
export class MemoryStorage {
  constructor() {
    this.events = [];
    this.blobs = new Map(); // digest -> Buffer
    this.quarantine = new Map(); // evidenceId -> record
    this.uploads = new Map(); // uploadId -> record
  }

  // --- event log ---------------------------------------------------------
  async appendEvent(evt) {
    this.events.push({ ...evt });
  }

  async listEvents() {
    return this.events.map((e) => ({ ...e }));
  }

  // --- blob store --------------------------------------------------------
  async saveBlob(digest, bytes) {
    if (!this.blobs.has(digest)) this.blobs.set(digest, Buffer.from(bytes));
  }

  async hasBlob(digest) {
    return this.blobs.has(digest);
  }

  async blobSize(digest) {
    const b = this.blobs.get(digest);
    return b ? b.length : null;
  }

  async readBlobRange(digest, { start = 0, end } = {}) {
    const b = this.blobs.get(digest);
    if (!b) return null;
    const size = b.length;
    const s = Math.max(0, start | 0);
    const e = end === undefined || end === null ? size : Math.min(size, (end | 0) + 1);
    return { bytes: b.subarray(s, e), start: s, end: e - 1, size };
  }

  async listQuarantine() {
    return [...this.quarantine.values()].map((q) => ({ ...q, data: undefined }));
  }

  // --- quarantine --------------------------------------------------------
  async saveQuarantine(evidenceId, entry, bytes) {
    const existing = this.quarantine.get(evidenceId);
    if (existing) return { ...existing, data: undefined };
    const rec = { evidenceId, receivedAt: entry.receivedAt, ...strip(entry) };
    this.quarantine.set(evidenceId, { ...rec, data: Buffer.from(bytes ?? []) });
    return rec;
  }

  async getQuarantine(evidenceId) {
    const rec = this.quarantine.get(evidenceId);
    return rec ? { ...rec, data: rec.data } : null;
  }

  // --- chunked uploads ---------------------------------------------------
  async createUpload({ declaredDigest, declaredSize = null, contentType = null } = {}) {
    const uploadId = 'up_' + randomUUID();
    const rec = {
      uploadId,
      declaredDigest,
      declaredSize: declaredSize == null ? null : Number(declaredSize),
      contentType,
      size: 0,
      received: [],
      hash: null,
      status: 'receiving',
      createdAt: new Date(0).toISOString(),
    };
    this.uploads.set(uploadId, rec);
    return { ...strip(rec), received: undefined };
  }

  async getUpload(uploadId) {
    const rec = this.uploads.get(uploadId);
    return rec ? { ...strip(rec), received: undefined } : null;
  }

  async writeUploadPart(uploadId, chunk, { offset } = {}) {
    const rec = this.uploads.get(uploadId);
    if (!rec) return null;
    if (rec.status !== 'receiving') return { ...strip(rec), received: undefined };
    const data = Buffer.from(chunk);
    if (offset !== undefined && offset !== null && offset !== rec.size) {
      const e = new RangeError('part offset does not match received bytes');
      e.code = 'bad-offset';
      throw e;
    }
    rec.received.push(data);
    rec.size += data.length;
    return { uploadId, size: rec.size, status: rec.status };
  }

  async finalizeUpload(uploadId, { claimedDigest } = {}) {
    const rec = this.uploads.get(uploadId);
    if (!rec) return null;
    if (rec.status === 'committed' || rec.status === 'rejected') {
      return { ...strip(rec), received: undefined };
    }
    const bytes = Buffer.concat(rec.received);
    const expected = claimedDigest || rec.declaredDigest;
    if (rec.declaredSize != null && rec.declaredSize !== bytes.length) {
      rec.status = 'rejected';
      const actualDigest = sha256Hex(bytes);
      const evidenceId = 'ev_' + actualDigest.slice('sha256:'.length, 14);
      await this.saveQuarantine(
        evidenceId,
        {
          receivedAt: new Date().toISOString(),
          context: 'staged-upload',
          uploadId,
          declaredDigest: expected ?? null,
          actualDigest,
          declaredSize: rec.declaredSize,
          size: bytes.length,
          contentType: rec.contentType,
          reason: 'size-mismatch',
        },
        bytes,
      );
      rec.quarantineEvidenceId = evidenceId;
      rec.actualDigest = actualDigest;
      return {
        ...strip(rec),
        received: undefined,
        status: 'rejected',
        quarantineEvidenceId: evidenceId,
        rejection: {
          reason: 'size-mismatch',
          expected: rec.declaredSize,
          actual: bytes.length,
        },
      };
    }
    const check = verifyDigest(expected, bytes);
    if (check.ok) {
      await this.saveBlob(check.digest, bytes);
      rec.status = 'committed';
      rec.digest = check.digest;
      rec.actualDigest = check.digest;
      return { ...strip(rec), received: undefined, digest: check.digest, actualDigest: check.digest };
    }
    rec.status = 'rejected';
    rec.actualDigest = check.actual;
    const evidenceId = 'ev_' + (check.actual ? check.actual.slice('sha256:'.length, 16) : randomUUID());
    await this.saveQuarantine(
      evidenceId,
      {
        receivedAt: new Date().toISOString(),
        context: 'staged-upload',
        uploadId,
        declaredDigest: expected,
        actualDigest: check.actual,
        declaredSize: rec.declaredSize,
        size: bytes.length,
        contentType: rec.contentType,
        reason: check.reason,
      },
      bytes,
    );
    rec.quarantineEvidenceId = evidenceId;
    return {
      ...strip(rec),
      received: undefined,
      status: 'rejected',
      quarantineEvidenceId: evidenceId,
      rejection: { reason: check.reason, expected: check.expected ?? expected, actual: check.actual },
    };
  }

  async abortUpload(uploadId) {
    return this.uploads.delete(uploadId);
  }

  // helper for the kernel inline-import path and tests
  putBlobForReplay(digest, bytes) {
    if (!this.blobs.has(digest)) this.blobs.set(digest, Buffer.from(bytes));
  }
}

function strip(rec) {
  const { received, hash, ...rest } = rec;
  return rest;
}
