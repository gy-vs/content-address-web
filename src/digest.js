import { createHash } from 'node:crypto';

export const DIGEST_RE = /^sha256:([0-9a-f]{64})$/;

export function sha256Hex(data) {
  return 'sha256:' + createHash('sha256').update(data).digest('hex');
}

export function isDigest(s) {
  return typeof s === 'string' && DIGEST_RE.test(s);
}

/**
 * Verify content against a declared content address.
 * Returns {ok:true, digest} or {ok:false, reason, expected, actual}.
 */
export function verifyDigest(declared, data) {
  if (!isDigest(declared)) {
    return { ok: false, reason: 'unsupported-digest-algorithm', declared };
  }
  const actual = sha256Hex(data);
  if (actual !== declared.toLowerCase()) {
    return { ok: false, reason: 'digest-mismatch', expected: declared.toLowerCase(), actual };
  }
  return { ok: true, digest: actual };
}

/**
 * Normalize content to a Buffer.
 *  - Buffer / Uint8Array: passed through
 *  - string: UTF-8 bytes
 *  - { encoding: 'base64', data: string }: decoded (JSON transport path)
 */
export function asBytes(value) {
  if (Buffer.isBuffer(value)) return value;
  if (value instanceof Uint8Array) return Buffer.from(value);
  if (value && typeof value === 'object' && typeof value.data === 'string') {
    const enc = (value.encoding ?? 'utf8').toLowerCase();
    return Buffer.from(value.data, enc === 'base64' ? 'base64' : 'utf8');
  }
  if (typeof value === 'string') return Buffer.from(value, 'utf8');
  throw new TypeError('content must be a Buffer, Uint8Array, string or {encoding,data}');
}

export function newHash() {
  return createHash('sha256');
}
