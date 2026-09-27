// 摘要（内容寻址）原语：所有对象以 sha256 摘要作为身份标识。
// 摘要的规范形态为 "sha256:" + 64 位小写十六进制。
import { createHash, randomUUID } from 'node:crypto';

const HEX64 = /^[0-9a-f]{64}$/;
export const DIGEST_RE = /^sha256:[0-9a-f]{64}$/;

/** 计算一块 Buffer 的规范摘要 */
export function sha256(buffer) {
  return createHash('sha256').update(buffer).digest('hex');
}

/** 流式计算摘要，可用于大对象，调用方不必把整体载入内存 */
export async function sha256Stream(readable) {
  const hash = createHash('sha256');
  for await (const chunk of readable) hash.update(chunk);
  return hash.digest('hex');
}

/** 由原始 hex 摘要生成规范 digest 字符串 */
export function digestFromHex(hex) {
  if (!HEX64.test(hex)) throw new TypeError(`非法的 sha256 hex: ${hex}`);
  return `sha256:${hex}`;
}

/** 判断是否为合法规范摘要；返回归一化摘要或 null */
export function normalizeDigest(value) {
  if (typeof value !== 'string') return null;
  const lower = value.toLowerCase();
  return DIGEST_RE.test(lower) ? lower : null;
}

/** 断言为合法摘要，非法时抛出 invalid-digest（属于调用方输入错误） */
export function assertDigest(value, field = 'digest') {
  const d = normalizeDigest(value);
  if (!d) {
    const err = new Error(`非法摘要 (${field}): ${String(value)}`);
    err.code = 'invalid-digest';
    throw err;
  }
  return d;
}

export function newId() {
  return randomUUID();
}
