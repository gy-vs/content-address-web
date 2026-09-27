// 测试夹具：每个用例独立临时目录；所有测试都走公开包入口（包自引用）。
import { mkdtemp, rm } from 'node:fs/promises';
import os from 'node:os';
import path from 'node:path';
import { Kernel, sha256, digestFromHex } from 'content-address-web';

export async function freshKernel(clock) {
  const dir = await mkdtemp(path.join(os.tmpdir(), 'caw-'));
  const kernel = await Kernel.create(dir, clock ? { clock } : undefined);
  return { kernel, dir, cleanup: () => rm(dir, { recursive: true, force: true }) };
}

/** 计算字节摘要（sha256:<hex>） */
export function dg(buffer) {
  return digestFromHex(sha256(Buffer.from(buffer)));
}

/** 导入一个 manifest（JSON 内容），返回摘要 */
export async function putJson(kernel, value, { refs, declaredDigest } = {}) {
  const bytes = Buffer.from(JSON.stringify(value));
  const r = await kernel.importObject({ data: bytes, parseJson: true, refs, declaredDigest });
  if (r.status !== 'accepted') throw new Error(`导入失败: ${JSON.stringify(r)}`);
  return r.digest;
}

/** 导入一个 blob（不解析 JSON） */
export async function putBlob(kernel, bytes, { refs, declaredDigest } = {}) {
  const r = await kernel.importObject({ data: Buffer.from(bytes), refs, declaredDigest });
  if (r.status !== 'accepted') throw new Error(`导入失败: ${JSON.stringify(r)}`);
  return r.digest;
}

/** 建两层结构：root manifest 引用 children */
export async function putManifestRef(kernel, children, extra = {}) {
  const refs = children.map((c, i) => ({ digest: c, label: `child[${i}]` }));
  return putJson(kernel, { kind: 'manifest', children, ...extra }, { refs });
}
