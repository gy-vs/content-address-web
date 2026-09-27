// 引用边推导规则。
// 有效对象可以是两种内容形态：
//  1. blob（默认）：原始字节，不产生任何出边。
//  2. manifest：合法 JSON，引用来自两处并集——
//     a) 显式声明的 refs：[{ digest, label }]
//     b) JSON 中任意字符串字段，只要取值是合法规范摘要（自动引用）
// 内容无法按 manifest 解析（parseJson 为 true 却非法 JSON）属于 malformed-content，
// 该对象被拒收、不产生任何引用，因此“错误对象不会继续传播”。
import { normalizeDigest } from './digest.js';

/**
 * @param {Buffer} bytes 对象原始字节
 * @param {object} opts
 * @param {boolean} [opts.parseJson] 调用方声明该对象按 manifest 解析
 * @param {Array<{digest?:string,label?:string}>} [opts.declaredRefs] 显式引用声明
 * @returns {{ refs: Array<{target:string,label:string,source:string}>, error?: string }}
 */
export function deriveRefs(bytes, { parseJson = false, declaredRefs = [] } = {}) {
  const refs = new Map(); // target -> edge（去重，显式声明优先保留标签）

  const addDeclared = (rawLabel, idx) => {
    if (rawLabel === null || rawLabel === undefined || typeof rawLabel === 'string') return rawLabel ?? null;
    throwRefError(`refs[${idx}].label 必须是字符串或 null`);
  };

  declaredRefs.forEach((r, idx) => {
    if (!r || typeof r !== 'object') throwRefError(`refs[${idx}] 必须是对象`);
    const target = normalizeDigest(r.digest);
    if (!target) throwRefError(`refs[${idx}].digest 非法: ${String(r.digest)}`);
    const label = addDeclared(r.label, idx);
    if (!refs.has(target)) {
      refs.set(target, { target, label: label ?? `refs[${idx}]`, source: 'declared' });
    } else if (refs.get(target).label.startsWith('$.')) {
      refs.set(target, { target, label: label ?? `refs[${idx}]`, source: 'declared' });
    }
  });

  if (parseJson) {
    let doc;
    try {
      doc = JSON.parse(bytes.toString('utf8'));
    } catch {
      return { refs: [], error: 'malformed-content' };
    }
    // 递归扫描所有字符串叶子；路径即标签，便于“按路径加载对象详情”
    walk(doc, '$', (value, path) => {
      const target = normalizeDigest(value);
      if (target && !refs.has(target)) {
        refs.set(target, { target, label: path, source: 'json' });
      }
    });
  }

  return { refs: [...refs.values()].sort((a, b) => a.target.localeCompare(b.target) || a.label.localeCompare(b.label)) };
}

function walk(value, path, visit) {
  if (typeof value === 'string') {
    visit(value, path);
    return;
  }
  if (Array.isArray(value)) {
    value.forEach((v, i) => walk(v, `${path}[${i}]`, visit));
    return;
  }
  if (value && typeof value === 'object') {
    for (const k of Object.keys(value)) walk(value[k], `${path}.${k}`, visit);
  }
}

function throwRefError(msg) {
  const err = new Error(msg);
  err.code = 'invalid-reference';
  throw err;
}
