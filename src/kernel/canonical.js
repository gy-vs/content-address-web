// 规范 JSON：用于把结构化对象映射成稳定字节，供证据快照与链哈希使用。
// 规则：对象键按字典序递归排序；不允许 undefined/函数；输出 UTF-8。
const OBJECT_PROTO = Object.prototype;

function sortValue(value) {
  if (value === null || typeof value !== 'object') {
    if (value === undefined || typeof value === 'function') {
      throw new TypeError('canonicalJSON 不支持 undefined/function');
    }
    return value;
  }
  if (Array.isArray(value)) return value.map(sortValue);
  const proto = Object.getPrototypeOf(value);
  if (proto !== OBJECT_PROTO && proto !== null) {
    throw new TypeError('canonicalJSON 仅支持普通对象/数组/标量');
  }
  const out = {};
  for (const key of Object.keys(value).sort()) out[key] = sortValue(value[key]);
  return out;
}

export function canonicalJSON(value) {
  return JSON.stringify(sortValue(value));
}

export function canonicalBytes(value) {
  return Buffer.from(canonicalJSON(value), 'utf8');
}
