// 哈希链事件日志：内核的唯一事实来源。
// 磁盘形态 root/log/events.jsonl，每行一条规范 JSON：
//   { seq, prevHash, hash, ts, id, type, payload }
// 链规则：
//   seq=0 为创世记录 prevHash=null；
//   其余记录 hash = sha256( canonicalJSON({seq,prevHash,ts,id,type,payload}) )。
// 事件载荷只允许存“指针”（摘要、路径、大小、分块摘要），对象内容永不进日志。
import { createReadStream, createWriteStream } from 'node:fs';
import { mkdir, rename, open } from 'node:fs/promises';
import path from 'node:path';
import { createInterface } from 'node:readline/promises';
import { pipeline } from 'node:stream/promises';
import { sha256, newId } from './digest.js';
import { canonicalJSON } from './canonical.js';

export const GENESIS_SEQ = 0;

export function recordHash({ seq, prevHash, ts, id, type, payload }) {
  return `sha256:${sha256(Buffer.from(
    canonicalJSON({ seq, prevHash, ts, id, type, payload }),
    'utf8',
  ))}`;
}

export class Log {
  constructor(file) {
    this.file = file;
    this.records = [];
    this.dir = path.dirname(file);
  }

  /** 打开日志：不存在则写入创世记录；存在则重放进内存 */
  static async open(file, clock = () => Date.now()) {
    const log = new Log(file);
    log.clock = clock;
    await mkdir(log.dir, { recursive: true });
    try {
      await log.replay();
    } catch (err) {
      if (err.code !== 'ENOENT') throw err;
    }
    if (log.records.length === 0) {
      await log.append('log.genesis', { schema: 1, note: '内容寻址快照审阅内核创世记录' });
    }
    return log;
  }

  get tip() {
    return this.records[this.records.length - 1] ?? null;
  }

  get headSeq() {
    return this.tip?.seq ?? -1;
  }

  get headHash() {
    return this.tip?.hash ?? null;
  }

  /**
   * 追加一条事件。串行写入 + fsync：
   * 返回 { record, line }，record 同时保留在内存链上。
   */
  async append(type, payload = {}, { id = newId(), ts } = {}) {
    if (typeof type !== 'string' || !type) throw new Error('事件类型非法');
    const seq = this.headSeq + 1;
    const prevHash = this.headHash;
    const record = {
      seq,
      prevHash,
      ts: ts ?? this.clock(),
      id,
      type,
      payload: structuredClone(payload),
    };
    record.hash = recordHash(record);
    const line = canonicalJSON(record) + '\n';
    const fh = await open(this.file, 'a');
    try {
      await fh.appendFile(line);
      await fh.sync();
    } finally {
      await fh.close();
    }
    this.records.push(record);
    return { record, line };
  }

  /** 从磁盘逐行重放；可选 onRecord 回调。验证链完整性，遇断点抛出 chain-broken。 */
  async replay(onRecord) {
    this.records = [];
    let prevHash = null;
    let lineNo = 0;
    const rl = createInterface({ input: createReadStream(this.file), crlfDelay: Infinity });
    for await (const line of rl) {
      lineNo += 1;
      if (!line.trim()) continue;
      let rec;
      try {
        rec = JSON.parse(line);
      } catch {
        throw broken(`第 ${lineNo} 行不是合法 JSON`, lineNo);
      }
      const { hash, ...body } = rec;
      const expect = recordHash(body);
      if (rec.seq !== this.records.length) {
        throw broken(`序号断裂：期望 ${this.records.length}，实际 ${rec.seq}`, lineNo, rec, null);
      }
      if (rec.prevHash !== prevHash) {
        throw broken('prevHash 不衔接', lineNo, rec, prevHash);
      }
      if (hash !== expect) {
        throw broken('记录哈希不匹配（日志可能被篡改）', lineNo, rec, expect);
      }
      this.records.push(rec);
      prevHash = hash;
      if (onRecord) onRecord(rec);
    }
    return this.records.slice();
  }

  /**
   * 完整性校验，不修改内存状态。
   * 返回 { ok, headSeq, headHash } 或 { ok:false, error:{line,reason,record,expected} }
   */
  async verify() {
    let prevHash = null;
    let lineNo = 0;
    let last = null;
    try {
      const rl = createInterface({ input: createReadStream(this.file), crlfDelay: Infinity });
      for await (const line of rl) {
        lineNo += 1;
        if (!line.trim()) continue;
        const rec = JSON.parse(line);
        const { hash, ...body } = rec;
        const expect = recordHash(body);
        if (rec.seq !== (last ? last.seq + 1 : 0)) throw broken('序号断裂', lineNo, rec);
        if (rec.prevHash !== prevHash) throw broken('prevHash 不衔接', lineNo, rec, prevHash);
        if (hash !== expect) throw broken('记录哈希不匹配', lineNo, rec, expect);
        prevHash = hash;
        last = rec;
      }
    } catch (err) {
      if (err.code === 'chain-broken') {
        return { ok: false, headSeq: lineNo - 1, error: err.detail };
      }
      throw err;
    }
    return { ok: true, headSeq: last?.seq ?? 0, headHash: last?.hash ?? null };
  }

  /** 当前内存链快照（按序），用于查询层只读访问 */
  slice() {
    return this.records.slice();
  }

  at(seq) {
    return this.records.find((r) => r.seq === seq) ?? null;
  }

  /** 把日志压缩重写到新文件（原子改名），仅用于修复/迁移；当前内核不主动调用 */
  async rewrite(records) {
    const tmp = this.file + '.rewrite';
    await pipeline(
      (function* () {
        for (const r of records) yield Buffer.from(canonicalJSON(r) + '\n');
      })(),
      createWriteStream(tmp),
    );
    await rename(tmp, this.file);
    this.records = records.slice();
  }
}

function broken(reason, line, record = null, expected = null) {
  const err = new Error(reason);
  err.code = 'chain-broken';
  err.detail = { line, reason, record, expected };
  return err;
}
