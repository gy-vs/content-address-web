// 块存储：只负责字节落盘，不理解引用关系。
// - 已提交对象：root/blobs/sha256/<ab>/<64hex>，内容寻址、不可变
// - 导入会话：root/tmp/import-<id>/chunk-<序号>（分块到达，全部到齐后提交）
// - 拒收证据：root/quarantine/<证据id>.bin（错误对象的原始字节保留以便定位失败输入）
// 事件日志记录的是路径/大小/摘要等指针，从不内联大内容。
import { createReadStream, createWriteStream } from 'node:fs';
import { mkdir, rename, rm, readdir, stat } from 'node:fs/promises';
import { existsSync } from 'node:fs';
import path from 'node:path';
import { pipeline } from 'node:stream/promises';
import { Writable, Transform } from 'node:stream';
import { createHash } from 'node:crypto';

export class BlobStore {
  constructor(root) {
    this.root = root;
    this.blobsDir = path.join(root, 'blobs', 'sha256');
    this.tmpDir = path.join(root, 'tmp');
    this.quarantineDir = path.join(root, 'quarantine');
  }

  static async open(root) {
    const store = new BlobStore(root);
    await mkdir(store.blobsDir, { recursive: true });
    await mkdir(store.tmpDir, { recursive: true });
    await mkdir(store.quarantineDir, { recursive: true });
    return store;
  }

  // ---- 路径规则 -------------------------------------------------------

  /** 已提交对象的磁盘路径（调用前可先 has() 判断） */
  blobPath(digest) {
    const hex = digest.slice('sha256:'.length);
    return path.join(this.blobsDir, hex.slice(0, 2), hex);
  }

  sessionDir(importId) {
    return path.join(this.tmpDir, `import-${importId}`);
  }

  chunkPath(importId, index) {
    return path.join(this.sessionDir(importId), `chunk-${index}`);
  }

  quarantinePath(evidenceId) {
    return path.join(this.quarantineDir, `${evidenceId}.bin`);
  }

  // ---- 导入会话 -------------------------------------------------------

  /**
   * 写入一个分块。iterable 可以是 Buffer、AsyncIterable<Buffer> 或 Web ReadableStream。
   * 上游只被消费一次：经 Transform 分流，一边算摘要一边落盘。
   * 返回块大小与块摘要（作为传输完整性证据写入事件链）。
   */
  async writeChunk(importId, index, iterable) {
    const dir = this.sessionDir(importId);
    await mkdir(dir, { recursive: true });
    const target = this.chunkPath(importId, index);
    const hash = createHash('sha256');
    let bytes = 0;
    const file = createWriteStream(target);
    await pipeline(
      toAsyncIter(iterable),
      new Transform({
        transform(chunk, _enc, cb) {
          bytes += chunk.length;
          hash.update(chunk);
          file.write(chunk, (err) => cb(err, chunk));
        },
        flush(cb) {
          file.end(() => cb());
        },
      }),
      new Writable({ objectMode: true, write(_c, _e, cb) { cb(); } }),
    );
    return { path: target, bytes, chunkDigest: `sha256:${hash.digest('hex')}` };
  }

  /**
   * 分块全部到齐后提交：流式拼接，一遍完成“落盘 commit + 整体摘要”。
   * 返回 { ok, actualDigest, size, commit }。
   * - 摘要匹配：原子改名进入内容寻址路径（已存在则幂等清理 commit）。
   * - 摘要不匹配：commit 保留在会话目录，交由调用方隔离取证。
   */
  async commitSession(importId, chunkCount, declaredDigest) {
    const dir = this.sessionDir(importId);
    const commit = path.join(dir, 'commit.bin');
    const hash = createHash('sha256');
    let size = 0;
    await pipeline(
      async function* () {
        for (let i = 0; i < chunkCount; i++) {
          const p = path.join(dir, `chunk-${i}`);
          if (!existsSync(p)) {
            const err = new Error(`缺少分块 #${i}`);
            err.code = 'chunk-missing';
            throw err;
          }
          const s = await stat(p);
          size += s.size;
          yield* createReadStream(p);
        }
      },
      new Transform({
        transform(chunk, _enc, cb) {
          hash.update(chunk);
          cb(null, chunk);
        },
      }),
      createWriteStream(commit),
    );
    const actualDigest = `sha256:${hash.digest('hex')}`;
    if (actualDigest !== declaredDigest) {
      return { ok: false, actualDigest, size, commit };
    }
    const finalPath = this.blobPath(actualDigest);
    await mkdir(path.dirname(finalPath), { recursive: true });
    if (existsSync(finalPath)) {
      await rm(commit, { force: true }); // 同内容对象已存在：幂等
    } else {
      await rename(commit, finalPath);
    }
    return { ok: true, actualDigest, size, path: finalPath };
  }

  /** 结束一个导入会话（成功或放弃后清理分块） */
  async removeSession(importId) {
    await rm(this.sessionDir(importId), { recursive: true, force: true });
  }

  /** 恢复时盘点未完成会话：{ importId, chunks: number[] }[] */
  async listPendingSessions() {
    let entries;
    try {
      entries = await readdir(this.tmpDir, { withFileTypes: true });
    } catch {
      return [];
    }
    const out = [];
    for (const e of entries) {
      if (!e.isDirectory() || !e.name.startsWith('import-')) continue;
      const importId = e.name.slice('import-'.length);
      const files = await readdir(path.join(this.tmpDir, e.name));
      const chunks = files
        .map((f) => /^chunk-(\d+)$/.exec(f))
        .filter(Boolean)
        .map((m) => Number(m[1]))
        .sort((a, b) => a - b);
      out.push({ importId, chunks });
    }
    return out;
  }

  // ---- 已提交对象 -----------------------------------------------------

  has(digest) {
    return existsSync(this.blobPath(digest));
  }

  /** 打开只读流；支持 HTTP Range 风格的半开区间 [start,end)，end 省略表示到结尾 */
  openRead(digest, { start = 0, end } = {}) {
    const p = this.blobPath(digest);
    const opts = { start };
    if (end !== undefined) opts.end = end - 1;
    return createReadStream(p, opts);
  }

  /** 小对象/测试便捷读取；大对象请用 openRead 流式处理 */
  async readAll(digest) {
    const chunks = [];
    await pipeline(
      this.openRead(digest),
      new Writable({
        write(chunk, _enc, cb) {
          chunks.push(Buffer.from(chunk));
          cb();
        },
      }),
    );
    return Buffer.concat(chunks);
  }

  async deleteBlob(digest) {
    await rm(this.blobPath(digest), { force: true });
  }

  // ---- 拒收证据 -------------------------------------------------------

  /** 把已拼好的错误对象字节（commit.bin）存入隔离区，返回证据指针 */
  async quarantineCommit(commitPath, evidenceId) {
    const dest = this.quarantinePath(evidenceId);
    await mkdir(path.dirname(dest), { recursive: true });
    await pipeline(createReadStream(commitPath), createWriteStream(dest));
    const s = await stat(dest);
    return { path: dest, bytes: s.size, evidenceId };
  }

  /** 把已提交到 blobs/ 的字节流转存隔离区（malformed-content 场景） */
  async quarantineBlob(digest, evidenceId) {
    const dest = this.quarantinePath(evidenceId);
    await mkdir(path.dirname(dest), { recursive: true });
    await pipeline(this.openRead(digest), createWriteStream(dest));
    const s = await stat(dest);
    return { path: dest, bytes: s.size, evidenceId };
  }
}

/** 把 Buffer / AsyncIterable / Web ReadableStream 统一成 Node 异步迭代器 */
async function* toAsyncIter(input) {
  if (Buffer.isBuffer(input) || input instanceof Uint8Array) {
    yield Buffer.from(input);
    return;
  }
  if (input && typeof input[Symbol.asyncIterator] === 'function') {
    for await (const c of input) yield Buffer.from(c);
    return;
  }
  if (input && typeof input.getReader === 'function') {
    const reader = input.getReader();
    while (true) {
      const { done, value } = await reader.read();
      if (done) return;
      if (value) yield Buffer.from(value);
    }
  }
  throw new TypeError('不支持的字节输入类型');
}
