// HTTP 适配层（公开模块入口 content-address-web/web）。
// 边界纪律：
//  - /summary、列表、图查询：JSON、分页、绝不携带对象字节；
//  - /content：对象字节的唯一出口，支持 Range 局部读取；
//  - 分块上传：/chunks/:i 接收字节流；错误一律结构化信封并保留证据。
import { createServer as httpCreateServer } from 'node:http';
import { Kernel, KernelError } from '../index.js';

const JSON_CT = 'application/json; charset=utf-8';

export async function createServer({ dataDir, kernel } = {}) {
  const k = kernel ?? await Kernel.create(dataDir);
  const server = httpCreateServer((req, res) => handle(k, req, res).catch((err) => sendError(res, err)));
  server.kernel = k;
  return server;
}

async function handle(k, req, res) {
  const url = new URL(req.url, 'http://localhost');
  const p = url.pathname;
  const method = req.method;
  const q = (name, def) => url.searchParams.get(name) ?? def;
  const pageOpts = () => ({
    cursor: url.searchParams.get('cursor'),
    limit: url.searchParams.get('limit') ? Number(url.searchParams.get('limit')) : undefined,
  });

  // ---- 健康与数据链 ---------------------------------------------------
  if (p === '/v1/health' && method === 'GET') {
    return json(res, 200, { ok: true, ...k.chainInfo(), state: k.stateInfo() });
  }
  if (p === '/v1/chain' && method === 'GET') {
    const v = await k.verifyChain();
    return json(res, v.ok ? 200 : 409, v);
  }

  // ---- 导入 -----------------------------------------------------------
  if (p === '/v1/imports' && method === 'POST') {
    const body = await readJson(req);
    return json(res, 201, await k.startImport(body));
  }
  if (p === '/v1/imports' && method === 'GET') {
    return json(res, 200, { pending: k.listPendingImports(), rejections: k.listRejections() });
  }
  if (p === '/v1/objects' && method === 'POST') {
    // 小对象便捷导入：声明摘要/类型，body 即原始字节
    const body = await readBytes(req);
    const result = await k.importObject({
      data: body,
      declaredDigest: req.headers['x-declared-digest'],
      parseJson: req.headers['x-parse-json'] === '1',
      clientRef: req.headers['x-client-ref'] ?? null,
      refs: req.headers['x-refs'] ? JSON.parse(req.headers['x-refs']) : undefined,
    });
    return json(res, result.status === 'rejected' ? 422 : 201, result);
  }
  let m;
  if ((m = /^\/v1\/imports\/([^/]+)\/chunks\/(\d+)$/.exec(p)) && method === 'PUT') {
    const data = await readBytes(req);
    const result = await k.uploadChunk({
      importId: decodeURIComponent(m[1]),
      index: Number(m[2]),
      data,
      chunkDigest: req.headers['x-chunk-digest'],
    });
    return json(res, result.status === 'duplicate' ? 200 : 201, result);
  }
  if ((m = /^\/v1\/imports\/([^/]+)\/end$/.exec(p)) && method === 'POST') {
    const body = await readJson(req);
    const result = await k.endImport({ importId: decodeURIComponent(m[1]), chunkCount: body?.chunkCount });
    return json(res, result.status === 'rejected' ? 422 : 200, result);
  }
  if ((m = /^\/v1\/imports\/([^/]+)\/abandon$/.exec(p)) && method === 'POST') {
    const body = await readJson(req);
    return json(res, 200, await k.abandonImport({ importId: decodeURIComponent(m[1]), reason: body?.reason }));
  }

  // ---- 对象：列表 / 状态 / 摘要 / 内容（字节唯一出口） / 保留 / 路径 ----
  if (p === '/v1/objects' && method === 'GET') {
    return json(res, 200, k.listObjects(pageOpts()));
  }
  if ((m = /^\/v1\/objects\/([^/]+)$/.exec(p)) && method === 'GET') {
    return json(res, 200, k.objectStatus(decodeURIComponent(m[1])));
  }
  if ((m = /^\/v1\/objects\/([^/]+)\/summary$/.exec(p)) && method === 'GET') {
    return json(res, 200, k.getSummary(decodeURIComponent(m[1])));
  }
  if ((m = /^\/v1\/objects\/([^/]+)\/content$/.exec(p)) && method === 'GET') {
    const digest = decodeURIComponent(m[1]);
    const range = parseRange(req.headers.range);
    const { stream, meta } = k.openContent(digest, range ?? {});
    res.setHeader('Content-Type', meta.contentType);
    res.setHeader('X-Content-Digest', meta.digest);
    res.setHeader('X-Content-Size', String(meta.size));
    res.setHeader('Accept-Ranges', 'bytes');
    if (range) {
      const total = meta.size;
      const end = meta.end - 1;
      res.writeHead(206, { 'Content-Range': `bytes ${meta.start}-${end}/${total}`, 'Content-Length': end - meta.start + 1 });
    } else {
      res.writeHead(200, { 'Content-Length': meta.end - meta.start });
    }
    stream.on('error', (err) => sendError(res, err));
    stream.pipe(res);
    return;
  }
  if ((m = /^\/v1\/objects\/([^/]+)\/retention$/.exec(p)) && method === 'GET') {
    return json(res, 200, k.retained(decodeURIComponent(m[1]), { snapshot: q('snapshot') }));
  }
  if (p === '/v1/graph/expand' && method === 'GET') {
    return json(res, 200, k.expand(rootSelector(url, pageOpts())));
  }
  if (p === '/v1/graph/path' && method === 'GET') {
    const root = q('root');
    if (!root) return json(res, 400, { error: { code: 'missing-param', message: '需要 root 摘要' } });
    const labels = url.searchParams.getAll('label');
    return json(res, 200, k.loadPath(root, labels, rootSelector(url, {})));
  }

  // ---- 快照与活动根 ---------------------------------------------------
  if (p === '/v1/snapshots' && method === 'GET') {
    return json(res, 200, { snapshots: k.listSnapshots(), activeRoots: k.getActiveRoots() });
  }
  if (p === '/v1/snapshots' && method === 'POST') {
    return json(res, 201, await k.createSnapshot(await readJson(req)));
  }
  if ((m = /^\/v1\/snapshots\/([^/]+)\/diff\/([^/]+)$/.exec(p)) && method === 'GET') {
    const result = k.diff(decodeURIComponent(m[1]), decodeURIComponent(m[2]), {
      aVersion: num(q('aVersion')),
      bVersion: num(q('bVersion')),
      limit: num(q('limit')),
      cursorShared: q('cursorShared'),
      cursorOnlyA: q('cursorOnlyA'),
      cursorOnlyB: q('cursorOnlyB'),
    });
    return json(res, 200, result);
  }
  if (p === '/v1/active-roots' && method === 'GET') {
    return json(res, 200, k.getActiveRoots(num(q('version'))));
  }
  if (p === '/v1/active-roots' && method === 'PUT') {
    return json(res, 200, await k.setActiveRoots(await readJson(req)));
  }

  // ---- 回收审阅 -------------------------------------------------------
  if (p === '/v1/reviews' && method === 'GET') {
    return json(res, 200, { reviews: k.listReviews(), decisions: k.listDecisions(), conflicts: k.listConflicts() });
  }
  if (p === '/v1/reviews' && method === 'POST') {
    return json(res, 201, await k.openReview(await readJson(req)));
  }
  if ((m = /^\/v1\/reviews\/([^/]+)$/.exec(p)) && method === 'GET') {
    return json(res, 200, k.getReview(decodeURIComponent(m[1])));
  }
  if ((m = /^\/v1\/reviews\/([^/]+)\/decisions$/.exec(p)) && method === 'POST') {
    const body = await readJson(req);
    const result = await k.confirmReview({ reviewId: decodeURIComponent(m[1]), ...body });
    return json(res, result.status === 'conflict' ? 409 : 200, result);
  }
  if ((m = /^\/v1\/reviews\/([^/]+)\/enact$/.exec(p)) && method === 'POST') {
    const body = await readJson(req);
    return json(res, 200, await k.enactDeletions({ reviewId: decodeURIComponent(m[1]), caller: body?.caller }));
  }

  return json(res, 404, { error: { code: 'not-found', message: `${method} ${p}` } });
}

function rootSelector(url, extra) {
  const sel = { ...extra };
  if (url.searchParams.get('roots')) sel.roots = url.searchParams.get('roots').split(',').filter(Boolean);
  if (url.searchParams.get('snapshot')) sel.snapshot = url.searchParams.get('snapshot');
  if (url.searchParams.get('snapshotVersion')) sel.snapshotVersion = Number(url.searchParams.get('snapshotVersion'));
  if (url.searchParams.get('activeVersion')) sel.activeVersion = Number(url.searchParams.get('activeVersion'));
  return sel;
}

function parseRange(header) {
  if (!header) return null;
  const m = /^bytes=(\d*)-(\d*)$/.exec(header.trim());
  if (!m) return null;
  const start = m[1] === '' ? null : Number(m[1]);
  const end = m[2] === '' ? null : Number(m[2]) + 1; // 半开区间
  if (start === null && end === null) return null;
  return start === null ? { end } : { start, end: end ?? undefined };
}

function num(v) {
  return v == null ? undefined : Number(v);
}

async function readJson(req) {
  try {
    const buf = await readBytes(req);
    if (buf.length === 0) return {};
    return JSON.parse(buf.toString('utf8'));
  } catch (err) {
    if (err.code === 'invalid-json' || err instanceof SyntaxError) {
      const e = new Error('请求体不是合法 JSON');
      e.code = 'invalid-json';
      throw e;
    }
    throw err;
  }
}

function readBytes(req, { maxBytes = 64 * 1024 * 1024 } = {}) {
  return new Promise((resolve, reject) => {
    const chunks = [];
    let size = 0;
    req.on('data', (c) => {
      size += c.length;
      if (size > maxBytes) {
        const err = new Error(`单次请求体超过 ${maxBytes} 字节，请使用分块导入`);
        err.code = 'payload-too-large';
        reject(err);
        req.destroy();
        return;
      }
      chunks.push(c);
    });
    req.on('end', () => resolve(Buffer.concat(chunks)));
    req.on('error', reject);
  });
}

function json(res, status, value) {
  const body = Buffer.from(JSON.stringify(value, replacer), 'utf8');
  res.writeHead(status, { 'Content-Type': JSON_CT, 'Content-Length': body.length });
  res.end(body);
}

function replacer(_key, value) {
  if (typeof value === 'bigint') return value.toString();
  return value;
}

function sendError(res, err) {
  const map = {
    'invalid-digest': 400,
    'invalid-reference': 400,
    'invalid-roots': 400,
    'invalid-snapshot-name': 400,
    'invalid-chunk-index': 400,
    'invalid-chunk-count': 400,
    'invalid-decision': 400,
    'invalid-caller': 400,
    'invalid-range': 400,
    'invalid-json': 400,
    'not-a-root': 400,
    'edge-not-found': 404,
    'ambiguous-label': 409,
    'snapshot-not-found': 404,
    'active-version-not-found': 404,
    'review-not-found': 404,
    'import-not-found': 404,
    'object-not-found': 404,
    'payload-too-large': 413,
    'import-exists': 409,
    'chunk-conflict': 409,
    'object-tombstoned': 410,
    'digest-mismatch': 422,
    'chunks-incomplete': 409,
  };
  const status = err instanceof KernelError ? (map[err.code] ?? 400) : (map[err.code] ?? 500);
  json(res, status, {
    error: {
      code: err.code || 'internal',
      message: err.message,
      ...(err.details && Object.keys(err.details).length ? { details: err.details } : {}),
    },
  });
}
