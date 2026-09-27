import http from 'node:http';
import { URL } from 'node:url';
import {
  ConflictError,
  NotFoundError,
  ValidationError,
} from '../errors.js';

const MAX_INLINE_JSON = 4 * 1024 * 1024; // JSON import path is for small objects
const DEFAULT_RANGE = 256 * 1024;
const MAX_RANGE = 8 * 1024 * 1024;

/**
 * Public HTTP entry point for content-address-web.
 *
 * Nothing ever serializes an object graph or a blob implicitly:
 *  - metadata endpoints return digests/sizes only
 *  - content lives behind /objects/:digest/content with Range semantics
 *  - large payloads arrive through the resumable upload API
 */
export async function createHttpServer(kernel, { port = 0, host = '127.0.0.1' } = {}) {
  const server = http.createServer((req, res) => {
    handle(kernel, req, res).catch((err) => sendError(res, err));
  });
  await new Promise((resolve) => server.listen(port, host, resolve));
  const address = server.address();
  return {
    server,
    url: `http://${host}:${address.port}`,
    close: () => new Promise((resolve) => server.close(resolve)),
  };
}

async function handle(kernel, req, res) {
  const url = new URL(req.url, 'http://localhost');
  const p = url.pathname;
  const method = req.method;

  if (method === 'GET' && p === '/healthz') return json(res, 200, { ok: true, revision: kernel.revision });
  if (method === 'GET' && p === '/state') {
    return json(res, 200, { revision: kernel.revision, stateHash: kernel.hash() });
  }
  if (method === 'GET' && (p === '/' || p === '/routes')) return routes(res);

  // batches
  if (method === 'POST' && p === '/batches') {
    const body = await readJson(req);
    const out = await kernel.importBatch(body);
    return json(res, 201, stripInternal(out));
  }
  if (method === 'GET' && p === '/batches') {
    return json(res, 200, kernel.listBatches(paging(url)));
  }
  let m;
  if (method === 'GET' && (m = p.match(/^\/batches\/([^/]+)$/))) {
    return json(res, 200, kernel.batch(m[1]));
  }

  // resumable uploads
  if (method === 'POST' && p === '/uploads') {
    const body = await readJson(req);
    return json(res, 201, await kernel.createUpload(body));
  }
  if (method === 'GET' && (m = p.match(/^\/uploads\/([^/]+)$/))) {
    const up = await kernel.storage.getUpload(m[1]);
    if (!up) throw new NotFoundError('unknown upload', { uploadId: m[1] });
    return json(res, 200, up);
  }
  if (method === 'PUT' && (m = p.match(/^\/uploads\/([^/]+)\/parts?$/))) {
    const offset = url.searchParams.has('offset') ? Number(url.searchParams.get('offset')) : undefined;
    const bytes = await readRaw(req);
    const out = await kernel.writeUploadPart(m[1], bytes, { offset });
    if (!out) throw new NotFoundError('unknown upload', { uploadId: m[1] });
    return json(res, 200, out);
  }
  if (method === 'POST' && (m = p.match(/^\/uploads\/([^/]+)\/commit$/))) {
    const body = await readJson(req);
    const out = await kernel.commitUpload(m[1], body);
    return json(res, 201, stripInternal(out));
  }
  if (method === 'DELETE' && (m = p.match(/^\/uploads\/([^/]+)$/))) {
    await kernel.abortUpload(m[1]);
    return json(res, 200, { aborted: true, uploadId: m[1] });
  }

  // snapshots
  if (method === 'GET' && p === '/snapshots') return json(res, 200, { snapshots: kernel.listSnapshots() });
  if (method === 'PUT' && (m = p.match(/^\/snapshots\/([^/]+)$/))) {
    const body = await readJson(req);
    const out = await kernel.defineSnapshot({ name: decodeURIComponent(m[1]), digest: body.digest, active: body.active });
    return json(res, 200, out);
  }
  if (method === 'GET' && (m = p.match(/^\/snapshots\/([^/]+)\/history$/))) {
    return json(res, 200, { name: decodeURIComponent(m[1]), history: kernel.snapshotHistory(decodeURIComponent(m[1])) });
  }

  // graph read model
  if (method === 'POST' && p === '/expand') {
    const body = await readJson(req);
    return json(res, 200, stripInternal(await kernel.expand(body.roots, options(body, url))));
  }
  if (method === 'GET' && (m = p.match(/^\/retainers\/(.+)$/))) {
    const digest = decodeURIComponent(m[1]);
    const rootsParam = url.searchParams.get('roots');
    const roots = rootsParam ? JSON.parse(rootsParam) : undefined;
    return json(res, 200, await kernel.retainersOf(digest, { rootSelectors: roots }));
  }
  if (method === 'POST' && p === '/compare') {
    const body = await readJson(req);
    return json(res, 200, await kernel.compare(body.left, body.right, options(body, url)));
  }
  if (method === 'POST' && p === '/review') {
    const body = await readJson(req);
    return json(res, 200, await kernel.review(body.roots, options(body, url)));
  }
  if (method === 'POST' && p === '/resolve') {
    const body = await readJson(req);
    return json(res, 200, await kernel.resolve(body.root, body.path ?? []));
  }

  // objects
  if (method === 'GET' && (m = p.match(/^\/objects\/([^/]+)$/))) {
    const d = m[1];
    const node = kernel.node(d);
    if (node.kind !== 'present') return json(res, node.kind === 'digest-mismatch' ? 422 : 404, node);
    return json(res, 200, node);
  }
  if (method === 'GET' && (m = p.match(/^\/objects\/([^/]+)\/edges\/(out|in)$/))) {
    const fn = m[2] === 'out' ? kernel.outgoingEdges : kernel.incomingEdges;
    return json(res, 200, fn.call(kernel, m[1], paging(url)));
  }
  if (method === 'GET' && (m = p.match(/^\/objects\/([^/]+)\/content$/))) {
    return streamContent(kernel, res, m[1], url, req.headers.range);
  }

  // gc review
  if (method === 'GET' && p === '/gc/candidates') return json(res, 200, kernel.gcCandidates());
  if (method === 'POST' && p === '/gc/confirm') {
    const body = await readJson(req);
    const out = await kernel.confirmGcCandidate(body.digest, {
      expectedRevision: body.expectedRevision,
      note: body.note,
    });
    return json(res, 201, out);
  }
  if (method === 'GET' && p === '/decisions') {
    return json(res, 200, { decisions: kernel.listDecisions({ candidate: url.searchParams.get('candidate') ?? undefined }) });
  }
  if (method === 'GET' && (m = p.match(/^\/decisions\/([^/]+)$/))) {
    return json(res, 200, kernel.decisionStatus(m[1]));
  }

  // evidence chain
  if (method === 'GET' && p === '/events') return json(res, 200, { events: await kernel.events() });
  if (method === 'GET' && p === '/quarantine') return json(res, 200, { evidence: await kernel.listQuarantine() });
  if (method === 'GET' && (m = p.match(/^\/quarantine\/([^/]+)$/))) {
    const rec = await kernel.quarantineEvidence(m[1]);
    return json(res, 200, { ...rec, data: rec.data ? rec.data.toString('base64') : null });
  }

  return json(res, 404, { error: 'not-found', path: p });
}

async function streamContent(kernel, res, digest, url, rangeHeader) {
  const node = kernel.node(digest);
  if (node.kind !== 'present') {
    return json(res, node.kind === 'digest-mismatch' ? 422 : 404, node);
  }
  let start = 0;
  let end = null;
  const qStart = url.searchParams.get('start');
  const qEnd = url.searchParams.get('end');
  if (qStart != null) start = Number(qStart);
  if (qEnd != null) end = Number(qEnd);

  if (rangeHeader) {
    const parsed = parseRange(rangeHeader, node.size);
    if (parsed) ({ start, end } = parsed);
  }
  if (end == null) end = Math.min(node.size - 1, start + DEFAULT_RANGE - 1);
  const wantFull = start === 0 && end >= node.size - 1 && !rangeHeader;
  const maxEnd = start + MAX_RANGE - 1;
  end = Math.min(end, maxEnd, node.size - 1);
  if (Number.isNaN(start) || Number.isNaN(end) || start < 0 || start >= node.size) {
    return json(res, 416, { error: 'range-not-satisfiable', size: node.size });
  }

  const chunk = await kernel.readContentRange(digest, { start, end });
  res.writeHead(wantFull ? 200 : 206, {
    'content-type': node.contentType ?? 'application/octet-stream',
    'content-length': chunk.bytes.length,
    'x-content-digest': digest,
    'x-object-size': String(node.size),
    'accept-ranges': 'bytes',
    'content-range': `bytes ${chunk.start}-${chunk.end}/${chunk.size}`,
  });
  res.end(chunk.bytes);
}

function parseRange(header, size) {
  const m = /bytes=(\d*)-(\d*)/.exec(header);
  if (!m) return null;
  if (m[1] === '' && m[2] !== '') {
    // suffix range
    const suffix = Number(m[2]);
    return { start: Math.max(0, size - suffix), end: size - 1 };
  }
  const start = Number(m[1] || 0);
  const end = m[2] === '' ? size - 1 : Number(m[2]);
  return { start, end };
}

function options(body, url) {
  return {
    cursor: body.cursor ?? url.searchParams.get('cursor'),
    pageSize: body.pageSize ?? num(url.searchParams.get('pageSize')),
    atRevision: body.atRevision ?? num(url.searchParams.get('atRevision')),
    cursors: body.cursors,
  };
}

function paging(url) {
  return {
    cursor: url.searchParams.get('cursor'),
    pageSize: num(url.searchParams.get('pageSize')) ?? undefined,
  };
}

function num(v) {
  if (v == null || v === '') return undefined;
  const n = Number(v);
  return Number.isFinite(n) ? n : undefined;
}

async function readJson(req) {
  const buf = await readRaw(req, MAX_INLINE_JSON);
  if (!buf.length) return {};
  try {
    return JSON.parse(buf.toString('utf8'));
  } catch (e) {
    throw new ValidationError('invalid JSON body: ' + e.message);
  }
}

function readRaw(req, limit = Infinity) {
  return new Promise((resolve, reject) => {
    const chunks = [];
    let size = 0;
    req.on('data', (c) => {
      size += c.length;
      if (size > limit) {
        reject(new ValidationError(`request body too large (limit ${limit} bytes)`));
        req.destroy();
        return;
      }
      chunks.push(c);
    });
    req.on('end', () => resolve(Buffer.concat(chunks)));
    req.on('error', reject);
  });
}

function stripInternal(out) {
  if (out && '_nodeIndex' in out) {
    const { _nodeIndex, ...rest } = out;
    return rest;
  }
  return out;
}

function json(res, status, payload) {
  const body = JSON.stringify(payload, mapReviver, 2);
  res.writeHead(status, { 'content-type': 'application/json' });
  res.end(body);
}

function mapReviver(key, value) {
  if (value instanceof Map) return Object.fromEntries(value);
  if (value instanceof Set) return [...value];
  return value;
}

function sendError(res, err) {
  let status = 500;
  let code = err.code || 'internal-error';
  if (err instanceof ValidationError) status = 400;
  else if (err instanceof NotFoundError) status = 404;
  else if (err instanceof ConflictError) status = 409;
  else if (err instanceof RangeError) {
    status = 416;
    code = err.code || 'range-error';
  }
  const payload = { error: code, message: err.message };
  if (err.details) payload.details = err.details;
  if (status >= 500) {
    // keep 5xx terse but do not crash the server
    payload.message = err.message || 'internal error';
  }
  json(res, status, payload);
}

function routes(res) {
  json(res, 200, {
    name: 'content-address-web',
    endpoints: [
      'GET  /healthz',
      'GET  /state',
      'POST /batches',
      'GET  /batches?cursor&pageSize',
      'GET  /batches/:id',
      'POST /uploads',
      'PUT  /uploads/:id/parts?offset=N',
      'POST /uploads/:id/commit',
      'GET  /uploads/:id',
      'DELETE /uploads/:id',
      'PUT  /snapshots/:name',
      'GET  /snapshots',
      'GET  /snapshots/:name/history',
      'POST /expand {roots}',
      'GET  /retainers/:digest?roots=',
      'POST /compare {left,right}',
      'POST /review {roots}',
      'POST /resolve {root,path}',
      'GET  /objects/:digest',
      'GET  /objects/:digest/edges/out|in',
      'GET  /objects/:digest/content?start&end  (Range: bytes=)',
      'GET  /gc/candidates',
      'POST /gc/confirm {digest,expectedRevision}',
      'GET  /decisions?candidate=',
      'GET  /decisions/:id',
      'GET  /events',
      'GET  /quarantine',
      'GET  /quarantine/:evidenceId',
    ],
  });
}
