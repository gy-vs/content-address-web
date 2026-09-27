/**
 * Opaque-cursor pagination. The cursor is a base64url-encoded offset; callers
 * must treat it as opaque. Nothing large is ever serialized implicitly: every
 * list endpoint goes through this helper.
 */

function encodeOffset(offset) {
  return Buffer.from(JSON.stringify({ v: 1, o: offset }), 'utf8').toString('base64url');
}

function decodeOffset(cursor) {
  if (!cursor) return 0;
  try {
    const parsed = JSON.parse(Buffer.from(cursor, 'base64url').toString('utf8'));
    if (typeof parsed?.o !== 'number' || parsed.o < 0) throw new Error('bad cursor');
    return parsed.o;
  } catch {
    throw new RangeError('invalid pagination cursor');
  }
}

export function paginate(list, { cursor = null, pageSize = 200, maxPageSize = 2000 } = {}) {
  const offset = decodeOffset(cursor);
  let size = Number.isFinite(pageSize) ? Math.floor(pageSize) : 200;
  if (!Number.isFinite(size) || size < 1) size = 200;
  size = Math.min(size, maxPageSize);

  const total = list.length;
  const safeOffset = Math.min(offset, total);
  const items = list.slice(safeOffset, safeOffset + size);
  const end = safeOffset + items.length;
  return {
    items,
    page: {
      size: items.length,
      offset: safeOffset,
      total,
      remaining: Math.max(0, total - end),
      nextCursor: end < total ? encodeOffset(end) : null,
    },
  };
}
