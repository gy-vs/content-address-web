import { createHash } from 'node:crypto';
import { EVT } from './fold.js';

/**
 * Deterministic fingerprint of the *content chain*: what exists, what edges
 * link it, where snapshots point and which decisions were made. Timestamps are
 * deliberately excluded so a replay onto another clock yields the same hash.
 * Evidence ids from storage are also excluded; they are implementation
 * pointers, not chain identity.
 */
export function stateHash(state) {
  const lines = [];

  const objects = [...state.objects.values()].sort((a, b) => a.digest.localeCompare(b.digest));
  for (const o of objects) {
    lines.push(['obj', o.digest, o.size, o.contentType ?? ''].join('\t'));
  }

  const rejected = [...state.rejected.keys()].sort();
  for (const d of rejected) {
    const list = state.rejected.get(d);
    // rejection identity: expected digest + actual digests + reasons in arrival order
    const summary = list
      .map((r) => `${r.reason}:${r.actualDigest ?? ''}`)
      .join(',');
    lines.push(['rej', d, summary].join('\t'));
  }

  const offered = [...state.offered].sort();
  for (const d of offered) lines.push(['offered', d].join('\t'));

  const edges = [];
  for (const [from, m] of state.edges) {
    for (const [to, meta] of m) edges.push({ from, to, seq: meta.seq });
  }
  edges.sort((a, b) => (a.seq - b.seq) || a.from.localeCompare(b.from) || a.to.localeCompare(b.to));
  for (const e of edges) lines.push(['edge', e.from, e.to].join('\t'));

  const snaps = [...state.snapshots.values()].sort((a, b) => a.seq - b.seq || a.name.localeCompare(b.name));
  for (const sn of snaps) {
    lines.push(['snap', sn.name, sn.target, sn.active ? '1' : '0'].join('\t'));
  }

  const decisions = [...state.decisions.values()].sort((a, b) =>
    a.decisionId.localeCompare(b.decisionId));
  for (const d of decisions) {
    const versions = [...d.objectVersions.entries()]
      .sort(([a], [b]) => a.localeCompare(b))
      .map(([k, v]) => `${k}=${v}`)
      .join(',');
    const roots = [...(d.rootBasis.roots ?? [])].map((r) =>
      `${r.snapshot ?? ''}:${r.digest}:${r.active ? '1' : '0'}`).join(',');
    lines.push(['dec', d.decisionId, d.candidate, d.basisRevision, roots, versions].join('\t'));
  }

  for (const dId of [...state.resolved.keys()].sort()) {
    const r = state.resolved.get(dId);
    lines.push(['resolved', dId, r.reason, [...r.retainedBy].sort().join(',')].join('\t'));
  }

  const h = createHash('sha256');
  h.update('content-address-web-state-v1\n');
  for (const l of lines) h.update(l + '\n');
  return 'sha256:' + h.digest('hex');
}

export { EVT };
