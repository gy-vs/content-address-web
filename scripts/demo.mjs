#!/usr/bin/env node
/**
 * End-to-end demonstration of the review workbench against a real kernel.
 * This is not a static fixture: every printed value is produced by the live
 * event-sourced kernel from the batches sent below.
 *
 * Run: npm run demo
 */
import { Kernel, MemoryStorage, sha256Hex } from '../src/index.js';

const k = await Kernel.recover(new MemoryStorage());

function obj(text) {
  const content = Buffer.from(text);
  return { digest: sha256Hex(content), content, text };
}

const line = (s) => console.log('\n=== ' + s + ' ===');

// --- phase 1: a root and an edge to an object that has not arrived yet -----
line('phase 1: edges arrive before their targets');
const root = obj('build-2201 root manifest');
const src = obj('src tree (late in batch 2)');
const cfg = obj('config blob (late in batch 3)');
const ghost = obj('referenced but never imported');

const b1 = await k.importBatch({
  batchId: 'bat-0001',
  items: [root],
  edges: [
    { from: root.digest, to: src.digest },
    { from: root.digest, to: ghost.digest },
  ],
});
console.log('batch 1 revision:', b1.revision);
let report = await k.review([{ digest: root.digest }]);
console.log('missing after batch 1:', report.counts.missingObjects);

// --- phase 2: partial arrival ---------------------------------------------
line('phase 2: one late object arrives, its own edge points further ahead');
await k.importBatch({
  batchId: 'bat-0002',
  items: [src],
  edges: [{ from: src.digest, to: cfg.digest }],
});
report = await k.review([{ digest: root.digest }]);
console.log('present:', report.counts.present, 'missing:', report.counts.missingObjects);

// --- phase 3: bad object is rejected, never propagated ----------------------
line('phase 3: corrupt payload under a claimed address');
const claimed = sha256Hex(Buffer.from('the-real-config'));
const corrupt = Buffer.from('tampered bytes');
const bad = await k.importBatch({
  batchId: 'bat-0003',
  items: [{ digest: claimed, content: corrupt }],
});
console.log('rejected item:', {
  digest: bad.items[0].digest.slice(0, 22) + '…',
  reason: bad.items[0].reason,
  evidenceId: bad.items[0].evidenceId,
  actual: bad.items[0].actual.slice(0, 22) + '…',
});

// --- phase 4: snapshots + inactive root -----------------------------------
line('phase 4: active and inactive snapshots');
const archive = obj('old release manifest');
const archiveOnly = obj('deprecated vendor blob');
await k.importBatch({
  batchId: 'bat-0004',
  items: [cfg, archive, archiveOnly],
  edges: [{ from: archive.digest, to: archiveOnly.digest }],
});
await k.defineSnapshot({ name: 'release/current', digest: root.digest, active: true });
await k.defineSnapshot({ name: 'release/2099', digest: archive.digest, active: false });

report = await k.review(['release/current', 'release/2099']);
console.log('missing objects       :', report.counts.missingObjects);
console.log('digest mismatches     :', report.counts.digestMismatches);
console.log('unreachable mismatches:', report.counts.unreachableMismatches);
console.log('inactive-only objects :', report.counts.inactiveOnlyObjects);

// --- phase 5: GC review + late re-retention -------------------------------
line('phase 5: GC confirm followed by a late arriving edge');
const gc = k.gcCandidates();
console.log('collectable count:', gc.counts.collectable, 'inactive-retained:', gc.counts.inactiveRetained);
// archiveOnly is retained only by the inactive snapshot; confirm it
const target = archiveOnly.digest;
const dec = await k.confirmGcCandidate(target, { expectedRevision: k.revision, note: 'obsolete vendor blob' });
console.log('decision:', dec.decisionId, 'bound at revision', dec.basisRevision, 'state', dec.basisStateHash.slice(0, 22) + '…');

// late object/edge reattaches archiveOnly under the active root
await k.importBatch({
  batchId: 'bat-0005',
  edges: [{ from: src.digest, to: archiveOnly.digest }],
});
const status = k.decisionStatus(dec.decisionId);
console.log('decision after late edge:', status.status, '/', status.resolution.reason, 'retainedBy', status.resolution.retainedBy);

// --- phase 6: replay proves one chain explains every state ----------------
line('phase 6: replay produces the identical state hash');
const events = await k.events();
const { replay, stateHash } = await import('../src/index.js');
const rebuilt = replay(events);
console.log('events replayed :', events.length);
console.log('revision match  :', rebuilt.revision === k.revision);
console.log('state hash match:', stateHash(rebuilt) === k.hash());
console.log('final hash      :', k.hash());
