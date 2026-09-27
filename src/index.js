// content-address-web 公开模块入口。
// 调用方通过这些稳定接口重复调用、重放与恢复；所有状态都能由同一条哈希链解释。
export { Kernel, KernelError, replayRecords, emptyState } from './kernel/kernel.js';
export { BlobStore } from './kernel/blobstore.js';
export { Log, recordHash } from './kernel/log.js';
export {
  closure,
  diffSnapshots,
  retainedBy,
  classifyObject,
  loadPath,
  resolveRoots,
  pageDigests,
  closureFingerprint,
  DEFAULT_PAGE_SIZE,
  MAX_PAGE_SIZE,
} from './kernel/graph.js';
export { deriveRefs } from './kernel/refs.js';
export { applyRecord, replayRecords as replayRecordsFromState } from './kernel/state.js';
export {
  sha256,
  sha256Stream,
  normalizeDigest,
  assertDigest,
  digestFromHex,
  DIGEST_RE,
} from './kernel/digest.js';
export { canonicalJSON } from './kernel/canonical.js';
