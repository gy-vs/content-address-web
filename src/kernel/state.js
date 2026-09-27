// 状态折叠：把哈希链事件逐条应用成内存状态。纯函数、不接触对象字节，
// 因此同一事件序列在重放/恢复后必然得到同一状态（同一条数据链解释所有状态）。
export function emptyState() {
  return {
    // 有效对象：digest -> 对象记录（含引用边、生命周期序号）
    objects: new Map(),
    // 进行中的分批导入：importId -> 会话记录
    pending: new Map(),
    // 拒收台账：按证据 id 存放（错误对象不进 objects，不能沿它传播）
    rejected: new Map(),
    rejectedByDigest: new Map(), // declaredDigest -> evidenceId[]
    abandoned: [],
    // 快照：name -> 最新版本；history 保留全部历史版本
    snapshots: new Map(),
    snapshotHistory: [],
    // 活动根集合版本史（审阅决定绑定其中某一版）
    activeRootsHistory: [{ version: 0, roots: [], seq: 0, ts: null, id: null }],
    // 审阅计划：reviewId -> 计划（候选集合与当时的基）
    reviews: new Map(),
    // 决定与冲突台账（只追加）
    decisions: [],
    conflicts: [],
    // 已执行的删除批次
    enactments: [],
  };
}

/**
 * 应用单条记录（原地修改 state，返回 state）。
 * 未知事件类型忽略，保持前向兼容；所有字段都来自事件载荷，无外部读取。
 */
export function applyRecord(state, rec) {
  const { type, payload, seq, ts, id } = rec;
  switch (type) {
    case 'log.genesis':
      break;

    case 'import.started': {
      state.pending.set(payload.importId, {
        importId: payload.importId,
        declaredDigest: payload.declaredDigest,
        parseJson: !!payload.parseJson,
        declaredRefs: payload.declaredRefs ?? [],
        expectedChunkCount: payload.chunkCount ?? null,
        chunks: [],
        startedSeq: seq,
        startedAt: ts,
        clientRef: payload.clientRef ?? null,
      });
      break;
    }

    case 'import.chunk': {
      const p = state.pending.get(payload.importId);
      if (!p) break; // 会话已终结的迟到分块：忽略（磁盘上也不存在会话目录）
      const existing = p.chunks.findIndex((c) => c.index === payload.index);
      const meta = { index: payload.index, bytes: payload.bytes, chunkDigest: payload.chunkDigest, seq };
      if (existing >= 0) p.chunks[existing] = meta;
      else p.chunks.push(meta);
      p.chunks.sort((a, b) => a.index - b.index);
      break;
    }

    case 'import.object-accepted': {
      state.pending.delete(payload.importId);
      const prior = state.objects.get(payload.digest);
      if (prior) {
        // 内容寻址幂等：仅当对象处于墓碑态时这是一次“复活”
        if (prior.tombstoned) {
          prior.tombstoned = false;
          prior.revivedSeq = seq;
          prior.revivedFromEnactSeq = prior.enactedSeq ?? null;
        }
        break;
      }
      state.objects.set(payload.digest, {
        digest: payload.digest,
        size: payload.size,
        parseJson: !!payload.parseJson,
        refs: payload.refs ?? [],
        chunks: payload.chunks ?? [],
        importId: payload.importId,
        clientRef: payload.clientRef ?? null,
        acceptedSeq: seq,
        acceptedAt: ts,
        acceptedRecordId: id,
        tombstoned: false,
        enactedSeq: null,
        revivedSeq: null,
      });
      break;
    }

    case 'import.object-rejected': {
      state.pending.delete(payload.importId);
      const entry = {
        evidenceId: payload.evidenceId,
        reason: payload.reason,
        declaredDigest: payload.declaredDigest ?? null,
        actualDigest: payload.actualDigest ?? null,
        size: payload.size ?? null,
        detail: payload.detail ?? null,
        parseJson: !!payload.parseJson,
        declaredRefs: payload.declaredRefs ?? [],
        chunks: payload.chunks ?? [],
        importId: payload.importId ?? null,
        clientRef: payload.clientRef ?? null,
        quarantinePath: payload.quarantinePath ?? null,
        seq,
        ts,
        recordId: id,
      };
      state.rejected.set(entry.evidenceId, entry);
      if (entry.declaredDigest) {
        const list = state.rejectedByDigest.get(entry.declaredDigest) ?? [];
        list.push(entry.evidenceId);
        state.rejectedByDigest.set(entry.declaredDigest, list);
      }
      break;
    }

    case 'import.abandoned': {
      const p = state.pending.get(payload.importId);
      state.pending.delete(payload.importId);
      state.abandoned.push({
        importId: payload.importId,
        chunksReceived: p ? p.chunks.map((c) => c.index) : [],
        declaredDigest: p ? p.declaredDigest : payload.declaredDigest ?? null,
        seq,
        ts,
        reason: payload.reason ?? 'abandoned',
      });
      break;
    }

    case 'snapshot.created': {
      const entry = {
        name: payload.name,
        version: payload.version,
        roots: payload.roots,
        seq,
        ts,
        recordId: id,
      };
      state.snapshotHistory.push(entry);
      state.snapshots.set(payload.name, entry);
      break;
    }

    case 'snapshot.activeroots-set': {
      state.activeRootsHistory.push({
        version: payload.version,
        roots: payload.roots,
        source: payload.source,
        seq,
        ts,
        recordId: id,
      });
      break;
    }

    case 'review.opened': {
      state.reviews.set(payload.reviewId, {
        reviewId: payload.reviewId,
        basis: payload.basis,
        candidates: payload.candidates,
        note: payload.note ?? null,
        openedSeq: seq,
        ts,
        recordId: id,
      });
      break;
    }

    case 'review.decided': {
      state.decisions.push({
        reviewId: payload.reviewId,
        digest: payload.digest,
        decision: payload.decision,
        caller: payload.caller,
        note: payload.note ?? null,
        basis: payload.basis,
        objectVersion: payload.objectVersion,
        seq,
        ts,
        recordId: id,
      });
      break;
    }

    case 'review.conflicted': {
      state.conflicts.push({
        reviewId: payload.reviewId,
        digest: payload.digest,
        caller: payload.caller,
        kind: payload.kind,
        detail: payload.detail ?? null,
        basis: payload.basis ?? null,
        existing: payload.existing ?? null,
        seq,
        ts,
        recordId: id,
      });
      break;
    }

    case 'review.delete-enacted': {
      state.enactments.push({
        reviewId: payload.reviewId,
        digests: payload.digests,
        caller: payload.caller,
        decisionSeqs: payload.decisionSeqs ?? [],
        basis: payload.basis ?? null,
        seq,
        ts,
        recordId: id,
      });
      for (const d of payload.digests) {
        const obj = state.objects.get(d);
        if (obj) {
          obj.tombstoned = true;
          obj.enactedSeq = seq;
        }
      }
      break;
    }

    default:
    // 未知事件：忽略（新版本事件不应破坏旧内核的重放骨架）
  }
  return state;
}

/** 纯重放：记录序列 -> 状态。恢复路径与正常路径共用本函数。 */
export function replayRecords(records, state = emptyState()) {
  for (const rec of records) applyRecord(state, rec);
  return state;
}
