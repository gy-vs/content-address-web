# content-address-web — 内容寻址快照审阅内核

用于检查一组构建快照**为什么保留某些对象、为什么可能误删另一些对象**的内核与工作台。

- 从多个根快照沿引用边展开对象图，路径证据逐跳保留
- 比较两个快照（含历史版本）的共享 / 独占部分
- 查询某个对象被哪些根（活动 / 非活动）保留
- 公开结果明确区分三类对象：**缺失对象**、**摘要不匹配**、**仅存在于非活动根下**
- 回收（GC）候选审阅：确认时绑定当时的根集合与对象版本；迟到对象让候选消失时写入不可变的 `gc-resolved` 记录而不是静默变化
- 所有可变状态是一条**只追加事件链**：可重复调用、重放、崩溃恢复，状态由同一条数据链解释
- 元数据与对象内容严格分边界传输：大对象走断点上传 + `Range` 读取，任何接口都不会把整个对象图或 blob 隐式序列化

零运行时依赖，Node.js ≥ 18（ESM）。

## 快速开始

```bash
npm test          # 19 个测试：分批导入 / 摘要错误 / 历史版本 / 并发确认 / 局部加载 / 大对象 / 磁盘恢复 / HTTP
npm run demo      # 活内核驱动的端到端场景（不是静态样例）
npm start -- --port 8080 --dir ./data   # 启动持久化 HTTP 服务，启动时自动重放 events.log
```

## 架构与数据链

```
调用方（根选择/展开/diff/回收确认）
        │ JSON（小对象） + 断点上传（大对象） + Range（内容读取）
        ▼
src/http/server.js            公开模块入口（零依赖 http）
        │
        ▼
src/kernel.js                 命令侧：导入校验、快照、GC 决策、乐观并发、历史投影
        │  append（互斥串行化）
        ▼
事件链（唯一可变真相）
  object-offered | object-imported | object-rejected
  edge-declared | snapshot-pointed | batch-recorded
  gc-confirmed   | gc-resolved
        │                               fold/replay（纯函数）
        ▼                                       ▼
src/storage（memory / file）            src/graph.js + src/analysis.js
  events.log（JSONL，只追加）             reach / retainers / compare /
  blobs/ab/cd/<hex>（内容寻址）           resolvePath / gcCandidates / review
  quarantine/（坏输入的原始字节+元数据）
  uploads/<id>/（断点上传暂存）
```

关键不变量：

1. **校验失败的对象不是节点**。`object-rejected` 只进入拒绝索引与隔离区；图展开在 `digest-mismatch` / `missing` / `offered` 节点处**停止下钻**，但记录问题和路径，错误对象不会被当作有效节点继续传播引用。
2. **引用可以先于对象到达**。`edge-declared` 独立索引；对象仅宣布摘要时记为 `object-offered`（`awaiting-content`），内容晚到后升级为 present。
3. **GC 决定不可变**。候选因迟到对象重新可达时，原决定保留，额外追加 `gc-resolved`（原因 `re-retained`，含保留方），任何状态都能在事件链中定位。
4. **决定绑定历史**。`gc-confirmed` 记录 `basisRevision`、`basisStateHash`、当时全部根指针（含 active 标志）以及当时活动根可达的全部对象版本（内容寻址下版本即其 sha256）。
5. **内容与元数据分离**。`GET /objects/:d` 只给 size/contentType/出入度等；字节只在 `/content` 后，默认最多返回 256 KiB，支持 `Range: bytes=`（206）。
6. **重放确定性**。状态指纹 `stateHash` 不包含时间戳与存储层证据 ID；在任何时钟上重放同一条事件链得到相同指纹（有测试覆盖）。

## 内核用法

```js
import { Kernel, MemoryStorage, FileStorage, sha256Hex } from 'content-address-web';

const k = await Kernel.recover(new MemoryStorage());   // 或 await FileStorage.create('./data')

// 分批导入：边可以指向还不存在的对象；单条失败不影响同批其它条目
await k.importBatch({
  items: [
    { digest: sha256Hex(root), content: root },
    { digest: childDigest }          // 仅宣布，内容晚到（offered）
  ],
  edges: [{ from: rootDigest, to: childDigest }],
});

await k.defineSnapshot({ name: 'release/current', digest: rootDigest, active: true });

const report = await k.review(['release/current', 'release/old']);
report.missingObjects            // 被引用但从未出现
report.digestMismatches          // 可达但校验失败（不继续遍历）
report.unreachableMismatches     // 校验失败且不可达（坏上传也不被藏起来）
report.inactiveOnlyObjects       // 仅非活动根可达

await k.expand([{ snapshot: 'release/current' }]);           // 分页展开 + 每节点路径证据
k.retainersOf(digest);                                        // 谁保留这个对象
await k.compare({snapshot:'r', atRevision: 10}, {snapshot:'r'}); // 历史 vs 当前
k.resolve({ snapshot: 'r' }, [childDigest, leafDigest]);    // 逐跳路径，断点显式

const gc = k.gcCandidates();      // collectable / inactive-retained
await k.confirmGcCandidate(d, { expectedRevision: gc.revision });
k.decisionStatus(decisionId);     // open / resolved(re-retained …)
```

并发确认：所有写操作在内部串行提交。确认时传 `expectedRevision`；过期会得到结构化冲突（`revision-stale` + 当前 revision、当前指纹、该候选现在的状态），调用方重新拉取候选后即可变基重试。对活动对象确认返回 `not-a-candidate` 冲突，未知摘要返回 404（带 kind）。

## HTTP 摘要

| 方法 & 路径 | 说明 |
| --- | --- |
| `POST /batches` | 小对象 + 边的批量导入（JSON，单请求 ≤ 4 MiB） |
| `POST /uploads` · `PUT /uploads/:id/parts?offset=N` · `POST /uploads/:id/commit` | 大对象断点上传，存储层流式校验，失败字节进隔离区 |
| `GET /uploads/:id` · `DELETE /uploads/:id` | 上传状态 / 中止 |
| `PUT /snapshots/:name` · `GET /snapshots` · `GET /snapshots/:name/history` | 根指针与历史 |
| `POST /expand` · `GET /retainers/:digest` · `POST /compare` · `POST /review` · `POST /resolve` | 图关系查看（游标分页） |
| `GET /objects/:digest` | 元数据（无内容），缺失 404、不匹配 422 |
| `GET /objects/:digest/edges/out\|in` | 分页出入边 |
| `GET /objects/:digest/content?start&end` | 字节范围，支持 `Range`，返回 206 与 `x-content-digest` |
| `GET /gc/candidates` · `POST /gc/confirm` · `GET /decisions/:id` | 回收审阅 |
| `GET /batches/:id` · `GET /events` · `GET /quarantine/:id` | 原始输入 → 中间状态 → 最终结果的证据链 |

内容字段在 JSON 中可以是字符串（UTF-8）或 `{ "encoding": "base64", "data": "…" }`。

## 证据与可定位性（验收对应）

- 每个导入批次有 `batchId`：`BATCH_RECORDED` 记录每个条目的最终状态（imported / already-present / offered / rejected）。
- 对象元数据带 `firstBatchId`；`GET /batches/:id` 可回看输入；`GET /events` 给出完整中间事件链。
- 摘要错误：原始字节、声明摘要、实际摘要、批次、大小保存在隔离区（内存或 `quarantine/` 目录），`evidenceId` 贯穿拒绝事件与对象详情。
- `GET /state` 与每次写响应都返回 revision 与 `stateHash`；重放后 revision/hash 一致即证明“状态仍由同一条数据链解释”。

## 目录

```
src/
  digest.js          sha256 内容寻址与校验
  errors.js          Validation / NotFound / Conflict（kind 细分）
  paging.js          不透明游标分页（所有列表接口）
  fold.js            事件类型与投影折叠（纯函数，重放）
  state-hash.js      确定性状态指纹
  graph.js           根解析、BFS 展开（路径证据）、保留方
  analysis.js        快照比较、路径解析、GC 候选、review 三分类
  kernel.js          命令内核：导入/快照/决策/并发/历史
  storage/memory.js  内存存储（测试）
  storage/file.js    磁盘存储：events.log + blobs + quarantine + uploads
  http/server.js     公开 HTTP 模块
test/                node:test 内置测试运行器
scripts/demo.mjs     活内核端到端演示
scripts/serve.mjs    持久化服务启动器（启动即重放）
```
