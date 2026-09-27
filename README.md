# content-address-web

**内容寻址快照审阅工作台内核**。用于检查一组构建快照为什么保留某些对象、又为什么可能误删另一些对象。

零运行时依赖（仅 Node ≥20 标准库）。所有状态由**一条哈希链事件日志**解释：重复调用、重放、进程恢复后，导入/快照/决定/冲突/删除都能回到同一状态，且任何篡改都能被定位。

## 它解决什么

调用方从多个根快照沿引用边展开对象闭包，比较两个快照的共享部分，查看某对象被哪些根保留；公开结果明确区分：

| 分类 | 含义 |
| --- | --- |
| `present` | 摘要校验通过、当前可达的有效对象 |
| `missing` | 被引用/作为根，但从未导入（先看到引用、对象晚到） |
| `digest-mismatch` | 该摘要下只有被拒收记录（摘要不符 / 声明 manifest 但内容畸形） |
| `inactive-only` | 当前活动根闭包不可达，但被某个历史/非活动快照保留 |
| `unreachable` | 不被任何快照保留的回收候选 |
| `tombstoned` | 已被确认的回收批次删除（决定记录保留；同内容重导会复活） |
| `unknown` | 完全未出现过的摘要 |

错误对象**不会**成为有效节点、其声明引用不会继续传播；已确认的回收候选即使因迟到引用复活/消失，冲突与决定都**有事件落链**，不会无声变化。

## 架构（同一条数据链）

```
调用方命令
   │
   ▼
Kernel（写命令经 #chain 串行化）
   │  追加事件 {seq,prevHash,ts,id,type,payload,hash}
   ▼
log/events.jsonl            ◄── 唯一事实来源（哈希链，每行规范 JSON，fsync 落盘）
   │  重放（纯函数 applyRecord）
   ▼
内存折叠状态（objects/pending/rejected/snapshots/reviews/decisions/conflicts）

objects/blobs/sha256/<ab>/<hex>   内容寻址字节（不可变，大对象只做顺序 IO）
objects/tmp/import-<id>/chunk-*   分批导入的分块
objects/quarantine/<证据id>.bin   被拒收对象的原始字节（失败输入证据）
```

- **事件载荷只存指针**（摘要、大小、分块摘要、路径、版本号），对象内容永不进日志；
- 链规则：`hash = sha256(canonicalJSON({seq,prevHash,ts,id,type,payload}))`，`prevHash` 指向上一条；
- 恢复 = 重放日志 + 盘点/对齐磁盘上未完成的导入会话。

## 传输边界

- 摘要、列表、图查询全部是 JSON，且**分页（游标）**，后端从不整体序列化对象图；
- 对象字节的唯一出口是 `openContent()` / `GET /content`，流式、支持半开区间 `[start,end)`（HTTP `Range` → 206）；
- 大对象分批上传（`startImport` → `uploadChunk` 乱序/重传 → `endImport`），提交时一遍完成拼接与摘要校验。

## 使用

### 作为库（公开入口 `content-address-web`）

```js
import { Kernel } from 'content-address-web';
import { sha256, digestFromHex } from 'content-address-web';
const dg = (s) => digestFromHex(sha256(Buffer.from(s)));

const k = await Kernel.create('./.data');

// 分批导入（引用可先于对象）
const leaf = dg('layer-bytes');
const imp = await k.startImport({ declaredDigest: dg(rootBytes), parseJson: true, chunkCount: 2 });
await k.uploadChunk({ importId: imp.importId, index: 0, data: rootBytes.subarray(0, 64) });
await k.uploadChunk({ importId: imp.importId, index: 1, data: rootBytes.subarray(64) });
await k.endImport({ importId: imp.importId });

await k.createSnapshot({ name: 'build', roots: [rootDigest] });
await k.setActiveRoots({ roots: [rootDigest] });

k.expand({});                              // 根闭包：reachable/missing/digestMismatch/tombstoned
k.diff('build', 'build', { aVersion: 1, bVersion: 2 }); // 共享 / 仅A / 仅B（分页）
k.retained(someDigest);                    // 被哪些根/快照保留，inactiveOnly
k.loadPath(root, ['$.config', 'layer'], { roots: [root] }); // 按路径局部加载

const review = await k.openReview({});     // 候选 + 绑定基（根版本 + 闭包指纹 + 对象版本）
await k.confirmReview({ reviewId: review.reviewId, digest, decision: 'confirm-delete', caller: 'ci' });
await k.enactDeletions({ reviewId: review.reviewId });

await k.verifyChain();                     // 哈希链完整性校验
```

### 作为 HTTP 服务（公开入口 `content-address-web/web`）

```bash
node src/cli.js --data ./.data --port 9090
# 或 npm run demo 查看端到端叙事
```

| 方法 & 路径 | 说明 |
| --- | --- |
| `GET  /v1/health` `/v1/chain` | 状态计数 / 哈希链校验 |
| `POST /v1/imports` | 开始分批导入 |
| `PUT  /v1/imports/:id/chunks/:i` | 上传分块（可乱序、幂等重发，`x-chunk-digest` 校验） |
| `POST /v1/imports/:id/end` `/abandon` | 提交校验 / 放弃 |
| `POST /v1/objects` | 小对象直传（body 即字节；错误返回 422 + 隔离证据） |
| `GET  /v1/objects` | 对象列表（游标分页，无字节） |
| `GET  /v1/objects/:d` `/summary` | 分类 / 摘要元数据 |
| `GET  /v1/objects/:d/content` | **字节唯一出口**，支持 `Range` |
| `GET  /v1/objects/:d/retention` | 被哪些根/快照保留 |
| `GET  /v1/graph/expand` | 根闭包展开（`roots=` / `snapshot=` / `activeVersion=`，分页） |
| `GET  /v1/graph/path?root=&label=&label=` | 按路径局部加载 |
| `POST/GET /v1/snapshots`，`GET /v1/snapshots/:a/diff/:b` | 快照历史与比较 |
| `GET/PUT /v1/active-roots` | 活动根集合版本 |
| `POST /v1/reviews` `GET /v1/reviews/:id` | 打开审阅 / 查看候选决定状态 |
| `POST /v1/reviews/:id/decisions` | 确认（200）/ 幂等 ack / **409 显式冲突** |
| `POST /v1/reviews/:id/enact` | 执行删除（迟到引用会跳过并落冲突） |

## 审阅决定如何绑定版本

打开审阅时记录 **basis**：根选择来源（explicit / snapshot@version / active）、根版本、事件序号、闭包内全部有序摘要及其指纹。每个候选保存对象版本（`acceptedSeq`/`revivedSeq`）。确认时按当下重算活性：

- 相同决定重复提交 → 幂等 `ack`；
- 相反决定并发到达 → 一个 `decided`，另一个 **409 `opposite-decision`**（带既有决定）；
- 根集合/闭包已漂移（迟到引用、换根）→ **409 `stale-basis`**，回报当前指纹；
- 执行删除时对象已重新可达 → 跳过并落 `stale-basis` 冲突，字节保留。

所有 `review.conflicted` 与 `review.decided` 都是只追加事件。

## 测试

```bash
npm test
```

48 个测试，全部通过**公开包入口**调用，覆盖：

- 分批导入（乱序、断点重传、幂等、崩溃后续传、孤儿分块清理、引用晚到）
- 摘要错误（拒收、隔离字节与分块证据链、畸形 manifest 不传播、坏根分类）
- 图关系（闭包、四分类、快照历史、版本比较、保留分析、inactive-only、分页）
- 历史版本（活动根版本、决定绑定基与对象版本、旧基冲突）
- 并发确认（不同候选并发落链、同候选相反决定冲突、幂等 ack、删除/墓碑/复活）
- 局部加载与大对象（按路径、Range、3 MiB 分块、摘要与字节边界）
- 哈希链（重启重放状态一致、篡改定位行号、伪造记录检出、输入→分块→对象证据串联）
- HTTP 端到端（经 `content-address-web/web` 全链路 + 重启后重复调用）

## 目录

```
src/kernel/digest.js     摘要原语
src/kernel/canonical.js  规范 JSON（链哈希）
src/kernel/blobstore.js  字节/分块/隔离
src/kernel/log.js        哈希链事件日志
src/kernel/state.js      纯函数状态折叠
src/kernel/refs.js       引用边推导
src/kernel/graph.js      闭包/差异/保留/路径/分页/指纹
src/kernel/kernel.js     命令汇聚（导入/快照/审阅/回收）
src/web/server.js        HTTP 适配
scripts/demo.js          可重复运行的端到端叙事
test/                    node:test 测试套件
```
