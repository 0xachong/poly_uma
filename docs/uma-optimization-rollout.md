# PolyUMA 低延迟链路优化进度与上线计划

> 最后更新：2026-08-04（Asia/Shanghai）
>
> 跟进范围：Master、Slave、Worker
>
> 核心目标：降低 UMA proposed/disputed 从链上产生到 Worker 可执行处理的延迟，重点改善同一区块、同一交易批量事件的尾延迟。

## 1. 当前线上状态

Master 已于 2026-08-04 部署以下提交：

```text
5b9f0b0 feat: add active catalog and optional UMA batches
```

部署结果：

- GitHub Actions Master 部署成功。
- Master `/healthz` 返回 `status=ok`。
- `syncer_lag_blocks=0`。
- 旧 `/uma/v1/ws/proposed`、`/uma/v1/ws/disputed` 保持单条消息兼容。
- `batch=true` 生产开关尚未开启，当前请求返回 HTTP 501。
- ActiveCatalog 和影子 Batch 代码已上线，但运行开关尚未开启。
- Slave、Worker 尚未切换新协议。

当前生产开关状态：

```text
SYNC_SHADOW_BATCH_ENABLE=false
ACTIVE_CATALOG_ENABLE=false
WS_BATCH_ENABLE=false
```

## 2. 已确定的协议与业务约束

### 2.1 业务幂等键

proposed/disputed 使用：

```text
processing_key = lowercase(condition_id) + ":" + event_type
```

示例：

```text
0xabc...:propose
0xabc...:dispute
```

约束：

- Master、Slave、Worker 均应以 `processing_key` 做业务幂等。
- propose 和 dispute 是两个独立业务事件。
- `transaction_hash + log_index` 保留用于链上审计和异常定位，不作为 Worker 下单幂等键。
- 同一个 `processing_key` 对应不同链上日志时必须记录异常，不能静默覆盖。

### 2.2 ActiveCatalog

正常热路径：

```text
解析 proposed/disputed 的 market_id
  → ActiveCatalog[market_id]（一次 Hash）
  → 获得 condition_id、标题、event、tags、tokens、outcomes、market type 等完整快照
```

内存只加载：

```text
active=true AND closed=false
```

不加载全部历史市场。SQLite 保留持久化快照，用于重启恢复、审计和异常修复。

任何 ActiveCatalog miss 均视为数据完整性异常：

```text
miss
  → 飞书报警
  → 独立 repair worker 精确查询 Gamma
  → 持久化快照
  → 恢复内存索引
  → 发送修复成功通知
```

修复任务不得阻塞实时事件 Worker。

### 2.3 Batch 协议

旧接口继续保留：

```text
GET /uma/v1/ws/proposed
GET /uma/v1/ws/disputed
```

可选 Batch 订阅：

```text
GET /uma/v1/ws/proposed?batch=true
GET /uma/v1/ws/disputed?batch=true
GET /uma/v2/ws/events?batch=true
```

兼容规则：

| 请求 | 行为 |
|---|---|
| 不传 `batch` | 原单条 JSON |
| `batch=false` | 原单条 JSON |
| `batch=true` 且服务端开关关闭 | HTTP 501 |
| `batch=true` 且服务端开关开启 | `uma_event_batch` v2 |

`/uma/v2/ws/events` 统一推送 propose 和 dispute，目标是让新 Slave 只维持一条 Master 上游连接。旧 v1 两个端点继续保留。

Batch 聚合键：

```text
block_number + transaction_hash
```

初始参数：

```text
idle_window = 2ms
max_wait    = 5ms
max_events  = 128
```

`batch_id` 只用于传输诊断；每条事件仍使用自己的 `processing_key` 幂等。

## 3. “影子运行”的含义

影子运行表示新逻辑在线计算和记录，但暂时不改变 Worker 收到的旧 WSS 消息，也不改变下单行为。

### 3.1 影子 Batch

开启：

```text
SYNC_SHADOW_BATCH_ENABLE=true
```

作用：

- 统计同一交易的事件数量。
- 统计第一条到最后一条的到达跨度。
- 模拟 2ms idle / 5ms max 策略会形成多少个 Batch。
- 判断同一交易事件簇是否会被拆包。
- 不改变当前单条 WSS 发送。

### 3.2 ActiveCatalog 影子查询

开启：

```text
ACTIVE_CATALOG_ENABLE=true
```

作用：

- 每条 proposed/disputed 做一次 `market_id` Hash 查找。
- 记录 hit、miss、repair。
- miss 时报警并自动修复。
- 旧单条 WSS 和 Worker 行为保持不变。

## 4. 阶段进度

### 临时市场映射诊断外挂（2026-08-04）

- [x] 独立订阅 Master `/uma/v2/ws/events?batch=true`，不接触主库和下单链路。
- [x] 以 WSS `source=delayed_replay` 识别真正的 condition 映射 miss；普通消息缺少 `market` 只代表完整快照 miss，不再混为一类。
- [x] 同时按 `market_id` 精确查询和按 `condition_id` 反查 Gamma。
- [x] 在 0/2/10/30 秒复查，JSONL 留存完整证据。
- [x] 提供本机统计接口 `127.0.0.1:8021`。
- [x] 以 4 个固定 worker 和 512 队列限制 Gamma 诊断压力。
- [x] systemd 服务 `uma-mapping-diagnoser` 已在 Master 旁路运行。

证据文件：`/opt/uma-sync/data/mapping-diagnostics.jsonl`。

首批实测原因：

- `catalog_refresh_window_race`：市场在 proposed 前不到 30 秒更新，落在目录刷新窗口之间；Gamma 精确接口与 condition 查询接口还可能短暂返回不同的 `updatedAt`。
- `active_catalog_coverage_gap`：Gamma 映射早已存在但 Master 无快照，需要检查增量扫描覆盖和快照持久化。
- `gamma_inactive_open_market`：Gamma 为 `active=false, closed=false`，按当前 active-only 目录规则永远不会命中；历史 `delayed_replay` 应与实时 miss 分开告警。
- `mapping_conflict`：Master 与 Gamma condition 映射不同，属于必须立即处理的真实冲突。

### 官方 Gamma 增量目录修正（2026-08-04）

依据 Polymarket 官方 `/markets/keyset` 文档：大集合应使用 opaque `next_cursor`/`after_cursor` 稳定分页，单页最多 100；keyset 明确拒绝 offset。原固定扫描 `/markets?order=updatedAt&offset=...` 前 1000 条的方式，在大量市场拥有相同更新时间时存在截断和排序漂移。

- [x] 快速增量改为 `/markets/keyset?order=updatedAt&ascending=false`。
- [x] 持久化高水位语义，扫描到“上轮最高更新时间 - 2 分钟”才停止。
- [x] 首次启动覆盖最近 15 分钟，不限制固定页数。
- [x] keyset 相同时间簇通过 opaque cursor 完整翻页。
- [x] 全量 `closed=false` keyset 兜底从每 5 秒 100 条提升为每 500ms 100 条。
- [x] init 获得 market_id 后执行一次独立精确预取，不与 proposed 实时重试 singleflight 相互阻塞。
- [x] 飞书告警拆成 `condition_mapping_miss`（高）与 `active_catalog_snapshot_miss`（warning）。

### CLOB 驱动的活跃市场生命周期（2026-08-04）

- [x] 使用官方 `GET /sampling-simplified-markets` 和 `next_cursor` 完整分页，CLOB condition_id 集合作为内存驻留资格源。
- [x] 只接受 `active && !closed && !archived && accepting_orders` 的市场；Gamma 仅补充标题、event、tag、token 等属性。
- [x] 当前 CLOB 活跃市场立即驻留；离开集合后保留 48 小时退出宽限，以覆盖临近结算的 UMA proposed/disputed。
- [x] 完整分页成功后才更新集合；任何超时、分页失败均保留上一版，不用半份数据执行淘汰。
- [x] 超过退出宽限后删除完整属性快照，但永久保留独立的 `market_id ↔ condition_id` 身份映射。
- [x] 启动时先完成 CLOB 集合刷新再订阅实时链路；刷新失败则继续启动并后台重试，checkpoint backfill 覆盖启动窗口。
- [x] 线上接口实测 10 页、9,444 个符合条件的唯一 condition_id；目标由 12 万级降至约 1 万级常驻。
- [x] 全量测试、关键包竞态测试和 `go vet` 通过。
- [ ] 灰度上线后观察 RSS、GC、队列等待、Catalog hit/miss 和首次精确修复至少 30 分钟。
- [ ] 确认稳定后再评估是否把 48 小时退出宽限缩短到 24 小时。

### proposed 实时 Gamma 热修复（2026-08-04）

- [x] 修复 init 预热提前返回：只有完整 condition_id 快照命中才跳过，单有 market 映射必须继续预热。
- [x] UMA init 强制预热的快照持久化 pin 48 小时，不受 sampling 集合清理影响。
- [x] 冷启动从本地库回补最近 7 天“已 init、尚未 proposed”的存量候选，8 个受控 worker 提前预热并 pin。
- [x] snapshot miss 报警移动到事件持久化去重之后，重连重放不再重复报警。
- [x] 删除 `ObserveSnapshotMiss` 的并行 Gamma 修复；异常事件只由 durable pending worker 执行一次 singleflight 修复。
- [x] `/uma/v1/proposed/latest` 不再二次查询 Gamma，内存路径直接返回事件携带的 MarketSnapshot。
- [ ] 上线后确认正常 proposed 的 `source=realtime`，`source=delayed_replay` 仅作为异常兜底。
- [ ] 观察 `active_catalog_miss_total` 增长率与修复报警至少 30 分钟。
- [x] Gamma 精确 market 缺少 question/event relation 时，按 condition_id 反查另一查询视图并选择更完整结果。
- [x] inactive/closed 的异常市场允许 force-pin 完整快照，不再被普通活跃资格阻止修复。
- [x] event relation 暂缺时以 market question/slug 提供可用标题降级，后续增量自动覆盖。
- [x] miss、repair_failed、repair_recovered 现场写入 `market_enrichment_incident`，包含交易坐标、字段摘要和耗时，保留 30 天。

### 全量活跃基线 + 增量活跃同步（2026-08-04）

- [x] Gamma `closed=false` keyset 全量扫描建立基线，只驻留 `active && !closed && !archived && acceptingOrders` 的完整快照。
- [x] 全量扫描使用独立 v2 cursor 状态，升级后从头构建，不继承旧 mapping-only 扫描进度。
- [x] Gamma `updatedAt` keyset 每 10 秒增量同步新增市场和状态变化。
- [x] sampling 集合降级为补充资格源，不再代表全量活跃市场。
- [x] 全量基线每 30 分钟重新校准；退出市场由 48 小时宽限和 UMA pin 控制生命周期。
- [x] 全量分页提升到 50ms/页，并在实时队列非空时立即让路；首次覆盖目标 1–2 分钟。
- [x] 增加非 sampling 但正常 active/accepting 市场的驻留测试，覆盖生产 market `3241754` 的问题类型。
- [x] 冷启动直接加载持久化 Last Known Good 快照并立即订阅，所有 CLOB/Gamma 网络刷新转后台。
- [x] 全量扫描过程中只增不删；仅当一次权威全量成功完成后，才清理超过 48 小时且无 UMA pin 的属性快照。
- [x] `market_condition_map` 身份映射长期保留，不参与属性快照退出清理。

### 阶段 0：旧协议兼容与幂等键

- [x] 定义 `processing_key=condition_id:event_type`。
- [x] propose/dispute 输出 `processing_key`。
- [x] 保持旧字段和值不变。
- [x] 增加旧单条 WSS 兼容测试。
- [x] 全量测试、竞态测试通过。

### 阶段 1：影子 Batcher

- [x] 按 `block_number + transaction_hash` 聚合。
- [x] 支持 idle window 和 max wait。
- [x] 记录事件数、到达跨度和模拟等待。
- [x] 默认关闭，不改变生产发送。
- [x] 增加聚合及最大等待单元测试。
- [ ] 在线开启并观察至少 24 小时。
- [ ] 覆盖事件数大于 20、50、100 的真实交易。
- [ ] 根据线上相邻日志 gap 的 p99/p99.9 确认最终参数。

### 阶段 2：ActiveCatalog 影子目录

- [x] 建立 `market_id → *MarketSnapshot` 内存索引。
- [x] SQLite 持久化活跃市场完整快照。
- [x] 只从 SQLite 加载 active、未 closed 市场。
- [x] 快照包含 market、event、tags、tokens、outcomes、market type 等字段。
- [x] miss 独立精确修复。
- [x] 飞书异常、修复失败及恢复通知。
- [x] `/healthz` 增加 Catalog 指标。
- [ ] 配置 `MARKET_MAPPING_ALERT_WEBHOOK`。
- [ ] 在线开启 `ACTIVE_CATALOG_ENABLE=true`。
- [ ] 连续观察 24～48 小时命中率。
- [ ] 分析所有 miss，不能用无限重试掩盖漏同步。
- [ ] 验证完整快照中 event/title/tags 的覆盖率。

### 阶段 3：可选 Batch WSS

- [x] 支持 `?batch=true` 能力协商。
- [x] 默认关闭，旧连接行为不变。
- [x] Batch 内按 `log_index` 排序。
- [x] 每条事件携带独立 `processing_key`。
- [x] Catalog 命中时 Batch 元素携带完整 `market` 快照。
- [x] 增加旧单条与新 Batch WSS 集成测试。
- [x] Master代码部署上线。
- [ ] 保持 `WS_BATCH_ENABLE=false` 完成影子观察。
- [ ] 部署影子 Slave 后再开启 `WS_BATCH_ENABLE=1`。
- [ ] 对账单条流与 Batch 展开后的 `processing_key` 集合。

### 阶段 4：Slave 共享上游 Relay

- [x] Master新增统一 `/uma/v2/ws/events` 接口。
- [x] Master 新增兼容订阅 `/uma/v2/ws/events?batch=true&format=compact`；默认 `format=full` 契约不变。
- [x] Slave 逐台灰度切换 compact：先 `43.135.4.241`，验证后再上线 `43.135.87.223`（2026-08-04）。
- [x] 两台 Slave 均保留旧 v1 full 契约，并新增共享 `/uma/v2/ws/events?batch=true&format=compact` 上游。
- [x] 真实事件双路验证：旧 full 约 2.3–2.4KB，compact 约 0.85KB；Tag、价格和链上定位字段一致。
- [ ] 修复 `deploy-slaves.yml` 自托管 Runner 缺少 `uma_slave_deploy_ed25519` 的自动部署凭据；本次已通过受控 SSH 手动原子发布并保留回滚二进制。
- [ ] 恢复真正的共享上游架构。
- [ ] 每台 Slave 只建立少量/单一 Master 上游连接。
- [ ] 新 Slave 上游使用 `batch=true`。
- [ ] 同时兼容 Master 单条和 Batch 输入。
- [ ] 对旧 Worker 将 Batch 拆成单条。
- [ ] 对新 Worker 保持 Batch 下发。
- [ ] 每客户端独立有界发送队列。
- [ ] 慢客户端不得阻塞其他连接。
- [ ] 增加游标、去重、断线补拉。
- [ ] 新 Slave 先以新端口影子部署。
- [ ] 与旧 Slave 对账事件数量、顺序、重复率和延迟。

### 阶段 5：Worker兼容 Batch 和完整快照

- [ ] Worker同时解析 `uma_event` 和 `uma_event_batch`。
- [ ] Batch 中每条事件独立处理，一个失败不得终止整批。
- [ ] 使用 `processing_key` 持久化原子去重。
- [ ] Worker直接使用 Master提供的 `market` 快照。
- [ ] 新旧市场数据进行影子对比。
- [ ] 停止 proposed/disputed 后逐条请求 Gamma。
- [ ] 先影子计算，不重复下单。
- [ ] 逐步灰度 1台、5台、1%、10%、50%、100%。

### 阶段 6：Slave Tag过滤

- [ ] Slave接收Master全部事件。
- [ ] Slave根据每条下游 WSS 的订阅规则过滤。
- [ ] 支持 `include_tag_ids`。
- [ ] 支持 `exclude_tag_ids`，排除优先。
- [ ] 支持 include/exclude market type。
- [ ] 使用 tag ID，不使用 label 做判断。
- [ ] 相同规则形成 SubscriptionGroup，避免逐客户端重复计算。
- [ ] 先运行 `would_deliver/would_filter` 影子过滤。
- [ ] 与 Worker现有过滤结论对账。
- [ ] 先启用 exclude，再启用 include。

### 阶段 7：性能优化

- [ ] Master每条/每批只序列化一次。
- [ ] Slave每条上游消息只解析一次。
- [ ] 同订阅组复用同一个只读 payload。
- [ ] Master事件写入改为批量 SQLite 事务。
- [ ] 去掉逐事件 `COUNT(*)` 计算 cursor 的热路径。
- [ ] 根据实测决定是否把真实 Batcher 前移到链上解码后、富化前。
- [ ] Batch 大于阈值后评估 Zstd level 1。
- [ ] JSON CPU/GC 未成为瓶颈前不引入 Protobuf。

## 5. 下一次上线建议

第一步只开启影子能力，不开放 Batch 生产订阅：

```env
SYNC_SHADOW_BATCH_ENABLE=true
SYNC_BATCH_IDLE_WINDOW=2ms
SYNC_BATCH_MAX_WAIT=5ms

ACTIVE_CATALOG_ENABLE=true
MARKET_MAPPING_ALERT_WEBHOOK=<Feishu webhook secret>

WS_BATCH_ENABLE=0
WS_BATCH_IDLE_WINDOW=2ms
WS_BATCH_MAX_WAIT=5ms
```

观察至少 24 小时后检查：

```text
active_catalog_count
active_catalog_hit_total
active_catalog_miss_total
active_catalog_repair_total

同一交易 event_count
arrival_span_us
simulated_wait_us
同一交易模拟 batch 数量
```

进入影子 Slave 前的门槛：

- Catalog有效 market_id 命中率不低于 99.99%。
- condition_id映射冲突为0。
- 所有 miss 均有明确原因及修复结果。
- 同一交易事件簇至少99%不超过一个模拟 Batch，或已根据实测调整窗口。
- 旧 WSS 事件数、下单数、成功率无异常变化。

## 6. 验收指标

Master：

```text
ActiveCatalog Hash lookup p99 < 100µs
master queue wait p99 持续下降
单条旧 WSS 兼容率 100%
单条流与 Batch 展开事件集合差异 = 0
```

Master → Slave：

```text
p99 < 50ms
无持续积压
无静默丢失
```

Slave：

```text
parse + route p99 < 5ms
单个慢客户端不影响其他客户端
过滤后公网下行字节明显下降
```

Worker：

```text
同一 processing_key 不重复处理或下单
Gamma逐事件查询接近归零
propose → CLOB / order 延迟明显下降
业务过滤和下单结果无异常漂移
```

## 7. 回滚方式

| 功能 | 回滚方式 |
|---|---|
| 影子 Batch | `SYNC_SHADOW_BATCH_ENABLE=false` |
| ActiveCatalog | `ACTIVE_CATALOG_ENABLE=false` |
| Batch WSS | `WS_BATCH_ENABLE=0` |
| Worker Batch | 重新连接并使用 `batch=false` |
| Slave过滤 | `SLAVE_FILTER_ENABLE=false` |
| 新 Slave | Worker切回旧 Slave地址 |

任何阶段出现异常时，只回滚当前阶段，不同时撤销已经验证稳定的前置能力。

## 8. 每日跟进记录模板

```text
日期：
线上版本：
开启的功能开关：

Catalog：
- active count：
- hit：
- miss：
- repair：
- 未解决异常：

Batch影子：
- 最大事件簇：
- 相邻日志gap p99/p99.9：
- 单batch覆盖率：
- 模拟新增等待p99：

业务：
- proposed/disputed数量：
- Worker接收数量：
- 下单数量/成功率：
- 重复processing_key：

本日结论：
下一步：
是否需要回滚：
```
