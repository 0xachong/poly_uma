# UMA Master / Slave 当前架构

> 更新日期：2026-08-06

## Master 架构

```mermaid
flowchart TB
    Chain["Polygon 链上<br/>UMA OO Events"]
    RPC["Polygon RPC / WSS"]
    Ingest["事件接收与解析<br/>去重 · 时间戳 · 优先级分类"]

    Chain --> RPC --> Ingest

    Ingest -->|ProposePrice / DisputePrice| SignalQ["高优先级 Signal Queue<br/>容量 1024"]
    Ingest -->|Initialized / Resolved / Settle| LifecycleQ["低优先级 Lifecycle Queue<br/>容量 3072"]

    SignalQ --> SW["2 个 Signal Workers"]
    LifecycleQ --> LW["2 个 Lifecycle Workers"]

    CatalogMem["内存 Catalog<br/>market / condition / token"]
    MarketDB[("uma_market.sqlite<br/>市场目录")]
    MaintDB[("uma_maintenance.sqlite<br/>维护任务 / 黑名单")]
    SignalDB[("uma_signal.sqlite<br/>Propose / Dispute<br/>WAL + 复合索引")]
    LifecycleDB[("uma_lifecycle.sqlite<br/>Init / Resolve / Settle<br/>WAL + 复合索引")]
    LegacyDB[("uma_oo_events.sqlite<br/>切换前只读归档")]

    MarketDB -. 启动预热 / 异步更新 .-> CatalogMem
    CatalogMem --> SW
    CatalogMem --> LW

    SW --> SignalDB
    LW --> LifecycleDB
    MaintDB -. 异步维护 .-> CatalogMem

    SignalDB -. 不再反向复制 .-> LegacyDB
    LifecycleDB -. 不再反向复制 .-> LegacyDB

    SW --> RealtimeMem["最近 2 小时内存副本<br/>幂等索引"]
    LW --> RealtimeMem

    RealtimeMem --> Batch["Compact Trade Batch<br/>batch_id · processing_key"]
    Batch --> WSS["Master WebSocket Hub"]
    RealtimeMem --> HTTP["HTTP API :8011"]

    Health["/healthz<br/>队列 · 延迟 · 区块差距 · 分库状态"]
    SignalQ -. 指标 .-> Health
    LifecycleQ -. 指标 .-> Health
    SignalDB -. 指标 .-> Health
    LifecycleDB -. 指标 .-> Health
```

Master 的 ProposePrice 关键路径：

```text
链上事件
  → Master 接收
  → Signal 高优先队列
  → 内存 Catalog 查询
  → uma_signal.sqlite 持久化
  → 内存副本
  → WebSocket 广播
```

ProposePrice 热路径不同步请求 Gamma，也不再反向写入旧事件库。信号库使用 WAL，并通过 `(timestamp, transaction_hash, log_index)` 复合索引避免计算 `cursor_id` 时全表扫描。

## Slave 架构

```mermaid
flowchart TB
    Master["UMA Master<br/>WebSocket :8011"]

    P1["/uma/v1/ws/proposed"]
    P2["/uma/v1/ws/disputed"]
    P3["/uma/v2/ws/events<br/>compact / compact_trade"]

    Master --> P1
    Master --> P2
    Master --> P3

    P1 --> Upstream["Slave 上游连接管理器<br/>自动重连 · 心跳 · 状态统计"]
    P2 --> Upstream
    P3 --> Upstream

    Upstream --> Stamp["Relay 时间戳注入<br/>slave_received_at_us<br/>slave_broadcast_at_us"]
    Stamp --> Hub["Slave WebSocket Hub<br/>每个客户端独立发送队列"]

    Hub --> Client1["Worker / Trading Bot"]
    Hub --> Client2["其他内部消费者"]
    Hub --> Probe["延迟探针"]

    Hub --> Slow["慢客户端隔离<br/>队列满则断开该客户端"]
    Slow -. 不阻塞 .-> Hub

    Health["/slave/healthz<br/>连接状态 · 重连次数<br/>消息数 · 慢客户端数"]
    Upstream -. 指标 .-> Health
    Hub -. 指标 .-> Health

    Reporter["延迟上报器"]
    Stamp --> Reporter
    Reporter --> Control["UMA Control Plane<br/>趋势图 · 节点状态 · 分阶段延迟"]
```

Slave 关键路径：

```text
Master WSS
  → Slave 上游连接
  → 注入接收/广播时间戳
  → Slave WebSocket Hub
  → Worker 独立发送队列
```

Slave 不执行 Gamma 查询、SQLite 持久化或交易业务判断，主要负责低开销转发、客户端隔离和延迟采集。单个慢 Worker 不会阻塞其他 Worker。

## 端到端链路

```mermaid
flowchart LR
    Chain["Polygon 链上"]
    Master["Master<br/>解析 · Catalog · Signal DB"]
    Slave["Slave<br/>低开销中继"]
    Worker["Worker<br/>策略 · 下单"]
    Exchange["Polymarket CLOB"]

    Chain -->|chain_to_master| Master
    Master -->|master_processing| Master
    Master -->|master_to_slave| Slave
    Slave -->|slave_processing| Slave
    Slave -->|slave_to_client| Worker
    Worker -->|order_response| Exchange
```

## 关键监控指标

- `chain_to_master`
- Master ProposePrice `queue_ms`
- Master ProposePrice `sqlite_ms`
- `master_to_worker_delay_ms`
- `order_response_ms`
- `order_verify_ms`
