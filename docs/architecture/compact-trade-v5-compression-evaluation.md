# Compact Trade v5 编码与压缩评估

## 协议协商

业务结构、序列化编码和压缩算法必须独立协商：

```text
/uma/v2/ws/events?batch=true&format=compact_trade&schema_version=5&encoding=json&compression=none&sports_types=moneyline,child_moneyline
```

- `format=compact_trade`：业务字段集合。
- `schema_version=5`：字段结构和语义版本。
- `encoding=json|protobuf`：序列化格式。
- `compression=none|zstd`：应用层压缩；不能把 Protobuf 称为压缩算法。
- Master 必须在握手阶段拒绝不支持的组合，不能建立连接后静默降级。
- 二进制帧内部仍携带 schema version，Worker 必须同时校验订阅参数和消息版本。

当前 v5 第一阶段只开放 `encoding=json&compression=none`。Master 继续支持显式 v4；迁移期未传 `schema_version` 时回落 v4。

## v5 JSON 精简结果

v4 的 `tokens[]` 与 `candidate_tokens[]` 重复发送 token ID，外层 envelope 还重复发送每个事件已有的 block/transaction。v5 删除 `candidate_tokens[]`，把可选 `uma_price` 合并到对应 token，并删除外层重复的 `block_number`、`transaction_hash`：

```json
{
  "t": "p",
  "c": "0xcondition",
  "m": "3386821",
  "p": "500000000000000000",
  "tokens": [
    {"token_id": "token-a", "outcome": "A", "outcome_price": 0.91, "uma_price": 0.5},
    {"token_id": "token-b", "outcome": "B", "outcome_price": 0.09, "uma_price": 0.5}
  ]
}
```

`p` 保留为链上原始 18 位定点整数用于审计；`uma_price` 是归一化后的 Worker 热路径值。市场不满足安全条件时省略 `uma_price`，Worker 安全跳过。

## 候选方案

| 方案 | 体积 | CPU/延迟 | 演进兼容性 | 结论 |
|---|---|---|---|---|
| 精简 JSON | 中 | 解码和排障最简单 | 好 | v5 首发与回退格式 |
| MessagePack | 中偏小 | 较快 | 缺少强 schema，字段语义容易漂移 | 不推荐作为长期主协议 |
| CBOR | 中偏小 | 较快 | 标准化优于 MessagePack，但 Go 下游生态不如 PB | 可作为通用工具协议，不作为交易主协议 |
| Protobuf | 小 | 编解码快，生成代码稳定 | 最好；支持新增可选字段与未知字段跳过 | 推荐的二进制编码 |
| FlatBuffers/Cap'n Proto | 小 | 可减少拷贝 | schema 和实现复杂，当前消息规模下收益有限 | 暂不采用 |
| WebSocket permessage-deflate | JSON 大批次压缩明显 | 若按连接重复压缩会放大 CPU 和长尾 | 浏览器兼容好 | 只允许预压缩/PreparedMessage 复用后采用 |
| Zstandard | 大批次压缩率和吞吐通常优于 gzip | 小消息有帧头和压缩开销 | Go Worker 易支持，浏览器不原生透明支持 | 推荐作为 PB/JSON 的可选应用层压缩 |

## 推荐组合

1. 小帧：`protobuf + none`。低于压缩阈值时不压缩，避免把几十到几百字节的消息压得更慢。
2. 大批次：`protobuf + zstd`。建议从未压缩 payload `>=4 KiB` 开始压缩，最终阈值以生产回放基准确定。
3. 调试和应急：`json + none`。
4. 不采用“每个 Worker 连接单独 gzip”。Master/Slave 必须对相同 payload 序列化、压缩一次，并把不可变二进制帧复用给所有匹配订阅者，否则约 2900 个连接会把压缩 CPU 放大为新的 fanout 长尾。

## Protobuf 设计约束

- Envelope、Event、Market、Token 使用稳定 field number；字段废弃后保留编号，禁止复用。
- `Token.uma_price` 使用 `optional double`，明确区分“值为 0”和“字段不存在”。
- 链上原始 `p` 保留为 string 或 bytes，不能用可能溢出的语言原生整数承载 int256。
- 微秒时间戳使用 int64。
- schema v5 的 JSON 与 Protobuf 必须具有相同业务语义；切换 encoding 不应额外提升 schema version。
- Worker 设置压缩后最大解压尺寸、最大 batch event 数和最大 token 字符串长度，防止压缩炸弹和异常帧占满内存。

## 上线与评估门槛

上线二进制格式前，用生产捕获的单事件、小批次和小时级突发批次分别回放，至少记录：

- 原始字节数、编码后字节数、压缩后字节数；
- Master 单次编码/压缩耗时及分配次数；
- Slave 单次解码、过滤、重编码和 fanout 耗时；
- Worker 解压与解码 P50/P95/P99；
- 100、500、1000、3000 连接下的 CPU、RSS 和 Master→Worker P99。

只有在端到端 P99 不退化、CPU 不形成新瓶颈且断线重连无版本误判后，才把 Worker 默认订阅从 JSON 切到 Protobuf。
