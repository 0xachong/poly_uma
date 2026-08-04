# UMA Compact Trade v4

## Subscription

`/uma/v2/ws/events?batch=true&format=compact_trade&sports_types=moneyline,child_moneyline`

The v4 contract is opt-in. Legacy v1 single events, v2 full batches and v3
compact batches are unchanged. Rollback consists of switching the worker URL
back to `format=compact` or a legacy v1 URL.

## Data path

Active and recently relevant markets are asynchronously loaded from Gamma and
the CLOB sampling catalog, persisted in `active_market_snapshot`, and indexed in
memory by lowercase condition ID. The realtime propose/dispute path performs one
O(1) lookup and never calls Gamma synchronously.

On a catalog miss, the existing durable pending-delivery queue isolates the
event from the realtime batch. The background resolver repairs and persists the
snapshot, then broadcasts `source=delayed_replay` with the original
`processing_key = lowercase(condition_id) + ":" + event_type`.

## Trade context

Each event carries the v3 audit/routing fields plus `processing_key`, `market`,
`tokens`, `candidate_tokens` and microsecond pipeline timestamps. `tokens` are
zipped only when token IDs, outcomes and prices have identical lengths in one
immutable snapshot. A mismatch produces no tokens or candidates and an explicit
server error log.

Candidates require a binary active market that is open, accepting orders and
has an enabled order book. Disputed, 0/1-priced and `other` slug markets are
excluded; only prices at least 0.8 are selected. Sports and esports candidates
also require `moneyline` or `child_moneyline`.

## Sports filtering

Slave applies `sports_types` independently for each downstream connection.
Tags `1`/`sports` and `64`/`esports` identify sports markets. Non-sports events
always pass. Sports events with an empty or non-allowed type are removed from
the batch. No `include_tag_ids` restriction is used.

## Rollout

1. Deploy Master with v4 disabled by absence of v4 subscribers; verify v1-v3.
2. Deploy one Slave and verify its dedicated shared `compact_trade` upstream.
3. Point one shadow worker at v4 and compare processing keys and candidates.
4. Roll Slaves one at a time, then switch workers in small groups.
5. Roll back clients by URL immediately; binaries remain backward compatible.

