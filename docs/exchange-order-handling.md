# Simulated exchange order-handling specification

## Command identity and client isolation

- A request is identified by `(client_id, request_id)`. An identical retry returns its cached response and does not apply the command again. Reusing that key with different command content is a protocol rejection.
- A successful retry response may include the original reports and sequence numbers, but the journal's execution-event history records those sequence numbers once.
- A client order ID is scoped to its client and cannot be reused during the exchange lifetime, even after its order is filled or cancelled.
- Only the owning client may cancel or replace an order. A cancel or replace of an unknown, terminal, or foreign order is rejected without changing the book.
- `cancel_all` affects only live orders owned by the requesting client. It emits ordinary cancel reports in ascending exchange-order ID order; an empty selection returns no reports. The request is idempotent like other commands.
- The exchange starts open. Logical control client 0 can set the session open or closed. A closed session rejects new orders and replacements before allocating order IDs; cancels remain available, and resting orders retain their priority. Client IDs are not authenticated. See [session controls](session-controls.md).
- Clients can assign or clear logical deadlines for their live orders. Control client 0 advances logical time, causing due orders to expire in deadline and exchange-ID order; expiry reports use request ID 0. See [order expiry](order-expiry.md).

## Limit orders and priority

- Prices are positive integer ticks; quantities are positive integer lots and must satisfy configured quantity and notional limits.
- Books are isolated by symbol. A buy crosses the lowest resting ask at or below its limit; a sell crosses the highest resting bid at or above its limit.
- Matching consumes the oldest order at the best price first. Trades execute at the resting order's price. A remaining incoming quantity rests with a new priority sequence.
- Cancel removes remaining quantity. Terminal cancel reports and snapshots use zero leaves; cumulative fills retain the amount already executed. Replace is cancel plus a newly accepted order with a new client order ID and loses all previous time priority. A rejected replacement leaves the original order unchanged.
- Each execution report has a monotonically increasing global report sequence and is delivered to the client represented by that report. A fill produces reports for both maker and taker.

## Advanced order policy

The matcher and in-process gateway implement these policies. `time_in_force` and `post_only` may be sent in JSONL; omitted fields retain the GTC/non-post-only defaults.

- **GTC:** the existing default. Match any immediately executable quantity and rest the remainder.
- **IOC:** execute immediately against all eligible visible price levels. Cancel any remainder without adding it to the book. The client receives fill reports and a final cancelled/leaves-zero report when quantity remains; an IOC with no match is accepted then cancelled.
- **FOK:** preflight the total eligible visible quantity while holding the exchange's serialized sequencing point. If it is insufficient, reject without changing the book, allocating an engine sequence, or allocating an exchange order ID. The client order ID remains consumed under the gateway's existing no-reuse rule. If sufficient, execute the whole quantity atomically; no remainder may rest.
- **Post-only:** reject an order if it would execute against the current opposite book. Do not reprice or adjust it. Rejection leaves the book and engine sequence unchanged; the client order ID remains consumed.
- **Replacement:** remains cancel-and-new with no inherited priority. The replacement's explicit TIF/post-only flags apply to the new order; omitted flags use GTC/non-post-only defaults.

FOK preflight and commit are performed under the same serialized exchange operation. Replacement preflights the new policy before cancelling the old order. The in-process gateway reserves the client order ID for rejected commands, but does not consume an exchange order ID or engine sequence for a post-only or insufficient-depth FOK rejection. These semantics apply only to visible liquidity in this simulator and do not claim exchange protocol compatibility.

Post-only is valid only with GTC. Unsupported combinations are rejected. IOC and FOK replacement orders use the same preflight rules. The gateway emits a final IOC cancelled report when quantity remains, with zero leaves; fills remain separately reported to both clients.

## Self-trade prevention

The simulator uses **cancel incoming remainder**. During the normal best-price/FIFO walk, if the next eligible maker belongs to the incoming client's own account, matching stops before that maker is changed. Any fills already made against earlier external orders remain valid; the incoming order is then cancelled with zero leaves, and the resting self order remains unchanged. If there were no earlier external fills, the gateway reports acceptance followed by cancellation. No self trade is emitted.

For FOK, same-client resting quantity is excluded from preflight availability. If external visible quantity cannot fill the entire order before a self order would be reached, the FOK is rejected without book mutation or engine/exchange ID allocation. The self-trade check and matching walk run within the same serialized gateway operation. Replacements use the same policy after preflighting and preserve the old order if validation rejects.

The production matcher uses indexed price levels and FIFO queues. A test-only reference model uses a flat order vector and scans it for the best eligible order, intentionally trading speed for implementation independence. It compares per-command outcomes, fills, visible resting-book order, quotes and periodic complete state fingerprints.

## Journal and recovery guarantees

Exchange journals use JSONL schema versions 1 and 2 and a monotonically increasing global journal sequence. Version 1 stores a full post-command state checksum on each line. Version 2 stores a per-record checksum and a full state checksum at the configured interval (4,096 commands by default). Recovery validates sequence, schema, record checksum, and the replayed accepted/rejected outcome for every command; it checks each stored full-state checkpoint and computes a fresh full fingerprint at end of replay. Version 1 remains readable. An incomplete final line is reported as truncated; records are never silently skipped.

The default writer flushes each line to its `Write` target; `--flush-every N` can batch N lines and the CLI flushes the final batch before returning. The file CLI does not call `fsync`/`FlushFileBuffers`; therefore this supports restart from complete records visible to the OS, but does **not** claim durability against power loss, OS crash, or storage-controller cache loss. A write error after an in-memory command may leave memory ahead of the persisted log; stop and recover from the journal rather than continuing that instance.

This exchange command journal is separate from IBKR market-depth recording and does not substitute for vendor feed capture.

## Benchmark boundaries

`exchange-benchmark` freezes JSONL requests before timing. `core` is the existing synthetic matcher submit/cancel workload. `gateway` includes JSON parsing, validation, sequencing, matching, report creation and response serialization. `journal` uses the same request bytes and includes exchange journal serialization, per-record integrity hashing, periodic full-state fingerprints, and configured writer flushes to a local temporary file. `--journal-checksum-interval 1` restores a full fingerprint per record for comparison; `--journal-flush-every N` measures batching. `replay` validates and applies the journal bytes held in memory, excluding disk-read latency. A requested logical CPU is set on the benchmark thread; the harness exits if pinning fails.

Results are synthetic single-process measurements. They do not represent IBKR feed latency, network gateway performance, open-loop queue latency, or live-market throughput. Per-command timing includes clock-read overhead. The journal benchmark's per-entry full-state checksum makes cost depend on resting book size.
