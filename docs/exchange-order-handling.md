# Simulated exchange order-handling specification

## Command identity and client isolation

- A request is identified by `(client_id, request_id)`. An identical retry returns its cached response and does not apply the command again. Reusing that key with different command content is a protocol rejection.
- A successful retry response may include the original reports and sequence numbers, but the journal's execution-event history records those sequence numbers once.
- A client order ID is scoped to its client and cannot be reused during the exchange lifetime, even after its order is filled or cancelled.
- Only the owning client may cancel or replace an order. A cancel or replace of an unknown, terminal, or foreign order is rejected without changing the book.

## Limit orders and priority

- Prices are positive integer ticks; quantities are positive integer lots and must satisfy configured quantity and notional limits.
- Books are isolated by symbol. A buy crosses the lowest resting ask at or below its limit; a sell crosses the highest resting bid at or above its limit.
- Matching consumes the oldest order at the best price first. Trades execute at the resting order's price. A remaining incoming quantity rests with a new priority sequence.
- Cancel removes remaining quantity. Replace is cancel plus a newly accepted order with a new client order ID and loses all previous time priority. A rejected replacement leaves the original order unchanged.
- Each execution report has a monotonically increasing global report sequence and is delivered to the client represented by that report. A fill produces reports for both maker and taker.

The production matcher uses indexed price levels and FIFO queues. A test-only reference model uses a flat order vector and scans it for the best eligible order, intentionally trading speed for implementation independence. It compares per-command outcomes, fills, visible resting-book order, quotes and periodic complete state fingerprints.

## Journal and recovery guarantees

Exchange journals use JSONL schema version 1 and a monotonically increasing global journal sequence. Each complete line records the request, accepted response or rejection, post-command state checksum and record checksum. Recovery validates sequence, schema, checksum, replayed outcome, and post-command checksum before returning reconstructed state. An incomplete final line is reported as truncated; records are never silently skipped.

The writer flushes each line to its `Write` target. The file CLI does not call `fsync`/`FlushFileBuffers`; therefore this supports restart from complete records visible to the OS, but does **not** claim durability against power loss, OS crash, or storage-controller cache loss. A write error after an in-memory command may leave memory ahead of the persisted log; stop and recover from the journal rather than continuing that instance.

This exchange command journal is separate from IBKR market-depth recording and does not substitute for vendor feed capture.

## Benchmark boundaries

`exchange-benchmark` freezes JSONL requests before timing. `core` is the existing synthetic matcher submit/cancel workload. `gateway` includes JSON parsing, validation, sequencing, matching, report creation and response serialization. `journal` uses the same request bytes and includes exchange journal serialization, checksum work, and a flush per entry to a local temporary file. `replay` validates and applies the journal bytes held in memory, excluding disk-read latency. A requested logical CPU is set on the benchmark thread; the harness exits if pinning fails.

Results are synthetic single-process measurements. They do not represent IBKR feed latency, network gateway performance, open-loop queue latency, or live-market throughput. Per-command timing includes clock-read overhead. The journal benchmark's per-entry full-state checksum makes cost depend on resting book size.
