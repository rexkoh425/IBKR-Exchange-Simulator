# Equity Matching Engine

A deterministic Rust limit-order matching engine for synthetic exchange
experiments. The implementation provides price-time priority, partial
and multi-level fills, cancellation, replace-as-cancel-and-new, integer tick/lot
arithmetic, and invariant checks. The IBKR capture command is read-only and never places orders.
Each price level stores a FIFO chain keyed by order IDs, allowing cancellation
to unlink an order without shifting the rest of the queue.

## Run

```powershell
cargo run --release
cargo test
```

Prices are integer ticks and quantities are integer lots. Each accepted submit
or cancel advances a local sequence. A replacement requires a fresh order ID so
that the new order receives no inherited queue priority.

## Synthetic matcher benchmark

Run `cargo run --release -- benchmark --events 1000000 --seed 2026 --trials 5`
to generate a deterministic mixed submit/cancel workload before timing the
matcher. It runs a warm-up, then emits one CSV row per measured trial. The
output reports submit/cancel/fill counts, commands/s, per-command p50/p95/p99
and maximum service time, and the final state fingerprint. The seeded workload
includes crossing prices and actual fills. Timing includes per-command clock
reads. This isolated matcher harness is not an open-loop queue or broker-feed
measurement.

The first 10M-event pinned run is documented in
[`benchmarks/matcher-10m-2026-11-09.md`](benchmarks/matcher-10m-2026-11-09.md),
with raw trial rows in the adjacent CSV. Treat that evidence as specific to its
recorded machine, workload, and timing boundary.

## Bounded pipeline benchmark

`cargo run --release -- pipeline-benchmark --events 10000000 --rate 1000000 --capacity 65536 --trials 5`
runs an open-loop synthetic producer through a bounded blocking channel and
checks the completed event count and payload checksum. It measures scheduled-
arrival-to-processing latency and producer-outstanding high-water mark. The consumer
currently performs only a deterministic checksum operation; this is a queue
baseline, not a matching-plus-persistence or IBKR pipeline measurement. Add
`--queue spsc` to benchmark a bounded `rtrb` SPSC ring. Use SPSC only when the
architecture truly has one producer and one consumer. Both paths retain and
count events under pressure instead of silently overwriting them.
Pin the benchmark threads with `--producer-core N --consumer-core M`; use IDs
reported as `available_cpu_ids` by the program and verify both pin-success
columns in the CSV. An example configuration is `--rate 1005000 --queue spsc
--producer-core 0 --consumer-core 12`.
The 10M-event trial results are in
[`benchmarks/pipeline-10m-2026-11-09.md`](benchmarks/pipeline-10m-2026-11-09.md).
The first comparison on this Windows machine reached about 998k completed
events/s for both queues. In a corrected 1M-event run, SPSC queue residence was
below 10 microseconds in two trials but had a 286.7 microsecond p99 outlier in
the third; throughput was below 1M/s throughout. This does not establish the
target. See [queue residence results](benchmarks/queue-residence-1m-2026-11-09.md)
and the [earlier comparison](benchmarks/queue-2m-2026-11-09.md). Queue residence
and scheduled-arrival-to-completion are reported separately.

With SPSC producer/consumer pinned to CPUs 0 and 12 and the offer rate set to
1.005M/s, three 10M-event trials completed at 1.0003–1.0005M/s with 0.597–0.623
µs p99 scheduled-to-complete and exact checksums. This meets the synthetic queue
target under that configuration; millisecond maximum outliers remain. Full
provenance and limits are in
[`benchmarks/pinned-spsc-10m-2026-11-09.md`](benchmarks/pinned-spsc-10m-2026-11-09.md).

### SPSC telemetry optimization

`pipeline-benchmark --queue spsc` samples queue occupancy every 64 pushes by
default. Set `--occupancy-sample-interval 1` to measure after every push, or
`--burst-size 32` to replay 32-event arrival bursts at the configured average
rate. The benchmark validates count, event order and an order-sensitive
checksum, and reports full-queue retries. See the [10-trial before/after
results](benchmarks/pipeline-optimization-2026-11-10.md) and its raw CSV files.
The measured synthetic throughput improvement does not represent IBKR market
data throughput; use `market-data-benchmark` for isolated normalization,
JSONL persistence, decoding and replay stages.
It does not establish those metrics for the IBKR capture or persistence path.

## Simulated exchange order gateway

`cargo run --release -- exchange-sim --input fixtures/exchange-session.jsonl`
replays client order requests as newline-delimited JSON and writes execution
reports as JSONL. Input can also come from standard input. Each request includes
`client_id`, a client-scoped idempotent `request_id`, and a tagged `command`:
`new`, `cancel`, or `replace`. The gateway emits monotone report sequence
numbers, exchange order IDs, engine sequence numbers, fill prices/quantities,
cumulative fills, leaves, and explicit reject reasons. A repeated identical
request replays its original reports; reusing a request ID for a different
command is a protocol error.

The simulator enforces optional quantity and price-times-quantity notional
limits with `--max-order-quantity` and `--max-order-notional`. Replace uses a
new client order ID, cancels the old order, and sends the replacement to the
back of its new price level; rejected replacements leave the old order live.
All clients share the same deterministic matching book, and client IDs scope
client order IDs. This is an in-process JSONL simulator, not a network FIX
gateway, and does not model exchange latency, auctions, fees, or venue-specific
order types.

The example fixture exercises acknowledgement, partial fill, cancel/replace,
full fill, and an idempotent retry. The gateway checks book/index consistency
after replay and returns a state fingerprint for repeat-run comparisons.

## Event log and replay

`eventlog` provides schema-versioned JSONL records for synthetic submit/cancel
commands. The recorder uses a local, contiguous ordinal, and replay rejects
unknown schema versions or gaps in that ordinal. Replaying the same stream
produces the same receipts and stable state fingerprint. The fingerprint covers
the engine state needed for future commands; it is not a cryptographic digest.

`market_data` defines versioned L1 quote/trade events and venue-scoped L2
position-based insert/update/delete records. Replay handles snapshots, gaps,
disconnects, and invalid books. JSONL round trips are supported. These are
provider-neutral replay types and fixtures.

## IBKR read-only capture

Copy `.env.example` to `.env`, then configure the socket/contract values. Start
TWS or IB Gateway and log in there; the API client does not take account
credentials. Run `cargo run --release -- ibkr-capture`. It subscribes to L1 and
positional market depth and writes timestamped JSONL under `data/ibkr/`. The
capture stores raw SDK debug payloads for inspection by default. Set
`IBKR_CAPTURE_RAW_PAYLOAD=false` to skip formatting and writing those raw strings;
the normalized market events remain, but the JSONL loses the original SDK text
needed for raw-payload audit/debug inspection. A synthetic paired benchmark
measured about 98% higher throughput without this formatting, but that is not a
live IBKR or disk-persistence measurement. Code 317 depth reset is recorded
explicitly. Terminal stream errors are logged as gaps. A bounded
writer queue (set by `IBKR_QUEUE_CAPACITY`) moves JSON encoding and disk writes
off the polling loop. If that queue fills, capture stops and appends a gap
record after draining queued events; it does not claim a complete capture.

This is an initial connected capture, not a lossless production feed handler.
The SDK has internal subscription buffering, the capture polls both streams on
one thread, and process/disk/network failures can still lose data. The local
bounded writer queue detects pressure but cannot bound the SDK's own internal
queues. A local
timestamp/ordinal only gives capture order, not exchange order. IBKR market-depth
initial rows do not provide an authoritative end-of-snapshot marker here, so do
not treat the reconstructed book as valid without a separate completeness rule.
Market data and depth access require the appropriate IBKR permissions and
subscriptions; depth venues and line limits constrain what is available.

## Project status

This repository is being implemented in phases. No live market data or broker
connectivity has been verified in this environment.
