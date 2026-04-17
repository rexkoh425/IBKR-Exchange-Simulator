# Equity Matching Engine

A deterministic Rust limit-order matching engine for synthetic exchange
experiments. The current implementation provides price-time priority, partial
and multi-level fills, cancellation, replace-as-cancel-and-new, integer tick/lot
arithmetic, and invariant checks. It does not connect to a broker or place orders.
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

## Bounded pipeline baseline

`cargo run --release -- pipeline-benchmark --events 10000000 --rate 1000000 --capacity 65536 --trials 5`
runs an open-loop synthetic producer through a bounded blocking channel and
checks the completed event count and payload checksum. It measures scheduled-
arrival-to-processing latency and producer-outstanding high-water mark. The consumer
currently performs only a deterministic checksum operation; this is a queue
baseline, not a matching-plus-persistence or IBKR pipeline measurement.
The 10M-event trial results are in
[`benchmarks/pipeline-10m-2026-11-09.md`](benchmarks/pipeline-10m-2026-11-09.md).

## Event log and replay

`eventlog` provides schema-versioned JSONL records for synthetic submit/cancel
commands. The recorder uses a local, contiguous ordinal, and replay rejects
unknown schema versions or gaps in that ordinal. Replaying the same stream
produces the same receipts and stable state fingerprint. The fingerprint covers
the engine state needed for future commands; it is not a cryptographic digest.

`market_data` defines versioned L1 quote/trade events and venue-scoped L2
position-based insert/update/delete records. Replay handles snapshots, gaps,
disconnects, and invalid books. JSONL round trips are supported. These are
provider-neutral replay types and fixtures; no IBKR client, subscription, or
live capture is included.

## Project status

This repository is being implemented in phases. Live IBKR adapters,
strategy/paper research, and bounded queue pipelines are not yet implemented.
No live market data or broker connectivity is claimed.
