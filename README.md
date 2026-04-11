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
output reports commands/s, per-command p50/p95/p99 and maximum service time,
and the final state fingerprint. Timing includes per-command clock reads. This
isolated matcher harness is not an open-loop queue or broker-feed measurement.

## Event log and replay

`eventlog` provides schema-versioned JSONL records for synthetic submit/cancel
commands. The recorder uses a local, contiguous ordinal, and replay rejects
unknown schema versions or gaps in that ordinal. Replaying the same stream
produces the same receipts and stable state fingerprint. The fingerprint covers
the engine state needed for future commands; it is not a cryptographic digest.

## Project status

This repository is being implemented in phases. Market-data ingestion and
strategy research are not yet implemented. No live
market data, broker connectivity, or performance results are claimed.
