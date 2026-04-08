# Equity Matching Engine

A deterministic Rust limit-order matching engine for synthetic exchange
experiments. The current implementation provides price-time priority, partial
and multi-level fills, cancellation, replace-as-cancel-and-new, integer tick/lot
arithmetic, and invariant checks. It does not connect to a broker or place orders.

## Run

```powershell
cargo run --release
cargo test
```

Prices are integer ticks and quantities are integer lots. Each accepted submit
or cancel advances a local sequence. A replacement requires a fresh order ID so
that the new order receives no inherited queue priority.

## Event log and replay

`eventlog` provides schema-versioned JSONL records for synthetic submit/cancel
commands. The recorder uses a local, contiguous ordinal, and replay rejects
unknown schema versions or gaps in that ordinal. Replaying the same stream
produces the same receipts and stable state fingerprint. The fingerprint covers
the engine state needed for future commands; it is not a cryptographic digest.

## Project status

This repository is being implemented in phases. Market-data ingestion, strategy
research, and reproducible load benchmarks are not yet implemented. No live
market data, broker connectivity, or performance results are claimed.
