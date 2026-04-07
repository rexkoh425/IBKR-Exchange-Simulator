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

## Project status

This repository is being implemented in phases. Market-data ingestion, event
recording and replay, strategy research, and reproducible load benchmarks are
not yet implemented. No live market data, broker connectivity, or performance
results are claimed.
