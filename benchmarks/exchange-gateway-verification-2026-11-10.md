# Simulated exchange gateway verification — 2026-11-10

## Delivered

The new `gateway` module wraps the existing deterministic FIFO matcher with a serial, in-process client order gateway. Clients submit limit orders, cancels, and replacements. The gateway assigns exchange order IDs and global execution report sequence numbers, emits per-fill reports for both sides, tracks cumulative/leaves quantities, applies per-order quantity/notional limits, and caches request results for idempotent client retries.

Client order IDs are scoped to a client and cannot be reused. A replace names the live original and supplies a fresh client order ID. It cancels the original and submits a new order, so the replacement loses its old FIFO priority. A rejected replace leaves the old order live. Requests are serialized by the caller; `exchange-sim` replays a JSONL request stream and writes a response per request.

This is a deterministic simulator interface, not a network FIX implementation. It currently models limit orders only and does not model exchange latency, auctions, fees, or venue-specific order types.

## Correctness evidence

- `cargo test`: 36 passed, including a fixed-seed 2,000-request mixed gateway flow that checks book/index invariants after every request and validates cumulative plus leaves quantities.
- Gateway tests cover partial and full fills, cancellation after partial execution, cancel/replace priority loss, risk rejection atomicity, idempotent retries, conflicting request IDs, deterministic replay and state fingerprints.
- `fixtures/exchange-session.jsonl` ran through the release CLI twice. Each run processed 5 requests and emitted 9 reports, with 1 idempotent retry and 0 rejections. Output JSONL was byte-identical; state fingerprint was `30f94680c477f1f2`.
- Full checks passed: `cargo fmt --check`, `cargo clippy --all-targets -- -D warnings`, `cargo test`, and `cargo build --release`.

## Existing matching-engine benchmark sanity check

After adding gateway code, the existing release matcher benchmark was run with:

```powershell
cargo run --release -- benchmark --events 1000000 --seed 2026 --trials 5 --warmup-events 100000
```

The five runs processed the same deterministic 1M-command workload and ended at the same matcher state fingerprint (`5591104470537973419`). Throughput was 7.31M, 7.72M, 8.00M, 7.76M, and 7.45M commands/s; median was **7.72M/s**. Reported per-command p99 service time was 300 ns in all five runs. Maximum service time ranged from 5.43 ms to 5.68 ms.

This is an unpinned, single-session sanity check of the existing matcher path, not a paired comparison against the earlier pinned 6.12M/s result. The workload size, CPU affinity and environment differ, so it does not establish a performance improvement or a gateway throughput figure. No dedicated throughput benchmark for gateway request handling has been added yet.

## Reproduction

```powershell
cargo run --release -- exchange-sim --input fixtures/exchange-session.jsonl
cargo run --release -- exchange-sim --input fixtures/exchange-session.jsonl --max-order-quantity 5000 --max-order-notional 500000
```

Without `--input`, the command reads JSONL from standard input. Each output line contains the request's reports. Fill reports for both the resting and incoming client are included in the triggering request's response; consumers can use `report_sequence` to order the shared simulated exchange event stream. A duplicate request returns the original report sequence numbers and sets `replayed=true`.
