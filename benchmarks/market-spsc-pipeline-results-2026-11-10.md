# Market event SPSC pipeline results — 2026-11-10

## Summary

Added `market-data-spsc-benchmark`, which generates IBKR-shaped L1/L2 callbacks, normalizes them into `MarketEvent`, sends them through the bounded `rtrb` SPSC queue, and applies them to `MarketReplay`. The producer streams events instead of retaining a million-event input vector. The command checks callback/event counts, consumer order, event checksums, and final replay state.

The replay allocation change in `92afe5f` improves the isolated replay stage, but the 1M-event integrated A/B does not establish a pipeline throughput improvement. Results are noisy and mixed. The measured producer-side cost of formatting and retaining raw callback text is substantial in the synthetic path. Removing that text is a meaningful throughput tradeoff, but it changes the persisted event's audit content, so live capture behavior remains unchanged.

## Experimental setup

- CPU: AMD Ryzen 9 9900X, 12 physical cores / 24 logical processors; Windows 11 Pro; `rustc 1.99.1 (58cf8f9c3 2026-10-13)`.
- Release builds on both versions; producer pinned to logical CPU 0, consumer to logical CPU 12. Both pins succeeded.
- The baseline executable uses the original replay lookup from commit `6562589`; the candidate uses the lookup change in `92afe5f`. Both use the same SPSC harness and instrumentation.
- Workload: one million synthetic callbacks per A/B trial, one synthetic instrument, L1/L2 mix, capacity 65,536, maximum `u64` offered rate (unpaced), ten alternating baseline/candidate pairs.
- Instrumentation-off means sampled stage timestamps and occupancy reads are disabled in both variants. The sample-64 comparison enables the identical sampling in both variants.
- Unpaced runs build a queue backlog. Their scheduled-to-completion and release-lateness numbers are not paced-arrival latency measurements. Queue residence remains a separate measured quantity.
- These are synthetic callbacks, not captured IBKR traffic. CPU time, RSS, cache misses and context-switch counts were not collected for this market benchmark. Windows ETW/WPR profiling was unavailable due to missing performance-profiling privilege.

## Integrated 1M-event A/B

| Instrumentation | Baseline median events/s (range) | Candidate median events/s (range) | Paired wins | Median paired change |
|---|---:|---:|---:|---:|
| Disabled | 665,133 (603,845–693,511) | 631,132 (566,103–669,288) | 3/10 | -4.05% |
| Sample every 64 events | 647,155 (610,955–702,856) | 674,265 (610,514–681,846) | 6/10 | +0.89% |

All trials consumed all one million queue events, had zero order errors, matched producer and consumer checksums, and produced the same replay fingerprint (`4bd5a8a8f867b8f4`). The small sampled median difference is not a robust integrated throughput gain. With sampling enabled, median producer normalization p99 was 2.2 µs for both versions. The isolated replay stage improved from 17.77M/s in the baseline 1M stage run to 21.82M/s in the candidate run; that stage is not the full-path bottleneck.

## Producer payload formatting diagnostic

Ten paired 100K-event runs compared the same candidate build with and without synthetic `format!("{tick:?}")` / `format!("{depth:?}")` raw-payload construction. With formatting enabled, median throughput was 670,604 events/s (606,492–724,635); with it disabled, 1,337,835 events/s (1,223,789–1,440,642), with the no-raw variant faster in 10/10 pairs. Both variants reconstructed the same normalized replay fingerprint and each run passed its internal order/checksum agreement.

This comparison intentionally changes `MarketEvent.raw_payload`; it is not byte-for-byte event equivalence. The IBKR capture loop currently formats raw L1/L2 callback debug text and stores it in JSONL. The result identifies a large optional cost, not an unconditional optimization: retaining raw source data supports audit/debug replay. The benchmark's `--no-raw-payload` flag exists only to measure that tradeoff; it does not alter live capture defaults.

The capture now supports `IBKR_CAPTURE_RAW_PAYLOAD=false` as an explicit opt-in to omit this formatting from live capture. The default remains `true`. This setting changes JSONL audit content and has not been measured against an actual IBKR session or disk writer.

## Paced load and capacities

One 100K-event characterization run per setting, sample interval 64, capacities 256 / 4,096 / 65,536:

| Offered rate | Capacity | Completed/s | Full retries | Scheduled-to-completion p99 | p99.9 |
|---:|---:|---:|---:|---:|---:|
| 250K/s | 256 | 249,996 | 0 | 2.60 ms | 4.86 ms |
| 250K/s | 4,096 | 249,995 | 0 | 1.62 ms | 4.69 ms |
| 250K/s | 65,536 | 249,994 | 0 | 2.80 ms | 4.86 ms |
| 500K/s | 256 | 499,979 | 276,462 | 4.17 ms | 4.96 ms |
| 500K/s | 4,096 | 499,982 | 0 | 13.7 µs | 35.5 µs |
| 500K/s | 65,536 | 499,976 | 0 | 6.9 µs | 37.6 µs |
| 1M/s | 256 | 614,414 | 0 | 62.3 ms | 62.7 ms |
| 1M/s | 4,096 | 608,897 | 0 | 63.6 ms | 64.2 ms |
| 1M/s | 65,536 | 716,089 | 0 | 39.1 ms | 39.6 ms |

These are single-run characterization results, not stable estimates. At 1M offered events/s the full producer/consumer path fell below the target rate and accumulated substantial release lateness; increasing queue capacity did not make the producer sustain 1M/s. At 500K/s with capacity 256, the producer retried pushes heavily while preserving all events. Queue-full retries are counted attempts, not dropped events: the benchmark retains the pending event and spins until it can enqueue.

The earlier telemetry-only test on a trivial synthetic `PipelineEvent` measured an 82.9% gain by reducing per-event occupancy reads. That gain does not carry over to this `MarketEvent` path: with identical instrumentation in the 1M market A/B, the no-sampling comparison favored baseline and the sample-64 comparison had only a 0.89% median paired change. This confirms that telemetry overhead is not the dominant cost in the market pipeline.

## Artifacts

- [`market-spsc-ab-1m-uninstrumented-2026-11-10.csv`](market-spsc-ab-1m-uninstrumented-2026-11-10.csv) — ten paired uninstrumented runs.
- [`market-spsc-ab-1m-sampled64-2026-11-10.csv`](market-spsc-ab-1m-sampled64-2026-11-10.csv) — ten paired sampled runs and stage p99s.
- [`market-spsc-payload-format-cost-100k-2026-11-10.csv`](market-spsc-payload-format-cost-100k-2026-11-10.csv) — ten paired raw-payload formatting diagnostics.
- [`market-spsc-paced-capacity-sweep-100k-2026-11-10.csv`](market-spsc-paced-capacity-sweep-100k-2026-11-10.csv) — paced 250K/500K/1M rates and three capacities.
- [`market-spsc-before-after-100k-2026-11-10.csv`](market-spsc-before-after-100k-2026-11-10.csv) — initial 100K paired A/B.
- [`market-spsc-telemetry-overhead-100k-2026-11-10.csv`](market-spsc-telemetry-overhead-100k-2026-11-10.csv) — telemetry on/off diagnostic.

Reproduce the unpaced full-path benchmark with:

```powershell
cargo run --release -- market-data-spsc-benchmark --events 1000000 --rate 18446744073709551615 --capacity 65536 --sample-interval 0 --warmup-events 100000 --producer-core 0 --consumer-core 12
```

Use `--sample-interval 64` for sampled stage/queue telemetry and `--no-raw-payload` only to quantify the raw-text retention tradeoff.

## Correctness and limits

`cargo fmt --check`, `cargo clippy --all-targets -- -D warnings`, `cargo test` (28 passed), and `cargo build --release` pass. The new unit test also verifies deterministic replay, checksums and event ordering through the queue.

The test establishes correctness for this synthetic benchmark. It does not prove that live IBKR capture or JSONL persistence is lossless under real market bursts, nor does it prove that this system can sustain 1M market events/s. Pacing tails and 1M/s shortfall remain unresolved; CPU scheduling is a possible contributor, but no OS trace was available to attribute the millisecond-scale lateness.
