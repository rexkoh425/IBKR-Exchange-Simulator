# SPSC pipeline optimization — 2026-11-10

## Result

The benchmark's per-event occupancy measurement called `Producer::slots()` after every push. `rtrb` documents that method as an Acquire load of the consumer index. The queue already performs its own synchronization; this extra load was only for benchmark telemetry and pulled the consumer's cache line into the producer on every event.

The candidate samples occupancy every 64 pushes. It does not change the queue, event ordering, or the queue's Acquire/Release synchronization. The sample is a sampled peak and can miss short occupancy spikes. The baseline and candidate are two settings of the same executable, built with the same compiler and flags.

| Workload and metric | Every-push baseline | Every-64 candidate | Change |
|---|---:|---:|---:|
| Unpaced throughput, 5M events/trial, capacity 4096, 10 paired trials | 9.21M events/s median | 16.84M events/s median | +82.9%; candidate faster in 10/10 pairs |
| 1M/s paced throughput, 1M events/trial, capacity 65536, 10 paired trials | 1,000,001/s median | 1,000,001/s median | No change; producer pacing limited the result |
| Scheduled-to-completion p50 at 1M/s | 200 ns | 200 ns | No change |
| Scheduled-to-completion p99 at 1M/s | 442,350 ns | 308,100 ns | 30.3% lower; candidate lower in 9/10 pairs |
| Scheduled-to-completion p99.9 at 1M/s | 4,637,300 ns | 4,619,200 ns | 0.39% lower; not a meaningful tail improvement |
| Maximum scheduled-to-completion across all 10 paced trials at 1M/s | 5,169,700 ns | 5,117,000 ns | 1.0% lower; scheduling spikes remain |
| Queue-residence p99 at 1M/s | 600 ns | 900 ns | 50% higher |
| CPU, separate unpaced resource trials | 1.61 core equivalents | 1.43 core equivalents | 11.5% lower median process CPU |
| Peak sampled working set, same resource trials | 124.5 MB | 123.1 MB | 1.1% lower; no memory-layout change was made |

The throughput gain is the strongest result. Under the 1M/s paced workload, the occupancy sampling change reduced the producer's deadline lag in the middle of the distribution, while the queue-residence p99 rose by 300 ns. Under unpaced maximum throughput, queue-residence p99 rose from 2.0 µs to 2.6 µs and p99.9 from 106 µs to 115 µs. More events are being processed, but the queue tail is slightly longer under saturation. The p99.9 and maximum scheduled-to-completion tails at 1M/s did not materially improve.

## Reproduction and environment

- Benchmark behavior under test: `9fa45e9` (`perf: sample SPSC telemetry off hot path`); `96f10da` only groups the same CLI settings into an options struct.
- CPU: AMD Ryzen 9 9900X, 12 physical cores / 24 logical processors
- OS: Microsoft Windows 11 Pro
- Rust: `rustc 1.99.1 (58cf8f9c3 2026-10-13)`
- Build: `cargo build --release`, thin LTO, one codegen unit
- Affinity: producer logical CPU 0, consumer logical CPU 12; Windows topology query confirmed they are on separate physical cores
- Event: 32-byte record (sequence, scheduled offset, enqueue offset, deterministic `u64` payload); payload generation occurs before timing
- Queue: bounded `rtrb` SPSC, capacity 4096 for unpaced tests and 65536 for the 1M/s A/B
- Correctness: every trial checks event count, expected sequence at each consume, and an order-sensitive rolling checksum

Unpaced command (the maximum `u64` offered rate gives each event a zero-nanosecond target, so no arrival pacing is applied):

```powershell
cargo run --release -- pipeline-benchmark --queue spsc --events 5000000 --rate 18446744073709551615 --capacity 4096 --trials 10 --warmup-events 100000 --producer-core 0 --consumer-core 12 --occupancy-sample-interval 64
```

Use `--occupancy-sample-interval 1` for the every-push baseline. For controlled 1M/s latency, use `--rate 1000000 --capacity 65536` with the same 1M-event workload and compare intervals 1 and 64. Use `--burst-size 32` to schedule 32-event bursts at the same average configured rate.

Raw per-trial artifacts:

- [`pipeline-max-throughput-ab-2026-11-10.csv`](pipeline-max-throughput-ab-2026-11-10.csv) — 10 alternating baseline/candidate pairs, 5M events per run.
- [`pipeline-occupancy-ab-2026-11-10.csv`](pipeline-occupancy-ab-2026-11-10.csv) — 10 alternating pairs at 1M configured arrivals/s.
- [`pipeline-resource-ab-2026-11-10.csv`](pipeline-resource-ab-2026-11-10.csv) — process CPU time and sampled working set for separate 5M-event unpaced runs.
- [`pipeline-max-throughput-2026-11-10.csv`](pipeline-max-throughput-2026-11-10.csv) — 10 candidate-only 5M-event throughput trials.
- [`pipeline-load-grid-2026-11-10.csv`](pipeline-load-grid-2026-11-10.csv) — 250K, 500K and 1M configured arrivals/s across capacities 256, 4096 and 65536, plus 32-event burst runs.

## Findings and limits

- The 1M/s controlled trials complete at the configured rate. They measure latency under a paced workload, not maximum capacity.
- The maximum-throughput result is measured from run start until the consumer drains all 5M events. The timer stops before sorting latency samples. Each trial sustains the workload for roughly 0.3–0.55 seconds.
- In the load grid, 32-event bursts at 250K, 500K and 1M average arrivals/s had median scheduled-to-completion p99 of about 3.4–3.5 µs, with no full-queue retries at capacity 4096. These were three trials per setting, so use them as characterization rather than a stable performance guarantee.
- At capacity 256 under unpaced load, the candidate encountered full-queue retries and retained all events. A full push is retried with spin; it does not drop or overwrite the event. The main 5M-event A/B also had occasional retries, and all event counts, order checks and checksums passed.
- CPU usage was measured externally as process CPU time divided by process lifetime and normalized by 24 logical processors. It includes process startup and shutdown. Working set is sampled every 10 ms and is not an exact peak. The benchmark does not report CPU counters per trial internally.
- A Windows ETW/WPR CPU trace could not be collected in this environment because the session lacks the system performance profiling privilege. The bottleneck diagnosis is based on source inspection and paired A/B measurements; hardware cache-miss and context-switch attribution remains unverified.
- The occasional 4–5 ms latency spikes persist in the paced runs. Their timings are consistent with a scheduling pause, but this experiment did not capture a trace that proves the cause.
- The synthetic queue event is not an IBKR market-data event. The separate `market-data-benchmark` command exercises L1/L2 normalization, JSONL persistence, JSONL decoding and replay. One 100K-callback run completed deterministically at about 958K in-memory normalize/serialize/decode/replay events/s; it is a single diagnostic run, not part of the SPSC A/B claim.

## Resume line

`- Raised synthetic SPSC throughput to 16.8M/s and cut 1M/s end-to-end p99 30% with zero loss`

This is within 115 visible characters including `- `. “Zero loss” applies to the tested synthetic trials, which checked count, sequence and checksum.
