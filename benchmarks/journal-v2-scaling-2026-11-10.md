# Exchange journal v2 checkpoint scaling

## Change and verification model

Schema v1 stores and verifies a complete exchange-state fingerprint after every command. Schema v2 stores a per-record checksum, replays and compares every accepted/rejected command outcome, and stores a complete state fingerprint every configured number of commands. Recovery verifies those checkpoints and computes a fresh full fingerprint at EOF. The default checkpoint interval is 4,096; interval 1 retains full-state verification on every record. Schema-v1 recovery remains supported, including append/resume with v2 records.

The per-record checksum and checkpoint fingerprints are non-cryptographic integrity checks. They do not replace differential/property tests. This experiment reduced repeated state traversal; it did not implement an incremental state hash.

## Method

- Windows x86_64 release build on AMD Ryzen 9 9900X; benchmark thread pinned to logical CPU 0, with affinity confirmed.
- Fixed request seed `20261110`; all five repetitions per workload used identical generated JSONL requests. Journal and recovery stages used the same input file/bytes.
- Flush policy was one record by default. A separate 100K comparison used flush batching of 64 records.
- Timed journal work includes request parsing, matching, report generation, per-record serialization/checksum, checkpoint work, local-file append, and configured flushes. Replay includes record parsing, outcome replay/comparison, checkpoint verification and final full-state fingerprint.
- Latencies include per-entry clock reads. Resource sampling is process-wide at 20 ms, includes all four benchmark stages and workload generation, and is not per-stage attribution.

## Results

Each reported number is the median of five trials. All stages reported zero mismatches; gateway, journal and recovered state checksums agreed. Raw rows are retained in each linked CSV.

| Workload / policy | Journal throughput | Journal p50 / p99 / p99.9 / max | Replay throughput | Replay p50 / p99 / p99.9 / max |
| --- | ---: | --- | ---: | --- |
| 1K, interval 1, flush 1 | 1.58K/s | 629 us / 1.296 ms / 1.635 ms / 5.90 ms | 1.59K/s | 634 us / 1.346 ms / 1.636 ms / 5.64 ms |
| 1K, interval 64, flush 1 | 62.5K/s | 5.1 us / 393 us / 1.083 ms / 1.159 ms | 66.8K/s | 3.6 us / 385 us / 1.071 ms / 1.164 ms |
| 1K, interval 4,096, flush 1 (default) | 157.6K/s | 5.0 us / 24.1 us / 65.3 us / 113 us | 179.4K/s | 3.4 us / 12.1 us / 59.5 us / 114 us |
| 10K, interval 64, flush 1 | 9.46K/s | 5.2 us / 4.43 ms / 11.78 ms / 14.85 ms | 9.56K/s | 3.7 us / 4.53 ms / 11.84 ms / 12.91 ms |
| 10K, interval 4,096, flush 1 | 119.9K/s | 5.1 us / 26.8 us / 67.3 us / 11.08 ms | 135.1K/s | 3.6 us / 11.7 us / 59.9 us / 11.13 ms |
| 100K, interval 4,096, flush 1 | 36.35K/s | 5.3 us / 32.6 us / 62.5 us / 167.0 ms | 37.63K/s | 3.8 us / 11.6 us / 36.0 us / 165.3 ms |
| 100K, interval 4,096, flush 64 | 40.10K/s | 3.2 us / 30.7 us / 62.3 us / 165.6 ms | 37.61K/s | 3.8 us / 11.6 us / 36.2 us / 164.0 ms |

At 1K, interval 4,096 improved journal throughput about **100x** relative to interval 1, with zero correctness mismatches. At 10K, interval 4,096 was about **12.7x** faster than interval 64. At 100K, flush batching improved journal throughput by about **10.3%** but left replay unchanged and retained checkpoint-driven long outliers. Flush interval 1 remains the default to keep the writer visibility window small.

The maximum samples at 10K/100K include rare 11–180 ms spikes while taking full state checkpoints. Throughput declines as retained client/order history grows. The next optimization should reduce checkpoint cost (for example, incrementally maintained state digests with periodic independent full verification), not remove outcome replay or claim that a rolling checksum proves state correctness.

## Sampled process resources

| Run | CPU time across benchmark process | Peak sampled working set |
| --- | ---: | ---: |
| 1K, interval 1 | 6.23 s | 9.98 MB |
| 1K, interval 64 | 0.16 s | 7.33 MB |
| 1K, interval 4,096 | 0.11 s | 7.37 MB |
| 10K, interval 4,096 | 1.11 s | 29.86 MB |
| 100K, interval 4,096, flush 1 | 30.63 s | 256.20 MB |
| 100K, interval 4,096, flush 64 | 29.14 s | 256.18 MB |

These measurements include core, gateway, journal and replay stages in one process and therefore cannot be attributed to an individual stage. Sampling can miss short working-set peaks.

## Reproduction

```powershell
cargo run --release -- exchange-benchmark --events 1000 --trials 5 --seed 20261110 --core 0 --journal-checksum-interval 1 --journal-flush-every 1
cargo run --release -- exchange-benchmark --events 1000 --trials 5 --seed 20261110 --core 0 --journal-checksum-interval 64 --journal-flush-every 1
cargo run --release -- exchange-benchmark --events 100000 --trials 5 --seed 20261110 --core 0 --journal-checksum-interval 4096 --journal-flush-every 1
cargo run --release -- exchange-benchmark --events 100000 --trials 5 --seed 20261110 --core 0 --journal-checksum-interval 4096 --journal-flush-every 64
```

Raw output: [1K interval 1](journal-v2-checkpoint-1-1k-2026-11-10.csv), [1K interval 64](journal-v2-checkpoint-64-1k-2026-11-10.csv), [1K default](journal-v2-default-1k-2026-11-10.csv), [10K interval 64](journal-v2-checkpoint64-10k-2026-11-10.csv), [10K interval 4,096](journal-v2-checkpoint4096-10k-2026-11-10.csv), [100K interval 4,096](journal-v2-checkpoint4096-100k-2026-11-10.csv), and [100K flush 64](journal-v2-checkpoint4096-flush64-100k-2026-11-10.csv).

## Process restart check

The release CLI wrote the fixture journal with flush interval 64, recovered it in a separate process, appended a new request with a different checkpoint interval, and recovered again. Both summary comparisons were byte-for-byte equal; the resumed journal contained six records and eight unique execution reports. Unit tests additionally verify v1 compatibility, v1-to-v2 append, checkpoint mismatch detection, truncation/corruption detection, and final flush behavior.

## Limits

Checkpoint interval trades repeated full-state verification frequency for speed. Full command outcomes are still recomputed and compared on every replay, and an independently computed final state fingerprint is returned for comparison with a separately retained summary. This does not provide crash durability: flush is not `fsync`/`FlushFileBuffers`. Benchmark requests are synthetic; these results do not represent an IBKR session, network gateway or live venue.
