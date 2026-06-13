# Exchange gateway and journal benchmarks

## Method

Release build, Windows x86_64, AMD Ryzen 9 9900X (24 logical CPUs). Benchmark thread pinned to logical CPU 0; pin succeeded. Frozen JSONL workload was generated once with seed `20261110`; each of five repetitions used the same 1,000 new-order requests. The output columns and every raw trial are in [the CSV artifact](exchange-gateway-benchmark-2026-11-10.csv). The matcher core uses the existing fixed-seed mixed submit/cancel workload; gateway, journal and recovery use the frozen JSONL file.

Gateway service time includes JSON parsing, request validation/idempotency sequencing, matching, execution report construction, and response serialization. Journal service time adds journal-record serialization, full state fingerprinting, append and a flush for every line to a local temporary file. Recovery service time includes record parsing, integrity checks, replay, outcome comparison and state checksums. Per-command timings include clock-read overhead.

## Five-trial medians

| Stage | Commands per trial | Throughput | p50 | p95 | p99 | p99.9 | Max | Mismatches |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| Matcher core | 1,000 | 7.89M/s | 100 ns | 200 ns | 400 ns | 2,600 ns | 3,900 ns | 0 |
| Parsed in-process gateway | 1,000 | 519.7k/s | 1.2 µs | 3.0 µs | 8.7 µs | 117.3 µs | 127.4 µs | 0 |
| Gateway with journal | 1,000 | 1.585k/s | 625 µs | 1.171 ms | 1.323 ms | 1.640 ms | 5.693 ms | 0 |
| Deterministic journal replay | 1,000 | 1.578k/s | 635 µs | 1.174 ms | 1.328 ms | 1.667 ms | 5.406 ms | 0 |

All five gateway, journal and replay state checksums matched (`846d557e2be02611`), and every journal replay validated recorded outcomes. The journal was 1,047,310 bytes for 1,000 commands. The low journal/replay throughput is real for this implementation: it computes a full sorted exchange-state fingerprint for every command, so command cost grows with retained order and idempotency history. A 10,000-command exploratory journal trial measured 159 commands/s and about 62.8 seconds, so it was stopped after that first trial instead of being presented as a five-trial result.

The checked-in fixture was also run through `exchange-sim --journal`, recovered by a separate `exchange-recover` process, then extended with one new client request and recovered again. Both original/recovered and resumed/recovered summary files compared byte-for-byte equal; the resumed journal had six contiguous records, eight unique execution reports and final state checksum `17756027732600482575`.

The five-trial gateway benchmark process used 6.28 seconds of sampled CPU time and reached 10.15 MB sampled peak working set. These are process-wide values across all four stages, not per-stage resource attribution; the CSV reports per-stage elapsed time and latency. The pinned 10M core rerun below used 17.14 seconds CPU and 629 MB sampled peak working set, including workload generation and latency sample storage. Working-set sampling interval was 20 ms.

## 10M pinned matcher rerun

`equity-matching-engine benchmark --events 10000000 --seed 20261109 --trials 5 --warmup-events 100000 --core 0` completed with successful CPU affinity. The identical workload had 7,507,060 submits, 2,492,940 cancels and 4,963,042 trades per trial; all final state fingerprints matched. Median throughput was **5.91M commands/s** (range 5.86M–6.07M/s), with p50 100 ns, p95 300 ns, p99 400 ns and p99.9 600 ns in every trial. The previous 6.68M/s artifact is faster; this rerun does not reproduce that throughput, but still supports the stated 5M/s and <1µs p99 for this synthetic matching-only workload.

Raw rows are in [the matcher CSV artifact](matcher-10m-pinned-rerun-2026-11-10.csv). No pinned/unpinned comparison is used as an optimization claim.

## Limits

This is a synchronous synthetic request workload, not a live IBKR or network gateway benchmark. It does not measure open-loop queue residence, market-data ingestion, kernel/network scheduling, or production persistence durability. The journal flushes but does not call `fsync`/`FlushFileBuffers`; see the [order-handling specification](../docs/exchange-order-handling.md).
