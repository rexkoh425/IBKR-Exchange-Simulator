# Self-trade-prevention gateway benchmark

## Method

Release build on Windows x86_64 with an AMD Ryzen 9 9900X. The benchmark thread was pinned to logical CPU 0. Five trials used the same 10,000-request JSONL workload generated with seed `20261110`. Gateway timing includes parsing, validation, sequencing, matching, reports, and response serialization. See the [raw trial rows](exchange-stp-gateway-2026-11-10.csv).

## Results

| Metric | Median across five trials | Trial range |
| --- | ---: | ---: |
| Throughput | 508.6k requests/s | 489.6k-519.4k/s |
| p50 | 1.3 us | 1.3-1.4 us |
| p99 | 5.0 us | 4.6-5.5 us |
| p99.9 | 47.7 us | 37.9-54.0 us |
| Maximum | 1.061 ms | 0.960-1.218 ms |
| Mismatches | 0 | 0 in every trial |

Against the retained same-workload pre-STP gateway trials, median throughput was 536.0k/s and median p99 was 4.7 us. The STP build measured **5.1% lower throughput** and **6.4% higher p99**. This is a measured cost, not an optimization claim. Both workloads were pinned and used the same seed, event count, and machine.

The measurement is a closed-loop synthetic request benchmark. It does not include network, live IBKR data, queue residence, or exchange latency. CPU and memory were not attributed per stage. These numbers do not represent live-market performance.
