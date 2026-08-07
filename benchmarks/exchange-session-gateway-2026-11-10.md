# Open-session gateway benchmark

The release benchmark ran on Windows x86_64 on an AMD Ryzen 9 9900X, pinned to logical CPU 0. Five trials used the same 10,000 frozen JSONL new-order requests and seed `20261110`. This workload leaves the session open throughout; it measures the ordinary request path, not session transition throughput. Gateway timing includes parsing, validation, matching, report creation and response serialization.

| Five-trial median | Before session controls | With session controls |
| --- | ---: | ---: |
| Gateway throughput | 508.6k requests/s | 506.5k requests/s |
| p50 | 1.3 us | 1.3 us |
| p99 | 5.0 us | 5.1 us |
| p99.9 | 47.7 us | 41.7 us |
| Maximum | 1.065 ms | 1.016 ms |
| Mismatches | 0 | 0 |

Median throughput was 0.4% lower and p99 2% higher in this run. The separate, non-interleaved runs are sensitive to noise; these results do not establish a speedup or isolate the cost of the session check. The gateway checksum was `0599ae12c47cc692` in every trial of both runs. The [prior raw trials](exchange-stp-gateway-2026-11-10.csv) and [new raw trials](exchange-session-gateway-2026-11-10.csv) are retained.

Results are synthetic and exclude network and live market-data latency. CPU time and memory were not measured per stage.
