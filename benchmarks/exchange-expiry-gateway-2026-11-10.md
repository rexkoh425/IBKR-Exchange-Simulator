# Gateway benchmark after logical expiry

Five release trials ran on Windows x86_64, AMD Ryzen 9 9900X, pinned to logical CPU 0. Each used the same 10,000 frozen new-order requests and seed `20261110`. This workload has no scheduled deadlines, so it measures the regular gateway path after expiry support was added, not expiry throughput.

| Five-trial median | Before expiry | After expiry |
| --- | ---: | ---: |
| Gateway throughput | 506.5k requests/s | 524.7k requests/s |
| p50 | 1.3 us | 1.3 us |
| p99 | 5.1 us | 5.0 us |
| p99.9 | 41.7 us | 38.3 us |
| Maximum | 1.016 ms | 1.025 ms |
| Mismatches | 0 | 0 |

The later median throughput was 3.6% higher. These runs were not interleaved and are subject to machine noise, so the difference is not evidence that expiry improved performance. The final gateway checksum was `0599ae12c47cc692` in every trial of both runs. See the [previous rows](exchange-session-gateway-2026-11-10.csv) and [expiry rows](exchange-expiry-gateway-2026-11-10.csv).

The benchmark includes parsing, validation, matching, reports and response serialization. It excludes network latency, live IBKR data and scheduled-expiry processing; CPU and memory were not attributed by stage.
