# Pinned SPSC queue result: 10M events

Release build on Windows, AMD64 Family 26 Model 68 (AuthenticAMD), 24 available logical CPU IDs. Producer pinned to CPU 0 and consumer to CPU 12. Each trial offered 10,000,000 deterministic events at 1,005,000/s into a 65,536-entry bounded SPSC ring. The consumer performed a checksum. The queue retries on full, so generated events were retained; completion count and checksum both matched.

| Trial seed | Completed/s | Zero loss | High-water | p99 queue residence | p99 scheduled-to-complete | Max scheduled-to-complete |
|---:|---:|---|---:|---:|---:|---:|
| 2027 | 1,000,472 | true | 641 | 0.5 µs | 0.598 µs | 1.073 ms |
| 2028 | 1,000,470 | true | 149 | 0.5 µs | 0.597 µs | 2.744 ms |
| 2027 repeat | 1,000,297 | true | 1,224 | 0.5 µs | 0.623 µs | 2.720 ms |

This meets 1M completed events/s and <10 µs p99 for the synthetic single-producer/single-consumer checksum benchmark in these three runs. Rare max-latency outliers remain in the millisecond range. The offer rate was 1.005M/s to account for measured harness overhead; the queue itself remained lightly occupied. Do not generalize this result to IBKR, JSON serialization, book reconstruction, multiple producers, other CPUs, or unpinned scheduling. Repeat on the target runtime with representative consumer work and longer soak before using it as a project or resume claim.
