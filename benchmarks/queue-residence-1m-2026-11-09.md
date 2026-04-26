# Queue residence versus scheduled-to-complete latency

Release build on Windows, 1,000,000 events offered at 1,000,000/s, capacity 65,536, three trials per queue, 50,000-event warmup. Queue residence measures enqueue-to-consume; scheduled-to-complete includes producer scheduling and queue delay. These are synthetic payload/checksum measurements, not IBKR results.

| Queue | Trial | Completed/s | Zero loss | p99 queue residence | p99 scheduled-to-complete |
|---|---:|---:|---|---:|---:|
| sync_channel | 1 | 997,034 | true | 172.1 µs | 186.2 µs |
| sync_channel | 2 | 996,471 | true | 88.6 µs | 112.9 µs |
| sync_channel | 3 | 996,881 | true | 113.2 µs | 265.2 µs |
| rtrb SPSC | 1 | 998,214 | true | 0.2 µs | 0.5 µs |
| rtrb SPSC | 2 | 998,265 | true | 0.1 µs | 0.2 µs |
| rtrb SPSC | 3 | 996,614 | true | 286.7 µs | 286.8 µs |

The SPSC queue has very low residence in two runs, with a large tail outlier in the third. It is the strongest candidate for a single-producer/single-consumer hot path, but these three runs do not establish reliable sub-10-µs p99. Completed throughput remained slightly below the offered 1M/s in every trial. The sync-channel high-water counter includes outstanding work beyond exact channel occupancy; do not compare it directly with SPSC ring occupancy.

Next experiments: pin threads to separate physical cores on the intended Linux target; test multiple capacities and sustained rates; record context switches and CPU utilization; then benchmark with actual decode, book updates, and a separate writer. Preserve both latency distributions and aggregate only after more repeated trials.
