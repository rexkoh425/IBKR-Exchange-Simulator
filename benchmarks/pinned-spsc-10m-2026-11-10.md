# Pinned SPSC queue: 10M event retest

Windows 10 Pro 25H2, AMD Ryzen 9 9900X (AMD64 Family 26 Model 68), Rust 1.99.1, release profile with thin LTO and one codegen unit. Producer and consumer affinity calls both succeeded for logical CPU IDs 0 and 12. Five 10M-event trials used seed range 2027–2031, capacity 65,536, and an open-loop offer rate of 1,005,000 events/s. Event generation occurred before timing. The consumer performed a checksum; the producer retried when the bounded ring was full.

The median completed rate was **999,958 events/s** (range 999,521–1,000,012). Every trial completed all 10M events and matched its checksum. Queue-residence p99 was 0.6 µs in four trials and 2.86 ms in one. Scheduled-to-complete p99 was below 10 µs in one of five trials; other trials measured 11.2 µs, 19.5 µs, 517.8 µs, and 3.80 ms. P99.9 queue residence was 3.46–26.61 ms, and maximum scheduled-to-complete latency was 5.11–31.65 ms.

This retest shows about 1M events/s and low p99 queue residence in most trials, with large scheduling/backlog tails. It does not establish a reliable end-to-end sub-10-µs p99. The earlier three-trial result dated 2026-11-09 was less variable and should not be used alone as the claim. Raw rows are in the adjacent CSV.

The Windows Performance Recorder CPU profile could not start because this environment lacks the system-performance profiling privilege (`0xc5585011`). A 50-ms external process sampler observed working set up to about 617 MB in an earlier 10M matcher run; treat that as a lower-bound sampled process footprint, not a precise peak for these queue trials.
