# Development progress

Last audited: 2026-11-10. Current local branch: `codex/exchange-simulator-roadmap`. The branch starts at existing local HEAD `8945ced`; `main` had 56 commits ahead of its tracked `origin/main` and one commit behind at audit. No remote changes were fetched or pushed. Working tree was clean before this program began.

## Milestone tracker

"Actual program commits" counts only commits created for this roadmap, not historical project work. Existing evidence is cited separately so completed features are not repeated.

Current total: **8 program commits**: `f6e88d1`, `c5437fe`, `b59426a`, `176c675`, `c07de0e`, `6866750`, `fab4ef9`, plus this substantive specification/progress update.

| Milestone | Planned | Actual program commits | Status | Existing evidence / verification |
| --- | ---: | ---: | --- | --- |
| Exchange specification and architecture | 12 | 1 | Complete | `f6e88d1` adds the audited roadmap, architecture/boundary decisions and behavior specification. |
| Advanced order handling | 14 | 4 | In progress | `c07de0e`, `6866750`, and `fab4ef9` specify and implement GTC/IOC/FOK/post-only end to end; STP, expiry and session rules remain. This document update records the completed slice. |
| Pre-trade risk controls | 12 | 0 | Partial | Per-order quantity/notional limits exist; client exposure, positions, rate controls and kill switches are missing. |
| Advanced correctness verification | 12 | 0 | Partial baseline exists | `src/reference_matcher.rs`: 100K deterministic commands across five seeds plus proptest; broaden when new semantics land. |
| High-performance journaling | 14 | 3 | In progress | `c5437fe` adds v2 periodic checkpoints, v1 recovery/resume, configurable flush batches and replay outcome verification. `b59426a` retains five-trial scaling artifacts; `176c675` documents the policy. Incremental state hashing remains future work. |
| Client sessions and sequencing | 12 | 0 | Partial | Client-scoped request idempotency/ownership and mutex serialization exist; no session lifecycle or cancel-on-disconnect. Racing lock acquisition order is not repeatable across independent runs. |
| Exchange market-data publishing | 14 | 0 | Not started | Provider-neutral IBKR market replay exists; simulated-book publisher/subscriptions are absent. |
| IBKR capture reliability | 10 | 0 | Partial | Read-only L1/L2 capture, disconnect/gap markers and depth reset handling exist; reconnect/resubscription completeness is not established. |
| Network order gateway | 14 | 0 | Not started | Current gateway is in-process JSONL; no TCP/FIX transport. |
| Low-latency optimization | 12 | 0 | Partial baseline exists | Pinned matcher and SPSC/multi-stage benchmarks exist. Journal fingerprinting is a known hot spot; no unmeasured speedup claims. |
| Deterministic paper trading | 12 | 0 | Not started | No strategy/accounting simulator. |
| CI, docs and release quality | 12 | 0 | Partial | README, specs and artifacts exist; no `.github` CI workflow was found in the repository audit. |

## Batch log

### Batch 0 — Existing project audit

- **Date:** 2026-11-10
- **Result:** inspected all 56 commits in local history, repository file inventory, clean worktree, current tests/benchmarks/docs, and available remote tracking metadata.
- **Branch:** created `codex/exchange-simulator-roadmap` from local HEAD; no history rewritten and no push.
- **Verification:** existing evidence reviewed in `benchmarks/` and gateway/journal implementation. This audit itself is not counted as a program commit.
- **Next:** complete the journal scaling evidence and then begin the advanced-order vertical slice.

### Batch 1 — Specification and journal checkpointing

- **Date:** 2026-11-10
- **Tasks:** Completed SPEC-01/02/03 as a compact architecture/specification slice. Implemented JOUR-01 profiling baseline, JOUR-02 schema-v2 periodic state checkpoints, and configurable flush cadence as part of JOUR-03.
- **Commits:** `f6e88d1` roadmap/specification/decisions; `c5437fe` schema-v2 checkpointed recovery, CLI policies and tests; the third batch commit retains raw scaling trials and this report/progress record.
- **Verification:** `cargo fmt --check`, `cargo clippy --all-targets -- -D warnings`, `cargo test` (51 passed), `cargo build --release`; release CLI process restart, append and recovery summaries compared byte-for-byte equal. Tests cover legacy v1 read/resume, v2 checkpoint mismatch, flush batching, corruption and truncation.
- **Measurements:** CPU 0 affinity confirmed; five fixed-workload trials at each size. Schema-v2 interval 4,096 measured 157.6K journal commands/s at 1K, 119.9K/s at 10K, and 36.35K/s at 100K; comparison checksums matched with zero mismatches. Flush batching to 64 gave 40.10K/s at 100K, but is not default. See `benchmarks/journal-v2-scaling-2026-11-10.md`.
- **Finding:** Interval 4,096 is much faster than a full-state fingerprint per command, while preserving outcome comparison on every replay and full state checks at checkpoints/end. At 100K, maximum samples still reached ~167 ms during full-state traversal; incrementally maintained state digests remain future work.
- **Next:** define self-trade prevention semantics and choose reject/cancel policy before coding; then add it to both engines and generated differential tests.

### Batch 2 — Advanced order gateway integration

- **Date:** 2026-11-10
- **Tasks:** completed the first ORD-01/COR-01 vertical slice. TIF and post-only fields flow through JSONL, matcher, gateway, replacement, reports, snapshots, replay fingerprints, and the flat-vector test oracle. Added deterministic policy-rejection and IOC lifecycle tests. Rejected FOK/post-only operations preserve the book, engine sequence, and exchange-ID allocation; client order IDs remain reserved.
- **Commits:** `fab4ef9` implements the gateway path and differential policy coverage; this documentation update records the completed slice.
- **Verification:** `cargo fmt --check`, `cargo clippy --all-targets -- -D warnings`, `cargo test` (60 passed), and `cargo build --release` passed. The existing 100K-command/five-seed reference comparison passed; proptest now generates GTC, IOC, FOK and post-only cases. Added JSONL default-field compatibility and IOC request coverage.
- **Correctness finding:** terminal cancel reports and snapshots now expose zero leaves while retaining cumulative fills. IOC emits fills, then a final cancelled/leaves-zero report when remainder remains. A failed policy check consumes neither engine sequence nor exchange order ID.
- **Performance:** no performance optimization is claimed in this slice. Existing matcher and journal benchmark artifacts remain unchanged.
- **Next:** define self-trade prevention semantics and choose reject/cancel policy before coding; then add it to both engines and generated differential tests.

## Current risks and blockers

- Journal full-state hashing now runs every 4,096 commands by default, but each checkpoint still traverses growing state and produced ~167 ms maximum samples at 100K. An incremental digest must retain replayed-outcome comparison and independent full-state verification.
- Benchmark CPU time and peak working set have only been sampled process-wide, not independently attributed to every exchange stage.
- Concurrent requests are serialized, but mutex acquisition is scheduler-dependent; deterministic replay requires recording the observed serialized order.
- No live IBKR connection or live trading has been verified. Keep those claims separate from fixture/synthetic verification.
- Advanced orders currently support GTC/IOC/FOK/post-only only. STP, mass cancellation, session boundaries, expiration and status query commands remain unimplemented.
