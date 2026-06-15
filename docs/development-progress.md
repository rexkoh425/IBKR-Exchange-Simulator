# Development progress

Last audited: 2026-11-10. Current local branch: `codex/exchange-simulator-roadmap`. The branch starts at existing local HEAD `8945ced`; `main` had 56 commits ahead of its tracked `origin/main` and one commit behind at audit. No remote changes were fetched or pushed. Working tree was clean before this program began.

## Milestone tracker

"Actual program commits" counts only commits created for this roadmap, not historical project work. Existing evidence is cited separately so completed features are not repeated.

| Milestone | Planned | Actual program commits | Status | Existing evidence / verification |
| --- | ---: | ---: | --- | --- |
| Exchange specification and architecture | 12 | 0 | In progress | `docs/exchange-order-handling.md`, README, gateway/journal benchmark evidence; roadmap and ADR ledger being added in batch 1. |
| Advanced order handling | 14 | 0 | Not started | Limit orders, cancel and replace exist; IOC/FOK/post-only/STP/expiry/session rules are missing. |
| Pre-trade risk controls | 12 | 0 | Partial | Per-order quantity/notional limits exist; client exposure, positions, rate controls and kill switches are missing. |
| Advanced correctness verification | 12 | 0 | Partial baseline exists | `src/reference_matcher.rs`: 100K deterministic commands across five seeds plus proptest; broaden when new semantics land. |
| High-performance journaling | 14 | 0 | Baseline measured; optimization next | Journal v1 recovery and corruption tests pass; every record computes full state fingerprint; measured median ~1.585K commands/s on the 1K workload. |
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
- **Next:** define the engineering plan and decisions, then improve journal scaling while retaining schema-v1 recovery and full-state verification.

## Current risks and blockers

- Journal full-state hashing is O(state size) per request and dominates the append/replay measurement. A faster checksum mode must retain replayed-outcome comparison and independently recomputable full-state fingerprints.
- Benchmark CPU time and peak working set have only been sampled process-wide, not independently attributed to every exchange stage.
- Concurrent requests are serialized, but mutex acquisition is scheduler-dependent; deterministic replay requires recording the observed serialized order.
- No live IBKR connection or live trading has been verified. Keep those claims separate from fixture/synthetic verification.
