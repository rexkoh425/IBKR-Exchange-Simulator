# Exchange simulator development roadmap

## Purpose and estimation

This is a work plan, not a commit quota. The 150-commit estimate below follows the requested milestone budgets; actual work may use fewer or more commits. A commit is counted only when it captures a tested implementation, a meaningful test, a reproducible benchmark, or a substantive design decision. Do not backdate commits or push without explicit permission.

The audit was performed on local HEAD `8945ced` (56 commits in the local history) on 2026-11-10. Existing features are dependencies and evidence, not work to reimplement. Details and current progress are in [`development-progress.md`](development-progress.md); binding architecture choices are in [`engineering-decisions.md`](engineering-decisions.md).

## Audit summary

| Area | Current state | Main gap |
| --- | --- | --- |
| Matching | FIFO price-time engine, GTC/IOC/FOK/post-only, STP and logical-clock order expiry; pinned 10M benchmark | Richer amendment and venue-specific semantics |
| Gateway | In-process multi-client JSONL, manual exchange session state, client-scoped lookup and mass cancel, reports, limits and idempotency | Client session lifecycle, exposure/position/rate controls, network transport |
| Correctness | Independent flat-vector model, 100K baseline commands across 5 seeds, proptest and invariants | Generated advanced-order cases added; broaden for risk/session types and deep books |
| Journal | v1/v2 JSONL append/recovery, periodic full-state checkpoints, record checksums and truncation detection | Incremental fingerprints, snapshot acceleration and stronger persistence boundaries |
| Market data | Provider-neutral L1/L2 normalization/replay, gaps, bounded writer, read-only IBKR capture | Reconnect/resubscribe lifecycle, stronger completeness/statistics; no exchange-book publisher |
| Queues | Bounded channel and SPSC benchmarks with retry/no-loss checks | Queue tests do not represent the full live market-data path |
| Paper trading | Not present | Deterministic account, positions, fees, slippage and example strategy |
| Network gateway | Not present | Framed local TCP protocol and connection tests |
| Release quality | README, fixtures and benchmark artifacts | CI workflow, cross-platform gates and release process |

## Milestones and work items

Each work item records objective, dependencies, approach, acceptance evidence, tests, benchmark expectation and the estimated commit allocation. Estimates sum to the requested 150 commits; they are planning envelopes, not minimums.

### M1 — Exchange specification and architecture (12 commits)

| Task | Objective, dependencies and approach | Acceptance, tests and benchmark evidence | Est. |
| --- | --- | --- | ---: |
| SPEC-01 | Specify price-time matching, order lifecycle and command/report fields; build on `docs/exchange-order-handling.md`. | Versioned behavior spec with examples; fixture cases for acceptance, fill, cancel and replace; no performance claim. | 4 |
| SPEC-02 | Document module boundaries, sequencing point and market-data/exchange separation; build on current crate layout. | Architecture diagram and decisions reviewed against code; links and CLI examples verified. | 4 |
| SPEC-03 | Define replay, persistence, determinism and benchmark contracts; distinguish simulator, broker feed and network claims. | Explicit durability and measurement boundaries; tests map to invariants; baseline artifact inventory. | 4 |

### M2 — Advanced order handling (14 commits)

| Task | Objective, dependencies and approach | Acceptance, tests and benchmark evidence | Est. |
| --- | --- | --- | ---: |
| ORD-01 | Add TIF/order flags IOC, FOK, GTC and post-only to gateway/matcher request handling. | IOC remainder cancels; FOK either fills all or changes no book state; post-only crossing order rejects; property tests compare reference. | 5 |
| ORD-02 | Define self-trade prevention and implement the selected cancel/reject policy. | Same-client crossing scenarios produce no self-fill; reports and remaining book agree in differential tests. | 3 |
| ORD-03 | Add mass cancel, order status query and exchange-session boundaries. | Deterministic bulk report ordering; no match outside configured session; focused fixtures and state invariants. | 3 |
| ORD-04 | Add expiry and amendment semantics while preserving FIFO rules. | Expiry is deterministic under replay; amended priority is explicitly tested; benchmark common GTC path for regression. | 3 |

### M3 — Pre-trade risk controls (12 commits)

| Task | Objective, dependencies and approach | Acceptance, tests and benchmark evidence | Est. |
| --- | --- | --- | ---: |
| RISK-01 | Retain and formalize current quantity/notional checks before book mutation. | Boundary and overflow tests; rejected order leaves book and priority unchanged. | 2 |
| RISK-02 | Add per-client outstanding quantity/notional, exposure and position limits. | Partial fills, cancel and replace update exposure exactly; replay yields same reject decisions. | 4 |
| RISK-03 | Add request-rate limits, client suspension and global/client kill switches. | Deterministic clock/window tests; kill switch prevents mutation and emits a reasoned report. | 4 |
| RISK-04 | Add risk audit/report fields and adversarial interaction tests. | Retries do not double-count risk; concurrent submissions serialize through risk and matching; measure added gateway cost. | 2 |

### M4 — Advanced correctness verification (12 commits)

| Task | Objective, dependencies and approach | Acceptance, tests and benchmark evidence | Est. |
| --- | --- | --- | ---: |
| COR-01 | Keep current 100K five-seed differential corpus and add command coverage for new order types. | Each command compares fills, reports and full observable book; minimized failures become deterministic fixtures. | 4 |
| COR-02 | Expand generated tests to deep books, many symbols/clients, cancellations, overflow and invalid inputs. | Proptest sequences reproduce from recorded seed; invariants run after every transition. | 3 |
| COR-03 | Add concurrency schedules and state-machine/model tests for risk and sessions. | No duplicate fill/report sequence; ownership and idempotency hold under racing requests; no flaky timing assertions. | 3 |
| COR-04 | Add fixture replay matrix and failure minimization tooling. | Every regression fixture runs in CI; artifacts include compact reproducer and oracle outcome. | 2 |

### M5 — High-performance journaling (14 commits)

| Task | Objective, dependencies and approach | Acceptance, tests and benchmark evidence | Est. |
| --- | --- | --- | ---: |
| JOUR-01 | Profile current v1 per-event full-state fingerprinting and preserve raw baseline. | Profile identifies cost by stage; baseline files remain unchanged; 1K/10K/100K scaling data. | 2 |
| JOUR-02 | Add versioned periodic full-state checkpoints plus per-record checksums. | v1 remains readable; v2 replays every command/outcome, validates checksums/checkpoints and recomputes a full final fingerprint. | 4 |
| JOUR-03 | Add configurable flush/batch/sync policies with explicit persistence guarantees. | Injected write/flush failures stop processing; flush and sync semantics documented and tested. | 3 |
| JOUR-04 | Add snapshot/checkpoint recovery and bounded replay from latest checkpoint. | Snapshot state equals replayed state; corrupt, truncated and mismatched snapshots fail closed. | 3 |
| JOUR-05 | Benchmark journal/recovery scaling and resource use against the retained baseline. | Frozen 1K/10K/100K datasets where feasible; p50/p95/p99/p99.9/max, throughput and resource provenance. | 2 |

### M6 — Client sessions and sequencing (12 commits)

| Task | Objective, dependencies and approach | Acceptance, tests and benchmark evidence | Est. |
| --- | --- | --- | ---: |
| SESS-01 | Add explicit session IDs and lifecycle while retaining client-scoped identity compatibility. | Connect/reconnect/expire transitions are journaled and replayable; session-scoped request dedup is tested. | 4 |
| SESS-02 | Add cancel-on-disconnect and session expiry policy. | Disconnect cleanup produces deterministic reports and leaves no unintended live orders. | 3 |
| SESS-03 | Define the global sequencing point for concurrent producers. | Sequence is unique and monotone in observed serialized arrival order; docs explicitly avoid promising cross-run mutex acquisition order. | 3 |
| SESS-04 | Add reconnect/retry integration fixtures and contention benchmark. | Retries cannot duplicate fills; multi-client stress has stable checksums given a recorded arrival order. | 2 |

### M7 — Exchange market-data publishing (14 commits)

| Task | Objective, dependencies and approach | Acceptance, tests and benchmark evidence | Est. |
| --- | --- | --- | ---: |
| PUB-01 | Publish per-symbol BBO and trade events from simulated exchange transitions. | Subscribers see exactly the reports/trades produced by the matcher; independent fixtures. | 3 |
| PUB-02 | Publish L2 price-level snapshots and sequenced deltas. | Snapshot-plus-delta consumer reconstructs identical observable book at every event. | 4 |
| PUB-03 | Add subscriptions, unsubscriptions and bounded subscriber queues. | Backpressure/gap policy explicit; unsubscribed symbols receive no messages; no silent overwrite. | 3 |
| PUB-04 | Add replay, gap detection and publisher benchmarks. | Per-symbol sequences detect missing deltas; report throughput, latency distribution and queue-full behavior. | 4 |

### M8 — IBKR capture reliability (10 commits)

| Task | Objective, dependencies and approach | Acceptance, tests and benchmark evidence | Est. |
| --- | --- | --- | ---: |
| IBKR-01 | Model connection state, disconnects, resets and capture completeness. | Error 317 and terminal errors mark state uncertain; recorded fixtures verify transitions. | 3 |
| IBKR-02 | Add reconnect/resubscription and explicit fresh snapshot boundary. | No invented updates; stale depth remains invalid until reset/rebuild criteria pass. | 3 |
| IBKR-03 | Add session-level capture health counters and shutdown behavior. | Queue pressure and gaps are recorded; summary agrees with JSONL; tests do not require live credentials. | 2 |
| IBKR-04 | Add opt-in live smoke procedure and operational caveats. | Manual verification steps documented; synthetic test results remain distinct from connected evidence. | 2 |

### M9 — Network order gateway (14 commits)

| Task | Objective, dependencies and approach | Acceptance, tests and benchmark evidence | Est. |
| --- | --- | --- | ---: |
| NET-01 | Define local length-prefixed JSON frame and protocol error rules. | Framing/version limits documented; malformed and oversized frame tests. | 3 |
| NET-02 | Add TCP accept loop and client session mapping around the in-process sequencer. | Core matcher stays transport-independent; multiple clients connect and disconnect in integration tests. | 4 |
| NET-03 | Add heartbeats, acknowledgements, reports and backpressure policy. | Partial reads/writes, abrupt close and slow reader are bounded and tested. | 4 |
| NET-04 | Load-test complete network request path and document limitations. | Frozen multi-client workload with latency distribution, CPU/memory, mismatches and affinity. | 3 |

### M10 — Low-latency optimization (12 commits)

| Task | Objective, dependencies and approach | Acceptance, tests and benchmark evidence | Est. |
| --- | --- | --- | ---: |
| PERF-01 | Establish reproducible profiles for core, gateway, journal and publisher. | Same frozen workload and machine/affinity across stages; allocation/resource profile saved. | 3 |
| PERF-02 | Optimize only measured matcher/gateway hot spots. | Differential/property suite unchanged and green; paired benchmark supports every performance claim. | 4 |
| PERF-03 | Reduce tail latency and allocation/copy costs in queue and serialization paths. | p99/p99.9/max tracked; no loss, ordering or replay regression. | 3 |
| PERF-04 | Add regression thresholds and raw result retention. | CI detects material regression without noisy hard timing assertions on shared runners. | 2 |

### M11 — Deterministic paper trading (12 commits)

| Task | Objective, dependencies and approach | Acceptance, tests and benchmark evidence | Est. |
| --- | --- | --- | ---: |
| PAPER-01 | Define deterministic strategy interface and event clock over recorded input. | No look-ahead access; same event log yields byte-identical strategy actions. | 3 |
| PAPER-02 | Add simulated account, positions, cash, fees and realized/unrealized P&L. | Accounting identity and fill/cancel/fee invariants property-tested. | 4 |
| PAPER-03 | Add explicit slippage/fill assumptions and one example strategy. | Reproducible run report includes assumptions; no claim of predictive performance. | 3 |
| PAPER-04 | Add end-to-end fixture and replay/backtest report. | Market-data replay and exchange matching remain distinct; outputs/checksum reproduced. | 2 |

### M12 — CI, documentation and release quality (12 commits)

| Task | Objective, dependencies and approach | Acceptance, tests and benchmark evidence | Est. |
| --- | --- | --- | ---: |
| REL-01 | Add CI format, Clippy, tests and release build on supported platforms. | CI is green on Windows and Linux; secrets absent from repository. | 4 |
| REL-02 | Add architecture/protocol docs and executable CLI examples. | Commands run in clean checkout; Mermaid and relative links checked. | 3 |
| REL-03 | Add fixture, integration and benchmark artifact workflow. | CI validates deterministic outputs; benchmark runs are opt-in and retain provenance. | 3 |
| REL-04 | Define versioning and release checklist. | Compatibility, journal schema, known limitations and release evidence reviewed. | 2 |

## Commit discipline

Use small conventional commits after a slice passes its focused gates. Keep one change per intent, and prefer several vertical slices over a large batch. Commit dates remain actual dates. Commit allocations above do not justify extra commits if a task is smaller than estimated.
