# Engineering decisions

This ledger records decisions that constrain implementation. Revisit a decision when evidence or requirements change; do not silently alter protocol or benchmark meaning.

## DEC-001 — Preserve the existing local history

- **Status:** Accepted, 2026-11-10
- **Decision:** Start the program on a new local branch from the current project HEAD. Preserve all existing commits; do not force-push or push without explicit authorization.
- **Reason:** The local branch contains the project implementation and historical benchmark artifacts, while its tracking branch is behind by one commit and local history must not be rewritten.
- **Consequence:** Compare/resolve upstream changes deliberately before any future integration. Never claim local-only work is on GitHub.

## DEC-002 — Keep matching deterministic and integer based

- **Status:** Accepted (existing behavior)
- **Decision:** Use integer price ticks and quantity lots; matching is price then FIFO time, with fills at the resting order's price.
- **Reason:** Avoid floating-point ordering ambiguity and make seeded replay repeatable.
- **Consequence:** Adapters must normalize external prices/sizes before submitting. Reject unrepresentable values rather than round silently.

## DEC-003 — Keep simulated exchange books isolated by symbol

- **Status:** Accepted (existing behavior)
- **Decision:** Each instrument owns an independent matching engine; symbols are not inferred from IBKR depth positions.
- **Reason:** Prevent cross-instrument matching and preserve clear data ownership.
- **Consequence:** New order lifecycle and market-data publisher work must retain per-symbol isolation.

## DEC-004 — Treat concurrent sequencing as serialization, not deterministic arrival

- **Status:** Accepted (existing behavior)
- **Decision:** The in-process mutex is the sequencing point. Global sequences are unique/monotone in the order requests acquire it, but racing acquisition order is not guaranteed to repeat across runs.
- **Reason:** A global total order exists only after the system observes and serializes arrivals; OS scheduling is outside the engine's deterministic model.
- **Consequence:** Journal/replay reproduces recorded order. Do not promise identical order for the same set of independently racing producers.

## DEC-005 — Journal checksum policy must separate integrity from state verification

- **Status:** Accepted, batch 1
- **Decision:** Retain schema-v1 recovery. Schema v2 verifies every replayed command outcome and record checksum, performs full exchange-state fingerprints at configured checkpoints, and computes a fresh full fingerprint at recovery completion. Default to a checkpoint every 4,096 commands, with interval 1 available for full-checksum-every-entry verification.
- **Reason:** The current v1 path sorts and serializes growing order/request history for every record, causing ~1.6K commands/s on the 1K benchmark. Replacing full hashes with a rolling hash alone would weaken state verification; checkpointed full hashes plus final recomputation preserve an independent state check while reducing repeated traversals.
- **Consequence:** Schema version and checkpoint presence are explicit per record; checkpoint and flush policies are exposed in CLI/benchmark options. Benchmarks compare equivalent input and retain v1 artifacts. Record checksums are non-cryptographic integrity aids, not proofs or substitutes for reference tests.

## DEC-006 — Flush is not crash durability

- **Status:** Accepted (existing journal behavior)
- **Decision:** `Write::flush` means data is passed to the writer/OS; it is not `fsync`/`FlushFileBuffers` and does not promise power-loss durability.
- **Reason:** Current implementation and evidence verify complete-record restart/replay only.
- **Consequence:** Any batching or sync option must name its boundary precisely and preserve a fail-stop rule on write errors.

## DEC-007 — Keep exchange journals separate from market-data recordings

- **Status:** Accepted
- **Decision:** The order-command journal records simulated exchange requests/results. IBKR market-depth capture has its own schema, source metadata and gap/completeness semantics.
- **Reason:** An order event log cannot stand in for vendor market data, and IBKR L2 depth is not individual exchange orders.
- **Consequence:** Any future publisher/backtest integration must explicitly translate between these domains and retain provenance.

## DEC-008 — Benchmark boundaries are named and frozen

- **Status:** Accepted
- **Decision:** Report core matching, gateway, journaling, replay, publisher, network and full-path numbers separately. Freeze workload inputs; record build, machine, affinity, trial count and latency boundary.
- **Reason:** Pinned and unpinned results, or matching-only and full-path results, are not directly comparable.
- **Consequence:** Only repeatable paired measurements justify optimization claims; synthetic results are never called live-market performance.

## DEC-009 — No protocol compliance by resemblance

- **Status:** Accepted
- **Decision:** FIX/OUCH may inform terminology and state design, but protocol compliance is claimed only after implementing and validating the actual wire specification.
- **Reason:** A JSON simulator and selected protocol concepts are not interoperable FIX/OUCH implementations.
- **Consequence:** Current gateway remains explicitly in-process JSONL until a separately tested network protocol exists.

## DEC-010 — Initial advanced order semantics

- **Status:** Accepted and implemented in matcher and in-process gateway (`fab4ef9`)
- **Decision:** GTC remains the default; IOC takes available crossing liquidity then cancels its remainder; FOK uses a serialized all-or-none preflight over visible book quantity; post-only rejects a would-take order without repricing.
- **Reason:** These policies are deterministic with the current single sequencing point and preserve existing price-time priority for any quantity that rests.
- **Consequence:** FOK check and match commit are atomic under the same serialized request. An unfillable FOK or crossing post-only request does not mutate book state or allocate an engine sequence/exchange order ID. The client order ID remains reserved under current gateway semantics. Replacement flags govern the new order and never transfer old priority. Terminal cancel reports expose zero leaves.

## DEC-011 â€” Self-trade prevention cancels the incoming remainder

- **Status:** Accepted policy; implementation pending
- **Decision:** Stop the incoming order before its next eligible maker if both orders belong to the same client. Keep the resting order and cancel any incoming remainder; external fills completed earlier in the price/FIFO walk remain valid.
- **Reason:** This deterministic policy avoids internal self-fills while preserving the resting order's priority and making no unsupported venue-specific compliance claim.
- **Consequence:** FOK availability excludes same-client orders. The self-trade guard is evaluated at the same serialized matching point as preflight/commit. The incoming exchange order ID and engine sequence are allocated only if the order passes policy preflight; an encountered self order after matching begins cancels the remainder.
