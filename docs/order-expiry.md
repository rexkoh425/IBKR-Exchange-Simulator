# Deterministic order expiry

## Behavior

- Exchange logical time starts at zero and advances only through a journaled `advance_time` command from logical control client 0. Backward time is rejected without changing state. Client IDs are not authenticated.
- An order owner can set or clear a deadline on an existing live order with `set_expiry`. A deadline must be strictly later than the current logical time. An invalid or foreign order cannot be scheduled.
- Advancing time expires all live orders whose deadlines are at or before the new time. Expirations are ordered by deadline, then exchange order ID, and produce an `expired` report for each owner with request ID 0 to identify exchange-generated reports.
- Expiry is allowed while the trading session is closed. An expired order leaves the book, retains its cumulative fills, and has zero leaves. Cancel, fill and replacement remove the old order's deadline; a new replacement does not inherit it.
- Identical command retries return cached responses and cannot expire or schedule orders twice. The clock and outstanding deadlines are included in state fingerprints and reconstructed by journal replay.
- Clock advancement preflights the required matcher and report sequence capacity. If capacity is insufficient, it rejects before changing the clock or any order. Order snapshots expose their deadlines, and recovery summaries expose logical time.

## Verification

Tests cover exact deadline boundaries, ordering, ownership, partial fills, cancellation and replacement cleanup, idempotency, backward-clock rejection, closed-session expiry and sequence exhaustion. Property tests compare the deadline schedule with a simple map, and the 100,000-command corpus compares the gateway with the independent matcher. A separate-process run of the [initial](../fixtures/expiry-control.jsonl) and [continuation](../fixtures/expiry-resume.jsonl) fixtures recovered the pending deadline at time 9 and the expired order at time 10, with matching summaries at both checkpoints.

This is a simulated logical clock, not a real-time scheduler or exchange calendar. The [gateway benchmark](../benchmarks/exchange-expiry-gateway-2026-11-10.md) measures the normal request path without scheduled expiry events.
