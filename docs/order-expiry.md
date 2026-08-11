# Deterministic order expiry

## Design

- Exchange logical time starts at zero and advances only through a journaled `advance_time` command from logical control client 0. Backward time is rejected without changing state. Client IDs are not authenticated.
- An order owner can set or clear a deadline on an existing live order with `set_expiry`. A deadline must be strictly later than the current logical time. An invalid or foreign order cannot be scheduled.
- Advancing time expires all live orders whose deadlines are at or before the new time. Expirations are ordered by deadline, then exchange order ID, and produce an `expired` report for each owner with request ID 0 to identify exchange-generated reports.
- Expiry is allowed while the trading session is closed. An expired order leaves the book, retains its cumulative fills, and has zero leaves. Cancel, fill and replacement remove the old order's deadline; a new replacement does not inherit it.
- Identical command retries return cached responses and cannot expire or schedule orders twice. The clock and outstanding deadlines are included in state fingerprints and reconstructed by journal replay.

## Acceptance

Verify exact deadline boundaries, deterministic ordering, ownership, partial fills, cancellation and replacement cleanup, idempotency, backward-clock rejection, closed-session expiry, and independent model agreement. Recover scheduled orders and clock state after a process restart. This mechanism is a simulated logical clock and does not implement exchange calendars or real-time scheduling.
