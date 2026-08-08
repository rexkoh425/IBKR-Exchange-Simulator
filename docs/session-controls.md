# Deterministic exchange session controls

## Behavior

- The exchange starts open. A `set_session` command selects `open` or `closed` at the serialized gateway sequencing point; no wall clock or calendar is consulted.
- Client ID 0 is the simulator's control identity. Other client IDs cannot change the session. Client IDs are supplied by callers and are not authenticated, so this is a simulation convention rather than a security boundary.
- When closed, new orders and replacements are rejected before consuming client order IDs, exchange order IDs, or engine sequences. A replacement rejection preserves the original order.
- Cancels, mass cancels, and read-only order lookup remain available. Closing retains live orders and their time priority; opening allows matching again.
- Session commands produce a response with no execution reports and are subject to normal client-scoped idempotency. Replaying an old request returns its cached response without repeating its transition.
- Journal entries capture session commands and their outcomes. Recovery applies them in observed serialized order, including across a restart. A closed session contributes to the state fingerprint.

## Example

```json
{"client_id":0,"request_id":1,"command":{"type":"set_session","state":"closed"}}
```

Run the [session fixture](../fixtures/session-control.jsonl) with `exchange-sim` and a journal, then recover with `exchange-recover`. The [continuation fixture](../fixtures/session-resume.jsonl) tests a closed-order rejection followed by reopening after a separate process restart.

## Verification and scope

Tests cover authorization by logical client ID, close-time rejections, cancellation while closed, retained FIFO priority, retries after later transitions, and journal recovery. The fixed-seed 100,000-command corpus and property-generated sequences compare transitions against the independent matcher. In a separate-process fixture run, the initial eight-entry closed session and the resumed eleven-entry open session each matched their recovered summaries byte for byte.

The feature is an in-process simulation control; it does not provide authenticated administration or an automatic market calendar. [Pinned open-session gateway measurements](../benchmarks/exchange-session-gateway-2026-11-10.md) use a synthetic workload that does not include session transitions.
