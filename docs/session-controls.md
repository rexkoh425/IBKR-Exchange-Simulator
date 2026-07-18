# Deterministic exchange session controls

## Intended behavior

- The exchange starts open. A `set_session` command selects `open` or `closed` at the serialized gateway sequencing point; no wall clock or calendar is consulted.
- Client ID 0 is the simulator's control identity. Other client IDs cannot change the session. Client IDs are supplied by callers and are not authenticated, so this is a simulation convention rather than a security boundary.
- When closed, new orders and replacements are rejected before consuming client order IDs, exchange order IDs, or engine sequences. A replacement rejection preserves the original order.
- Cancels, mass cancels, and read-only order lookup remain available. Closing retains live orders and their time priority; opening allows matching again.
- Session commands produce a response with no execution reports and are subject to normal client-scoped idempotency. Replaying an old request returns its cached response without repeating its transition.
- Journal entries capture session commands and their outcomes. Recovery applies them in observed serialized order, including across a restart. A closed session contributes to the state fingerprint.

## Acceptance

Verify open/close authorization, close-time rejections, cancellation while closed, retained FIFO priority, retries after later transitions, and deterministic journal recovery. Test the behavior against the independent matcher. The feature is an in-process simulation control; it does not provide exchange authentication or an automatic market calendar.
