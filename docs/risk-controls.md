# Client outstanding-order risk

## Design

- In addition to the existing per-order quantity and notional caps, configure per-client caps for total live quantity and limit-price notional across all symbols and sides. Defaults are unlimited.
- Before accepting a new order, compare current live exposure plus the full incoming quantity and limit-price notional to the caps. This conservative check applies even when some or all of the order could execute immediately.
- Before replacement, subtract the old order's remaining exposure, then add the full replacement. Rejection leaves the original order and its priority intact. Existing client-order-ID reservation behavior remains unchanged.
- Exposure is derived from live order records. Partial fills reduce it; cancellation, expiry and full fills remove it. A replacement starts with its own quantity and price. Buy and sell exposure are added rather than netted.
- Arithmetic is checked. If a configured aggregate cannot be represented, reject conservatively. Unlimited defaults bypass aggregate checks on the order path.
- Limits are simulator configuration, not authenticated client controls. Journal recovery must use the same limits as the original run, as with existing per-order limits.

## Acceptance

Test boundaries, overflow, partial fills, cancellation, expiry, cross-symbol and cross-side aggregation, replacement, retries, concurrency and recovery. Compare the production gateway with the independent reference matcher and measure the regular gateway path before and after the change.
