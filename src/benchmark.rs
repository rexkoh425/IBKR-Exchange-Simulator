//! Repeatable synthetic mixed-order benchmark for the matching hot path.

use crate::{MatchingEngine, OrderRequest, Side};
use std::collections::HashMap;
use std::time::{Duration, Instant};

#[derive(Debug, Clone, Copy)]
enum WorkItem {
    Submit(OrderRequest),
    Cancel(u64),
}

struct GeneratedWorkload {
    items: Vec<WorkItem>,
    submit_count: usize,
    cancel_count: usize,
    trade_count: usize,
    matched_lots: u64,
}

#[derive(Debug, Clone, PartialEq)]
pub struct TrialResult {
    pub seed: u64,
    pub events: usize,
    pub submit_count: usize,
    pub cancel_count: usize,
    pub trade_count: usize,
    pub matched_lots: u64,
    pub elapsed: Duration,
    pub completed_events_per_second: f64,
    pub p50_service_ns: u64,
    pub p95_service_ns: u64,
    pub p99_service_ns: u64,
    pub p999_service_ns: u64,
    pub max_service_ns: u64,
    pub final_state_fingerprint: u64,
}

/// Run a single deterministic trial. Workload generation is outside the timed region.
pub fn run_trial(event_count: usize, seed: u64) -> TrialResult {
    let workload = make_workload(event_count, seed);
    let mut engine = MatchingEngine::new();
    let mut service_times = Vec::with_capacity(workload.items.len());
    let started = Instant::now();
    for item in &workload.items {
        let event_started = Instant::now();
        match item {
            WorkItem::Submit(order) => {
                engine.submit(*order);
            }
            WorkItem::Cancel(order_id) => {
                engine
                    .cancel(*order_id)
                    .expect("pre-generated cancellation must be valid");
            }
        }
        service_times.push(event_started.elapsed().as_nanos().min(u128::from(u64::MAX)) as u64);
    }
    let elapsed = started.elapsed();
    service_times.sort_unstable();
    let completed_events_per_second = workload.items.len() as f64 / elapsed.as_secs_f64();
    TrialResult {
        seed,
        events: workload.items.len(),
        submit_count: workload.submit_count,
        cancel_count: workload.cancel_count,
        trade_count: workload.trade_count,
        matched_lots: workload.matched_lots,
        elapsed,
        completed_events_per_second,
        p50_service_ns: percentile(&service_times, 50),
        p95_service_ns: percentile(&service_times, 95),
        p99_service_ns: percentile(&service_times, 99),
        p999_service_ns: percentile(&service_times, 999),
        max_service_ns: service_times.last().copied().unwrap_or(0),
        final_state_fingerprint: engine.state_fingerprint(),
    }
}

fn percentile(sorted: &[u64], percentile: usize) -> u64 {
    if sorted.is_empty() {
        return 0;
    }
    let rank = (percentile * sorted.len()).div_ceil(100).max(1);
    sorted[rank.saturating_sub(1).min(sorted.len() - 1)]
}

fn make_workload(event_count: usize, seed: u64) -> GeneratedWorkload {
    let mut random = SplitMix64(seed);
    let mut shadow = MatchingEngine::new();
    let mut live_order_ids = Vec::new();
    let mut live_indices = HashMap::new();
    let mut workload = Vec::with_capacity(event_count);
    let mut next_order_id = 1_u64;
    let mut submit_count = 0;
    let mut cancel_count = 0;
    let mut trade_count = 0;
    let mut matched_lots = 0;
    while workload.len() < event_count {
        if !live_order_ids.is_empty() && random.next().is_multiple_of(4) {
            let live_index = (random.next() as usize) % live_order_ids.len();
            let order_id = live_order_ids.swap_remove(live_index);
            live_indices.remove(&order_id);
            if live_index < live_order_ids.len() {
                live_indices.insert(live_order_ids[live_index], live_index);
            }
            shadow
                .cancel(order_id)
                .expect("live-order generator stays in sync");
            workload.push(WorkItem::Cancel(order_id));
            cancel_count += 1;
            continue;
        }
        let side = if random.next() & 1 == 0 {
            Side::Buy
        } else {
            Side::Sell
        };
        let offset = (random.next() % 21) as i64 - 10;
        let price_ticks = 10_000 + offset;
        let order = OrderRequest {
            order_id: next_order_id,
            side,
            price_ticks,
            quantity_lots: random.next() % 100 + 1,
        };
        next_order_id += 1;
        let result = shadow.submit(order);
        for trade in result.trades {
            trade_count += 1;
            matched_lots += trade.quantity_lots;
            if !shadow.contains_live_order(trade.maker_order_id) {
                if let Some(index) = live_indices.remove(&trade.maker_order_id) {
                    live_order_ids.swap_remove(index);
                    if index < live_order_ids.len() {
                        live_indices.insert(live_order_ids[index], index);
                    }
                }
            }
        }
        if result.resting_quantity_lots > 0 {
            live_indices.insert(order.order_id, live_order_ids.len());
            live_order_ids.push(order.order_id);
        }
        workload.push(WorkItem::Submit(order));
        submit_count += 1;
    }
    GeneratedWorkload {
        items: workload,
        submit_count,
        cancel_count,
        trade_count,
        matched_lots,
    }
}

struct SplitMix64(u64);

impl SplitMix64 {
    fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9e3779b97f4a7c15);
        let mut value = self.0;
        value = (value ^ (value >> 30)).wrapping_mul(0xbf58476d1ce4e5b9);
        value = (value ^ (value >> 27)).wrapping_mul(0x94d049bb133111eb);
        value ^ (value >> 31)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fixed_seed_repeats_state_and_measurement_shape() {
        let first = run_trial(2_000, 7);
        let second = run_trial(2_000, 7);
        assert_eq!(
            first.final_state_fingerprint,
            second.final_state_fingerprint
        );
        assert_eq!(first.events, 2_000);
        assert!(first.submit_count > 0);
        assert!(first.cancel_count > 0);
        assert!(first.trade_count > 0);
        assert!(first.matched_lots > 0);
        assert!(first.completed_events_per_second > 0.0);
        assert!(first.p50_service_ns <= first.p95_service_ns);
        assert!(first.p95_service_ns <= first.p99_service_ns);
        assert!(first.p99_service_ns <= first.max_service_ns);
    }
}
