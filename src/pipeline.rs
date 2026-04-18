//! Open-loop bounded in-memory pipeline benchmark using a blocking bounded channel.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{mpsc::sync_channel, Arc, Barrier, OnceLock};
use std::thread;
use std::time::{Duration, Instant};

#[derive(Debug, Clone, PartialEq)]
pub struct PipelineResult {
    pub events_offered: usize,
    pub events_completed: usize,
    pub capacity: usize,
    pub offered_events_per_second: u64,
    pub completed_events_per_second: f64,
    pub zero_loss: bool,
    pub p50_end_to_end_ns: u64,
    pub p95_end_to_end_ns: u64,
    pub p99_end_to_end_ns: u64,
    pub max_end_to_end_ns: u64,
    pub high_water_mark: usize,
    pub checksum: u64,
}

#[derive(Debug)]
struct PipelineEvent {
    scheduled_offset_ns: u64,
    payload: u64,
}

/// Run an open-loop offered-rate trial. Send blocks at the bounded queue when
/// full, retaining every generated event while making queue delay visible.
pub fn run_trial(
    event_count: usize,
    offered_rate: u64,
    capacity: usize,
    seed: u64,
) -> PipelineResult {
    assert!(event_count > 0, "event count must be positive");
    assert!(offered_rate > 0, "offered rate must be positive");
    assert!(capacity > 0, "queue capacity must be positive");

    let payloads = generate_payloads(event_count, seed);
    let expected_checksum = payloads
        .iter()
        .fold(0_u64, |sum, value| sum.wrapping_add(*value));
    let (sender, receiver) = sync_channel::<PipelineEvent>(capacity);
    let barrier = Arc::new(Barrier::new(2));
    let shared_start = Arc::new(OnceLock::<Instant>::new());
    let outstanding = Arc::new(AtomicUsize::new(0));
    let high_water = Arc::new(AtomicUsize::new(0));
    let worker_barrier = barrier.clone();
    let worker_start = shared_start.clone();
    let worker_outstanding = outstanding.clone();
    let worker = thread::spawn(move || {
        let mut latencies = Vec::with_capacity(event_count);
        let mut checksum = 0_u64;
        let mut completed = 0;
        worker_barrier.wait();
        let start = loop {
            if let Some(start) = worker_start.get() {
                break *start;
            }
            thread::yield_now();
        };
        while let Ok(event) = receiver.recv() {
            worker_outstanding.fetch_sub(1, Ordering::Relaxed);
            let completion_offset = start.elapsed().as_nanos().min(u128::from(u64::MAX)) as u64;
            latencies.push(completion_offset.saturating_sub(event.scheduled_offset_ns));
            checksum = checksum.wrapping_add(event.payload);
            completed += 1;
        }
        latencies.sort_unstable();
        let drained_at = start.elapsed();
        (completed, checksum, latencies, drained_at)
    });

    barrier.wait();
    let start = Instant::now();
    shared_start.set(start).expect("pipeline start is set once");
    for (index, payload) in payloads.into_iter().enumerate() {
        let target_ns = ((index as u128) * 1_000_000_000_u128 / u128::from(offered_rate))
            .min(u128::from(u64::MAX)) as u64;
        let target = start + Duration::from_nanos(target_ns);
        while Instant::now() < target {
            std::hint::spin_loop();
        }
        let outstanding_count = outstanding.fetch_add(1, Ordering::Relaxed) + 1;
        high_water.fetch_max(outstanding_count, Ordering::Relaxed);
        sender
            .send(PipelineEvent {
                scheduled_offset_ns: target_ns,
                payload,
            })
            .expect("pipeline worker exited early");
    }
    drop(sender);
    let (events_completed, checksum, latencies, elapsed) =
        worker.join().expect("pipeline worker panicked");
    let total_events = event_count;
    PipelineResult {
        events_offered: total_events,
        events_completed,
        capacity,
        offered_events_per_second: offered_rate,
        completed_events_per_second: events_completed as f64 / elapsed.as_secs_f64(),
        zero_loss: events_completed == total_events && checksum == expected_checksum,
        p50_end_to_end_ns: percentile(&latencies, 50),
        p95_end_to_end_ns: percentile(&latencies, 95),
        p99_end_to_end_ns: percentile(&latencies, 99),
        max_end_to_end_ns: latencies.last().copied().unwrap_or(0),
        // std::sync::mpsc::SyncSender does not expose queue occupancy. The
        // configured hard bound is known; occupancy instrumentation is a later phase.
        high_water_mark: high_water.load(Ordering::Relaxed),
        checksum,
    }
}

fn percentile(sorted: &[u64], percentile: usize) -> u64 {
    if sorted.is_empty() {
        return 0;
    }
    let rank = (percentile * sorted.len()).div_ceil(100).max(1);
    sorted[rank.saturating_sub(1).min(sorted.len() - 1)]
}

fn generate_payloads(event_count: usize, seed: u64) -> Vec<u64> {
    let mut state = seed;
    (0..event_count)
        .map(|_| {
            state = state.wrapping_add(0x9e3779b97f4a7c15);
            let mut value = state;
            value = (value ^ (value >> 30)).wrapping_mul(0xbf58476d1ce4e5b9);
            value = (value ^ (value >> 27)).wrapping_mul(0x94d049bb133111eb);
            value ^ (value >> 31)
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bounded_pipeline_completes_every_event_and_checks_payloads() {
        let result = run_trial(10_000, 100_000, 128, 44);
        assert_eq!(result.events_offered, 10_000);
        assert_eq!(result.events_completed, 10_000);
        assert!(result.zero_loss);
        assert!(result.completed_events_per_second > 0.0);
        assert!(result.p50_end_to_end_ns <= result.p95_end_to_end_ns);
        assert!(result.p95_end_to_end_ns <= result.p99_end_to_end_ns);
        assert!(result.p99_end_to_end_ns <= result.max_end_to_end_ns);
    }
}
