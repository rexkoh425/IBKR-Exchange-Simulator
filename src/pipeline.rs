//! Open-loop bounded in-memory pipeline benchmark using a blocking bounded channel.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{mpsc::sync_channel, Arc, Barrier, OnceLock};
use std::thread;
use std::time::{Duration, Instant};

#[derive(Debug, Clone, PartialEq)]
pub struct PipelineResult {
    pub events_offered: usize,
    pub events_completed: usize,
    pub ordering_errors: usize,
    pub queue_full_retries: Option<usize>,
    pub capacity: usize,
    pub offered_events_per_second: u64,
    pub completed_events_per_second: f64,
    pub zero_loss: bool,
    pub p50_end_to_end_ns: u64,
    pub p95_end_to_end_ns: u64,
    pub p99_end_to_end_ns: u64,
    pub p999_end_to_end_ns: u64,
    pub max_end_to_end_ns: u64,
    pub p99_queue_residence_ns: u64,
    pub p999_queue_residence_ns: u64,
    pub max_queue_residence_ns: u64,
    pub high_water_mark: usize,
    pub checksum: u64,
    pub producer_pin_succeeded: bool,
    pub consumer_pin_succeeded: bool,
}

#[derive(Debug)]
struct PipelineEvent {
    sequence: usize,
    scheduled_offset_ns: u64,
    enqueued_offset_ns: u64,
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
    run_trial_with_affinity(event_count, offered_rate, capacity, seed, None, None)
}

pub fn run_trial_with_affinity(
    event_count: usize,
    offered_rate: u64,
    capacity: usize,
    seed: u64,
    producer_core: Option<usize>,
    consumer_core: Option<usize>,
) -> PipelineResult {
    assert!(event_count > 0, "event count must be positive");
    assert!(offered_rate > 0, "offered rate must be positive");
    assert!(capacity > 0, "queue capacity must be positive");

    let payloads = generate_payloads(event_count, seed);
    let expected_checksum = expected_checksum(&payloads);
    let (sender, receiver) = sync_channel::<PipelineEvent>(capacity);
    let barrier = Arc::new(Barrier::new(2));
    let shared_start = Arc::new(OnceLock::<Instant>::new());
    let outstanding = Arc::new(AtomicUsize::new(0));
    let high_water = Arc::new(AtomicUsize::new(0));
    let worker_barrier = barrier.clone();
    let worker_start = shared_start.clone();
    let worker_outstanding = outstanding.clone();
    let worker = thread::spawn(move || {
        let consumer_pin_succeeded = consumer_core.map(pin_current_thread).unwrap_or(false);
        let mut latencies = Vec::with_capacity(event_count);
        let mut queue_residences = Vec::with_capacity(event_count);
        let mut checksum = 0_u64;
        let mut completed = 0;
        let mut ordering_errors = 0;
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
            queue_residences.push(completion_offset.saturating_sub(event.enqueued_offset_ns));
            if event.sequence != completed {
                ordering_errors += 1;
            }
            checksum = update_checksum(checksum, event.sequence, event.payload);
            completed += 1;
        }
        let drained_at = start.elapsed();
        latencies.sort_unstable();
        queue_residences.sort_unstable();
        (
            completed,
            checksum,
            latencies,
            queue_residences,
            drained_at,
            consumer_pin_succeeded,
            ordering_errors,
        )
    });

    let producer_pin_succeeded = producer_core.map(pin_current_thread).unwrap_or(false);
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
        let enqueued_offset_ns = start.elapsed().as_nanos().min(u128::from(u64::MAX)) as u64;
        sender
            .send(PipelineEvent {
                sequence: index,
                scheduled_offset_ns: target_ns,
                enqueued_offset_ns,
                payload,
            })
            .expect("pipeline worker exited early");
    }
    drop(sender);
    let (
        events_completed,
        checksum,
        latencies,
        queue_residences,
        elapsed,
        consumer_pin_succeeded,
        ordering_errors,
    ) = worker.join().expect("pipeline worker panicked");
    let total_events = event_count;
    PipelineResult {
        events_offered: total_events,
        events_completed,
        ordering_errors,
        queue_full_retries: None,
        capacity,
        offered_events_per_second: offered_rate,
        completed_events_per_second: events_completed as f64 / elapsed.as_secs_f64(),
        zero_loss: events_completed == total_events
            && ordering_errors == 0
            && checksum == expected_checksum,
        p50_end_to_end_ns: percentile(&latencies, 50),
        p95_end_to_end_ns: percentile(&latencies, 95),
        p99_end_to_end_ns: percentile(&latencies, 99),
        p999_end_to_end_ns: percentile_per_mille(&latencies, 999),
        max_end_to_end_ns: latencies.last().copied().unwrap_or(0),
        p99_queue_residence_ns: percentile(&queue_residences, 99),
        p999_queue_residence_ns: percentile_per_mille(&queue_residences, 999),
        max_queue_residence_ns: queue_residences.last().copied().unwrap_or(0),
        // std::sync::mpsc::SyncSender does not expose queue occupancy. The
        // configured hard bound is known; occupancy instrumentation is a later phase.
        high_water_mark: high_water.load(Ordering::Relaxed),
        checksum,
        producer_pin_succeeded,
        consumer_pin_succeeded,
    }
}

/// Open-loop benchmark using a bounded, wait-free SPSC ring. This model is
/// appropriate only when exactly one producer feeds exactly one consumer.
pub fn run_spsc_trial(
    event_count: usize,
    offered_rate: u64,
    capacity: usize,
    seed: u64,
) -> PipelineResult {
    run_spsc_trial_with_affinity(event_count, offered_rate, capacity, seed, None, None)
}

pub fn run_spsc_trial_with_affinity(
    event_count: usize,
    offered_rate: u64,
    capacity: usize,
    seed: u64,
    producer_core: Option<usize>,
    consumer_core: Option<usize>,
) -> PipelineResult {
    run_spsc_trial_with_affinity_and_sampling(
        event_count,
        offered_rate,
        capacity,
        seed,
        producer_core,
        consumer_core,
        64,
    )
}

/// SPSC trial with configurable queue occupancy sampling. A value of one
/// samples every push; larger values reduce telemetry reads on the hot path.
pub fn run_spsc_trial_with_affinity_and_sampling(
    event_count: usize,
    offered_rate: u64,
    capacity: usize,
    seed: u64,
    producer_core: Option<usize>,
    consumer_core: Option<usize>,
    occupancy_sample_interval: usize,
) -> PipelineResult {
    run_spsc_trial_with_affinity_sampling_and_burst(
        event_count,
        offered_rate,
        capacity,
        seed,
        producer_core,
        consumer_core,
        occupancy_sample_interval,
        1,
    )
}

pub fn run_spsc_trial_with_affinity_sampling_and_burst(
    event_count: usize,
    offered_rate: u64,
    capacity: usize,
    seed: u64,
    producer_core: Option<usize>,
    consumer_core: Option<usize>,
    occupancy_sample_interval: usize,
    burst_size: usize,
) -> PipelineResult {
    assert!(event_count > 0 && offered_rate > 0 && capacity > 0);
    assert!(occupancy_sample_interval > 0);
    assert!(burst_size > 0);
    let payloads = generate_payloads(event_count, seed);
    let expected_checksum = expected_checksum(&payloads);
    let (mut producer, mut consumer) = rtrb::RingBuffer::<PipelineEvent>::new(capacity);
    let start_cell = Arc::new(OnceLock::<Instant>::new());
    let consumer_start = start_cell.clone();
    let worker = thread::spawn(move || {
        let consumer_pin_succeeded = consumer_core.map(pin_current_thread).unwrap_or(false);
        let mut latencies = Vec::with_capacity(event_count);
        let mut queue_residences = Vec::with_capacity(event_count);
        let mut checksum = 0_u64;
        let mut completed = 0;
        let mut ordering_errors = 0;
        loop {
            match consumer.pop() {
                Ok(event) => {
                    let start = *consumer_start.get().expect("benchmark start initialized");
                    let done = start.elapsed().as_nanos().min(u128::from(u64::MAX)) as u64;
                    latencies.push(done.saturating_sub(event.scheduled_offset_ns));
                    queue_residences.push(done.saturating_sub(event.enqueued_offset_ns));
                    if event.sequence != completed {
                        ordering_errors += 1;
                    }
                    checksum = update_checksum(checksum, event.sequence, event.payload);
                    completed += 1;
                }
                Err(_) => {
                    if completed == event_count {
                        break;
                    }
                    std::hint::spin_loop();
                }
            }
        }
        let drained_at = consumer_start
            .get()
            .expect("benchmark start initialized")
            .elapsed();
        latencies.sort_unstable();
        queue_residences.sort_unstable();
        (
            completed,
            checksum,
            latencies,
            queue_residences,
            drained_at,
            consumer_pin_succeeded,
            ordering_errors,
        )
    });
    let producer_pin_succeeded = producer_core.map(pin_current_thread).unwrap_or(false);
    let start = Instant::now();
    start_cell.set(start).expect("pipeline start is set once");
    let mut high_water = 0;
    let mut queue_full_retries = 0;
    for (index, payload) in payloads.into_iter().enumerate() {
        let schedule_index = (index / burst_size) * burst_size;
        let target_ns = ((schedule_index as u128) * 1_000_000_000_u128 / u128::from(offered_rate))
            .min(u128::from(u64::MAX)) as u64;
        let target = start + Duration::from_nanos(target_ns);
        while Instant::now() < target {
            std::hint::spin_loop();
        }
        let enqueued_offset_ns = start.elapsed().as_nanos().min(u128::from(u64::MAX)) as u64;
        loop {
            match producer.push(PipelineEvent {
                sequence: index,
                scheduled_offset_ns: target_ns,
                enqueued_offset_ns,
                payload,
            }) {
                Ok(()) => break,
                Err(rtrb::PushError::Full(_)) => {
                    queue_full_retries += 1;
                    std::hint::spin_loop()
                }
            }
        }
        // `slots()` performs an Acquire load of the consumer index. Sampling
        // occupancy bounds the telemetry cost; the reported peak can miss
        // short-lived peaks between samples.
        if index % occupancy_sample_interval == 0 {
            high_water = high_water.max(capacity - producer.slots());
        }
    }
    let (
        events_completed,
        checksum,
        latencies,
        queue_residences,
        elapsed,
        consumer_pin_succeeded,
        ordering_errors,
    ) = worker.join().expect("SPSC worker panicked");
    PipelineResult {
        events_offered: event_count,
        events_completed,
        ordering_errors,
        queue_full_retries: Some(queue_full_retries),
        capacity,
        offered_events_per_second: offered_rate,
        completed_events_per_second: events_completed as f64 / elapsed.as_secs_f64(),
        zero_loss: events_completed == event_count
            && ordering_errors == 0
            && checksum == expected_checksum,
        p50_end_to_end_ns: percentile(&latencies, 50),
        p95_end_to_end_ns: percentile(&latencies, 95),
        p99_end_to_end_ns: percentile(&latencies, 99),
        p999_end_to_end_ns: percentile_per_mille(&latencies, 999),
        max_end_to_end_ns: latencies.last().copied().unwrap_or(0),
        p99_queue_residence_ns: percentile(&queue_residences, 99),
        p999_queue_residence_ns: percentile_per_mille(&queue_residences, 999),
        max_queue_residence_ns: queue_residences.last().copied().unwrap_or(0),
        high_water_mark: high_water,
        checksum,
        producer_pin_succeeded,
        consumer_pin_succeeded,
    }
}

fn pin_current_thread(core_id: usize) -> bool {
    core_affinity::set_for_current(core_affinity::CoreId { id: core_id })
}

fn percentile(sorted: &[u64], percentile: usize) -> u64 {
    if sorted.is_empty() {
        return 0;
    }
    let rank = (percentile * sorted.len()).div_ceil(100).max(1);
    sorted[rank.saturating_sub(1).min(sorted.len() - 1)]
}

fn percentile_per_mille(sorted: &[u64], per_mille: usize) -> u64 {
    if sorted.is_empty() {
        return 0;
    }
    let rank = per_mille
        .saturating_mul(sorted.len())
        .div_ceil(1_000)
        .max(1);
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

fn expected_checksum(payloads: &[u64]) -> u64 {
    payloads
        .iter()
        .enumerate()
        .fold(0_u64, |checksum, (sequence, payload)| {
            update_checksum(checksum, sequence, *payload)
        })
}

fn update_checksum(checksum: u64, sequence: usize, payload: u64) -> u64 {
    let event = payload
        .wrapping_add((sequence as u64).wrapping_mul(0x9e3779b97f4a7c15))
        .wrapping_mul(0xbf58476d1ce4e5b9);
    checksum.rotate_left(13) ^ event
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bounded_pipeline_completes_every_event_and_checks_payloads() {
        let result = run_trial(10_000, 100_000, 128, 44);
        assert_eq!(result.events_offered, 10_000);
        assert_eq!(result.events_completed, 10_000);
        assert_eq!(result.ordering_errors, 0);
        assert_eq!(result.queue_full_retries, None);
        assert!(result.zero_loss);
        assert!(result.completed_events_per_second > 0.0);
        assert!(result.p50_end_to_end_ns <= result.p95_end_to_end_ns);
        assert!(result.p95_end_to_end_ns <= result.p99_end_to_end_ns);
        assert!(result.p99_end_to_end_ns <= result.max_end_to_end_ns);
    }

    #[test]
    fn bounded_spsc_pipeline_completes_every_event_and_checks_payloads() {
        let result = run_spsc_trial(10_000, 100_000, 128, 44);
        assert_eq!(result.events_completed, 10_000);
        assert_eq!(result.ordering_errors, 0);
        assert!(result.queue_full_retries.is_some());
        assert!(result.zero_loss);
        assert!(result.p50_end_to_end_ns <= result.p99_end_to_end_ns);
    }

    #[test]
    fn checksum_changes_when_event_order_changes() {
        let first = update_checksum(update_checksum(0, 0, 11), 1, 22);
        let reordered = update_checksum(update_checksum(0, 1, 22), 0, 11);
        assert_ne!(first, reordered);
    }
}
