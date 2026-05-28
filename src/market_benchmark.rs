//! Offline benchmark for IBKR-shaped normalization, JSONL persistence and replay.
use crate::ibkr::{normalize_depth, L1Normalizer};
use crate::market_data::{MarketEvent, MarketPayload, MarketReplay};
use ibapi::contracts::tick_types::TickType;
use ibapi::market_data::realtime::{MarketDepth, MarketDepths, TickPrice, TickSize, TickTypes};
use std::fs::{File, OpenOptions};
use std::io::{BufRead, BufReader, BufWriter, Write};
use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Barrier, OnceLock};
use std::thread;
use std::time::{Duration, Instant};

#[derive(Debug, Clone, PartialEq)]
pub struct StageResult {
    pub events: usize,
    pub elapsed_ns: u64,
    pub events_per_second: f64,
    pub p50_ns: u64,
    pub p95_ns: u64,
    pub p99_ns: u64,
    pub p999_ns: u64,
    pub max_ns: u64,
}

#[derive(Debug, Clone, PartialEq)]
pub struct MarketPipelineResult {
    pub source_callbacks: usize,
    pub normalized_events: usize,
    pub file_bytes: u64,
    pub persistence_flush_ns: u64,
    pub normalization: StageResult,
    pub persistence: StageResult,
    pub decode: StageResult,
    pub replay: StageResult,
    pub in_memory_end_to_end: StageResult,
    pub event_checksum: u64,
    pub state_fingerprint: u64,
    pub deterministic_replay: bool,
    pub incomplete: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MarketSpscOptions {
    pub events: usize,
    pub offered_rate: u64,
    pub capacity: usize,
    pub sample_interval: usize,
    pub burst_size: usize,
    pub retain_raw_payload: bool,
    pub producer_core: Option<usize>,
    pub consumer_core: Option<usize>,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct LatencyDistribution {
    pub samples: usize,
    pub p50_ns: u64,
    pub p95_ns: u64,
    pub p99_ns: u64,
    pub p999_ns: u64,
    pub max_ns: u64,
}

#[derive(Debug, Clone, PartialEq)]
pub struct MarketSpscResult {
    pub source_callbacks: usize,
    pub queue_events: usize,
    pub applied_events: usize,
    pub capacity: usize,
    pub offered_rate: u64,
    pub completed_events_per_second: f64,
    pub queue_full_retries: usize,
    pub sampled_high_water: usize,
    pub producer_pin_succeeded: bool,
    pub consumer_pin_succeeded: bool,
    pub event_order_errors: usize,
    pub checksums_match: bool,
    pub checksum: u64,
    pub state_fingerprint: u64,
    pub incomplete: bool,
    pub release_lateness: LatencyDistribution,
    pub normalization: LatencyDistribution,
    pub queue_push: LatencyDistribution,
    pub queue_residence: LatencyDistribution,
    pub consumer_replay: LatencyDistribution,
    pub scheduled_to_completion: LatencyDistribution,
}

#[derive(Debug)]
struct QueuedMarketEvent {
    sequence: usize,
    enqueue_start_ns: u64,
    event: MarketEvent,
}

#[derive(Debug)]
struct ProducerTiming {
    scheduled_ns: u64,
    producer_wake_ns: u64,
    normalization_ns: u64,
    queue_push_ns: u64,
}

#[derive(Debug)]
struct ConsumerTiming {
    enqueue_start_ns: u64,
    dequeue_ns: u64,
    replay_ns: u64,
    completion_ns: u64,
}

#[derive(Debug)]
enum SyntheticCallback {
    L1(TickTypes),
    Depth(MarketDepths),
}

pub fn run(events: usize, output_path: &Path) -> Result<MarketPipelineResult, String> {
    if events == 0 {
        return Err("event count must be positive".into());
    }
    let raw = generate_callbacks(events);
    let mut normalized = Vec::with_capacity(events + 4);
    let mut normalization_times = Vec::with_capacity(events);
    let mut l1 = L1Normalizer::default();
    let mut ordinal = 1_u64;
    normalized.push(MarketEvent::from_source(
        ordinal,
        "AAPL",
        "synthetic_ibkr",
        1,
        MarketPayload::SessionStart {
            session_id: "bench".into(),
        },
        None,
    ));
    ordinal += 1;
    normalized.push(MarketEvent::from_source(
        ordinal,
        "AAPL",
        "synthetic_ibkr",
        2,
        MarketPayload::DepthSnapshotBegin {
            venue: "NASDAQ".into(),
        },
        None,
    ));
    for (index, callback) in raw.iter().enumerate() {
        let started = Instant::now();
        let (payload, raw_payload) = match callback {
            SyntheticCallback::L1(tick) => (l1.normalize(tick, 0.01)?, Some(format!("{tick:?}"))),
            SyntheticCallback::Depth(depth) => {
                let payload = normalize_depth(depth, 0.01, "NASDAQ")?;
                (payload, Some(format!("{depth:?}")))
            }
        };
        let receive_time_ns = 3 + index as u64 * 1_000;
        ordinal += 1;
        normalized.push(MarketEvent::from_source(
            ordinal,
            "AAPL",
            "synthetic_ibkr",
            receive_time_ns,
            payload,
            raw_payload,
        ));
        normalization_times.push(elapsed_ns(started));
    }
    ordinal += 1;
    normalized.push(MarketEvent::from_source(
        ordinal,
        "AAPL",
        "synthetic_ibkr",
        ordinal * 1_000,
        MarketPayload::DepthSnapshotComplete {
            venue: "NASDAQ".into(),
        },
        None,
    ));
    ordinal += 1;
    normalized.push(MarketEvent::from_source(
        ordinal,
        "AAPL",
        "synthetic_ibkr",
        ordinal * 1_000,
        MarketPayload::SessionEnd {
            session_id: "bench".into(),
            complete: true,
        },
        None,
    ));

    let file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(output_path)
        .map_err(|error| {
            format!(
                "cannot create benchmark output {}: {error}",
                output_path.display()
            )
        })?;
    let mut writer = BufWriter::with_capacity(1024 * 1024, file);
    let mut persistence_times = Vec::with_capacity(normalized.len());
    let persistence_started = Instant::now();
    for event in &normalized {
        let started = Instant::now();
        serde_json::to_writer(&mut writer, event).map_err(|error| error.to_string())?;
        writer.write_all(b"\n").map_err(|error| error.to_string())?;
        persistence_times.push(elapsed_ns(started));
    }
    let flush_started = Instant::now();
    writer.flush().map_err(|error| error.to_string())?;
    let persistence_flush_ns = elapsed_ns(flush_started);
    let persistence_elapsed = elapsed_ns(persistence_started);
    let file_bytes = writer
        .get_ref()
        .metadata()
        .map_err(|error| error.to_string())?
        .len();
    drop(writer);

    let file = File::open(output_path).map_err(|error| error.to_string())?;
    let mut decoded = Vec::with_capacity(normalized.len());
    let mut decode_times = Vec::with_capacity(normalized.len());
    let mut reader = BufReader::with_capacity(1024 * 1024, file);
    let mut line = String::new();
    loop {
        line.clear();
        let bytes = reader
            .read_line(&mut line)
            .map_err(|error| error.to_string())?;
        if bytes == 0 {
            break;
        }
        let started = Instant::now();
        decoded.push(
            serde_json::from_str::<MarketEvent>(line.trim_end())
                .map_err(|error| error.to_string())?,
        );
        decode_times.push(elapsed_ns(started));
    }

    let mut replay = MarketReplay::new();
    let mut replay_times = Vec::with_capacity(decoded.len());
    for event in &decoded {
        let started = Instant::now();
        replay.apply(event).map_err(|error| error.to_string())?;
        replay_times.push(elapsed_ns(started));
    }
    let state_fingerprint = replay.state_fingerprint();
    let incomplete = replay.has_incomplete_data();

    let mut e2e_replay = MarketReplay::new();
    let mut e2e_times = Vec::with_capacity(raw.len());
    let mut e2e_l1 = L1Normalizer::default();
    let mut scratch = Vec::with_capacity(512);
    let mut e2e_ordinal = 2_u64;
    e2e_replay
        .apply(&normalized[0])
        .map_err(|error| error.to_string())?;
    e2e_replay
        .apply(&normalized[1])
        .map_err(|error| error.to_string())?;
    for (index, callback) in raw.iter().enumerate() {
        let started = Instant::now();
        let (payload, raw_payload) = match callback {
            SyntheticCallback::L1(tick) => {
                (e2e_l1.normalize(tick, 0.01)?, Some(format!("{tick:?}")))
            }
            SyntheticCallback::Depth(depth) => (
                normalize_depth(depth, 0.01, "NASDAQ")?,
                Some(format!("{depth:?}")),
            ),
        };
        e2e_ordinal += 1;
        let event = MarketEvent::from_source(
            e2e_ordinal,
            "AAPL",
            "synthetic_ibkr",
            3 + index as u64 * 1_000,
            payload,
            raw_payload,
        );
        scratch.clear();
        serde_json::to_writer(&mut scratch, &event).map_err(|error| error.to_string())?;
        let decoded_event: MarketEvent =
            serde_json::from_slice(&scratch).map_err(|error| error.to_string())?;
        e2e_replay
            .apply(&decoded_event)
            .map_err(|error| error.to_string())?;
        e2e_times.push(elapsed_ns(started));
    }
    for event in &normalized[events + 2..] {
        e2e_replay.apply(event).map_err(|error| error.to_string())?;
    }
    let deterministic_replay = decoded.len() == normalized.len()
        && decoded
            .iter()
            .zip(&normalized)
            .all(|(left, right)| left == right)
        && replay.state_fingerprint() == replay_again(&decoded)?
        && replay.state_fingerprint() == e2e_replay.state_fingerprint();
    let event_checksum = checksum_events(&decoded)?;
    let total_events = normalized.len();
    Ok(MarketPipelineResult {
        source_callbacks: events,
        normalized_events: total_events,
        file_bytes,
        persistence_flush_ns,
        normalization: stage(
            events,
            &normalization_times,
            normalization_times.iter().sum(),
        ),
        persistence: stage(total_events, &persistence_times, persistence_elapsed),
        decode: stage(decoded.len(), &decode_times, decode_times.iter().sum()),
        replay: stage(decoded.len(), &replay_times, replay_times.iter().sum()),
        in_memory_end_to_end: stage(events, &e2e_times, e2e_times.iter().sum()),
        event_checksum,
        state_fingerprint,
        deterministic_replay,
        incomplete: incomplete || e2e_replay.has_incomplete_data(),
    })
}

/// End-to-end synthetic IBKR-shaped callback normalization, bounded SPSC
/// transfer, and deterministic replay. `sample_interval == 0` disables timing
/// and occupancy telemetry while retaining ordering and checksum validation.
pub fn run_spsc(options: MarketSpscOptions) -> Result<MarketSpscResult, String> {
    if options.events == 0
        || options.offered_rate == 0
        || options.capacity == 0
        || options.burst_size == 0
    {
        return Err("events, rate, capacity, and burst size must be positive".into());
    }
    let total_replay_events = options.events + 4;
    let (mut producer, mut consumer) = rtrb::RingBuffer::<QueuedMarketEvent>::new(options.capacity);
    let start_cell = Arc::new(OnceLock::<Instant>::new());
    let worker_start = start_cell.clone();
    let barrier = Arc::new(Barrier::new(2));
    let worker_barrier = barrier.clone();
    let consumer_failed = Arc::new(AtomicBool::new(false));
    let worker_failed = consumer_failed.clone();
    let event_count = options.events;
    let sample_interval = options.sample_interval;
    let consumer_core = options.consumer_core;

    let worker = thread::spawn(move || -> Result<ConsumerOutput, String> {
        let pin_succeeded = consumer_core.map(pin_current_thread).unwrap_or(false);
        let mut replay = MarketReplay::new();
        replay
            .apply(&session_start_event())
            .map_err(|error| error.to_string())?;
        replay
            .apply(&depth_snapshot_begin_event())
            .map_err(|error| error.to_string())?;
        let mut consumer_samples =
            Vec::with_capacity(sample_capacity(event_count, sample_interval));
        let mut checksum = 0xcbf29ce484222325_u64;
        let mut order_errors = 0;
        worker_barrier.wait();
        let start = loop {
            if let Some(start) = worker_start.get() {
                break *start;
            }
            thread::yield_now();
        };

        for expected_sequence in 0..event_count {
            let queued = loop {
                match consumer.pop() {
                    Ok(queued) => break queued,
                    Err(_) => {
                        if worker_failed.load(Ordering::Relaxed) {
                            return Err("market replay consumer failed".into());
                        }
                        std::hint::spin_loop();
                    }
                }
            };
            if queued.sequence != expected_sequence
                || queued.event.ordinal != expected_sequence as u64 + 3
            {
                order_errors += 1;
            }
            let sampled = sample_interval > 0 && expected_sequence % sample_interval == 0;
            let dequeue_ns = sampled.then(|| elapsed_ns(start));
            let process_started = sampled.then(Instant::now);
            if let Err(error) = replay.apply(&queued.event) {
                worker_failed.store(true, Ordering::Relaxed);
                return Err(error.to_string());
            }
            let replay_ns = process_started.map_or(0, elapsed_ns);
            checksum = checksum_market_event(checksum, &queued.event);
            if sampled {
                consumer_samples.push(ConsumerTiming {
                    enqueue_start_ns: queued.enqueue_start_ns,
                    dequeue_ns: dequeue_ns.unwrap_or_default(),
                    replay_ns,
                    completion_ns: elapsed_ns(start),
                });
            }
        }

        replay
            .apply(&depth_snapshot_complete_event(event_count))
            .map_err(|error| error.to_string())?;
        replay
            .apply(&session_end_event(event_count))
            .map_err(|error| error.to_string())?;
        let finished_ns = elapsed_ns(start);
        Ok(ConsumerOutput {
            applied_events: total_replay_events,
            checksum,
            order_errors,
            state_fingerprint: replay.state_fingerprint(),
            incomplete: replay.has_incomplete_data(),
            samples: consumer_samples,
            finished_ns,
            pin_succeeded,
        })
    });

    let producer_pin_succeeded = options
        .producer_core
        .map(pin_current_thread)
        .unwrap_or(false);
    barrier.wait();
    let start = Instant::now();
    start_cell
        .set(start)
        .map_err(|_| "market pipeline start was already initialized".to_owned())?;
    let mut normalizer = L1Normalizer::default();
    let mut producer_samples = Vec::with_capacity(sample_capacity(event_count, sample_interval));
    let mut producer_checksum = 0xcbf29ce484222325_u64;
    let mut queue_full_retries = 0_usize;
    let mut sampled_high_water = 0_usize;

    for index in 0..event_count {
        let schedule_index = (index / options.burst_size) * options.burst_size;
        let scheduled_ns = ((schedule_index as u128) * 1_000_000_000_u128
            / u128::from(options.offered_rate))
        .min(u128::from(u64::MAX)) as u64;
        let target = start + Duration::from_nanos(scheduled_ns);
        while Instant::now() < target {
            std::hint::spin_loop();
        }
        let sampled = sample_interval > 0 && index % sample_interval == 0;
        let producer_wake_ns = if sampled { elapsed_ns(start) } else { 0 };
        let normalization_started = sampled.then(Instant::now);
        let callback = synthetic_callback(index);
        let (payload, raw_payload) = match &callback {
            SyntheticCallback::L1(tick) => (
                normalizer.normalize(tick, 0.01)?,
                options.retain_raw_payload.then(|| format!("{tick:?}")),
            ),
            SyntheticCallback::Depth(depth) => (
                normalize_depth(depth, 0.01, "NASDAQ")?,
                options.retain_raw_payload.then(|| format!("{depth:?}")),
            ),
        };
        let event = MarketEvent::from_source(
            index as u64 + 3,
            "AAPL",
            "synthetic_ibkr",
            3 + index as u64 * 1_000,
            payload,
            raw_payload,
        );
        producer_checksum = checksum_market_event(producer_checksum, &event);
        let normalization_ns = normalization_started.map_or(0, elapsed_ns);
        let enqueue_start_ns = if sampled { elapsed_ns(start) } else { 0 };
        let queue_push_started = sampled.then(Instant::now);
        let mut pending = Some(QueuedMarketEvent {
            sequence: index,
            enqueue_start_ns,
            event,
        });
        loop {
            match producer.push(pending.take().expect("pending event is retained")) {
                Ok(()) => break,
                Err(rtrb::PushError::Full(event)) => {
                    pending = Some(event);
                    queue_full_retries += 1;
                    if consumer_failed.load(Ordering::Relaxed) {
                        return Err("market replay consumer failed while queue was full".into());
                    }
                    std::hint::spin_loop();
                }
            }
        }
        let queue_push_ns = queue_push_started.map_or(0, elapsed_ns);
        if sampled {
            producer_samples.push(ProducerTiming {
                scheduled_ns,
                producer_wake_ns,
                normalization_ns,
                queue_push_ns,
            });
            sampled_high_water = sampled_high_water.max(options.capacity - producer.slots());
        }
    }

    drop(producer);
    let consumer_output = worker
        .join()
        .map_err(|_| "market replay consumer panicked".to_owned())??;
    let (release_lateness, normalization, queue_push, queue_residence, consumer_replay, end_to_end) =
        latency_distributions(&producer_samples, &consumer_output.samples);
    Ok(MarketSpscResult {
        source_callbacks: event_count,
        queue_events: event_count,
        applied_events: consumer_output.applied_events,
        capacity: options.capacity,
        offered_rate: options.offered_rate,
        completed_events_per_second: event_count as f64 * 1e9
            / consumer_output.finished_ns.max(1) as f64,
        queue_full_retries,
        sampled_high_water,
        producer_pin_succeeded,
        consumer_pin_succeeded: consumer_output.pin_succeeded,
        event_order_errors: consumer_output.order_errors,
        checksums_match: producer_checksum == consumer_output.checksum,
        checksum: consumer_output.checksum,
        state_fingerprint: consumer_output.state_fingerprint,
        incomplete: consumer_output.incomplete,
        release_lateness,
        normalization,
        queue_push,
        queue_residence,
        consumer_replay,
        scheduled_to_completion: end_to_end,
    })
}

struct ConsumerOutput {
    applied_events: usize,
    checksum: u64,
    order_errors: usize,
    state_fingerprint: u64,
    incomplete: bool,
    samples: Vec<ConsumerTiming>,
    finished_ns: u64,
    pin_succeeded: bool,
}

fn session_start_event() -> MarketEvent {
    MarketEvent::from_source(
        1,
        "AAPL",
        "synthetic_ibkr",
        1,
        MarketPayload::SessionStart {
            session_id: "bench".into(),
        },
        None,
    )
}

fn depth_snapshot_begin_event() -> MarketEvent {
    MarketEvent::from_source(
        2,
        "AAPL",
        "synthetic_ibkr",
        2,
        MarketPayload::DepthSnapshotBegin {
            venue: "NASDAQ".into(),
        },
        None,
    )
}

fn depth_snapshot_complete_event(callbacks: usize) -> MarketEvent {
    MarketEvent::from_source(
        callbacks as u64 + 3,
        "AAPL",
        "synthetic_ibkr",
        3 + callbacks as u64 * 1_000,
        MarketPayload::DepthSnapshotComplete {
            venue: "NASDAQ".into(),
        },
        None,
    )
}

fn session_end_event(callbacks: usize) -> MarketEvent {
    MarketEvent::from_source(
        callbacks as u64 + 4,
        "AAPL",
        "synthetic_ibkr",
        4 + callbacks as u64 * 1_000,
        MarketPayload::SessionEnd {
            session_id: "bench".into(),
            complete: true,
        },
        None,
    )
}

fn pin_current_thread(core_id: usize) -> bool {
    core_affinity::set_for_current(core_affinity::CoreId { id: core_id })
}

fn sample_capacity(events: usize, interval: usize) -> usize {
    if interval == 0 {
        0
    } else {
        events.div_ceil(interval)
    }
}

fn latency_distributions(
    producer: &[ProducerTiming],
    consumer: &[ConsumerTiming],
) -> (
    LatencyDistribution,
    LatencyDistribution,
    LatencyDistribution,
    LatencyDistribution,
    LatencyDistribution,
    LatencyDistribution,
) {
    let count = producer.len().min(consumer.len());
    let mut release_lateness = Vec::with_capacity(count);
    let mut normalization = Vec::with_capacity(count);
    let mut queue_push = Vec::with_capacity(count);
    let mut queue_residence = Vec::with_capacity(count);
    let mut consumer_replay = Vec::with_capacity(count);
    let mut end_to_end = Vec::with_capacity(count);
    for (producer, consumer) in producer.iter().zip(consumer).take(count) {
        release_lateness.push(
            producer
                .producer_wake_ns
                .saturating_sub(producer.scheduled_ns),
        );
        normalization.push(producer.normalization_ns);
        queue_push.push(producer.queue_push_ns);
        queue_residence.push(
            consumer
                .dequeue_ns
                .saturating_sub(consumer.enqueue_start_ns),
        );
        consumer_replay.push(consumer.replay_ns);
        end_to_end.push(consumer.completion_ns.saturating_sub(producer.scheduled_ns));
    }
    (
        distribution(release_lateness),
        distribution(normalization),
        distribution(queue_push),
        distribution(queue_residence),
        distribution(consumer_replay),
        distribution(end_to_end),
    )
}

fn distribution(mut values: Vec<u64>) -> LatencyDistribution {
    values.sort_unstable();
    LatencyDistribution {
        samples: values.len(),
        p50_ns: percentile(&values, 50),
        p95_ns: percentile(&values, 95),
        p99_ns: percentile(&values, 99),
        p999_ns: percentile_per_mille(&values, 999),
        max_ns: values.last().copied().unwrap_or_default(),
    }
}

fn checksum_bytes(hash: &mut u64, bytes: &[u8]) {
    for byte in bytes {
        *hash = (*hash ^ u64::from(*byte)).wrapping_mul(0x100000001b3);
    }
}

fn checksum_u64(hash: &mut u64, value: u64) {
    checksum_bytes(hash, &value.to_le_bytes());
}

fn checksum_str(hash: &mut u64, value: &str) {
    checksum_u64(hash, value.len() as u64);
    checksum_bytes(hash, value.as_bytes());
}

fn checksum_market_event(mut hash: u64, event: &MarketEvent) -> u64 {
    checksum_u64(&mut hash, u64::from(event.schema_version));
    checksum_u64(&mut hash, event.ordinal);
    checksum_str(&mut hash, &event.instrument);
    checksum_str(&mut hash, &event.source);
    match event.source_time_ns {
        Some(value) => {
            checksum_bytes(&mut hash, &[1]);
            checksum_u64(&mut hash, value);
        }
        None => checksum_bytes(&mut hash, &[0]),
    }
    checksum_u64(&mut hash, event.receive_time_ns);
    match &event.raw_payload {
        Some(value) => {
            checksum_bytes(&mut hash, &[1]);
            checksum_str(&mut hash, value);
        }
        None => checksum_bytes(&mut hash, &[0]),
    }
    match &event.payload {
        MarketPayload::Quote {
            bid_ticks,
            bid_lots,
            ask_ticks,
            ask_lots,
        } => {
            checksum_bytes(&mut hash, &[0]);
            checksum_option_i64(&mut hash, *bid_ticks);
            checksum_u64(&mut hash, *bid_lots);
            checksum_option_i64(&mut hash, *ask_ticks);
            checksum_u64(&mut hash, *ask_lots);
        }
        MarketPayload::Trade {
            price_ticks,
            quantity_lots,
        } => {
            checksum_bytes(&mut hash, &[1]);
            checksum_u64(&mut hash, *price_ticks as u64);
            checksum_u64(&mut hash, *quantity_lots);
        }
        MarketPayload::OtherTick {
            tick_type,
            value_f64_bits,
        } => {
            checksum_bytes(&mut hash, &[2]);
            checksum_str(&mut hash, tick_type);
            checksum_option_u64(&mut hash, *value_f64_bits);
        }
        MarketPayload::SessionStart { session_id } => {
            checksum_bytes(&mut hash, &[3]);
            checksum_str(&mut hash, session_id);
        }
        MarketPayload::SessionEnd {
            session_id,
            complete,
        } => {
            checksum_bytes(&mut hash, &[4, u8::from(*complete)]);
            checksum_str(&mut hash, session_id);
        }
        MarketPayload::DepthSnapshotBegin { venue } => {
            checksum_bytes(&mut hash, &[5]);
            checksum_str(&mut hash, venue);
        }
        MarketPayload::DepthSnapshotComplete { venue } => {
            checksum_bytes(&mut hash, &[6]);
            checksum_str(&mut hash, venue);
        }
        MarketPayload::DepthUpdate {
            venue,
            side,
            action,
        } => {
            checksum_bytes(&mut hash, &[7]);
            checksum_str(&mut hash, venue);
            checksum_bytes(
                &mut hash,
                &[match side {
                    crate::market_data::BookSide::Bid => 0,
                    crate::market_data::BookSide::Ask => 1,
                }],
            );
            match action {
                crate::market_data::DepthAction::Insert { position, level } => {
                    checksum_bytes(&mut hash, &[0]);
                    checksum_u64(&mut hash, *position as u64);
                    checksum_depth_level(&mut hash, level);
                }
                crate::market_data::DepthAction::Update { position, level } => {
                    checksum_bytes(&mut hash, &[1]);
                    checksum_u64(&mut hash, *position as u64);
                    checksum_depth_level(&mut hash, level);
                }
                crate::market_data::DepthAction::Delete { position } => {
                    checksum_bytes(&mut hash, &[2]);
                    checksum_u64(&mut hash, *position as u64);
                }
            }
        }
        MarketPayload::DepthGap { venue, reason } => {
            checksum_bytes(&mut hash, &[8]);
            checksum_str(&mut hash, venue);
            checksum_str(&mut hash, reason);
        }
        MarketPayload::Disconnect { feed, reason } => {
            checksum_bytes(&mut hash, &[9]);
            checksum_str(&mut hash, feed);
            checksum_str(&mut hash, reason);
        }
        MarketPayload::Reconnect { feed } => {
            checksum_bytes(&mut hash, &[10]);
            checksum_str(&mut hash, feed);
        }
    }
    hash
}

fn checksum_option_i64(hash: &mut u64, value: Option<i64>) {
    match value {
        Some(value) => {
            checksum_bytes(hash, &[1]);
            checksum_u64(hash, value as u64);
        }
        None => checksum_bytes(hash, &[0]),
    }
}

fn checksum_option_u64(hash: &mut u64, value: Option<u64>) {
    match value {
        Some(value) => {
            checksum_bytes(hash, &[1]);
            checksum_u64(hash, value);
        }
        None => checksum_bytes(hash, &[0]),
    }
}

fn checksum_depth_level(hash: &mut u64, level: &crate::market_data::DepthLevel) {
    checksum_u64(hash, level.price_ticks as u64);
    checksum_u64(hash, level.quantity_lots);
    checksum_str(hash, &level.market_maker);
}

fn generate_callbacks(events: usize) -> Vec<SyntheticCallback> {
    (0..events).map(synthetic_callback).collect()
}

fn synthetic_callback(index: usize) -> SyntheticCallback {
    if index.is_multiple_of(10) {
        let is_bid = index.is_multiple_of(20);
        SyntheticCallback::Depth(MarketDepths::MarketDepth(MarketDepth {
            position: 0,
            operation: if index < 20 { 0 } else { 1 },
            side: if is_bid { 1 } else { 0 },
            price: if is_bid { 100.00 } else { 100.01 },
            size: 10.0 + (index % 90) as f64,
        }))
    } else if index % 4 == 1 {
        SyntheticCallback::L1(TickTypes::Price(TickPrice {
            tick_type: TickType::Bid,
            price: 100.00 + ((index / 4) % 5) as f64 * 0.01,
            ..TickPrice::default()
        }))
    } else if index % 4 == 2 {
        SyntheticCallback::L1(TickTypes::Size(TickSize {
            tick_type: TickType::BidSize,
            size: (1 + index % 500) as f64,
        }))
    } else if index % 4 == 3 {
        SyntheticCallback::L1(TickTypes::Price(TickPrice {
            tick_type: TickType::Ask,
            price: 100.01 + ((index / 4) % 5) as f64 * 0.01,
            ..TickPrice::default()
        }))
    } else {
        SyntheticCallback::L1(TickTypes::Size(TickSize {
            tick_type: TickType::AskSize,
            size: (1 + index % 500) as f64,
        }))
    }
}

fn replay_again(events: &[MarketEvent]) -> Result<u64, String> {
    let mut replay = MarketReplay::new();
    for event in events {
        replay.apply(event).map_err(|error| error.to_string())?;
    }
    Ok(replay.state_fingerprint())
}

fn checksum_events(events: &[MarketEvent]) -> Result<u64, String> {
    let mut checksum = 0xcbf29ce484222325_u64;
    for event in events {
        for byte in serde_json::to_vec(event).map_err(|error| error.to_string())? {
            checksum = (checksum ^ u64::from(byte)).wrapping_mul(0x100000001b3);
        }
    }
    Ok(checksum)
}

fn stage(events: usize, timings: &[u64], elapsed_ns: u64) -> StageResult {
    let mut sorted = timings.to_vec();
    sorted.sort_unstable();
    let rate = if elapsed_ns == 0 {
        0.0
    } else {
        events as f64 * 1e9 / elapsed_ns as f64
    };
    StageResult {
        events,
        elapsed_ns,
        events_per_second: rate,
        p50_ns: percentile(&sorted, 50),
        p95_ns: percentile(&sorted, 95),
        p99_ns: percentile(&sorted, 99),
        p999_ns: percentile_per_mille(&sorted, 999),
        max_ns: sorted.last().copied().unwrap_or(0),
    }
}

fn elapsed_ns(started: Instant) -> u64 {
    started.elapsed().as_nanos().min(u128::from(u64::MAX)) as u64
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{SystemTime, UNIX_EPOCH};

    #[test]
    fn integrated_market_pipeline_round_trips_and_replays_deterministically() {
        let path = std::env::temp_dir().join(format!(
            "market-benchmark-test-{}-{}.jsonl",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let result = run(1_000, &path).unwrap();
        std::fs::remove_file(&path).unwrap();

        assert_eq!(result.source_callbacks, 1_000);
        assert_eq!(result.normalized_events, 1_004);
        assert!(result.deterministic_replay);
        assert!(!result.incomplete);
        assert!(result.file_bytes > 0);
        assert!(result.in_memory_end_to_end.events_per_second > 0.0);
    }

    #[test]
    fn market_spsc_pipeline_checks_order_loss_and_replay_determinism() {
        let options = MarketSpscOptions {
            events: 2_000,
            offered_rate: 250_000,
            capacity: 64,
            sample_interval: 32,
            burst_size: 8,
            retain_raw_payload: true,
            producer_core: None,
            consumer_core: None,
        };
        let first = run_spsc(options).unwrap();
        let second = run_spsc(options).unwrap();

        assert_eq!(first.queue_events, options.events);
        assert_eq!(first.applied_events, options.events + 4);
        assert_eq!(first.event_order_errors, 0);
        assert!(first.checksums_match);
        assert_eq!(first.checksum, second.checksum);
        assert_eq!(first.state_fingerprint, second.state_fingerprint);
        assert!(!first.incomplete);
        assert_eq!(first.scheduled_to_completion.samples, 63);
        assert_eq!(first.release_lateness.samples, 63);
    }
}
