//! Offline benchmark for IBKR-shaped normalization, JSONL persistence and replay.
use crate::ibkr::{normalize_depth, L1Normalizer};
use crate::market_data::{MarketEvent, MarketPayload, MarketReplay};
use ibapi::contracts::tick_types::TickType;
use ibapi::market_data::realtime::{MarketDepth, MarketDepths, TickPrice, TickSize, TickTypes};
use std::fs::{File, OpenOptions};
use std::io::{BufRead, BufReader, BufWriter, Write};
use std::path::Path;
use std::time::Instant;

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

fn generate_callbacks(events: usize) -> Vec<SyntheticCallback> {
    (0..events)
        .map(|index| {
            if index % 10 == 0 {
                let is_bid = index % 20 == 0;
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
        })
        .collect()
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
}
