//! Frozen-workload benchmarks for the simulated exchange request path.

use crate::gateway::{GatewayConfig, GatewayOutcome, GatewayRequest, SimulatedExchange};
use crate::journal::{self, ExchangeJournal, JournalOptions};
use serde::{Deserialize, Serialize};
use std::fs::File;
use std::io::{BufRead, BufWriter, Cursor, Write};
use std::time::Instant;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn gateway_benchmark_uses_supplied_risk_limits() {
        let workload = frozen_workload(20, 7);
        let unrestricted = run_gateway(&workload, 1).unwrap();
        let config = GatewayConfig {
            max_client_open_quantity_lots: 0,
            ..GatewayConfig::default()
        };
        let restricted = run_gateway_with_config(&workload, 1, config).unwrap();
        assert_eq!(restricted.commands, 20);
        assert_eq!(restricted.mismatches, 0);
        assert_ne!(restricted.state_checksum, unrestricted.state_checksum);
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StageResult {
    pub stage: String,
    pub trial: usize,
    pub commands: usize,
    pub throughput_per_second: f64,
    pub elapsed_ns: u128,
    pub p50_ns: u64,
    pub p95_ns: u64,
    pub p99_ns: u64,
    pub p999_ns: u64,
    pub max_ns: u64,
    pub mismatches: usize,
    pub state_checksum: u64,
    pub journal_bytes: usize,
}

pub fn frozen_workload(commands: usize, seed: u64) -> Vec<u8> {
    let mut out = Vec::with_capacity(commands.saturating_mul(140));
    let mut state = seed;
    for n in 1..=commands {
        state = state.wrapping_add(0x9e3779b97f4a7c15);
        let noise = (state ^ (state >> 30)).wrapping_mul(0xbf58476d1ce4e5b9);
        let side = if n % 2 == 0 { "Buy" } else { "Sell" };
        let price = 10_000 + (noise % 3) as i64;
        let request = GatewayRequest {
            client_id: (n % 4 + 1) as u64,
            request_id: (n / 4 + 1) as u64,
            command: crate::gateway::GatewayCommand::New {
                client_order_id: n as u64,
                symbol: "BENCH".into(),
                side: if side == "Buy" {
                    crate::Side::Buy
                } else {
                    crate::Side::Sell
                },
                price_ticks: price,
                quantity_lots: 1 + (noise % 5),
                time_in_force: crate::TimeInForce::Gtc,
                post_only: false,
            },
        };
        serde_json::to_writer(&mut out, &request).expect("request serialization");
        out.push(b'\n');
    }
    out
}

pub fn run_gateway(bytes: &[u8], trial: usize) -> Result<StageResult, String> {
    run_gateway_with_config(bytes, trial, GatewayConfig::default())
}

pub fn run_gateway_with_config(
    bytes: &[u8],
    trial: usize,
    config: GatewayConfig,
) -> Result<StageResult, String> {
    let mut exchange = SimulatedExchange::new(config);
    let mut latencies = Vec::with_capacity(bytes.iter().filter(|byte| **byte == b'\n').count());
    let mut output = Vec::new();
    let start = Instant::now();
    let mut mismatches = 0;
    for line in Cursor::new(bytes).lines() {
        let tick = Instant::now();
        let request = line
            .map_err(|error| error.to_string())
            .and_then(|line| serde_json::from_str(&line).map_err(|error| error.to_string()))?;
        let outcome = match exchange.handle(request) {
            Ok(response) => GatewayOutcome::Response(response),
            Err(error) => {
                mismatches += 1;
                GatewayOutcome::Error(error)
            }
        };
        serde_json::to_writer(&mut output, &outcome).map_err(|error| error.to_string())?;
        output.push(b'\n');
        latencies.push(tick.elapsed().as_nanos().min(u128::from(u64::MAX)) as u64);
    }
    let elapsed = start.elapsed();
    Ok(finish(
        "gateway",
        trial,
        latencies,
        elapsed,
        mismatches,
        exchange.state_fingerprint(),
        0,
    ))
}

pub fn run_journal(
    bytes: &[u8],
    trial: usize,
    path: &std::path::Path,
    options: JournalOptions,
) -> Result<(StageResult, Vec<u8>), String> {
    let file = File::create(path).map_err(|error| error.to_string())?;
    let writer = BufWriter::new(file);
    let mut journal = ExchangeJournal::with_options(GatewayConfig::default(), writer, options);
    let mut latencies = Vec::with_capacity(bytes.iter().filter(|byte| **byte == b'\n').count());
    let start = Instant::now();
    let mut mismatches = 0;
    for line in Cursor::new(bytes).lines() {
        let tick = Instant::now();
        let request = line
            .map_err(|error| error.to_string())
            .and_then(|line| serde_json::from_str(&line).map_err(|error| error.to_string()))?;
        if journal.handle(request).is_err() {
            mismatches += 1;
        }
        latencies.push(tick.elapsed().as_nanos().min(u128::from(u64::MAX)) as u64);
    }
    let elapsed = start.elapsed();
    let checksum = journal.exchange().state_fingerprint();
    let mut writer = journal.into_writer().map_err(|error| error.to_string())?;
    writer.flush().map_err(|error| error.to_string())?;
    drop(writer);
    let journal_bytes = std::fs::read(path).map_err(|error| error.to_string())?;
    let size = journal_bytes.len();
    let result = finish(
        "journal", trial, latencies, elapsed, mismatches, checksum, size,
    );
    Ok((result, journal_bytes))
}

pub fn run_replay(journal_bytes: &[u8], trial: usize) -> Result<StageResult, String> {
    let commands = journal_bytes.iter().filter(|byte| **byte == b'\n').count();
    let start = Instant::now();
    let (recovered, latencies) =
        journal::recover_profiled(Cursor::new(journal_bytes), GatewayConfig::default())
            .map_err(|error| error.to_string())?;
    let elapsed = start.elapsed();
    let mut result = finish(
        "replay",
        trial,
        latencies,
        elapsed,
        0,
        recovered.summary.state_checksum,
        journal_bytes.len(),
    );
    result.commands = commands;
    Ok(result)
}

pub fn append_outcomes_to<W: Write>(bytes: &[u8], mut writer: W) -> Result<(), String> {
    let mut exchange = SimulatedExchange::new(GatewayConfig::default());
    for request in parse_workload(bytes)? {
        let outcome = match exchange.handle(request) {
            Ok(response) => GatewayOutcome::Response(response),
            Err(error) => GatewayOutcome::Error(error),
        };
        serde_json::to_writer(&mut writer, &outcome).map_err(|error| error.to_string())?;
        writer.write_all(b"\n").map_err(|error| error.to_string())?;
    }
    Ok(())
}

fn parse_workload(bytes: &[u8]) -> Result<Vec<GatewayRequest>, String> {
    Cursor::new(bytes)
        .lines()
        .map(|line| {
            let line = line.map_err(|error| error.to_string())?;
            serde_json::from_str(&line).map_err(|error| error.to_string())
        })
        .collect()
}

fn finish(
    stage: &str,
    trial: usize,
    mut latencies: Vec<u64>,
    elapsed: std::time::Duration,
    mismatches: usize,
    checksum: u64,
    journal_bytes: usize,
) -> StageResult {
    latencies.sort_unstable();
    let commands = latencies.len();
    let quantile = |per_mille: usize| -> u64 {
        if latencies.is_empty() {
            return 0;
        }
        let index = (per_mille * latencies.len()).div_ceil(1_000).max(1) - 1;
        latencies[index.min(latencies.len() - 1)]
    };
    StageResult {
        stage: stage.into(),
        trial,
        commands,
        throughput_per_second: commands as f64 / elapsed.as_secs_f64(),
        elapsed_ns: elapsed.as_nanos(),
        p50_ns: quantile(500),
        p95_ns: quantile(950),
        p99_ns: quantile(990),
        p999_ns: quantile(999),
        max_ns: latencies.last().copied().unwrap_or(0),
        mismatches,
        state_checksum: checksum,
        journal_bytes,
    }
}
