//! Versioned append-only journal for deterministic simulated-exchange replay.
//!
//! Records are flushed to the writer after every request but are not fsynced.
//! This detects torn/corrupt records and supports process restart from complete
//! records; it does not promise power-loss or OS-crash durability.

use crate::gateway::{
    ExchangeStateSnapshot, ExecutionReport, GatewayConfig, GatewayOutcome, GatewayRequest,
    OrderStateSnapshot, OrderStatus, SimulatedExchange,
};
use serde::{Deserialize, Serialize};
use std::io::{BufRead, Write};
use std::time::Instant;

pub const LEGACY_EXCHANGE_JOURNAL_SCHEMA_VERSION: u16 = 1;
pub const EXCHANGE_JOURNAL_SCHEMA_VERSION: u16 = 2;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExchangeJournalEntry {
    pub schema_version: u16,
    pub journal_sequence: u64,
    pub request: GatewayRequest,
    pub outcome: GatewayOutcome,
    #[serde(default)]
    pub state_checksum: Option<u64>,
    pub checksum: u64,
}

/// Controls the journal's full-state verification and writer flush cadence.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct JournalOptions {
    /// Store and verify a full fingerprint every N commands. One verifies each
    /// record; larger intervals avoid repeated whole-state traversals.
    pub full_state_checksum_interval: u64,
    /// Flush the writer after this many records. This is not an fsync policy.
    pub flush_every_entries: u64,
}

impl Default for JournalOptions {
    fn default() -> Self {
        Self {
            full_state_checksum_interval: 4096,
            flush_every_entries: 1,
        }
    }
}

#[derive(Debug)]
pub enum JournalError {
    Io(std::io::Error),
    SequenceExhausted,
    TruncatedRecord {
        line: u64,
    },
    CorruptRecord {
        line: u64,
        detail: String,
    },
    UnsupportedVersion {
        line: u64,
        version: u16,
    },
    SequenceMismatch {
        line: u64,
        expected: u64,
        actual: u64,
    },
    ChecksumMismatch {
        line: u64,
    },
    OutcomeMismatch {
        line: u64,
    },
    StateChecksumMismatch {
        line: u64,
        expected: u64,
        actual: u64,
    },
}

impl std::fmt::Display for JournalError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Io(error) => write!(formatter, "journal I/O error: {error}"),
            Self::SequenceExhausted => formatter.write_str("journal sequence space exhausted"),
            Self::TruncatedRecord { line } => {
                write!(formatter, "journal line {line} is missing its newline terminator")
            }
            Self::CorruptRecord { line, detail } => {
                write!(formatter, "journal line {line} is corrupt: {detail}")
            }
            Self::UnsupportedVersion { line, version } => {
                write!(formatter, "journal line {line} uses unsupported schema {version}")
            }
            Self::SequenceMismatch { line, expected, actual } => write!(
                formatter,
                "journal line {line} sequence mismatch: expected {expected}, got {actual}"
            ),
            Self::ChecksumMismatch { line } => {
                write!(formatter, "journal line {line} checksum mismatch")
            }
            Self::OutcomeMismatch { line } => {
                write!(formatter, "journal line {line} does not replay to its recorded outcome")
            }
            Self::StateChecksumMismatch { line, expected, actual } => write!(
                formatter,
                "journal line {line} state checksum mismatch: expected {expected:016x}, got {actual:016x}"
            ),
        }
    }
}

impl std::error::Error for JournalError {}

impl From<std::io::Error> for JournalError {
    fn from(error: std::io::Error) -> Self {
        Self::Io(error)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct JournalSummary {
    pub entries: u64,
    pub accepted_requests: u64,
    pub rejected_commands: u64,
    pub protocol_rejections: u64,
    pub execution_reports: Vec<ExecutionReport>,
    pub order_states: Vec<OrderStateSnapshot>,
    pub state_checksum: u64,
}

impl JournalSummary {
    fn new() -> Self {
        Self {
            entries: 0,
            accepted_requests: 0,
            rejected_commands: 0,
            protocol_rejections: 0,
            execution_reports: Vec::new(),
            order_states: Vec::new(),
            state_checksum: 0,
        }
    }

    fn record_outcome(&mut self, outcome: &GatewayOutcome) {
        match outcome {
            GatewayOutcome::Response(response) => {
                if response
                    .reports
                    .iter()
                    .any(|report| report.status == OrderStatus::Rejected)
                {
                    self.rejected_commands += 1;
                } else {
                    self.accepted_requests += 1;
                }
                if !response.replayed {
                    self.execution_reports.extend(response.reports.clone());
                }
            }
            GatewayOutcome::Error(_) => self.protocol_rejections += 1,
        }
    }

    fn finish(&mut self, exchange: &SimulatedExchange) {
        self.order_states = exchange.order_states();
        self.state_checksum = exchange.state_fingerprint();
    }

    pub fn rejected_execution_reports(&self) -> usize {
        self.execution_reports
            .iter()
            .filter(|report| report.status == OrderStatus::Rejected)
            .count()
    }

    pub fn snapshot(&self) -> ExchangeStateSnapshot {
        ExchangeStateSnapshot {
            state_fingerprint: self.state_checksum,
            orders: self.order_states.clone(),
        }
    }
}

#[derive(Debug)]
pub struct RecoveryResult {
    pub exchange: SimulatedExchange,
    pub summary: JournalSummary,
}

/// Recorder that applies requests and appends their deterministic result.
/// A failed write is returned immediately; the caller must stop using the
/// in-memory exchange because the mutation may not have reached the journal.
pub struct ExchangeJournal<W: Write> {
    exchange: SimulatedExchange,
    writer: W,
    next_sequence: u64,
    summary: JournalSummary,
    options: JournalOptions,
    pending_flush_entries: u64,
}

impl<W: Write> ExchangeJournal<W> {
    pub fn new(config: GatewayConfig, writer: W) -> Self {
        Self::with_options(config, writer, JournalOptions::default())
    }

    pub fn with_options(config: GatewayConfig, writer: W, options: JournalOptions) -> Self {
        Self {
            exchange: SimulatedExchange::new(config),
            writer,
            next_sequence: 0,
            summary: JournalSummary::new(),
            options: JournalOptions {
                full_state_checksum_interval: options.full_state_checksum_interval.max(1),
                flush_every_entries: options.flush_every_entries.max(1),
            },
            pending_flush_entries: 0,
        }
    }

    pub fn from_recovery(recovery: RecoveryResult, writer: W) -> Self {
        Self::from_recovery_with_options(recovery, writer, JournalOptions::default())
    }

    pub fn from_recovery_with_options(
        recovery: RecoveryResult,
        writer: W,
        options: JournalOptions,
    ) -> Self {
        Self {
            exchange: recovery.exchange,
            writer,
            next_sequence: recovery.summary.entries,
            summary: recovery.summary,
            options: JournalOptions {
                full_state_checksum_interval: options.full_state_checksum_interval.max(1),
                flush_every_entries: options.flush_every_entries.max(1),
            },
            pending_flush_entries: 0,
        }
    }

    pub fn handle(&mut self, request: GatewayRequest) -> Result<GatewayOutcome, JournalError> {
        let sequence = self
            .next_sequence
            .checked_add(1)
            .ok_or(JournalError::SequenceExhausted)?;
        let outcome = match self.exchange.handle(request.clone()) {
            Ok(response) => GatewayOutcome::Response(response),
            Err(error) => GatewayOutcome::Error(error),
        };
        let state_checksum = (sequence % self.options.full_state_checksum_interval == 0)
            .then(|| self.exchange.state_fingerprint());
        let checksum = entry_checksum_v2(
            EXCHANGE_JOURNAL_SCHEMA_VERSION,
            sequence,
            &request,
            &outcome,
            state_checksum,
        );
        let entry = ExchangeJournalEntry {
            schema_version: EXCHANGE_JOURNAL_SCHEMA_VERSION,
            journal_sequence: sequence,
            request,
            outcome: outcome.clone(),
            state_checksum,
            checksum,
        };
        serde_json::to_writer(&mut self.writer, &entry).map_err(|error| {
            JournalError::Io(std::io::Error::other(format!(
                "serialize journal entry: {error}"
            )))
        })?;
        self.writer.write_all(b"\n")?;
        self.pending_flush_entries += 1;
        self.next_sequence = sequence;
        self.summary.entries = sequence;
        self.summary.record_outcome(&outcome);
        if self.pending_flush_entries >= self.options.flush_every_entries {
            self.flush()?;
        }
        Ok(outcome)
    }

    /// Flush buffered records to the underlying writer. This does not request
    /// durable storage from the operating system.
    pub fn flush(&mut self) -> Result<(), JournalError> {
        self.writer.flush()?;
        self.pending_flush_entries = 0;
        Ok(())
    }

    pub fn summary(&self) -> JournalSummary {
        let mut summary = self.summary.clone();
        summary.finish(&self.exchange);
        summary
    }

    pub fn exchange(&self) -> &SimulatedExchange {
        &self.exchange
    }

    pub fn into_writer(mut self) -> Result<W, JournalError> {
        self.flush()?;
        Ok(self.writer)
    }
}

/// Apply and journal an input JSONL stream while writing one JSON outcome per
/// request. The returned summary contains the same ordered report history that
/// recovery reconstructs from the journal.
pub fn run_jsonl_recorded<R: BufRead, O: Write, J: Write>(
    reader: R,
    mut output: O,
    journal: &mut ExchangeJournal<J>,
) -> Result<JournalSummary, String> {
    for (line_index, line) in reader.lines().enumerate() {
        let line = line.map_err(|error| format!("line {}: {error}", line_index + 1))?;
        if line.trim().is_empty() {
            continue;
        }
        let request: GatewayRequest = serde_json::from_str(&line)
            .map_err(|error| format!("line {}: {error}", line_index + 1))?;
        let outcome = journal
            .handle(request)
            .map_err(|error| format!("line {}: {error}", line_index + 1))?;
        serde_json::to_writer(&mut output, &outcome).map_err(|error| error.to_string())?;
        output.write_all(b"\n").map_err(|error| error.to_string())?;
    }
    journal.flush().map_err(|error| error.to_string())?;
    output.flush().map_err(|error| error.to_string())?;
    Ok(journal.summary())
}

/// Validate every journal frame, replay requests, and compare stored outcomes
/// and state checksums before returning the reconstructed exchange.
pub fn recover<R: BufRead>(
    reader: R,
    config: GatewayConfig,
) -> Result<RecoveryResult, JournalError> {
    recover_profiled(reader, config).map(|(result, _)| result)
}

/// Recover a journal and return per-entry validation/replay service times.
pub fn recover_profiled<R: BufRead>(
    mut reader: R,
    config: GatewayConfig,
) -> Result<(RecoveryResult, Vec<u64>), JournalError> {
    let mut exchange = SimulatedExchange::new(config);
    let mut summary = JournalSummary::new();
    let mut line_number = 0_u64;
    let mut expected_sequence = 1_u64;
    let mut bytes = Vec::new();
    let mut latencies = Vec::new();
    loop {
        let entry_started = Instant::now();
        bytes.clear();
        if reader.read_until(b'\n', &mut bytes)? == 0 {
            break;
        }
        line_number += 1;
        if bytes.last() != Some(&b'\n') {
            return Err(JournalError::TruncatedRecord { line: line_number });
        }
        bytes.pop();
        if bytes.last() == Some(&b'\r') {
            bytes.pop();
        }
        let entry: ExchangeJournalEntry =
            serde_json::from_slice(&bytes).map_err(|error| JournalError::CorruptRecord {
                line: line_number,
                detail: error.to_string(),
            })?;
        if entry.journal_sequence != expected_sequence {
            return Err(JournalError::SequenceMismatch {
                line: line_number,
                expected: expected_sequence,
                actual: entry.journal_sequence,
            });
        }
        let calculated_checksum = match entry.schema_version {
            LEGACY_EXCHANGE_JOURNAL_SCHEMA_VERSION => entry_checksum_v1(
                entry.schema_version,
                entry.journal_sequence,
                &entry.request,
                &entry.outcome,
                entry
                    .state_checksum
                    .ok_or_else(|| JournalError::CorruptRecord {
                        line: line_number,
                        detail: "schema v1 record has no full state checksum".into(),
                    })?,
            ),
            EXCHANGE_JOURNAL_SCHEMA_VERSION => entry_checksum_v2(
                entry.schema_version,
                entry.journal_sequence,
                &entry.request,
                &entry.outcome,
                entry.state_checksum,
            ),
            version => {
                return Err(JournalError::UnsupportedVersion {
                    line: line_number,
                    version,
                });
            }
        };
        if calculated_checksum != entry.checksum {
            return Err(JournalError::ChecksumMismatch { line: line_number });
        }
        let actual_outcome = match exchange.handle(entry.request) {
            Ok(response) => GatewayOutcome::Response(response),
            Err(error) => GatewayOutcome::Error(error),
        };
        if actual_outcome != entry.outcome {
            return Err(JournalError::OutcomeMismatch { line: line_number });
        }
        if let Some(expected_state_checksum) = entry.state_checksum {
            let actual_state_checksum = exchange.state_fingerprint();
            if actual_state_checksum != expected_state_checksum {
                return Err(JournalError::StateChecksumMismatch {
                    line: line_number,
                    expected: expected_state_checksum,
                    actual: actual_state_checksum,
                });
            }
        }
        summary.entries = entry.journal_sequence;
        summary.record_outcome(&actual_outcome);
        latencies.push(entry_started.elapsed().as_nanos().min(u128::from(u64::MAX)) as u64);
        expected_sequence = expected_sequence
            .checked_add(1)
            .ok_or(JournalError::SequenceExhausted)?;
    }
    exchange
        .check_invariants()
        .map_err(|detail| JournalError::CorruptRecord {
            line: line_number,
            detail: format!("recovered exchange invariant failed: {detail}"),
        })?;
    summary.finish(&exchange);
    Ok((RecoveryResult { exchange, summary }, latencies))
}

fn entry_checksum_v1(
    schema_version: u16,
    sequence: u64,
    request: &GatewayRequest,
    outcome: &GatewayOutcome,
    state_checksum: u64,
) -> u64 {
    let encoded = serde_json::to_vec(&(schema_version, sequence, request, outcome, state_checksum))
        .expect("journal checksum fields serialize");
    encoded.iter().fold(0xcbf29ce484222325_u64, |hash, byte| {
        (hash ^ u64::from(*byte)).wrapping_mul(0x100000001b3)
    })
}

fn entry_checksum_v2(
    schema_version: u16,
    sequence: u64,
    request: &GatewayRequest,
    outcome: &GatewayOutcome,
    state_checksum: Option<u64>,
) -> u64 {
    let encoded = serde_json::to_vec(&(schema_version, sequence, request, outcome, state_checksum))
        .expect("journal checksum fields serialize");
    encoded.iter().fold(0xcbf29ce484222325_u64, |hash, byte| {
        (hash ^ u64::from(*byte)).wrapping_mul(0x100000001b3)
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::gateway::{GatewayCommand, GatewayError, GatewayRequest};
    use crate::Side;
    use std::io::Cursor;
    use std::{
        cell::{Cell, RefCell},
        rc::Rc,
    };

    fn requests() -> Vec<GatewayRequest> {
        vec![
            GatewayRequest {
                client_id: 1,
                request_id: 1,
                command: GatewayCommand::New {
                    client_order_id: 10,
                    symbol: "AAPL".into(),
                    side: Side::Sell,
                    price_ticks: 100,
                    quantity_lots: 5,
                },
            },
            GatewayRequest {
                client_id: 2,
                request_id: 1,
                command: GatewayCommand::New {
                    client_order_id: 20,
                    symbol: "AAPL".into(),
                    side: Side::Buy,
                    price_ticks: 100,
                    quantity_lots: 2,
                },
            },
            GatewayRequest {
                client_id: 1,
                request_id: 2,
                command: GatewayCommand::Replace {
                    client_order_id: 10,
                    replacement_client_order_id: 11,
                    price_ticks: 99,
                    quantity_lots: 3,
                },
            },
            GatewayRequest {
                client_id: 2,
                request_id: 2,
                command: GatewayCommand::New {
                    client_order_id: 20,
                    symbol: "AAPL".into(),
                    side: Side::Buy,
                    price_ticks: 100,
                    quantity_lots: 2,
                },
            },
        ]
    }

    #[test]
    fn journal_recovers_full_gateway_state_and_report_history() {
        let mut bytes = Vec::new();
        let mut journal = ExchangeJournal::new(GatewayConfig::default(), &mut bytes);
        let mut original_outcomes = Vec::new();
        for request in requests() {
            original_outcomes.push(journal.handle(request).unwrap());
        }
        let original_summary = journal.summary();
        assert_eq!(original_summary.entries, 4);
        assert_eq!(original_summary.accepted_requests, 3);
        assert_eq!(original_summary.rejected_commands, 1);
        assert_eq!(original_summary.execution_reports.len(), 6);
        let original_state = ExchangeStateSnapshot {
            state_fingerprint: journal.exchange().state_fingerprint(),
            orders: journal.exchange().order_states(),
        };
        let original_reports: Vec<_> = original_outcomes
            .iter()
            .flat_map(|outcome| match outcome {
                GatewayOutcome::Response(response) => response.reports.clone(),
                GatewayOutcome::Error(_) => Vec::new(),
            })
            .collect();
        assert_eq!(original_reports, original_summary.execution_reports);
        drop(journal);

        let recovered = recover(Cursor::new(&bytes), GatewayConfig::default()).unwrap();
        assert_eq!(recovered.summary, original_summary);
        assert_eq!(
            ExchangeStateSnapshot {
                state_fingerprint: recovered.exchange.state_fingerprint(),
                orders: recovered.exchange.order_states(),
            },
            original_state
        );
        assert_eq!(recovered.summary.execution_reports.len(), 6);
        assert!(recovered.exchange.check_invariants().is_ok());
        assert_eq!(original_outcomes.len(), 4);
    }

    #[test]
    fn journal_records_protocol_rejections_and_replays_them() {
        let mut bytes = Vec::new();
        let mut journal = ExchangeJournal::new(GatewayConfig::default(), &mut bytes);
        let request = requests()[0].clone();
        assert!(matches!(
            journal.handle(request.clone()).unwrap(),
            GatewayOutcome::Response(_)
        ));
        let conflicting = GatewayRequest {
            command: GatewayCommand::New {
                client_order_id: 99,
                symbol: "AAPL".into(),
                side: Side::Buy,
                price_ticks: 90,
                quantity_lots: 1,
            },
            ..request
        };
        assert_eq!(
            journal.handle(conflicting).unwrap(),
            GatewayOutcome::Error(GatewayError::IdempotencyConflict)
        );
        drop(journal);
        let recovered = recover(Cursor::new(&bytes), GatewayConfig::default()).unwrap();
        assert_eq!(recovered.summary.entries, 2);
        assert_eq!(recovered.summary.accepted_requests, 1);
        assert_eq!(recovered.summary.protocol_rejections, 1);
    }

    #[test]
    fn idempotent_retry_does_not_duplicate_journal_execution_history() {
        let mut bytes = Vec::new();
        let mut journal = ExchangeJournal::new(GatewayConfig::default(), &mut bytes);
        let request = requests()[0].clone();
        journal.handle(request.clone()).unwrap();
        journal.handle(request).unwrap();
        let summary = journal.summary();
        assert_eq!(summary.entries, 2);
        assert_eq!(summary.accepted_requests, 2);
        assert_eq!(summary.execution_reports.len(), 1);
        drop(journal);
        let recovered = recover(Cursor::new(&bytes), GatewayConfig::default()).unwrap();
        assert_eq!(recovered.summary, summary);
    }

    #[test]
    fn journal_detects_truncated_and_corrupt_records() {
        let mut bytes = Vec::new();
        let mut journal = ExchangeJournal::new(GatewayConfig::default(), &mut bytes);
        journal.handle(requests()[0].clone()).unwrap();
        drop(journal);
        let mut truncated = bytes.clone();
        truncated.pop();
        assert!(matches!(
            recover(Cursor::new(truncated), GatewayConfig::default()),
            Err(JournalError::TruncatedRecord { line: 1 })
        ));
        let mut corrupted = bytes;
        let needle = b"\"price_ticks\":100";
        let mutation_index = corrupted
            .windows(needle.len())
            .position(|window| window == needle)
            .expect("record contains a price");
        corrupted[mutation_index + needle.len() - 1] = b'1';
        assert!(matches!(
            recover(Cursor::new(corrupted), GatewayConfig::default()),
            Err(JournalError::CorruptRecord { .. } | JournalError::ChecksumMismatch { .. })
        ));
    }

    #[test]
    fn schema_v1_journals_remain_recoverable() {
        let request = requests()[0].clone();
        let mut exchange = SimulatedExchange::new(GatewayConfig::default());
        let outcome = GatewayOutcome::Response(exchange.handle(request.clone()).unwrap());
        let state_checksum = exchange.state_fingerprint();
        let entry = ExchangeJournalEntry {
            schema_version: LEGACY_EXCHANGE_JOURNAL_SCHEMA_VERSION,
            journal_sequence: 1,
            checksum: entry_checksum_v1(
                LEGACY_EXCHANGE_JOURNAL_SCHEMA_VERSION,
                1,
                &request,
                &outcome,
                state_checksum,
            ),
            request,
            outcome: outcome.clone(),
            state_checksum: Some(state_checksum),
        };
        let mut bytes = serde_json::to_vec(&entry).unwrap();
        bytes.push(b'\n');
        let recovered = recover(Cursor::new(bytes), GatewayConfig::default()).unwrap();
        assert_eq!(recovered.summary.entries, 1);
        assert_eq!(recovered.summary.state_checksum, state_checksum);
        assert_eq!(recovered.summary.execution_reports.len(), 1);
    }

    #[test]
    fn schema_v1_can_be_resumed_and_appended_as_schema_v2() {
        let request = requests()[0].clone();
        let mut exchange = SimulatedExchange::new(GatewayConfig::default());
        let outcome = GatewayOutcome::Response(exchange.handle(request.clone()).unwrap());
        let state_checksum = exchange.state_fingerprint();
        let entry = ExchangeJournalEntry {
            schema_version: LEGACY_EXCHANGE_JOURNAL_SCHEMA_VERSION,
            journal_sequence: 1,
            checksum: entry_checksum_v1(
                LEGACY_EXCHANGE_JOURNAL_SCHEMA_VERSION,
                1,
                &request,
                &outcome,
                state_checksum,
            ),
            request,
            outcome,
            state_checksum: Some(state_checksum),
        };
        let mut legacy = serde_json::to_vec(&entry).unwrap();
        legacy.push(b'\n');
        let recovery = recover(Cursor::new(&legacy), GatewayConfig::default()).unwrap();

        let mut appended = Vec::new();
        let mut journal = ExchangeJournal::from_recovery(recovery, &mut appended);
        journal.handle(requests()[1].clone()).unwrap();
        drop(journal);
        legacy.extend_from_slice(&appended);
        let recovered = recover(Cursor::new(legacy), GatewayConfig::default()).unwrap();
        assert_eq!(recovered.summary.entries, 2);
        assert_eq!(recovered.summary.execution_reports.len(), 3);
    }

    #[test]
    fn schema_v2_checkpoints_verify_full_state_and_skip_other_traversals() {
        let mut bytes = Vec::new();
        let options = JournalOptions {
            full_state_checksum_interval: 8,
            flush_every_entries: 4,
        };
        let mut journal =
            ExchangeJournal::with_options(GatewayConfig::default(), &mut bytes, options);
        for n in 1..=20 {
            journal
                .handle(GatewayRequest {
                    client_id: n % 3 + 1,
                    request_id: n,
                    command: GatewayCommand::New {
                        client_order_id: n,
                        symbol: "CHK".into(),
                        side: if n % 2 == 0 { Side::Buy } else { Side::Sell },
                        price_ticks: 100 + (n % 5) as i64,
                        quantity_lots: 1,
                    },
                })
                .unwrap();
        }
        let expected_summary = journal.summary();
        journal.flush().unwrap();
        drop(journal);

        let mut entries: Vec<ExchangeJournalEntry> = bytes
            .split(|byte| *byte == b'\n')
            .filter(|line| !line.is_empty())
            .map(|line| serde_json::from_slice(line).unwrap())
            .collect();
        assert_eq!(entries.len(), 20);
        for (index, entry) in entries.iter().enumerate() {
            assert_eq!(entry.schema_version, EXCHANGE_JOURNAL_SCHEMA_VERSION);
            assert_eq!(
                entry.state_checksum.is_some(),
                (index as u64 + 1).is_multiple_of(8)
            );
        }
        let recovered = recover(Cursor::new(&bytes), GatewayConfig::default()).unwrap();
        assert_eq!(recovered.summary, expected_summary);

        let checkpoint = &mut entries[7];
        checkpoint.state_checksum = checkpoint.state_checksum.map(|sum| sum.wrapping_add(1));
        checkpoint.checksum = entry_checksum_v2(
            checkpoint.schema_version,
            checkpoint.journal_sequence,
            &checkpoint.request,
            &checkpoint.outcome,
            checkpoint.state_checksum,
        );
        let mut corrupted_checkpoint = Vec::new();
        for entry in entries {
            serde_json::to_writer(&mut corrupted_checkpoint, &entry).unwrap();
            corrupted_checkpoint.push(b'\n');
        }
        assert!(matches!(
            recover(Cursor::new(corrupted_checkpoint), GatewayConfig::default()),
            Err(JournalError::StateChecksumMismatch { line: 8, .. })
        ));
    }

    #[test]
    fn configurable_flush_batches_records_and_flushes_on_finish() {
        #[derive(Clone, Default)]
        struct CountingWriter {
            bytes: Rc<RefCell<Vec<u8>>>,
            flushes: Rc<Cell<usize>>,
        }

        impl Write for CountingWriter {
            fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
                self.bytes.borrow_mut().extend_from_slice(bytes);
                Ok(bytes.len())
            }

            fn flush(&mut self) -> std::io::Result<()> {
                self.flushes.set(self.flushes.get() + 1);
                Ok(())
            }
        }

        let writer = CountingWriter::default();
        let flushes = writer.flushes.clone();
        let bytes = writer.bytes.clone();
        let options = JournalOptions {
            full_state_checksum_interval: 8,
            flush_every_entries: 3,
        };
        let mut journal = ExchangeJournal::with_options(GatewayConfig::default(), writer, options);
        for request in requests().into_iter().take(4) {
            journal.handle(request).unwrap();
        }
        assert_eq!(flushes.get(), 1);
        let writer = journal.into_writer().unwrap();
        assert_eq!(flushes.get(), 2);
        drop(writer);
        assert_eq!(
            bytes
                .borrow()
                .split(|byte| *byte == b'\n')
                .filter(|line| !line.is_empty())
                .count(),
            4
        );
    }
}
