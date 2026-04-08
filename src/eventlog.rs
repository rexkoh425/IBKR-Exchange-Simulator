//! Versioned JSONL command capture and deterministic matching-engine replay.

use crate::{MatchingEngine, OrderId, OrderRequest, RejectReason, RestingOrder, SubmitResult};
use serde::{Deserialize, Serialize};
use std::io::{BufRead, Write};

pub const EVENT_SCHEMA_VERSION: u16 = 1;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum EngineCommand {
    Submit { order: OrderRequest },
    Cancel { order_id: OrderId },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RecordedCommand {
    pub schema_version: u16,
    pub ordinal: u64,
    pub command: EngineCommand,
}

impl RecordedCommand {
    pub fn new(ordinal: u64, command: EngineCommand) -> Self {
        Self {
            schema_version: EVENT_SCHEMA_VERSION,
            ordinal,
            command,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReplayOutcome {
    Submit(SubmitResult),
    Cancel(Result<RestingOrder, RejectReason>),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplayReceipt {
    pub ordinal: u64,
    pub outcome: ReplayOutcome,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplayReport {
    pub receipts: Vec<ReplayReceipt>,
    pub final_state_fingerprint: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReplayError {
    UnsupportedSchema { ordinal: u64, version: u16 },
    InvalidOrdinal { expected: u64, actual: u64 },
}

impl std::fmt::Display for ReplayError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::UnsupportedSchema { ordinal, version } => write!(
                f,
                "event {ordinal} uses unsupported schema version {version}"
            ),
            Self::InvalidOrdinal { expected, actual } => {
                write!(f, "expected event ordinal {expected}, got {actual}")
            }
        }
    }
}

impl std::error::Error for ReplayError {}

/// Write one JSON object per line, preserving the supplied command order.
pub fn write_jsonl<W: Write>(
    writer: &mut W,
    events: &[RecordedCommand],
) -> Result<(), serde_json::Error> {
    for event in events {
        serde_json::to_writer(&mut *writer, event)?;
        writer.write_all(b"\n").map_err(serde_json::Error::io)?;
    }
    Ok(())
}

/// Read JSONL records and report malformed input with its 1-based line number.
pub fn read_jsonl<R: BufRead>(reader: R) -> Result<Vec<RecordedCommand>, String> {
    let mut events = Vec::new();
    for (line_index, line_result) in reader.lines().enumerate() {
        let line_number = line_index + 1;
        let line = line_result.map_err(|error| format!("line {line_number}: {error}"))?;
        if line.trim().is_empty() {
            continue;
        }
        let event =
            serde_json::from_str(&line).map_err(|error| format!("line {line_number}: {error}"))?;
        events.push(event);
    }
    Ok(events)
}

/// Apply a captured command stream to a fresh engine in ordinal order.
pub fn replay(
    engine: &mut MatchingEngine,
    events: &[RecordedCommand],
) -> Result<ReplayReport, ReplayError> {
    let mut receipts = Vec::with_capacity(events.len());
    for (index, event) in events.iter().enumerate() {
        let expected = u64::try_from(index + 1).unwrap_or(u64::MAX);
        if event.schema_version != EVENT_SCHEMA_VERSION {
            return Err(ReplayError::UnsupportedSchema {
                ordinal: event.ordinal,
                version: event.schema_version,
            });
        }
        if event.ordinal != expected {
            return Err(ReplayError::InvalidOrdinal {
                expected,
                actual: event.ordinal,
            });
        }
        let outcome = match &event.command {
            EngineCommand::Submit { order } => ReplayOutcome::Submit(engine.submit(*order)),
            EngineCommand::Cancel { order_id } => ReplayOutcome::Cancel(engine.cancel(*order_id)),
        };
        receipts.push(ReplayReceipt {
            ordinal: event.ordinal,
            outcome,
        });
    }
    Ok(ReplayReport {
        receipts,
        final_state_fingerprint: engine.state_fingerprint(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Side;
    use std::io::Cursor;

    fn fixture() -> Vec<RecordedCommand> {
        vec![
            RecordedCommand::new(
                1,
                EngineCommand::Submit {
                    order: OrderRequest {
                        order_id: 10,
                        side: Side::Sell,
                        price_ticks: 101,
                        quantity_lots: 4,
                    },
                },
            ),
            RecordedCommand::new(
                2,
                EngineCommand::Submit {
                    order: OrderRequest {
                        order_id: 11,
                        side: Side::Sell,
                        price_ticks: 101,
                        quantity_lots: 3,
                    },
                },
            ),
            RecordedCommand::new(
                3,
                EngineCommand::Submit {
                    order: OrderRequest {
                        order_id: 12,
                        side: Side::Buy,
                        price_ticks: 105,
                        quantity_lots: 5,
                    },
                },
            ),
            RecordedCommand::new(4, EngineCommand::Cancel { order_id: 11 }),
        ]
    }

    #[test]
    fn jsonl_round_trip_replays_to_identical_state() {
        let events = fixture();
        let mut bytes = Vec::new();
        write_jsonl(&mut bytes, &events).unwrap();
        let decoded = read_jsonl(Cursor::new(bytes)).unwrap();
        assert_eq!(decoded, events);
        let first = replay(&mut MatchingEngine::new(), &decoded).unwrap();
        let second = replay(&mut MatchingEngine::new(), &decoded).unwrap();
        assert_eq!(first, second);
        assert_eq!(first.receipts.len(), 4);
    }

    #[test]
    fn replay_rejects_bad_ordinals_and_schema_versions() {
        let mut events = fixture();
        events[1].ordinal = 3;
        assert_eq!(
            replay(&mut MatchingEngine::new(), &events),
            Err(ReplayError::InvalidOrdinal {
                expected: 2,
                actual: 3
            })
        );
        let mut events = fixture();
        events[0].schema_version = EVENT_SCHEMA_VERSION + 1;
        assert_eq!(
            replay(&mut MatchingEngine::new(), &events),
            Err(ReplayError::UnsupportedSchema {
                ordinal: 1,
                version: EVENT_SCHEMA_VERSION + 1
            })
        );
    }

    #[test]
    fn malformed_jsonl_reports_line_number() {
        let error = read_jsonl(Cursor::new(b"\nnot-json\n".to_vec())).unwrap_err();
        assert!(error.starts_with("line 2:"));
    }
}
