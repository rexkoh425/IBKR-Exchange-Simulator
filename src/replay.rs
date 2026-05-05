//! Streaming deterministic replay of normalized market-data JSONL.
use crate::market_data::{MarketEvent, MarketPayload, MarketReplay};
use std::io::{BufRead, Write};
use std::time::Duration;

#[derive(Debug, Clone, Copy)]
pub struct ReplayOptions {
    /// Playback multiplier. Accepted range is 0.01x through 1000x.
    pub speed: f64,
    /// Wait for an Enter key between events. Enter `p` to pause, `q` to stop.
    pub step: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplaySummary {
    pub events_applied: u64,
    pub checksum: u64,
    pub state_fingerprint: u64,
    pub depth_updates: u64,
    pub quotes: u64,
    pub gaps: u64,
    pub out_of_order_receive_timestamps: u64,
    pub incomplete: bool,
    pub stopped_by_user: bool,
}

pub fn replay_reader<R: BufRead, S: BufRead, W: Write>(
    reader: R,
    mut step_input: S,
    output: &mut W,
    options: ReplayOptions,
) -> Result<ReplaySummary, String> {
    if !options.speed.is_finite() || !(0.01..=1000.0).contains(&options.speed) {
        return Err("replay speed must be within 0.01x..=1000x".into());
    }
    let mut replay = MarketReplay::new();
    let mut checksum = 0xcbf29ce484222325_u64;
    let mut events_applied = 0_u64;
    let mut depth_updates = 0_u64;
    let mut quotes = 0_u64;
    let mut gaps = 0_u64;
    let mut out_of_order_receive_timestamps = 0_u64;
    let mut previous_receive_time = None;
    let mut stopped_by_user = false;
    let mut previous_wall_event_time = None;

    for (line_index, line_result) in reader.lines().enumerate() {
        let line_number = line_index + 1;
        let line = line_result.map_err(|error| format!("line {line_number}: {error}"))?;
        if line.trim().is_empty() {
            continue;
        }
        let event: MarketEvent =
            serde_json::from_str(&line).map_err(|error| format!("line {line_number}: {error}"))?;
        if previous_receive_time.is_some_and(|previous| event.receive_time_ns < previous) {
            out_of_order_receive_timestamps += 1;
        }
        if !options.step {
            if let Some(previous) = previous_wall_event_time {
                let delta = event.receive_time_ns.saturating_sub(previous);
                // Cap a single source-time pause so one bad timestamp cannot stall playback.
                let wait_ns = ((delta as f64) / options.speed).min(100_000_000.0) as u64;
                if wait_ns > 0 {
                    std::thread::sleep(Duration::from_nanos(wait_ns));
                }
            }
        } else if !step_once(&mut step_input, output, event.ordinal)? {
            stopped_by_user = true;
            break;
        }

        let bytes = serde_json::to_vec(&event).map_err(|error| error.to_string())?;
        for byte in bytes {
            checksum = (checksum ^ u64::from(byte)).wrapping_mul(0x100000001b3);
        }
        match event.payload {
            MarketPayload::DepthUpdate { .. } => depth_updates += 1,
            MarketPayload::DepthSnapshotBegin { .. }
            | MarketPayload::DepthSnapshotComplete { .. } => depth_updates += 1,
            MarketPayload::DepthGap { .. } | MarketPayload::Disconnect { .. } => gaps += 1,
            MarketPayload::Quote { .. } => quotes += 1,
            _ => {}
        }
        replay
            .apply(&event)
            .map_err(|error| format!("line {line_number}: {error}"))?;
        events_applied += 1;
        previous_receive_time = Some(event.receive_time_ns);
        previous_wall_event_time = Some(event.receive_time_ns);
    }
    Ok(ReplaySummary {
        events_applied,
        checksum,
        state_fingerprint: replay.state_fingerprint(),
        depth_updates,
        quotes,
        gaps,
        out_of_order_receive_timestamps,
        incomplete: replay.has_incomplete_data(),
        stopped_by_user,
    })
}

fn step_once<S: BufRead, W: Write>(
    input: &mut S,
    output: &mut W,
    ordinal: u64,
) -> Result<bool, String> {
    loop {
        write!(output, "event {ordinal}: Enter=step, p=pause, q=stop > ")
            .map_err(|e| e.to_string())?;
        output.flush().map_err(|e| e.to_string())?;
        let mut command = String::new();
        if input.read_line(&mut command).map_err(|e| e.to_string())? == 0 {
            return Ok(false);
        }
        match command.trim().to_ascii_lowercase().as_str() {
            "q" => return Ok(false),
            "p" => continue,
            _ => return Ok(true),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::market_data::BookSide;
    use crate::market_data::{write_jsonl, DepthAction, DepthLevel};
    use std::io::Cursor;

    fn event(ordinal: u64, payload: MarketPayload) -> MarketEvent {
        MarketEvent::new(ordinal, "AAPL", ordinal * 1_000_000, payload)
    }

    #[test]
    fn replay_is_deterministic_and_flags_unfinished_depth_snapshot() {
        let events = vec![
            event(
                1,
                MarketPayload::Quote {
                    bid_ticks: Some(100),
                    bid_lots: 4,
                    ask_ticks: Some(101),
                    ask_lots: 3,
                },
            ),
            event(
                2,
                MarketPayload::DepthSnapshotBegin {
                    venue: "NASDAQ".into(),
                },
            ),
            event(
                3,
                MarketPayload::DepthUpdate {
                    venue: "NASDAQ".into(),
                    side: BookSide::Bid,
                    action: DepthAction::Insert {
                        position: 0,
                        level: DepthLevel {
                            price_ticks: 100,
                            quantity_lots: 4,
                            market_maker: "M1".into(),
                        },
                    },
                },
            ),
        ];
        let mut bytes = Vec::new();
        write_jsonl(&mut bytes, &events).unwrap();
        let options = ReplayOptions {
            speed: 1000.0,
            step: false,
        };
        let first = replay_reader(
            Cursor::new(bytes.clone()),
            Cursor::new(Vec::<u8>::new()),
            &mut Vec::new(),
            options,
        )
        .unwrap();
        let second = replay_reader(
            Cursor::new(bytes),
            Cursor::new(Vec::<u8>::new()),
            &mut Vec::new(),
            options,
        )
        .unwrap();
        assert_eq!(first, second);
        assert_eq!(first.events_applied, 3);
        assert_eq!(first.quotes, 1);
        assert!(first.incomplete);
    }

    #[test]
    fn replay_rejects_ordinal_gaps_and_marks_disconnect_incomplete() {
        let events = vec![
            event(
                1,
                MarketPayload::Disconnect {
                    feed: "IBKR".into(),
                    reason: "fixture".into(),
                },
            ),
            event(
                3,
                MarketPayload::Reconnect {
                    feed: "IBKR".into(),
                },
            ),
        ];
        let mut bytes = Vec::new();
        write_jsonl(&mut bytes, &events).unwrap();
        let result = replay_reader(
            Cursor::new(bytes),
            Cursor::new(Vec::<u8>::new()),
            &mut Vec::new(),
            ReplayOptions {
                speed: 1000.0,
                step: false,
            },
        );
        assert!(result
            .unwrap_err()
            .contains("expected market event ordinal 2"));
    }

    #[test]
    fn step_mode_supports_single_steps_pause_and_stop() {
        let events = vec![
            event(
                1,
                MarketPayload::SessionStart {
                    session_id: "s".into(),
                },
            ),
            event(
                2,
                MarketPayload::SessionEnd {
                    session_id: "s".into(),
                    complete: true,
                },
            ),
            event(
                3,
                MarketPayload::SessionStart {
                    session_id: "next".into(),
                },
            ),
        ];
        let mut bytes = Vec::new();
        write_jsonl(&mut bytes, &events).unwrap();
        let input = Cursor::new(b"\np\n\nq\n".to_vec());
        let summary = replay_reader(
            Cursor::new(bytes),
            input,
            &mut Vec::new(),
            ReplayOptions {
                speed: 1.0,
                step: true,
            },
        )
        .unwrap();
        assert_eq!(summary.events_applied, 2);
        assert!(summary.stopped_by_user);
    }
}
