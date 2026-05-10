//! Typed, deterministic replay state for captured L1 and positional L2 updates.
//!
//! This module does not connect to IBKR. A future adapter can translate provider
//! callbacks into these versioned, locally ordered records.

use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::io::{BufRead, Write};

pub const MARKET_EVENT_SCHEMA_VERSION: u16 = 2;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum BookSide {
    Bid,
    Ask,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DepthLevel {
    pub price_ticks: i64,
    pub quantity_lots: u64,
    pub market_maker: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum DepthAction {
    Insert { position: usize, level: DepthLevel },
    Update { position: usize, level: DepthLevel },
    Delete { position: usize },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum MarketPayload {
    Quote {
        bid_ticks: Option<i64>,
        bid_lots: u64,
        ask_ticks: Option<i64>,
        ask_lots: u64,
    },
    Trade {
        price_ticks: i64,
        quantity_lots: u64,
    },
    OtherTick {
        tick_type: String,
        value_f64_bits: Option<u64>,
    },
    SessionStart {
        session_id: String,
    },
    SessionEnd {
        session_id: String,
        complete: bool,
    },
    DepthSnapshotBegin {
        venue: String,
    },
    DepthSnapshotComplete {
        venue: String,
    },
    DepthUpdate {
        venue: String,
        side: BookSide,
        action: DepthAction,
    },
    DepthGap {
        venue: String,
        reason: String,
    },
    Disconnect {
        feed: String,
        reason: String,
    },
    Reconnect {
        feed: String,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MarketEvent {
    pub schema_version: u16,
    pub ordinal: u64,
    pub instrument: String,
    #[serde(default = "default_source")]
    pub source: String,
    pub source_time_ns: Option<u64>,
    pub receive_time_ns: u64,
    pub payload: MarketPayload,
    #[serde(default)]
    pub raw_payload: Option<String>,
}

fn default_source() -> String {
    "synthetic".into()
}

impl MarketEvent {
    pub fn new(
        ordinal: u64,
        instrument: impl Into<String>,
        receive_time_ns: u64,
        payload: MarketPayload,
    ) -> Self {
        Self {
            schema_version: MARKET_EVENT_SCHEMA_VERSION,
            ordinal,
            instrument: instrument.into(),
            source: default_source(),
            source_time_ns: None,
            receive_time_ns,
            payload,
            raw_payload: None,
        }
    }

    pub fn from_source(
        ordinal: u64,
        instrument: impl Into<String>,
        source: impl Into<String>,
        receive_time_ns: u64,
        payload: MarketPayload,
        raw_payload: Option<String>,
    ) -> Self {
        let mut event = Self::new(ordinal, instrument, receive_time_ns, payload);
        event.source = source.into();
        event.raw_payload = raw_payload;
        event
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Level1State {
    pub valid: bool,
    pub bid_ticks: Option<i64>,
    pub bid_lots: u64,
    pub ask_ticks: Option<i64>,
    pub ask_lots: u64,
    pub last_trade: Option<(i64, u64)>,
    pub last_receive_time_ns: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct DepthBook {
    pub bids: Vec<DepthLevel>,
    pub asks: Vec<DepthLevel>,
    pub valid: bool,
    pub snapshot_in_progress: bool,
    pub invalid_reason: Option<String>,
}

impl DepthBook {
    pub fn levels(&self, side: BookSide) -> &[DepthLevel] {
        match side {
            BookSide::Bid => &self.bids,
            BookSide::Ask => &self.asks,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MarketReplayError {
    UnsupportedSchema {
        ordinal: u64,
        version: u16,
    },
    InvalidOrdinal {
        expected: u64,
        actual: u64,
    },
    InvalidDepthPosition {
        ordinal: u64,
        position: usize,
        length: usize,
    },
    InvalidSnapshotTransition {
        ordinal: u64,
        venue: String,
    },
    InvalidMarketValue {
        ordinal: u64,
        reason: String,
    },
    InvalidSessionTransition {
        ordinal: u64,
        session_id: String,
    },
}

impl std::fmt::Display for MarketReplayError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::UnsupportedSchema { ordinal, version } => write!(
                f,
                "market event {ordinal} uses unsupported schema {version}"
            ),
            Self::InvalidOrdinal { expected, actual } => {
                write!(f, "expected market event ordinal {expected}, got {actual}")
            }
            Self::InvalidDepthPosition {
                ordinal,
                position,
                length,
            } => write!(
                f,
                "event {ordinal} depth position {position} is invalid for {length} levels"
            ),
            Self::InvalidSnapshotTransition { ordinal, venue } => write!(
                f,
                "event {ordinal} has invalid snapshot transition for {venue}"
            ),
            Self::InvalidMarketValue { ordinal, reason } => {
                write!(f, "event {ordinal} has invalid market value: {reason}")
            }
            Self::InvalidSessionTransition {
                ordinal,
                session_id,
            } => write!(
                f,
                "event {ordinal} has invalid transition for session {session_id}"
            ),
        }
    }
}

impl std::error::Error for MarketReplayError {}

#[derive(Debug, Default)]
pub struct MarketReplay {
    next_ordinal: u64,
    level1: HashMap<String, Level1State>,
    depth: HashMap<(String, String), DepthBook>,
    disconnected_feeds: HashMap<String, String>,
    saw_gap: bool,
    open_sessions: HashSet<String>,
}

impl MarketReplay {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn apply(&mut self, event: &MarketEvent) -> Result<(), MarketReplayError> {
        let expected = self.next_ordinal + 1;
        if event.schema_version != MARKET_EVENT_SCHEMA_VERSION {
            return Err(MarketReplayError::UnsupportedSchema {
                ordinal: event.ordinal,
                version: event.schema_version,
            });
        }
        if event.ordinal != expected {
            return Err(MarketReplayError::InvalidOrdinal {
                expected,
                actual: event.ordinal,
            });
        }
        self.next_ordinal = event.ordinal;

        match &event.payload {
            MarketPayload::Quote {
                bid_ticks,
                bid_lots,
                ask_ticks,
                ask_lots,
            } => {
                if bid_ticks.is_some_and(|price| price <= 0)
                    || ask_ticks.is_some_and(|price| price <= 0)
                {
                    return Err(MarketReplayError::InvalidMarketValue {
                        ordinal: event.ordinal,
                        reason: "quote prices must be positive ticks".into(),
                    });
                }
                let state = self.level1.entry(event.instrument.clone()).or_default();
                state.valid = true;
                state.bid_ticks = *bid_ticks;
                state.bid_lots = *bid_lots;
                state.ask_ticks = *ask_ticks;
                state.ask_lots = *ask_lots;
                state.last_receive_time_ns = Some(event.receive_time_ns);
            }
            MarketPayload::Trade {
                price_ticks,
                quantity_lots,
            } => {
                if *price_ticks <= 0 || *quantity_lots == 0 {
                    return Err(MarketReplayError::InvalidMarketValue {
                        ordinal: event.ordinal,
                        reason: "trade price and quantity must be positive".into(),
                    });
                }
                let state = self.level1.entry(event.instrument.clone()).or_default();
                state.valid = true;
                state.last_trade = Some((*price_ticks, *quantity_lots));
                state.last_receive_time_ns = Some(event.receive_time_ns);
            }
            MarketPayload::OtherTick { .. } => {}
            MarketPayload::SessionStart { session_id } => {
                if !self.open_sessions.insert(session_id.clone()) {
                    return Err(MarketReplayError::InvalidSessionTransition {
                        ordinal: event.ordinal,
                        session_id: session_id.clone(),
                    });
                }
            }
            MarketPayload::SessionEnd {
                session_id,
                complete,
            } => {
                if !self.open_sessions.remove(session_id) {
                    return Err(MarketReplayError::InvalidSessionTransition {
                        ordinal: event.ordinal,
                        session_id: session_id.clone(),
                    });
                }
                if !complete {
                    self.saw_gap = true;
                }
            }
            MarketPayload::DepthSnapshotBegin { venue } => {
                let book = self
                    .depth
                    .entry((event.instrument.clone(), venue.clone()))
                    .or_default();
                book.bids.clear();
                book.asks.clear();
                book.valid = false;
                book.snapshot_in_progress = true;
                book.invalid_reason = Some("depth snapshot is being rebuilt".into());
            }
            MarketPayload::DepthSnapshotComplete { venue } => {
                let key = (event.instrument.clone(), venue.clone());
                let book = self.depth.get_mut(&key).ok_or_else(|| {
                    MarketReplayError::InvalidSnapshotTransition {
                        ordinal: event.ordinal,
                        venue: venue.clone(),
                    }
                })?;
                if !book.snapshot_in_progress {
                    return Err(MarketReplayError::InvalidSnapshotTransition {
                        ordinal: event.ordinal,
                        venue: venue.clone(),
                    });
                }
                book.snapshot_in_progress = false;
                book.valid = true;
                book.invalid_reason = None;
            }
            MarketPayload::DepthUpdate {
                venue,
                side,
                action,
            } => {
                let book = self
                    .depth
                    .entry((event.instrument.clone(), venue.clone()))
                    .or_default();
                if !book.valid && !book.snapshot_in_progress {
                    return Ok(());
                }
                let level_count = match side {
                    BookSide::Bid => book.bids.len(),
                    BookSide::Ask => book.asks.len(),
                };
                let bad_position = match action {
                    DepthAction::Insert { position, .. } => *position > level_count,
                    DepthAction::Update { position, .. } | DepthAction::Delete { position } => {
                        *position >= level_count
                    }
                };
                if bad_position {
                    let position = match action {
                        DepthAction::Insert { position, .. }
                        | DepthAction::Update { position, .. }
                        | DepthAction::Delete { position } => *position,
                    };
                    return invalid_position(book, event.ordinal, position, level_count);
                }
                let levels = match side {
                    BookSide::Bid => &mut book.bids,
                    BookSide::Ask => &mut book.asks,
                };
                match action {
                    DepthAction::Insert { position, level } => {
                        validate_level(event.ordinal, level)?;
                        levels.insert(*position, level.clone());
                    }
                    DepthAction::Update { position, level } => {
                        validate_level(event.ordinal, level)?;
                        levels[*position] = level.clone();
                    }
                    DepthAction::Delete { position } => {
                        levels.remove(*position);
                    }
                }
            }
            MarketPayload::DepthGap { venue, reason } => {
                self.saw_gap = true;
                let book = self
                    .depth
                    .entry((event.instrument.clone(), venue.clone()))
                    .or_default();
                book.valid = false;
                book.snapshot_in_progress = false;
                book.invalid_reason = Some(reason.clone());
            }
            MarketPayload::Disconnect { feed, reason } => {
                self.saw_gap = true;
                self.disconnected_feeds.insert(feed.clone(), reason.clone());
                if let Some(state) = self.level1.get_mut(&event.instrument) {
                    state.valid = false;
                }
                for ((instrument, _), book) in &mut self.depth {
                    if *instrument == event.instrument {
                        book.valid = false;
                        book.snapshot_in_progress = false;
                        book.invalid_reason = Some(format!("feed {feed} disconnected: {reason}"));
                    }
                }
            }
            MarketPayload::Reconnect { feed } => {
                self.disconnected_feeds.remove(feed);
            }
        }
        Ok(())
    }

    pub fn level1(&self, instrument: &str) -> Option<&Level1State> {
        self.level1.get(instrument)
    }
    pub fn depth_book(&self, instrument: &str, venue: &str) -> Option<&DepthBook> {
        self.depth.get(&(instrument.to_owned(), venue.to_owned()))
    }
    pub fn feed_disconnect_reason(&self, feed: &str) -> Option<&str> {
        self.disconnected_feeds.get(feed).map(String::as_str)
    }

    pub fn has_incomplete_data(&self) -> bool {
        self.saw_gap
            || !self.open_sessions.is_empty()
            || self.depth.values().any(|book| !book.valid)
    }

    pub fn state_fingerprint(&self) -> u64 {
        fn add(hash: &mut u64, bytes: &[u8]) {
            for byte in bytes {
                *hash = (*hash ^ u64::from(*byte)).wrapping_mul(0x100000001b3);
            }
        }
        fn add_str(hash: &mut u64, value: &str) {
            add(hash, &(value.len() as u64).to_le_bytes());
            add(hash, value.as_bytes());
        }
        fn add_option_i64(hash: &mut u64, value: Option<i64>) {
            match value {
                Some(number) => {
                    add(hash, &[1]);
                    add(hash, &number.to_le_bytes());
                }
                None => add(hash, &[0]),
            }
        }
        let mut hash = 0xcbf29ce484222325_u64;
        add(&mut hash, &self.next_ordinal.to_le_bytes());
        add(&mut hash, &[u8::from(self.saw_gap)]);
        let mut sessions: Vec<_> = self.open_sessions.iter().collect();
        sessions.sort_unstable();
        for session in sessions {
            add_str(&mut hash, session);
        }
        let mut instruments: Vec<_> = self.level1.iter().collect();
        instruments.sort_by_key(|(instrument, _)| *instrument);
        for (instrument, state) in instruments {
            add_str(&mut hash, instrument);
            add(&mut hash, &[u8::from(state.valid)]);
            add_option_i64(&mut hash, state.bid_ticks);
            add(&mut hash, &state.bid_lots.to_le_bytes());
            add_option_i64(&mut hash, state.ask_ticks);
            add(&mut hash, &state.ask_lots.to_le_bytes());
            if let Some((price, quantity)) = state.last_trade {
                add(&mut hash, &[1]);
                add(&mut hash, &price.to_le_bytes());
                add(&mut hash, &quantity.to_le_bytes());
            } else {
                add(&mut hash, &[0]);
            }
            if let Some(received) = state.last_receive_time_ns {
                add(&mut hash, &[1]);
                add(&mut hash, &received.to_le_bytes());
            } else {
                add(&mut hash, &[0]);
            }
        }
        let mut books: Vec<_> = self.depth.iter().collect();
        books.sort_by_key(|((instrument, venue), _)| (instrument, venue));
        for ((instrument, venue), book) in books {
            add_str(&mut hash, instrument);
            add_str(&mut hash, venue);
            add(
                &mut hash,
                &[u8::from(book.valid), u8::from(book.snapshot_in_progress)],
            );
            if let Some(reason) = &book.invalid_reason {
                add(&mut hash, &[1]);
                add_str(&mut hash, reason);
            } else {
                add(&mut hash, &[0]);
            }
            for (side, levels) in [(BookSide::Bid, &book.bids), (BookSide::Ask, &book.asks)] {
                add(
                    &mut hash,
                    &[match side {
                        BookSide::Bid => 0,
                        BookSide::Ask => 1,
                    }],
                );
                add(&mut hash, &(levels.len() as u64).to_le_bytes());
                for level in levels {
                    add(&mut hash, &level.price_ticks.to_le_bytes());
                    add(&mut hash, &level.quantity_lots.to_le_bytes());
                    add_str(&mut hash, &level.market_maker);
                }
            }
        }
        let mut feeds: Vec<_> = self.disconnected_feeds.iter().collect();
        feeds.sort_by_key(|(feed, _)| *feed);
        for (feed, reason) in feeds {
            add_str(&mut hash, feed);
            add_str(&mut hash, reason);
        }
        hash
    }
}

pub fn write_jsonl<W: Write>(
    writer: &mut W,
    events: &[MarketEvent],
) -> Result<(), serde_json::Error> {
    for event in events {
        serde_json::to_writer(&mut *writer, event)?;
        writer.write_all(b"\n").map_err(serde_json::Error::io)?;
    }
    Ok(())
}

pub fn read_jsonl<R: BufRead>(reader: R) -> Result<Vec<MarketEvent>, String> {
    let mut events = Vec::new();
    for (line_index, line_result) in reader.lines().enumerate() {
        let line_number = line_index + 1;
        let line = line_result.map_err(|error| format!("line {line_number}: {error}"))?;
        if line.trim().is_empty() {
            continue;
        }
        events.push(
            serde_json::from_str(&line).map_err(|error| format!("line {line_number}: {error}"))?,
        );
    }
    Ok(events)
}

fn validate_level(ordinal: u64, level: &DepthLevel) -> Result<(), MarketReplayError> {
    if level.price_ticks <= 0 || level.quantity_lots == 0 {
        return Err(MarketReplayError::InvalidMarketValue {
            ordinal,
            reason: "depth price and quantity must be positive".into(),
        });
    }
    Ok(())
}

fn invalid_position<T>(
    book: &mut DepthBook,
    ordinal: u64,
    position: usize,
    length: usize,
) -> Result<T, MarketReplayError> {
    book.valid = false;
    book.snapshot_in_progress = false;
    book.invalid_reason = Some(format!("invalid positional update at event {ordinal}"));
    Err(MarketReplayError::InvalidDepthPosition {
        ordinal,
        position,
        length,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn event(ordinal: u64, payload: MarketPayload) -> MarketEvent {
        MarketEvent::new(ordinal, "AAPL", ordinal * 1_000, payload)
    }
    fn level(price_ticks: i64, quantity_lots: u64, market_maker: &str) -> DepthLevel {
        DepthLevel {
            price_ticks,
            quantity_lots,
            market_maker: market_maker.into(),
        }
    }

    #[test]
    fn l1_and_positional_l2_replay_reconstruct_expected_state() {
        let mut replay = MarketReplay::new();
        replay
            .apply(&event(
                1,
                MarketPayload::Quote {
                    bid_ticks: Some(100),
                    bid_lots: 7,
                    ask_ticks: Some(102),
                    ask_lots: 4,
                },
            ))
            .unwrap();
        replay
            .apply(&event(
                2,
                MarketPayload::Trade {
                    price_ticks: 101,
                    quantity_lots: 2,
                },
            ))
            .unwrap();
        replay
            .apply(&event(
                3,
                MarketPayload::DepthSnapshotBegin {
                    venue: "XNAS".into(),
                },
            ))
            .unwrap();
        replay
            .apply(&event(
                4,
                MarketPayload::DepthUpdate {
                    venue: "XNAS".into(),
                    side: BookSide::Bid,
                    action: DepthAction::Insert {
                        position: 0,
                        level: level(100, 7, "maker-a"),
                    },
                },
            ))
            .unwrap();
        replay
            .apply(&event(
                5,
                MarketPayload::DepthUpdate {
                    venue: "XNAS".into(),
                    side: BookSide::Bid,
                    action: DepthAction::Insert {
                        position: 1,
                        level: level(99, 8, "maker-b"),
                    },
                },
            ))
            .unwrap();
        replay
            .apply(&event(
                6,
                MarketPayload::DepthUpdate {
                    venue: "XNAS".into(),
                    side: BookSide::Bid,
                    action: DepthAction::Update {
                        position: 1,
                        level: level(98, 9, "maker-b"),
                    },
                },
            ))
            .unwrap();
        replay
            .apply(&event(
                7,
                MarketPayload::DepthUpdate {
                    venue: "XNAS".into(),
                    side: BookSide::Ask,
                    action: DepthAction::Insert {
                        position: 0,
                        level: level(102, 4, "maker-c"),
                    },
                },
            ))
            .unwrap();
        replay
            .apply(&event(
                8,
                MarketPayload::DepthSnapshotComplete {
                    venue: "XNAS".into(),
                },
            ))
            .unwrap();
        assert_eq!(replay.level1("AAPL").unwrap().last_trade, Some((101, 2)));
        let book = replay.depth_book("AAPL", "XNAS").unwrap();
        assert!(book.valid);
        assert_eq!(
            book.bids,
            vec![level(100, 7, "maker-a"), level(98, 9, "maker-b")]
        );
        assert_eq!(book.asks, vec![level(102, 4, "maker-c")]);
    }

    #[test]
    fn depth_gap_invalidates_state_until_a_new_snapshot_completes() {
        let mut replay = MarketReplay::new();
        replay
            .apply(&event(
                1,
                MarketPayload::DepthSnapshotBegin {
                    venue: "XNAS".into(),
                },
            ))
            .unwrap();
        replay
            .apply(&event(
                2,
                MarketPayload::DepthUpdate {
                    venue: "XNAS".into(),
                    side: BookSide::Ask,
                    action: DepthAction::Insert {
                        position: 0,
                        level: level(102, 1, "maker"),
                    },
                },
            ))
            .unwrap();
        replay
            .apply(&event(
                3,
                MarketPayload::DepthSnapshotComplete {
                    venue: "XNAS".into(),
                },
            ))
            .unwrap();
        replay
            .apply(&event(
                4,
                MarketPayload::DepthGap {
                    venue: "XNAS".into(),
                    reason: "dropped update".into(),
                },
            ))
            .unwrap();
        assert!(!replay.depth_book("AAPL", "XNAS").unwrap().valid);
        replay
            .apply(&event(
                5,
                MarketPayload::DepthUpdate {
                    venue: "XNAS".into(),
                    side: BookSide::Ask,
                    action: DepthAction::Delete { position: 0 },
                },
            ))
            .unwrap();
        assert!(!replay.depth_book("AAPL", "XNAS").unwrap().valid);
        replay
            .apply(&event(
                6,
                MarketPayload::DepthSnapshotBegin {
                    venue: "XNAS".into(),
                },
            ))
            .unwrap();
        replay
            .apply(&event(
                7,
                MarketPayload::DepthUpdate {
                    venue: "XNAS".into(),
                    side: BookSide::Ask,
                    action: DepthAction::Insert {
                        position: 0,
                        level: level(103, 2, "maker"),
                    },
                },
            ))
            .unwrap();
        replay
            .apply(&event(
                8,
                MarketPayload::DepthSnapshotComplete {
                    venue: "XNAS".into(),
                },
            ))
            .unwrap();
        assert!(replay.depth_book("AAPL", "XNAS").unwrap().valid);
    }

    #[test]
    fn invalid_depth_position_marks_book_invalid_and_rejects_event() {
        let mut replay = MarketReplay::new();
        replay
            .apply(&event(
                1,
                MarketPayload::DepthSnapshotBegin {
                    venue: "XNAS".into(),
                },
            ))
            .unwrap();
        let result = replay.apply(&event(
            2,
            MarketPayload::DepthUpdate {
                venue: "XNAS".into(),
                side: BookSide::Bid,
                action: DepthAction::Delete { position: 0 },
            },
        ));
        assert!(matches!(
            result,
            Err(MarketReplayError::InvalidDepthPosition { .. })
        ));
        assert!(!replay.depth_book("AAPL", "XNAS").unwrap().valid);
    }

    #[test]
    fn market_events_round_trip_as_jsonl_and_disconnect_marks_l1_stale() {
        let events = vec![
            event(
                1,
                MarketPayload::Quote {
                    bid_ticks: Some(100),
                    bid_lots: 1,
                    ask_ticks: Some(101),
                    ask_lots: 2,
                },
            ),
            event(
                2,
                MarketPayload::Disconnect {
                    feed: "l1".into(),
                    reason: "connection lost".into(),
                },
            ),
        ];
        let mut bytes = Vec::new();
        write_jsonl(&mut bytes, &events).unwrap();
        let decoded = read_jsonl(std::io::Cursor::new(bytes)).unwrap();
        assert_eq!(decoded, events);
        let mut replay = MarketReplay::new();
        for item in &decoded {
            replay.apply(item).unwrap();
        }
        assert!(!replay.level1("AAPL").unwrap().valid);
        assert_eq!(replay.feed_disconnect_reason("l1"), Some("connection lost"));
    }
}
