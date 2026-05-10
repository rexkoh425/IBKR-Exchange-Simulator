//! IBKR TWS/Gateway market-data capture. Account login remains in TWS/Gateway.
use crate::market_data::{BookSide, DepthAction, DepthLevel, MarketEvent, MarketPayload};
use ibapi::client::blocking::Client;
use ibapi::contracts::tick_types::TickType;
use ibapi::contracts::Contract;
use ibapi::market_data::realtime::{MarketDepths, TickTypes};
use ibapi::market_data::SmartDepth;
use ibapi::subscriptions::SubscriptionItem;
use std::fs::{self, File, OpenOptions};
use std::io::{BufWriter, Write};
use std::path::PathBuf;
use std::sync::mpsc::{sync_channel, SyncSender, TrySendError};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

#[derive(Debug, Clone)]
pub struct Config {
    pub host: String,
    pub port: u16,
    pub client_id: i32,
    pub symbol: String,
    pub currency: String,
    pub exchange: String,
    pub depth_rows: i32,
    pub tick_size: f64,
    pub capture_dir: PathBuf,
    pub seconds: u64,
    pub queue_capacity: usize,
}

impl Config {
    pub fn from_env() -> Result<Self, String> {
        load_env_file()?;
        let get = |key: &str, default: &str| std::env::var(key).unwrap_or_else(|_| default.into());
        let parse_u16 = |key: &str, default: &str| {
            get(key, default)
                .parse::<u16>()
                .map_err(|_| format!("invalid {key}"))
        };
        let parse_i32 = |key: &str, default: &str| {
            get(key, default)
                .parse::<i32>()
                .map_err(|_| format!("invalid {key}"))
        };
        let parse_u64 = |key: &str, default: &str| {
            get(key, default)
                .parse::<u64>()
                .map_err(|_| format!("invalid {key}"))
        };
        let parse_usize = |key: &str, default: &str| {
            get(key, default)
                .parse::<usize>()
                .map_err(|_| format!("invalid {key}"))
        };
        let parse_f64 = |key: &str, default: &str| {
            get(key, default)
                .parse::<f64>()
                .map_err(|_| format!("invalid {key}"))
        };
        let config = Self {
            host: get("IBKR_HOST", "127.0.0.1"),
            port: parse_u16("IBKR_PORT", "4002")?,
            client_id: parse_i32("IBKR_CLIENT_ID", "71")?,
            symbol: get("IBKR_SYMBOL", "AAPL"),
            currency: get("IBKR_CURRENCY", "USD"),
            exchange: get("IBKR_EXCHANGE", "SMART"),
            depth_rows: parse_i32("IBKR_DEPTH_ROWS", "5")?,
            tick_size: parse_f64("IBKR_TICK_SIZE", "0.01")?,
            capture_dir: get("IBKR_CAPTURE_DIR", "data/ibkr").into(),
            seconds: parse_u64("IBKR_SECONDS", "60")?,
            queue_capacity: parse_usize("IBKR_QUEUE_CAPACITY", "65536")?,
        };
        if config.port == 0
            || config.depth_rows <= 0
            || !config.tick_size.is_finite()
            || config.tick_size <= 0.0
            || config.seconds == 0
            || config.queue_capacity == 0
        {
            return Err("port, depth rows, tick size and duration must be positive".into());
        }
        Ok(config)
    }
}

fn load_env_file() -> Result<(), String> {
    let path = PathBuf::from(".env");
    if !path.exists() {
        return Ok(());
    }
    let content = fs::read_to_string(path).map_err(|e| format!("cannot read .env: {e}"))?;
    for (line_no, line) in content.lines().enumerate() {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        let (key, value) = line
            .split_once('=')
            .ok_or_else(|| format!("invalid .env line {}", line_no + 1))?;
        let key = key.trim();
        if !key.starts_with("IBKR_") {
            continue;
        }
        if std::env::var_os(key).is_none() {
            let value = value.trim().trim_matches('"').trim_matches('\'');
            // Never print values: .env contains local connection configuration.
            std::env::set_var(key, value);
        }
    }
    Ok(())
}

#[derive(Debug, Default)]
pub struct L1Normalizer {
    bid_ticks: Option<i64>,
    bid_lots: u64,
    ask_ticks: Option<i64>,
    ask_lots: u64,
}

impl L1Normalizer {
    pub fn normalize(&mut self, tick: &TickTypes, tick_size: f64) -> Result<MarketPayload, String> {
        let mut quote_changed = false;
        match tick {
            TickTypes::Price(value) => match value.tick_type {
                TickType::Bid => {
                    self.bid_ticks = to_ticks(value.price, tick_size)?;
                    quote_changed = true;
                }
                TickType::Ask => {
                    self.ask_ticks = to_ticks(value.price, tick_size)?;
                    quote_changed = true;
                }
                _ => {
                    return Ok(MarketPayload::OtherTick {
                        tick_type: format!("{:?}", value.tick_type),
                        value_f64_bits: Some(value.price.to_bits()),
                    })
                }
            },
            TickTypes::Size(value) => match value.tick_type {
                TickType::BidSize => {
                    self.bid_lots = to_lots(value.size)?;
                    quote_changed = true;
                }
                TickType::AskSize => {
                    self.ask_lots = to_lots(value.size)?;
                    quote_changed = true;
                }
                _ => {
                    return Ok(MarketPayload::OtherTick {
                        tick_type: format!("{:?}", value.tick_type),
                        value_f64_bits: Some(value.size.to_bits()),
                    })
                }
            },
            TickTypes::PriceSize(value) => {
                quote_changed |=
                    self.update_price(value.price_tick_type, value.price, tick_size)?;
                quote_changed |= self.update_size(value.size_tick_type, value.size)?;
                if !quote_changed {
                    return Ok(MarketPayload::OtherTick {
                        tick_type: format!("{:?}", value.price_tick_type),
                        value_f64_bits: Some(value.price.to_bits()),
                    });
                }
            }
            TickTypes::SnapshotEnd => {
                return Ok(MarketPayload::OtherTick {
                    tick_type: "SnapshotEnd".into(),
                    value_f64_bits: None,
                })
            }
            TickTypes::Generic(value) => {
                return Ok(MarketPayload::OtherTick {
                    tick_type: format!("{:?}", value.tick_type),
                    value_f64_bits: Some(value.value.to_bits()),
                })
            }
            other => {
                return Ok(MarketPayload::OtherTick {
                    tick_type: tick_name(other).to_owned(),
                    value_f64_bits: None,
                })
            }
        }
        debug_assert!(quote_changed);
        Ok(MarketPayload::Quote {
            bid_ticks: self.bid_ticks,
            bid_lots: self.bid_lots,
            ask_ticks: self.ask_ticks,
            ask_lots: self.ask_lots,
        })
    }

    fn update_price(
        &mut self,
        tick_type: TickType,
        price: f64,
        tick_size: f64,
    ) -> Result<bool, String> {
        match tick_type {
            TickType::Bid => {
                self.bid_ticks = to_ticks(price, tick_size)?;
                Ok(true)
            }
            TickType::Ask => {
                self.ask_ticks = to_ticks(price, tick_size)?;
                Ok(true)
            }
            _ => Ok(false),
        }
    }

    fn update_size(&mut self, tick_type: TickType, size: f64) -> Result<bool, String> {
        match tick_type {
            TickType::BidSize => {
                self.bid_lots = to_lots(size)?;
                Ok(true)
            }
            TickType::AskSize => {
                self.ask_lots = to_lots(size)?;
                Ok(true)
            }
            _ => Ok(false),
        }
    }
}

fn tick_name(tick: &TickTypes) -> &'static str {
    match tick {
        TickTypes::String(_) => "String",
        TickTypes::Generic(_) => "Generic",
        TickTypes::OptionComputation(_) => "OptionComputation",
        TickTypes::SnapshotEnd => "SnapshotEnd",
        TickTypes::RequestParameters(_) => "RequestParameters",
        TickTypes::PriceSize(_) => "PriceSize",
        TickTypes::MarketDataType(_) => "MarketDataType",
        TickTypes::Price(_) => "Price",
        TickTypes::Size(_) => "Size",
    }
}

fn to_ticks(price: f64, tick_size: f64) -> Result<Option<i64>, String> {
    if !price.is_finite() {
        return Err("IBKR price is not finite".into());
    }
    if price <= 0.0 {
        return Ok(None);
    }
    let ticks = price / tick_size;
    let rounded = ticks.round();
    if (ticks - rounded).abs() > 1e-7 || rounded > i64::MAX as f64 {
        return Err(format!(
            "price {price} is not representable in configured ticks"
        ));
    }
    Ok(Some(rounded as i64))
}

fn to_lots(size: f64) -> Result<u64, String> {
    if !size.is_finite() || size < 0.0 || size.fract().abs() > 1e-7 || size > u64::MAX as f64 {
        return Err(format!(
            "size {size} is not representable as integer shares"
        ));
    }
    Ok(size.round() as u64)
}

pub fn normalize_depth(
    depth: &MarketDepths,
    tick_size: f64,
    venue: &str,
) -> Result<MarketPayload, String> {
    let (position, operation, side, price, size, market_maker) = match depth {
        MarketDepths::Reset => {
            return Ok(MarketPayload::DepthSnapshotBegin {
                venue: venue.into(),
            })
        }
        MarketDepths::MarketDepth(value) => (
            value.position,
            value.operation,
            value.side,
            value.price,
            value.size,
            String::new(),
        ),
        MarketDepths::MarketDepthL2(value) => (
            value.position,
            value.operation,
            value.side,
            value.price,
            value.size,
            value.market_maker.clone(),
        ),
    };
    let position =
        usize::try_from(position).map_err(|_| "negative IBKR depth position".to_owned())?;
    let side = match side {
        0 => BookSide::Ask,
        1 => BookSide::Bid,
        _ => return Err(format!("unknown IBKR depth side {side}")),
    };
    let action = match operation {
        0 => DepthAction::Insert {
            position,
            level: make_depth_level(price, size, tick_size, market_maker)?,
        },
        1 => DepthAction::Update {
            position,
            level: make_depth_level(price, size, tick_size, market_maker)?,
        },
        2 => DepthAction::Delete { position },
        _ => return Err(format!("unknown IBKR depth operation {operation}")),
    };
    Ok(MarketPayload::DepthUpdate {
        venue: venue.into(),
        side,
        action,
    })
}

fn make_depth_level(
    price: f64,
    size: f64,
    tick_size: f64,
    market_maker: String,
) -> Result<DepthLevel, String> {
    let price_ticks =
        to_ticks(price, tick_size)?.ok_or_else(|| "IBKR depth price is nonpositive".to_owned())?;
    let quantity_lots = to_lots(size)?;
    if quantity_lots == 0 {
        return Err("IBKR depth size is zero".into());
    }
    Ok(DepthLevel {
        price_ticks,
        quantity_lots,
        market_maker,
    })
}

pub fn capture(config: Config) -> Result<(), String> {
    fs::create_dir_all(&config.capture_dir).map_err(|e| e.to_string())?;
    let stamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    let path = config
        .capture_dir
        .join(format!("{}-{stamp}.jsonl", config.symbol));
    let address = format!("{}:{}", config.host, config.port);
    let client = Client::connect(&address, config.client_id)
        .map_err(|e| format!("IBKR connect failed: {e}"))?;
    let contract = Contract::stock(&config.symbol)
        .on_exchange(&config.exchange)
        .in_currency(&config.currency)
        .build();
    let l1 = client
        .market_data(&contract)
        .subscribe()
        .map_err(|e| format!("IBKR L1 subscription failed: {e}"))?;
    let l2 = client
        .market_depth(&contract, config.depth_rows)
        .smart_depth(SmartDepth::No)
        .subscribe()
        .map_err(|e| format!("IBKR depth subscription failed: {e}"))?;

    let out =
        BufWriter::with_capacity(1024 * 1024, File::create(&path).map_err(|e| e.to_string())?);
    let (sender, receiver) = sync_channel::<MarketEvent>(config.queue_capacity);
    let writer = std::thread::spawn(move || -> Result<u64, String> {
        let mut out = out;
        let mut written = 0_u64;
        while let Ok(event) = receiver.recv() {
            serde_json::to_writer(&mut out, &event).map_err(|e| e.to_string())?;
            out.write_all(b"\n").map_err(|e| e.to_string())?;
            written = event.ordinal;
        }
        out.flush().map_err(|e| e.to_string())?;
        Ok(written)
    });

    let started = Instant::now();
    let mut ordinal = 0_u64;
    let mut quote = L1Normalizer::default();
    let mut failure = None;
    let session_id = format!("{}-{stamp}", config.symbol);
    if let Err(error) = send_event(
        &sender,
        &mut ordinal,
        &config,
        MarketPayload::SessionStart {
            session_id: session_id.clone(),
        },
        None,
    ) {
        failure = Some(error);
    }
    if let Err(error) = send_event(
        &sender,
        &mut ordinal,
        &config,
        MarketPayload::DepthSnapshotBegin {
            venue: config.exchange.clone(),
        },
        Some("IBKR initial depth; completion boundary unavailable".into()),
    ) {
        failure = Some(error);
    }
    while failure.is_none() && started.elapsed() < Duration::from_secs(config.seconds) {
        let mut did_work = false;
        if let Some(item) = l1.next_timeout(Duration::from_millis(1)) {
            did_work = true;
            match item {
                Ok(SubscriptionItem::Data(tick)) => {
                    match quote.normalize(&tick, config.tick_size) {
                        Ok(payload) => {
                            if let Err(error) = send_event(
                                &sender,
                                &mut ordinal,
                                &config,
                                payload,
                                Some(format!("{tick:?}")),
                            ) {
                                failure = Some(error);
                            }
                        }
                        Err(error) => failure = Some(format!("invalid L1 value: {error}")),
                    }
                }
                Ok(SubscriptionItem::Notice(notice)) => {
                    let payload = MarketPayload::OtherTick {
                        tick_type: "IBKRNotice".into(),
                        value_f64_bits: None,
                    };
                    if let Err(error) = send_event(
                        &sender,
                        &mut ordinal,
                        &config,
                        payload,
                        Some(format!("{notice:?}")),
                    ) {
                        failure = Some(error);
                    }
                }
                Err(error) => failure = Some(format!("IBKR L1 terminal error: {error}")),
            }
        }
        if failure.is_some() {
            break;
        }
        if let Some(item) = l2.next_timeout(Duration::from_millis(1)) {
            did_work = true;
            match item {
                Ok(SubscriptionItem::Data(depth)) => {
                    match normalize_depth(&depth, config.tick_size, &config.exchange) {
                        Ok(payload) => {
                            if let Err(error) = send_event(
                                &sender,
                                &mut ordinal,
                                &config,
                                payload,
                                Some(format!("{depth:?}")),
                            ) {
                                failure = Some(error);
                            }
                        }
                        Err(error) => failure = Some(format!("invalid depth update: {error}")),
                    }
                }
                Ok(SubscriptionItem::Notice(notice)) => {
                    let payload = MarketPayload::OtherTick {
                        tick_type: "IBKRNotice".into(),
                        value_f64_bits: None,
                    };
                    if let Err(error) = send_event(
                        &sender,
                        &mut ordinal,
                        &config,
                        payload,
                        Some(format!("{notice:?}")),
                    ) {
                        failure = Some(error);
                    }
                }
                Err(error) => failure = Some(format!("IBKR L2 terminal error: {error}")),
            }
        }
        if !did_work {
            std::thread::yield_now();
        }
    }
    if failure.is_none() {
        if let Err(error) = send_event(
            &sender,
            &mut ordinal,
            &config,
            MarketPayload::SessionEnd {
                session_id: session_id.clone(),
                complete: true,
            },
            None,
        ) {
            failure = Some(error);
        }
    }
    drop(sender);
    let written = writer
        .join()
        .map_err(|_| "capture writer panicked".to_owned())??;
    if let Some(reason) = failure {
        append_incomplete_markers(&path, written + 1, &config, &session_id, &reason)?;
        eprintln!(
            "capture stopped with incomplete session after {written} normalized events: {reason}"
        );
    } else {
        eprintln!("capture completed cleanly with {written} normalized events; initial depth remains conservatively incomplete");
    }
    Ok(())
}

fn send_event(
    sender: &SyncSender<MarketEvent>,
    ordinal: &mut u64,
    config: &Config,
    payload: MarketPayload,
    raw_payload: Option<String>,
) -> Result<(), String> {
    *ordinal += 1;
    let receive_time_ns = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos()
        .min(u128::from(u64::MAX)) as u64;
    let event = MarketEvent::from_source(
        *ordinal,
        &config.symbol,
        "IBKR",
        receive_time_ns,
        payload,
        raw_payload,
    );
    sender.try_send(event).map_err(|error| match error {
        TrySendError::Full(_) => "bounded capture queue full; capture stopped".to_owned(),
        TrySendError::Disconnected(_) => "capture writer stopped".to_owned(),
    })
}

fn append_incomplete_markers(
    path: &std::path::Path,
    ordinal: u64,
    config: &Config,
    session_id: &str,
    reason: &str,
) -> Result<(), String> {
    let file = OpenOptions::new()
        .append(true)
        .open(path)
        .map_err(|e| e.to_string())?;
    let mut out = BufWriter::new(file);
    let time = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos()
        .min(u128::from(u64::MAX)) as u64;
    let markers = [
        MarketEvent::from_source(
            ordinal,
            &config.symbol,
            "IBKR",
            time,
            MarketPayload::Disconnect {
                feed: "IBKR".into(),
                reason: reason.into(),
            },
            None,
        ),
        MarketEvent::from_source(
            ordinal + 1,
            &config.symbol,
            "IBKR",
            time,
            MarketPayload::DepthGap {
                venue: config.exchange.clone(),
                reason: reason.into(),
            },
            None,
        ),
        MarketEvent::from_source(
            ordinal + 2,
            &config.symbol,
            "IBKR",
            time,
            MarketPayload::SessionEnd {
                session_id: session_id.to_owned(),
                complete: false,
            },
            None,
        ),
    ];
    for marker in &markers {
        serde_json::to_writer(&mut out, marker).map_err(|e| e.to_string())?;
        out.write_all(b"\n").map_err(|e| e.to_string())?;
    }
    out.flush().map_err(|e| e.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use ibapi::market_data::realtime::{TickPrice, TickSize};

    #[test]
    fn l1_bid_ask_ticks_normalize_to_integer_ticks_and_shares() {
        let mut normalizer = L1Normalizer::default();
        let bid_price = TickTypes::Price(TickPrice {
            tick_type: TickType::Bid,
            price: 100.25,
            ..TickPrice::default()
        });
        let bid_size = TickTypes::Size(TickSize {
            tick_type: TickType::BidSize,
            size: 12.0,
        });
        let ask_price = TickTypes::Price(TickPrice {
            tick_type: TickType::Ask,
            price: 100.30,
            ..TickPrice::default()
        });
        let ask_size = TickTypes::Size(TickSize {
            tick_type: TickType::AskSize,
            size: 8.0,
        });
        assert_eq!(
            normalizer.normalize(&bid_price, 0.01).unwrap(),
            MarketPayload::Quote {
                bid_ticks: Some(10025),
                bid_lots: 0,
                ask_ticks: None,
                ask_lots: 0
            }
        );
        assert_eq!(
            normalizer.normalize(&bid_size, 0.01).unwrap(),
            MarketPayload::Quote {
                bid_ticks: Some(10025),
                bid_lots: 12,
                ask_ticks: None,
                ask_lots: 0
            }
        );
        assert_eq!(
            normalizer.normalize(&ask_price, 0.01).unwrap(),
            MarketPayload::Quote {
                bid_ticks: Some(10025),
                bid_lots: 12,
                ask_ticks: Some(10030),
                ask_lots: 0
            }
        );
        assert_eq!(
            normalizer.normalize(&ask_size, 0.01).unwrap(),
            MarketPayload::Quote {
                bid_ticks: Some(10025),
                bid_lots: 12,
                ask_ticks: Some(10030),
                ask_lots: 8
            }
        );
    }

    #[test]
    fn rejects_unrepresentable_prices_and_fractional_share_sizes() {
        let mut normalizer = L1Normalizer::default();
        let price = TickTypes::Price(TickPrice {
            tick_type: TickType::Bid,
            price: 100.253,
            ..TickPrice::default()
        });
        assert!(normalizer.normalize(&price, 0.01).is_err());
        let size = TickTypes::Size(TickSize {
            tick_type: TickType::BidSize,
            size: 1.5,
        });
        assert!(normalizer.normalize(&size, 0.01).is_err());
    }

    #[test]
    fn depth_insert_preserves_position_side_market_maker_and_raw_fields() {
        let depth = MarketDepths::MarketDepthL2(ibapi::market_data::realtime::MarketDepthL2 {
            position: 2,
            market_maker: "NASDAQ".into(),
            operation: 0,
            side: 1,
            price: 101.25,
            size: 20.0,
            smart_depth: false,
        });
        assert_eq!(
            normalize_depth(&depth, 0.01, "NASDAQ").unwrap(),
            MarketPayload::DepthUpdate {
                venue: "NASDAQ".into(),
                side: BookSide::Bid,
                action: DepthAction::Insert {
                    position: 2,
                    level: DepthLevel {
                        price_ticks: 10125,
                        quantity_lots: 20,
                        market_maker: "NASDAQ".into()
                    }
                },
            }
        );
        let reset = normalize_depth(&MarketDepths::Reset, 0.01, "NASDAQ").unwrap();
        assert_eq!(
            reset,
            MarketPayload::DepthSnapshotBegin {
                venue: "NASDAQ".into()
            }
        );

        let event = MarketEvent::from_source(1, "AAPL", "IBKR", 123, reset, Some("Reset".into()));
        let mut bytes = Vec::new();
        crate::market_data::write_jsonl(&mut bytes, std::slice::from_ref(&event)).unwrap();
        assert_eq!(
            crate::market_data::read_jsonl(std::io::Cursor::new(bytes)).unwrap(),
            vec![event]
        );
    }

    #[test]
    fn incomplete_capture_markers_preserve_ordinal_order_and_open_session() {
        let path = std::env::temp_dir().join(format!(
            "ibkr-gap-test-{}.jsonl",
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let config = Config {
            host: "127.0.0.1".into(),
            port: 4002,
            client_id: 1,
            symbol: "AAPL".into(),
            currency: "USD".into(),
            exchange: "SMART".into(),
            depth_rows: 5,
            tick_size: 0.01,
            capture_dir: std::env::temp_dir(),
            seconds: 1,
            queue_capacity: 4,
        };
        let mut file = BufWriter::new(File::create(&path).unwrap());
        let start = MarketEvent::from_source(
            1,
            "AAPL",
            "IBKR",
            1,
            MarketPayload::SessionStart {
                session_id: "s1".into(),
            },
            None,
        );
        crate::market_data::write_jsonl(&mut file, &[start]).unwrap();
        file.flush().unwrap();
        append_incomplete_markers(&path, 2, &config, "s1", "buffer overflow").unwrap();
        let events =
            crate::market_data::read_jsonl(std::io::BufReader::new(File::open(&path).unwrap()))
                .unwrap();
        assert_eq!(
            events.iter().map(|event| event.ordinal).collect::<Vec<_>>(),
            vec![1, 2, 3, 4]
        );
        let mut replay = crate::market_data::MarketReplay::new();
        for event in &events {
            replay.apply(event).unwrap();
        }
        assert!(replay.has_incomplete_data());
        let _ = fs::remove_file(path);
    }
}
