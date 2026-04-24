//! IBKR TWS/Gateway market-data capture. Account login remains in TWS/Gateway.
use ibapi::client::blocking::Client;
use ibapi::contracts::Contract;
use ibapi::market_data::realtime::MarketDepths;
use ibapi::market_data::SmartDepth;
use ibapi::subscriptions::SubscriptionItem;
use serde::Serialize;
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

#[derive(Debug, Serialize)]
struct Record {
    capture_ordinal: u64,
    receive_time_ns: u64,
    source: &'static str,
    data: String,
}

pub fn capture(config: Config) -> Result<(), String> {
    fs::create_dir_all(&config.capture_dir).map_err(|e| e.to_string())?;
    let stamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();
    let path = config
        .capture_dir
        .join(format!("{}-{stamp}.jsonl", config.symbol));
    let out =
        BufWriter::with_capacity(1024 * 1024, File::create(&path).map_err(|e| e.to_string())?);
    let (sender, receiver) = sync_channel::<Record>(config.queue_capacity);
    let writer = std::thread::spawn(move || -> Result<u64, String> {
        let mut out = out;
        let mut written = 0_u64;
        while let Ok(record) = receiver.recv() {
            serde_json::to_writer(&mut out, &record).map_err(|e| e.to_string())?;
            out.write_all(b"\n").map_err(|e| e.to_string())?;
            written = record.capture_ordinal;
        }
        out.flush().map_err(|e| e.to_string())?;
        Ok(written)
    });
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
    let started = Instant::now();
    let mut ordinal = 0_u64;
    let mut dropped_or_invalid = 0_u64;
    let mut overflow_reason = None;
    while started.elapsed() < Duration::from_secs(config.seconds) {
        let mut did_work = false;
        if let Some(item) = l1.next_timeout(Duration::from_millis(1)) {
            did_work = true;
            match item {
                Ok(SubscriptionItem::Data(tick)) => {
                    if let Err(reason) =
                        send_record(&sender, &mut ordinal, "l1", &format!("{tick:?}"))
                    {
                        overflow_reason = Some(reason);
                        dropped_or_invalid += 1;
                        break;
                    }
                }
                Ok(SubscriptionItem::Notice(n)) => {
                    if let Err(reason) =
                        send_record(&sender, &mut ordinal, "notice", &format!("{n:?}"))
                    {
                        overflow_reason = Some(reason);
                        dropped_or_invalid += 1;
                        break;
                    }
                }
                Err(e) => {
                    if let Err(reason) = send_record(
                        &sender,
                        &mut ordinal,
                        "gap",
                        &format!("L1 terminal error: {e}"),
                    ) {
                        overflow_reason = Some(reason);
                    }
                    dropped_or_invalid += 1;
                    break;
                }
            }
        }
        if let Some(item) = l2.next_timeout(Duration::from_millis(1)) {
            did_work = true;
            match item {
                Ok(SubscriptionItem::Data(MarketDepths::Reset)) => {
                    if let Err(reason) = send_record(
                        &sender,
                        &mut ordinal,
                        "depth_reset",
                        "code 317; book must be cleared and rebuilt",
                    ) {
                        overflow_reason = Some(reason);
                    }
                }
                Ok(SubscriptionItem::Data(depth)) => {
                    if let Err(reason) =
                        send_record(&sender, &mut ordinal, "l2", &format!("{depth:?}"))
                    {
                        overflow_reason = Some(reason);
                        dropped_or_invalid += 1;
                        break;
                    }
                }
                Ok(SubscriptionItem::Notice(n)) => {
                    if let Err(reason) =
                        send_record(&sender, &mut ordinal, "notice", &format!("{n:?}"))
                    {
                        overflow_reason = Some(reason);
                        dropped_or_invalid += 1;
                        break;
                    }
                }
                Err(e) => {
                    if let Err(reason) = send_record(
                        &sender,
                        &mut ordinal,
                        "gap",
                        &format!("L2 terminal error: {e}"),
                    ) {
                        overflow_reason = Some(reason);
                    }
                    dropped_or_invalid += 1;
                    break;
                }
            }
        }
        if !did_work {
            std::thread::yield_now();
        }
    }
    drop(sender);
    let written = writer
        .join()
        .map_err(|_| "capture writer panicked".to_owned())??;
    let had_overflow = overflow_reason.is_some();
    if let Some(reason) = overflow_reason {
        append_gap(&path, written + 1, &reason)?;
    }
    eprintln!("capture_records={written},invalidating_errors={dropped_or_invalid},overflow={had_overflow}; raw SDK values are preserved; see README for limits");
    Ok(())
}

fn send_record(
    sender: &SyncSender<Record>,
    ordinal: &mut u64,
    source: &'static str,
    data: &str,
) -> Result<(), String> {
    *ordinal += 1;
    let record = Record {
        capture_ordinal: *ordinal,
        receive_time_ns: SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos()
            .min(u128::from(u64::MAX)) as u64,
        source,
        data: data.into(),
    };
    sender.try_send(record).map_err(|error| match error {
        TrySendError::Full(_) => {
            "bounded capture queue full; captured stream stopped and marked with a gap".to_owned()
        }
        TrySendError::Disconnected(_) => {
            "capture writer stopped; captured stream marked with a gap".to_owned()
        }
    })
}

fn append_gap(path: &std::path::Path, ordinal: u64, reason: &str) -> Result<(), String> {
    let file = OpenOptions::new()
        .append(true)
        .open(path)
        .map_err(|e| e.to_string())?;
    let mut out = BufWriter::new(file);
    let record = Record {
        capture_ordinal: ordinal,
        receive_time_ns: SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos()
            .min(u128::from(u64::MAX)) as u64,
        source: "gap",
        data: reason.to_owned(),
    };
    serde_json::to_writer(&mut out, &record).map_err(|e| e.to_string())?;
    out.write_all(b"\n")
        .and_then(|_| out.flush())
        .map_err(|e| e.to_string())
}
