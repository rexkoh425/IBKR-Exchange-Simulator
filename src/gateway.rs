//! Deterministic in-process exchange gateway for simulated client order flow.
//!
//! Requests are serialized by the caller. `request_id` provides idempotent
//! retries per client, while client order IDs are unique for the gateway's
//! lifetime. Replace requests cancel the old order and submit a fresh order,
//! so they do not inherit time priority.

use crate::{MatchingEngine, OrderRequest, RejectReason, Side, Trade};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::io::{BufRead, Write};
use std::sync::{Arc, Mutex};

fn default_symbol() -> String {
    "DEFAULT".to_owned()
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OrderStateSnapshot {
    pub client_id: u64,
    pub client_order_id: u64,
    pub exchange_order_id: u64,
    pub symbol: String,
    pub priority_sequence: u64,
    pub side: Side,
    pub price_ticks: i64,
    pub order_quantity_lots: u64,
    pub cumulative_quantity_lots: u64,
    pub leaves_quantity_lots: u64,
    pub status: OrderStatus,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct GatewayConfig {
    pub max_order_quantity_lots: u64,
    pub max_order_notional_ticks_lots: u128,
}

impl Default for GatewayConfig {
    fn default() -> Self {
        Self {
            max_order_quantity_lots: u64::MAX,
            max_order_notional_ticks_lots: u128::MAX,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GatewayRequest {
    pub client_id: u64,
    pub request_id: u64,
    pub command: GatewayCommand,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum GatewayCommand {
    New {
        client_order_id: u64,
        #[serde(default = "default_symbol")]
        symbol: String,
        side: Side,
        price_ticks: i64,
        quantity_lots: u64,
    },
    Cancel {
        client_order_id: u64,
    },
    Replace {
        client_order_id: u64,
        replacement_client_order_id: u64,
        price_ticks: i64,
        quantity_lots: u64,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OrderStatus {
    Accepted,
    PartiallyFilled,
    Filled,
    Cancelled,
    Rejected,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", content = "detail", rename_all = "snake_case")]
pub enum GatewayRejectReason {
    InvalidSymbol,
    InvalidClientOrderId,
    DuplicateClientOrderId,
    UnknownClientOrderId,
    OrderNotLive,
    QuantityLimit,
    NotionalLimit,
    Engine(RejectReason),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExecutionReport {
    pub report_sequence: u128,
    pub client_id: u64,
    pub request_id: u64,
    pub client_order_id: u64,
    pub symbol: Option<String>,
    pub exchange_order_id: Option<u64>,
    pub engine_sequence: Option<u64>,
    pub status: OrderStatus,
    pub side: Option<Side>,
    pub price_ticks: Option<i64>,
    pub order_quantity_lots: u64,
    pub cumulative_quantity_lots: u64,
    pub leaves_quantity_lots: u64,
    pub last_quantity_lots: u64,
    pub last_price_ticks: Option<i64>,
    pub reject_reason: Option<GatewayRejectReason>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GatewayResponse {
    pub client_id: u64,
    pub request_id: u64,
    /// True when this response replays a cached result for an identical request.
    pub replayed: bool,
    pub reports: Vec<ExecutionReport>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "outcome", content = "value", rename_all = "snake_case")]
pub enum GatewayOutcome {
    Response(GatewayResponse),
    Error(GatewayError),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum GatewayError {
    InvalidRequestId,
    IdempotencyConflict,
    ExchangeOrderIdExhausted,
    LockPoisoned,
}

impl std::fmt::Display for GatewayError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::InvalidRequestId => formatter.write_str("request_id must be greater than zero"),
            Self::IdempotencyConflict => {
                formatter.write_str("request_id was already used for a different command")
            }
            Self::ExchangeOrderIdExhausted => {
                formatter.write_str("simulated exchange order ID space is exhausted")
            }
            Self::LockPoisoned => formatter.write_str("simulated exchange state lock is poisoned"),
        }
    }
}

impl std::error::Error for GatewayError {}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
struct ClientOrderKey {
    client_id: u64,
    client_order_id: u64,
}

#[derive(Debug, Clone)]
struct OrderRecord {
    exchange_order_id: u64,
    last_request_id: u64,
    symbol: String,
    priority_sequence: u64,
    side: Side,
    price_ticks: i64,
    order_quantity_lots: u64,
    cumulative_quantity_lots: u64,
    status: OrderStatus,
}

#[derive(Debug, Clone)]
struct CachedRequest {
    command: GatewayCommand,
    reports: Vec<ExecutionReport>,
}

#[derive(Debug)]
pub struct SimulatedExchange {
    config: GatewayConfig,
    engines: HashMap<String, MatchingEngine>,
    next_exchange_order_id: u64,
    next_report_sequence: u128,
    orders: HashMap<ClientOrderKey, OrderRecord>,
    used_client_order_ids: HashSet<ClientOrderKey>,
    live_orders: HashMap<u64, ClientOrderKey>,
    request_cache: HashMap<(u64, u64), CachedRequest>,
}

impl Default for SimulatedExchange {
    fn default() -> Self {
        Self::new(GatewayConfig::default())
    }
}

impl SimulatedExchange {
    pub fn new(config: GatewayConfig) -> Self {
        Self {
            config,
            engines: HashMap::new(),
            next_exchange_order_id: 0,
            next_report_sequence: 0,
            orders: HashMap::new(),
            used_client_order_ids: HashSet::new(),
            live_orders: HashMap::new(),
            request_cache: HashMap::new(),
        }
    }

    /// Apply one client request. Repeating an identical `(client_id, request_id)`
    /// returns the original reports without touching the simulated exchange.
    pub fn handle(&mut self, request: GatewayRequest) -> Result<GatewayResponse, GatewayError> {
        if request.request_id == 0 {
            return Err(GatewayError::InvalidRequestId);
        }
        let request_key = (request.client_id, request.request_id);
        if let Some(cached) = self.request_cache.get(&request_key) {
            if cached.command != request.command {
                return Err(GatewayError::IdempotencyConflict);
            }
            return Ok(GatewayResponse {
                client_id: request.client_id,
                request_id: request.request_id,
                replayed: true,
                reports: cached.reports.clone(),
            });
        }

        let reports = match request.command.clone() {
            GatewayCommand::New {
                client_order_id,
                symbol,
                side,
                price_ticks,
                quantity_lots,
            } => self.submit_new(
                request.client_id,
                request.request_id,
                client_order_id,
                symbol,
                side,
                price_ticks,
                quantity_lots,
            )?,
            GatewayCommand::Cancel { client_order_id } => {
                self.cancel(request.client_id, request.request_id, client_order_id)
            }
            GatewayCommand::Replace {
                client_order_id,
                replacement_client_order_id,
                price_ticks,
                quantity_lots,
            } => self.replace(
                request.client_id,
                request.request_id,
                client_order_id,
                replacement_client_order_id,
                price_ticks,
                quantity_lots,
            )?,
        };
        self.request_cache.insert(
            request_key,
            CachedRequest {
                command: request.command,
                reports: reports.clone(),
            },
        );
        Ok(GatewayResponse {
            client_id: request.client_id,
            request_id: request.request_id,
            replayed: false,
            reports,
        })
    }

    pub fn best_bid(&self) -> Option<(i64, u128)> {
        self.best_bid_for("DEFAULT")
    }

    pub fn best_ask(&self) -> Option<(i64, u128)> {
        self.best_ask_for("DEFAULT")
    }

    pub fn best_bid_for(&self, symbol: &str) -> Option<(i64, u128)> {
        self.engines.get(symbol).and_then(MatchingEngine::best_bid)
    }

    pub fn best_ask_for(&self, symbol: &str) -> Option<(i64, u128)> {
        self.engines.get(symbol).and_then(MatchingEngine::best_ask)
    }

    pub fn live_order_count(&self) -> usize {
        self.engines
            .values()
            .map(MatchingEngine::live_order_count)
            .sum()
    }

    pub fn order_states(&self) -> Vec<OrderStateSnapshot> {
        let mut states: Vec<_> = self
            .orders
            .iter()
            .map(|(key, record)| OrderStateSnapshot {
                client_id: key.client_id,
                client_order_id: key.client_order_id,
                exchange_order_id: record.exchange_order_id,
                symbol: record.symbol.clone(),
                priority_sequence: record.priority_sequence,
                side: record.side,
                price_ticks: record.price_ticks,
                order_quantity_lots: record.order_quantity_lots,
                cumulative_quantity_lots: record.cumulative_quantity_lots,
                leaves_quantity_lots: record.order_quantity_lots - record.cumulative_quantity_lots,
                status: record.status,
            })
            .collect();
        states.sort_by_key(|state| (state.client_id, state.client_order_id));
        states
    }

    pub fn book_orders(&self) -> Vec<OrderStateSnapshot> {
        let mut orders: Vec<_> = self
            .order_states()
            .into_iter()
            .filter(|order| {
                matches!(
                    order.status,
                    OrderStatus::Accepted | OrderStatus::PartiallyFilled
                )
            })
            .collect();
        orders.sort_by(|left, right| {
            left.symbol
                .cmp(&right.symbol)
                .then_with(|| side_code(left.side).cmp(&side_code(right.side)))
                .then_with(|| match left.side {
                    Side::Buy => right.price_ticks.cmp(&left.price_ticks),
                    Side::Sell => left.price_ticks.cmp(&right.price_ticks),
                })
                .then_with(|| left.priority_sequence.cmp(&right.priority_sequence))
        });
        orders
    }

    pub fn check_invariants(&self) -> Result<(), String> {
        for (symbol, engine) in &self.engines {
            engine
                .check_invariants()
                .map_err(|error| format!("symbol {symbol}: {error}"))?;
        }
        if self.live_orders.len() != self.live_order_count() {
            return Err("gateway and matching engine disagree on live order count".into());
        }
        for (exchange_id, key) in &self.live_orders {
            let record = self
                .orders
                .get(key)
                .ok_or_else(|| format!("live exchange order {exchange_id} has no client record"))?;
            if record.exchange_order_id != *exchange_id
                || !self
                    .engines
                    .get(&record.symbol)
                    .is_some_and(|engine| engine.contains_live_order(*exchange_id))
                || matches!(
                    record.status,
                    OrderStatus::Filled | OrderStatus::Cancelled | OrderStatus::Rejected
                )
            {
                return Err(format!(
                    "live exchange order {exchange_id} has inconsistent status"
                ));
            }
        }
        for (key, record) in &self.orders {
            let is_live = matches!(
                record.status,
                OrderStatus::Accepted | OrderStatus::PartiallyFilled
            );
            if !self.used_client_order_ids.contains(key) {
                return Err(format!(
                    "client order {}:{} is absent from ID history",
                    key.client_id, key.client_order_id
                ));
            }
            if is_live != self.live_orders.contains_key(&record.exchange_order_id) {
                return Err(format!(
                    "client order {}:{} has inconsistent live status",
                    key.client_id, key.client_order_id
                ));
            }
        }
        Ok(())
    }

    /// Fingerprint exchange book state, client-visible order state, ID history,
    /// and idempotency cache so replay comparisons include future behavior.
    pub fn state_fingerprint(&self) -> u64 {
        fn add(hash: &mut u64, bytes: &[u8]) {
            for byte in bytes {
                *hash = (*hash ^ u64::from(*byte)).wrapping_mul(0x100000001b3);
            }
        }
        let mut hash = 0xcbf29ce484222325_u64;
        let mut symbols: Vec<_> = self.engines.keys().collect();
        symbols.sort_unstable();
        for symbol in symbols {
            add(&mut hash, symbol.as_bytes());
            add(
                &mut hash,
                &self.engines[symbol].state_fingerprint().to_le_bytes(),
            );
        }
        add(&mut hash, &self.next_exchange_order_id.to_le_bytes());
        add(&mut hash, &self.next_report_sequence.to_le_bytes());
        let mut order_keys: Vec<_> = self.orders.keys().copied().collect();
        order_keys.sort_unstable();
        for key in order_keys {
            let record = &self.orders[&key];
            add(&mut hash, &key.client_id.to_le_bytes());
            add(&mut hash, &key.client_order_id.to_le_bytes());
            add(&mut hash, &record.exchange_order_id.to_le_bytes());
            add(&mut hash, &record.last_request_id.to_le_bytes());
            add(&mut hash, record.symbol.as_bytes());
            add(&mut hash, &record.priority_sequence.to_le_bytes());
            add(
                &mut hash,
                &[side_code(record.side), status_code(record.status)],
            );
            add(&mut hash, &record.price_ticks.to_le_bytes());
            add(&mut hash, &record.order_quantity_lots.to_le_bytes());
            add(&mut hash, &record.cumulative_quantity_lots.to_le_bytes());
        }
        let mut used_keys: Vec<_> = self.used_client_order_ids.iter().copied().collect();
        used_keys.sort_unstable();
        for key in used_keys {
            add(&mut hash, &key.client_id.to_le_bytes());
            add(&mut hash, &key.client_order_id.to_le_bytes());
        }
        let mut cached_keys: Vec<_> = self.request_cache.keys().copied().collect();
        cached_keys.sort_unstable();
        for key in cached_keys {
            let cached = &self.request_cache[&key];
            add(&mut hash, &key.0.to_le_bytes());
            add(&mut hash, &key.1.to_le_bytes());
            let command = serde_json::to_vec(&cached.command).expect("command serialization");
            add(&mut hash, &command);
            for report in &cached.reports {
                let bytes = serde_json::to_vec(report).expect("report serialization");
                add(&mut hash, &bytes);
            }
        }
        hash
    }

    #[allow(clippy::too_many_arguments)]
    fn submit_new(
        &mut self,
        client_id: u64,
        request_id: u64,
        client_order_id: u64,
        symbol: String,
        side: Side,
        price_ticks: i64,
        quantity_lots: u64,
    ) -> Result<Vec<ExecutionReport>, GatewayError> {
        let key = ClientOrderKey {
            client_id,
            client_order_id,
        };
        if client_order_id == 0 {
            return Ok(vec![self.reject(
                client_id,
                request_id,
                client_order_id,
                None,
                Some(side),
                Some(price_ticks),
                quantity_lots,
                GatewayRejectReason::InvalidClientOrderId,
            )]);
        }
        if !self.used_client_order_ids.insert(key) {
            return Ok(vec![self.reject(
                client_id,
                request_id,
                client_order_id,
                self.orders.get(&key).map(|record| record.exchange_order_id),
                Some(side),
                Some(price_ticks),
                quantity_lots,
                GatewayRejectReason::DuplicateClientOrderId,
            )]);
        }
        if symbol.trim().is_empty() {
            return Ok(vec![self.reject(
                client_id,
                request_id,
                client_order_id,
                None,
                Some(side),
                Some(price_ticks),
                quantity_lots,
                GatewayRejectReason::InvalidSymbol,
            )]);
        }
        if let Some(reason) = self.risk_rejection(price_ticks, quantity_lots) {
            return Ok(vec![self.reject(
                client_id,
                request_id,
                client_order_id,
                None,
                Some(side),
                Some(price_ticks),
                quantity_lots,
                reason,
            )]);
        }
        let exchange_order_id = self.allocate_exchange_order_id()?;
        let result = self
            .engines
            .entry(symbol.clone())
            .or_default()
            .submit(OrderRequest {
                order_id: exchange_order_id,
                side,
                price_ticks,
                quantity_lots,
            });
        if let Some(reason) = result.rejection {
            return Ok(vec![self.reject(
                client_id,
                request_id,
                client_order_id,
                None,
                Some(side),
                Some(price_ticks),
                quantity_lots,
                GatewayRejectReason::Engine(reason),
            )]);
        }
        self.orders.insert(
            key,
            OrderRecord {
                exchange_order_id,
                last_request_id: request_id,
                symbol,
                priority_sequence: result.sequence.expect("accepted order has engine sequence"),
                side,
                price_ticks,
                order_quantity_lots: quantity_lots,
                cumulative_quantity_lots: 0,
                status: OrderStatus::Accepted,
            },
        );
        self.reports_for_submit(
            client_id,
            request_id,
            key,
            exchange_order_id,
            side,
            price_ticks,
            quantity_lots,
            result.trades,
            result.resting_quantity_lots,
            result.sequence,
        )
    }

    fn cancel(
        &mut self,
        client_id: u64,
        request_id: u64,
        client_order_id: u64,
    ) -> Vec<ExecutionReport> {
        let key = ClientOrderKey {
            client_id,
            client_order_id,
        };
        let Some(mut record) = self.orders.get(&key).cloned() else {
            return vec![self.reject(
                client_id,
                request_id,
                client_order_id,
                None,
                None,
                None,
                0,
                GatewayRejectReason::UnknownClientOrderId,
            )];
        };
        if !self.live_orders.contains_key(&record.exchange_order_id) {
            return vec![self.reject(
                client_id,
                request_id,
                client_order_id,
                Some(record.exchange_order_id),
                Some(record.side),
                Some(record.price_ticks),
                record.order_quantity_lots,
                GatewayRejectReason::OrderNotLive,
            )];
        }
        let cancelled = self
            .engines
            .get_mut(&record.symbol)
            .expect("order symbol has a matching engine")
            .cancel(record.exchange_order_id)
            .expect("gateway live index and engine book agree");
        record.status = OrderStatus::Cancelled;
        self.orders.insert(key, record.clone());
        self.live_orders.remove(&record.exchange_order_id);
        vec![self.report(
            client_id,
            request_id,
            client_order_id,
            Some(record.exchange_order_id),
            Some(cancelled.sequence),
            OrderStatus::Cancelled,
            Some(record.side),
            Some(record.price_ticks),
            record.order_quantity_lots,
            record.cumulative_quantity_lots,
            cancelled.remaining_lots,
            0,
            None,
            None,
        )]
    }

    fn replace(
        &mut self,
        client_id: u64,
        request_id: u64,
        client_order_id: u64,
        replacement_client_order_id: u64,
        price_ticks: i64,
        quantity_lots: u64,
    ) -> Result<Vec<ExecutionReport>, GatewayError> {
        let old_key = ClientOrderKey {
            client_id,
            client_order_id,
        };
        let new_key = ClientOrderKey {
            client_id,
            client_order_id: replacement_client_order_id,
        };
        if replacement_client_order_id == 0 {
            return Ok(vec![self.reject(
                client_id,
                request_id,
                replacement_client_order_id,
                None,
                None,
                Some(price_ticks),
                quantity_lots,
                GatewayRejectReason::InvalidClientOrderId,
            )]);
        }
        if !self.used_client_order_ids.insert(new_key) {
            return Ok(vec![self.reject(
                client_id,
                request_id,
                replacement_client_order_id,
                self.orders
                    .get(&new_key)
                    .map(|record| record.exchange_order_id),
                None,
                Some(price_ticks),
                quantity_lots,
                GatewayRejectReason::DuplicateClientOrderId,
            )]);
        }
        let Some(old_record) = self.orders.get(&old_key).cloned() else {
            return Ok(vec![self.reject(
                client_id,
                request_id,
                replacement_client_order_id,
                None,
                None,
                Some(price_ticks),
                quantity_lots,
                GatewayRejectReason::UnknownClientOrderId,
            )]);
        };
        if !self.live_orders.contains_key(&old_record.exchange_order_id) {
            return Ok(vec![self.reject(
                client_id,
                request_id,
                replacement_client_order_id,
                None,
                None,
                Some(price_ticks),
                quantity_lots,
                GatewayRejectReason::OrderNotLive,
            )]);
        }
        if let Some(reason) = self.risk_rejection(price_ticks, quantity_lots) {
            return Ok(vec![self.reject(
                client_id,
                request_id,
                replacement_client_order_id,
                None,
                Some(old_record.side),
                Some(price_ticks),
                quantity_lots,
                reason,
            )]);
        }
        let exchange_order_id = self.allocate_exchange_order_id()?;
        let replaced = self
            .engines
            .get_mut(&old_record.symbol)
            .expect("order symbol has a matching engine")
            .replace(
                old_record.exchange_order_id,
                OrderRequest {
                    order_id: exchange_order_id,
                    side: old_record.side,
                    price_ticks,
                    quantity_lots,
                },
            );
        if let Some(reason) = replaced.rejection {
            return Ok(vec![self.reject(
                client_id,
                request_id,
                replacement_client_order_id,
                None,
                Some(old_record.side),
                Some(price_ticks),
                quantity_lots,
                GatewayRejectReason::Engine(reason),
            )]);
        }

        let mut reports = Vec::new();
        let mut old_record = old_record;
        old_record.status = OrderStatus::Cancelled;
        self.orders.insert(old_key, old_record.clone());
        self.live_orders.remove(&old_record.exchange_order_id);
        reports.push(self.report(
            client_id,
            request_id,
            client_order_id,
            Some(old_record.exchange_order_id),
            replaced.cancelled_sequence,
            OrderStatus::Cancelled,
            Some(old_record.side),
            Some(old_record.price_ticks),
            old_record.order_quantity_lots,
            old_record.cumulative_quantity_lots,
            old_record.order_quantity_lots - old_record.cumulative_quantity_lots,
            0,
            None,
            None,
        ));

        self.orders.insert(
            new_key,
            OrderRecord {
                exchange_order_id,
                last_request_id: request_id,
                symbol: old_record.symbol.clone(),
                priority_sequence: replaced
                    .replacement
                    .sequence
                    .expect("accepted replacement has engine sequence"),
                side: old_record.side,
                price_ticks,
                order_quantity_lots: quantity_lots,
                cumulative_quantity_lots: 0,
                status: OrderStatus::Accepted,
            },
        );
        reports.extend(self.reports_for_submit(
            client_id,
            request_id,
            new_key,
            exchange_order_id,
            old_record.side,
            price_ticks,
            quantity_lots,
            replaced.replacement.trades,
            replaced.replacement.resting_quantity_lots,
            replaced.replacement.sequence,
        )?);
        Ok(reports)
    }

    #[allow(clippy::too_many_arguments)]
    fn reports_for_submit(
        &mut self,
        client_id: u64,
        request_id: u64,
        taker_key: ClientOrderKey,
        taker_exchange_id: u64,
        taker_side: Side,
        taker_price: i64,
        taker_quantity: u64,
        trades: Vec<Trade>,
        resting_quantity: u64,
        engine_sequence: Option<u64>,
    ) -> Result<Vec<ExecutionReport>, GatewayError> {
        let mut reports = Vec::with_capacity(trades.len().saturating_mul(2).max(1));
        let had_trades = !trades.is_empty();
        for trade in trades {
            let maker_key = *self
                .live_orders
                .get(&trade.maker_order_id)
                .expect("matching engine maker is represented by gateway state");
            let mut maker = self.orders[&maker_key].clone();
            maker.cumulative_quantity_lots = maker
                .cumulative_quantity_lots
                .checked_add(trade.quantity_lots)
                .expect("cumulative fills cannot exceed submitted quantity");
            let maker_leaves = maker.order_quantity_lots - maker.cumulative_quantity_lots;
            maker.status = if maker_leaves == 0 {
                OrderStatus::Filled
            } else {
                OrderStatus::PartiallyFilled
            };
            self.orders.insert(maker_key, maker.clone());
            if maker_leaves == 0 {
                self.live_orders.remove(&trade.maker_order_id);
            }
            reports.push(self.report(
                maker_key.client_id,
                maker.last_request_id,
                maker_key.client_order_id,
                Some(trade.maker_order_id),
                Some(trade.sequence),
                maker.status,
                Some(maker.side),
                Some(maker.price_ticks),
                maker.order_quantity_lots,
                maker.cumulative_quantity_lots,
                maker_leaves,
                trade.quantity_lots,
                Some(trade.price_ticks),
                None,
            ));

            let mut taker = self.orders[&taker_key].clone();
            taker.cumulative_quantity_lots = taker
                .cumulative_quantity_lots
                .checked_add(trade.quantity_lots)
                .expect("cumulative fills cannot exceed submitted quantity");
            let taker_leaves = taker.order_quantity_lots - taker.cumulative_quantity_lots;
            taker.status = if taker_leaves == 0 {
                OrderStatus::Filled
            } else {
                OrderStatus::PartiallyFilled
            };
            self.orders.insert(taker_key, taker.clone());
            reports.push(self.report(
                client_id,
                request_id,
                taker_key.client_order_id,
                Some(taker_exchange_id),
                Some(trade.sequence),
                taker.status,
                Some(taker_side),
                Some(taker_price),
                taker_quantity,
                taker.cumulative_quantity_lots,
                taker_leaves,
                trade.quantity_lots,
                Some(trade.price_ticks),
                None,
            ));
        }
        if !had_trades {
            reports.push(self.report(
                client_id,
                request_id,
                taker_key.client_order_id,
                Some(taker_exchange_id),
                engine_sequence,
                OrderStatus::Accepted,
                Some(taker_side),
                Some(taker_price),
                taker_quantity,
                0,
                resting_quantity,
                0,
                None,
                None,
            ));
        }
        if resting_quantity > 0 {
            self.live_orders.insert(taker_exchange_id, taker_key);
        }
        Ok(reports)
    }

    fn risk_rejection(&self, price_ticks: i64, quantity_lots: u64) -> Option<GatewayRejectReason> {
        if price_ticks <= 0 || quantity_lots == 0 {
            return Some(GatewayRejectReason::Engine(if price_ticks <= 0 {
                RejectReason::InvalidPrice
            } else {
                RejectReason::InvalidQuantity
            }));
        }
        if quantity_lots > self.config.max_order_quantity_lots {
            return Some(GatewayRejectReason::QuantityLimit);
        }
        let notional = (price_ticks as u128).checked_mul(u128::from(quantity_lots));
        if notional.is_none_or(|value| value > self.config.max_order_notional_ticks_lots) {
            return Some(GatewayRejectReason::NotionalLimit);
        }
        None
    }

    fn allocate_exchange_order_id(&mut self) -> Result<u64, GatewayError> {
        self.next_exchange_order_id = self
            .next_exchange_order_id
            .checked_add(1)
            .ok_or(GatewayError::ExchangeOrderIdExhausted)?;
        Ok(self.next_exchange_order_id)
    }

    #[allow(clippy::too_many_arguments)]
    fn reject(
        &mut self,
        client_id: u64,
        request_id: u64,
        client_order_id: u64,
        exchange_order_id: Option<u64>,
        side: Option<Side>,
        price_ticks: Option<i64>,
        quantity_lots: u64,
        reason: GatewayRejectReason,
    ) -> ExecutionReport {
        self.report(
            client_id,
            request_id,
            client_order_id,
            exchange_order_id,
            None,
            OrderStatus::Rejected,
            side,
            price_ticks,
            quantity_lots,
            0,
            0,
            0,
            None,
            Some(reason),
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn report(
        &mut self,
        client_id: u64,
        request_id: u64,
        client_order_id: u64,
        exchange_order_id: Option<u64>,
        engine_sequence: Option<u64>,
        status: OrderStatus,
        side: Option<Side>,
        price_ticks: Option<i64>,
        order_quantity_lots: u64,
        cumulative_quantity_lots: u64,
        leaves_quantity_lots: u64,
        last_quantity_lots: u64,
        last_price_ticks: Option<i64>,
        reject_reason: Option<GatewayRejectReason>,
    ) -> ExecutionReport {
        self.next_report_sequence = self
            .next_report_sequence
            .checked_add(1)
            .expect("u128 execution report sequence exhausted");
        ExecutionReport {
            report_sequence: self.next_report_sequence,
            client_id,
            request_id,
            client_order_id,
            symbol: self
                .orders
                .get(&ClientOrderKey {
                    client_id,
                    client_order_id,
                })
                .map(|record| record.symbol.clone()),
            exchange_order_id,
            engine_sequence,
            status,
            side,
            price_ticks,
            order_quantity_lots,
            cumulative_quantity_lots,
            leaves_quantity_lots,
            last_quantity_lots,
            last_price_ticks,
            reject_reason,
        }
    }
}

/// Thread-safe facade that makes each request transition linearizable. When
/// requests race, mutex acquisition order defines the serialized exchange order;
/// the assigned report sequences are monotone for that observed order.
#[derive(Debug, Clone)]
pub struct ConcurrentSimulatedExchange {
    inner: Arc<Mutex<SimulatedExchange>>,
}

impl ConcurrentSimulatedExchange {
    pub fn new(config: GatewayConfig) -> Self {
        Self {
            inner: Arc::new(Mutex::new(SimulatedExchange::new(config))),
        }
    }

    pub fn handle(&self, request: GatewayRequest) -> Result<GatewayResponse, GatewayError> {
        self.inner
            .lock()
            .map_err(|_| GatewayError::LockPoisoned)?
            .handle(request)
    }

    pub fn snapshot(&self) -> Result<ExchangeStateSnapshot, GatewayError> {
        let exchange = self.inner.lock().map_err(|_| GatewayError::LockPoisoned)?;
        Ok(ExchangeStateSnapshot {
            state_fingerprint: exchange.state_fingerprint(),
            orders: exchange.order_states(),
        })
    }

    pub fn check_invariants(&self) -> Result<(), String> {
        self.inner
            .lock()
            .map_err(|_| "simulated exchange state lock is poisoned".to_owned())?
            .check_invariants()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExchangeStateSnapshot {
    pub state_fingerprint: u64,
    pub orders: Vec<OrderStateSnapshot>,
}

fn side_code(side: Side) -> u8 {
    match side {
        Side::Buy => 0,
        Side::Sell => 1,
    }
}

fn status_code(status: OrderStatus) -> u8 {
    match status {
        OrderStatus::Accepted => 0,
        OrderStatus::PartiallyFilled => 1,
        OrderStatus::Filled => 2,
        OrderStatus::Cancelled => 3,
        OrderStatus::Rejected => 4,
    }
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct JsonlSummary {
    pub requests: u64,
    pub replayed_requests: u64,
    pub reports: u64,
    pub rejections: u64,
    pub state_fingerprint: u64,
}

/// Read JSONL client requests and write one JSONL response per request.
pub fn run_jsonl<R: BufRead, W: Write>(
    reader: R,
    mut writer: W,
    config: GatewayConfig,
) -> Result<JsonlSummary, String> {
    let mut exchange = SimulatedExchange::new(config);
    let mut summary = JsonlSummary::default();
    for (line_index, line) in reader.lines().enumerate() {
        let line = line.map_err(|error| format!("line {}: {error}", line_index + 1))?;
        if line.trim().is_empty() {
            continue;
        }
        let request: GatewayRequest = serde_json::from_str(&line)
            .map_err(|error| format!("line {}: {error}", line_index + 1))?;
        let response = exchange
            .handle(request)
            .map_err(|error| format!("line {}: {error}", line_index + 1))?;
        summary.requests += 1;
        summary.replayed_requests += u64::from(response.replayed);
        summary.reports += response.reports.len() as u64;
        summary.rejections += response
            .reports
            .iter()
            .filter(|report| report.status == OrderStatus::Rejected)
            .count() as u64;
        serde_json::to_writer(&mut writer, &response).map_err(|error| error.to_string())?;
        writer.write_all(b"\n").map_err(|error| error.to_string())?;
    }
    writer.flush().map_err(|error| error.to_string())?;
    summary.state_fingerprint = exchange.state_fingerprint();
    exchange
        .check_invariants()
        .map_err(|error| format!("exchange invariant failed: {error}"))?;
    Ok(summary)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn request(client_id: u64, request_id: u64, command: GatewayCommand) -> GatewayRequest {
        GatewayRequest {
            client_id,
            request_id,
            command,
        }
    }

    fn new_order(id: u64, side: Side, price: i64, quantity: u64) -> GatewayCommand {
        new_order_for_symbol(default_symbol(), id, side, price, quantity)
    }

    fn new_order_for_symbol(
        symbol: impl Into<String>,
        id: u64,
        side: Side,
        price: i64,
        quantity: u64,
    ) -> GatewayCommand {
        GatewayCommand::New {
            client_order_id: id,
            symbol: symbol.into(),
            side,
            price_ticks: price,
            quantity_lots: quantity,
        }
    }

    #[test]
    fn partial_fills_then_cancel_reconcile_cumulative_and_leaves() {
        let mut exchange = SimulatedExchange::default();
        let ask = exchange
            .handle(request(1, 1, new_order(11, Side::Sell, 100, 5)))
            .unwrap();
        assert_eq!(ask.reports[0].status, OrderStatus::Accepted);
        let buy = exchange
            .handle(request(2, 1, new_order(21, Side::Buy, 100, 3)))
            .unwrap();
        assert_eq!(buy.reports.len(), 2);
        assert_eq!(buy.reports[0].client_order_id, 11);
        assert_eq!(buy.reports[0].status, OrderStatus::PartiallyFilled);
        assert_eq!(buy.reports[0].cumulative_quantity_lots, 3);
        assert_eq!(buy.reports[0].leaves_quantity_lots, 2);
        assert_eq!(buy.reports[1].client_order_id, 21);
        assert_eq!(buy.reports[1].status, OrderStatus::Filled);
        assert_eq!(buy.reports[1].last_price_ticks, Some(100));
        let cancel = exchange
            .handle(request(
                1,
                2,
                GatewayCommand::Cancel {
                    client_order_id: 11,
                },
            ))
            .unwrap();
        assert_eq!(cancel.reports[0].status, OrderStatus::Cancelled);
        assert_eq!(cancel.reports[0].cumulative_quantity_lots, 3);
        assert_eq!(cancel.reports[0].leaves_quantity_lots, 2);
        assert_eq!(exchange.live_order_count(), 0);
        assert!(exchange.check_invariants().is_ok());
    }

    #[test]
    fn request_retry_is_idempotent_and_conflicting_retry_is_rejected() {
        let mut exchange = SimulatedExchange::default();
        let command = new_order(1, Side::Buy, 99, 2);
        let first = exchange.handle(request(3, 7, command.clone())).unwrap();
        let retry = exchange.handle(request(3, 7, command)).unwrap();
        assert!(!first.replayed);
        assert!(retry.replayed);
        assert_eq!(first.reports, retry.reports);
        assert_eq!(exchange.live_order_count(), 1);
        assert_eq!(
            exchange.handle(request(3, 7, new_order(2, Side::Buy, 99, 2))),
            Err(GatewayError::IdempotencyConflict)
        );
        assert_eq!(exchange.live_order_count(), 1);
    }

    #[test]
    fn replacement_cancels_old_order_and_loses_fifo_priority() {
        let mut exchange = SimulatedExchange::default();
        exchange
            .handle(request(1, 1, new_order(1, Side::Buy, 100, 2)))
            .unwrap();
        exchange
            .handle(request(2, 1, new_order(1, Side::Buy, 100, 2)))
            .unwrap();
        let replaced = exchange
            .handle(request(
                1,
                2,
                GatewayCommand::Replace {
                    client_order_id: 1,
                    replacement_client_order_id: 2,
                    price_ticks: 100,
                    quantity_lots: 2,
                },
            ))
            .unwrap();
        assert_eq!(replaced.reports[0].status, OrderStatus::Cancelled);
        assert_eq!(replaced.reports[1].status, OrderStatus::Accepted);
        let sell = exchange
            .handle(request(3, 1, new_order(2, Side::Sell, 100, 4)))
            .unwrap();
        assert_eq!(sell.reports[0].client_order_id, 1);
        assert_eq!(sell.reports[0].client_id, 2);
        assert_eq!(sell.reports[0].status, OrderStatus::Filled);
        assert_eq!(sell.reports[1].client_order_id, 2);
        assert_eq!(sell.reports[1].client_id, 3);
        assert_eq!(sell.reports[2].client_order_id, 2);
        assert_eq!(sell.reports[2].client_id, 1);
        assert_eq!(sell.reports[2].status, OrderStatus::Filled);
        assert_eq!(sell.reports[3].client_id, 3);
        assert_eq!(sell.reports[3].client_order_id, 2);
        assert_eq!(sell.reports[1].status, OrderStatus::PartiallyFilled);
        assert!(exchange.check_invariants().is_ok());
    }

    #[test]
    fn risk_rejection_does_not_change_the_exchange_book() {
        let config = GatewayConfig {
            max_order_quantity_lots: 5,
            max_order_notional_ticks_lots: 500,
        };
        let mut exchange = SimulatedExchange::new(config);
        let before = exchange.state_fingerprint();
        let rejected = exchange
            .handle(request(1, 1, new_order(1, Side::Buy, 101, 5)))
            .unwrap();
        assert_eq!(rejected.reports[0].status, OrderStatus::Rejected);
        assert_eq!(
            rejected.reports[0].reject_reason,
            Some(GatewayRejectReason::NotionalLimit)
        );
        assert_eq!(exchange.live_order_count(), 0);
        // The request cache and report sequence advance, but the matching book does not.
        assert_ne!(exchange.state_fingerprint(), before);
        assert!(exchange.check_invariants().is_ok());
    }

    #[test]
    fn risk_rejected_replace_keeps_the_original_order_live() {
        let config = GatewayConfig {
            max_order_quantity_lots: 5,
            max_order_notional_ticks_lots: 500,
        };
        let mut exchange = SimulatedExchange::new(config);
        exchange
            .handle(request(1, 1, new_order(1, Side::Buy, 90, 2)))
            .unwrap();
        let before = exchange.best_bid();
        let response = exchange
            .handle(request(
                1,
                2,
                GatewayCommand::Replace {
                    client_order_id: 1,
                    replacement_client_order_id: 2,
                    price_ticks: 101,
                    quantity_lots: 5,
                },
            ))
            .unwrap();
        assert_eq!(response.reports[0].status, OrderStatus::Rejected);
        assert_eq!(
            response.reports[0].reject_reason,
            Some(GatewayRejectReason::NotionalLimit)
        );
        assert_eq!(exchange.best_bid(), before);
        assert_eq!(exchange.live_order_count(), 1);
        assert!(exchange.check_invariants().is_ok());
    }

    #[test]
    fn mixed_client_order_flow_keeps_book_and_reports_consistent() {
        let mut exchange = SimulatedExchange::default();
        let mut random = 0x9e37_79b9_7f4a_7c15_u64;
        let mut last_report_sequence = 0_u128;
        let mut known_orders = Vec::new();
        for index in 1..=2_000_u64 {
            random ^= random << 7;
            random ^= random >> 9;
            random ^= random << 8;
            let client_id = random % 4;
            let request_id = index;
            let command = match random % 4 {
                0 | 1 => {
                    let client_order_id = index;
                    known_orders.push((client_id, client_order_id));
                    new_order(
                        client_order_id,
                        if random & 4 == 0 {
                            Side::Buy
                        } else {
                            Side::Sell
                        },
                        98 + ((random >> 4) % 5) as i64,
                        1 + ((random >> 9) % 8),
                    )
                }
                2 if !known_orders.is_empty() => {
                    let (target_client, client_order_id) =
                        known_orders[(random as usize >> 8) % known_orders.len()];
                    GatewayCommand::Cancel {
                        client_order_id: if target_client == client_id {
                            client_order_id
                        } else {
                            u64::MAX - client_order_id
                        },
                    }
                }
                3 if !known_orders.is_empty() => {
                    let (target_client, client_order_id) =
                        known_orders[(random as usize >> 8) % known_orders.len()];
                    GatewayCommand::Replace {
                        client_order_id: if target_client == client_id {
                            client_order_id
                        } else {
                            u64::MAX - client_order_id
                        },
                        replacement_client_order_id: index,
                        price_ticks: 98 + ((random >> 4) % 5) as i64,
                        quantity_lots: 1 + ((random >> 9) % 8),
                    }
                }
                _ => new_order(index, Side::Buy, 100, 1),
            };
            let response = exchange
                .handle(request(client_id, request_id, command))
                .unwrap();
            for report in response.reports {
                assert!(report.report_sequence > last_report_sequence);
                last_report_sequence = report.report_sequence;
                if report.status != OrderStatus::Rejected {
                    assert_eq!(
                        report.cumulative_quantity_lots + report.leaves_quantity_lots,
                        report.order_quantity_lots
                    );
                }
            }
            exchange
                .check_invariants()
                .expect("gateway and matching engine state must remain consistent");
        }
    }

    #[test]
    fn deterministic_jsonl_replay_matches_byte_for_byte() {
        let input = concat!(
            "{\"client_id\":1,\"request_id\":1,\"command\":{\"type\":\"new\",\"client_order_id\":1,\"side\":\"sell\",\"price_ticks\":100,\"quantity_lots\":2}}\n",
            "{\"client_id\":2,\"request_id\":1,\"command\":{\"type\":\"new\",\"client_order_id\":1,\"side\":\"buy\",\"price_ticks\":100,\"quantity_lots\":1}}\n",
            "{\"client_id\":1,\"request_id\":2,\"command\":{\"type\":\"cancel\",\"client_order_id\":1}}\n"
        );
        let mut first = Vec::new();
        let one = run_jsonl(input.as_bytes(), &mut first, GatewayConfig::default()).unwrap();
        let mut second = Vec::new();
        let two = run_jsonl(input.as_bytes(), &mut second, GatewayConfig::default()).unwrap();
        assert_eq!(first, second);
        assert_eq!(one, two);
        assert_eq!(one.requests, 3);
        assert_eq!(one.reports, 4);
        assert_eq!(one.rejections, 0);
    }

    #[test]
    fn symbols_have_independent_books_and_reports_name_the_instrument() {
        let mut exchange = SimulatedExchange::default();
        let ask = exchange
            .handle(request(
                10,
                1,
                new_order_for_symbol("AAPL", 1, Side::Sell, 100, 2),
            ))
            .unwrap();
        assert_eq!(ask.reports[0].symbol.as_deref(), Some("AAPL"));
        exchange
            .handle(request(
                11,
                1,
                new_order_for_symbol("MSFT", 1, Side::Sell, 100, 1),
            ))
            .unwrap();
        let buy = exchange
            .handle(request(
                20,
                1,
                new_order_for_symbol("AAPL", 1, Side::Buy, 100, 2),
            ))
            .unwrap();
        assert_eq!(buy.reports.len(), 2);
        assert!(buy
            .reports
            .iter()
            .all(|report| report.symbol.as_deref() == Some("AAPL")));
        assert_eq!(exchange.best_ask_for("AAPL"), None);
        assert_eq!(exchange.best_ask_for("MSFT"), Some((100, 1)));
        assert!(exchange.check_invariants().is_ok());
    }

    #[test]
    fn simultaneous_identical_requests_apply_once_and_replay_one_report_sequence() {
        use std::sync::{Arc, Barrier};
        use std::thread;

        let exchange = ConcurrentSimulatedExchange::new(GatewayConfig::default());
        let barrier = Arc::new(Barrier::new(12));
        let mut workers = Vec::new();
        for _ in 0..12 {
            let exchange = exchange.clone();
            let barrier = Arc::clone(&barrier);
            workers.push(thread::spawn(move || {
                barrier.wait();
                exchange
                    .handle(request(44, 7, new_order(5, Side::Buy, 90, 3)))
                    .unwrap()
            }));
        }
        let responses: Vec<_> = workers
            .into_iter()
            .map(|worker| worker.join().unwrap())
            .collect();
        assert_eq!(
            responses
                .iter()
                .filter(|response| !response.replayed)
                .count(),
            1
        );
        assert_eq!(
            responses
                .iter()
                .filter(|response| response.replayed)
                .count(),
            11
        );
        assert!(responses
            .iter()
            .all(|response| response.reports == responses[0].reports));
        assert_eq!(exchange.snapshot().unwrap().orders.len(), 1);
        exchange.check_invariants().unwrap();
    }

    #[test]
    fn concurrent_conflicting_retries_accept_one_command_and_reject_the_other() {
        use std::sync::{Arc, Barrier};
        use std::thread;

        let exchange = ConcurrentSimulatedExchange::new(GatewayConfig::default());
        let barrier = Arc::new(Barrier::new(2));
        let requests = [
            request(9, 41, new_order(1, Side::Buy, 99, 2)),
            request(9, 41, new_order(2, Side::Buy, 98, 3)),
        ];
        let workers: Vec<_> = requests
            .into_iter()
            .map(|request| {
                let exchange = exchange.clone();
                let barrier = Arc::clone(&barrier);
                thread::spawn(move || {
                    barrier.wait();
                    exchange.handle(request)
                })
            })
            .collect();
        let results: Vec<_> = workers
            .into_iter()
            .map(|worker| worker.join().unwrap())
            .collect();
        assert_eq!(results.iter().filter(|result| result.is_ok()).count(), 1);
        assert_eq!(
            results
                .iter()
                .filter(|result| **result == Err(GatewayError::IdempotencyConflict))
                .count(),
            1
        );
        let state = exchange.snapshot().unwrap();
        assert_eq!(state.orders.len(), 1);
        assert_eq!(exchange.check_invariants(), Ok(()));
    }

    #[test]
    fn clients_cannot_cancel_or_replace_another_clients_order() {
        let mut exchange = SimulatedExchange::default();
        exchange
            .handle(request(100, 1, new_order(55, Side::Sell, 100, 4)))
            .unwrap();
        let before = exchange.book_orders();
        let cancel = exchange
            .handle(request(
                200,
                1,
                GatewayCommand::Cancel {
                    client_order_id: 55,
                },
            ))
            .unwrap();
        assert_eq!(
            cancel.reports[0].reject_reason,
            Some(GatewayRejectReason::UnknownClientOrderId)
        );
        let replace = exchange
            .handle(request(
                200,
                2,
                GatewayCommand::Replace {
                    client_order_id: 55,
                    replacement_client_order_id: 56,
                    price_ticks: 99,
                    quantity_lots: 3,
                },
            ))
            .unwrap();
        assert_eq!(
            replace.reports[0].reject_reason,
            Some(GatewayRejectReason::UnknownClientOrderId)
        );
        assert_eq!(exchange.book_orders(), before);
        let owner_cancel = exchange
            .handle(request(
                100,
                2,
                GatewayCommand::Cancel {
                    client_order_id: 55,
                },
            ))
            .unwrap();
        assert_eq!(owner_cancel.reports[0].status, OrderStatus::Cancelled);
        assert_eq!(exchange.live_order_count(), 0);
    }

    #[test]
    fn concurrent_clients_receive_distinct_fills_with_global_report_sequences() {
        use std::thread;

        let exchange = ConcurrentSimulatedExchange::new(GatewayConfig::default());
        const ORDER_COUNT: u64 = 24;
        for index in 0..ORDER_COUNT {
            exchange
                .handle(request(
                    100 + index,
                    1,
                    new_order_for_symbol("NYSE:AAPL", 1, Side::Sell, 100, 1),
                ))
                .unwrap();
        }
        exchange
            .handle(request(
                500,
                1,
                new_order_for_symbol("NASDAQ:AAPL", 1, Side::Sell, 100, 1),
            ))
            .unwrap();

        let workers: Vec<_> = (0..ORDER_COUNT)
            .map(|index| {
                let exchange = exchange.clone();
                thread::spawn(move || {
                    exchange
                        .handle(request(
                            1_000 + index,
                            1,
                            new_order_for_symbol("NYSE:AAPL", 1, Side::Buy, 100, 1),
                        ))
                        .unwrap()
                })
            })
            .collect();
        let responses: Vec<_> = workers
            .into_iter()
            .map(|worker| worker.join().unwrap())
            .collect();
        let mut report_sequences: Vec<_> = responses
            .iter()
            .flat_map(|response| response.reports.iter().map(|report| report.report_sequence))
            .collect();
        report_sequences.sort_unstable();
        report_sequences.dedup();
        assert_eq!(report_sequences.len(), (ORDER_COUNT * 2) as usize);
        assert!(report_sequences
            .windows(2)
            .all(|pair| pair[1] == pair[0] + 1));
        let mut maker_recipients = HashSet::new();
        let mut buyer_recipients = HashSet::new();
        for response in &responses {
            assert_eq!(response.reports.len(), 2);
            let maker = &response.reports[0];
            let taker = &response.reports[1];
            assert_eq!(maker.status, OrderStatus::Filled);
            assert_eq!(taker.status, OrderStatus::Filled);
            assert_eq!(maker.symbol.as_deref(), Some("NYSE:AAPL"));
            assert_eq!(taker.symbol.as_deref(), Some("NYSE:AAPL"));
            assert_eq!(taker.client_id, response.client_id);
            assert_eq!(taker.request_id, response.request_id);
            assert!((100..100 + ORDER_COUNT).contains(&maker.client_id));
            assert!(maker_recipients.insert(maker.client_id));
            assert!(buyer_recipients.insert(taker.client_id));
            assert_eq!(maker.last_quantity_lots, 1);
            assert_eq!(taker.last_quantity_lots, 1);
        }
        assert_eq!(maker_recipients.len(), ORDER_COUNT as usize);
        assert_eq!(buyer_recipients.len(), ORDER_COUNT as usize);
        let snapshot = exchange.snapshot().unwrap();
        assert_eq!(snapshot.orders.len(), (ORDER_COUNT * 2 + 1) as usize);
        assert_eq!(exchange.check_invariants(), Ok(()));
    }
}
