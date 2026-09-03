//! Deterministic in-process exchange gateway for simulated client order flow.
//!
//! Requests are serialized by the caller. `request_id` provides idempotent
//! retries per client, while client order IDs are unique for the gateway's
//! lifetime. Replace requests cancel the old order and submit a fresh order,
//! so they do not inherit time priority.

use crate::expiry::ExpirySchedule;
use crate::session::SessionState;
use crate::{MatchingEngine, OrderOptions, OrderRequest, RejectReason, Side, TimeInForce, Trade};
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
    pub time_in_force: TimeInForce,
    pub post_only: bool,
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
        #[serde(default)]
        time_in_force: TimeInForce,
        #[serde(default)]
        post_only: bool,
    },
    Cancel {
        client_order_id: u64,
    },
    CancelAll,
    SetSession {
        state: SessionState,
    },
    SetExpiry {
        client_order_id: u64,
        deadline: Option<u64>,
    },
    Replace {
        client_order_id: u64,
        replacement_client_order_id: u64,
        price_ticks: i64,
        quantity_lots: u64,
        #[serde(default)]
        time_in_force: TimeInForce,
        #[serde(default)]
        post_only: bool,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OrderStatus {
    Accepted,
    PartiallyFilled,
    Filled,
    Cancelled,
    Expired,
    Rejected,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", content = "detail", rename_all = "snake_case")]
pub enum GatewayRejectReason {
    SessionClosed,
    ExpiryNotFuture,
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
    UnauthorizedSessionControl,
    UnauthorizedClockControl,
    ClockRegression,
    EngineSequenceExhausted,
    ReportSequenceExhausted,
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
            Self::UnauthorizedSessionControl => {
                formatter.write_str("only control client 0 may change the session")
            }
            Self::UnauthorizedClockControl => {
                formatter.write_str("only control client 0 may advance time")
            }
            Self::ClockRegression => formatter.write_str("logical time cannot move backwards"),
            Self::EngineSequenceExhausted => {
                formatter.write_str("matcher sequence space is exhausted")
            }
            Self::ReportSequenceExhausted => {
                formatter.write_str("execution report sequence space is exhausted")
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
    time_in_force: TimeInForce,
    post_only: bool,
}

#[derive(Debug, Clone)]
struct CachedRequest {
    command: GatewayCommand,
    reports: Vec<ExecutionReport>,
}

#[derive(Debug)]
pub struct SimulatedExchange {
    config: GatewayConfig,
    session: SessionState,
    expiry: ExpirySchedule,
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
            session: SessionState::Open,
            expiry: ExpirySchedule::default(),
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
                time_in_force,
                post_only,
            } => self.submit_new(
                request.client_id,
                request.request_id,
                client_order_id,
                symbol,
                side,
                price_ticks,
                quantity_lots,
                OrderOptions {
                    time_in_force,
                    post_only,
                },
            )?,
            GatewayCommand::Cancel { client_order_id } => {
                self.cancel(request.client_id, request.request_id, client_order_id)
            }
            GatewayCommand::CancelAll => self.cancel_all(request.client_id, request.request_id),
            GatewayCommand::SetSession { state } => {
                if request.client_id != 0 {
                    return Err(GatewayError::UnauthorizedSessionControl);
                }
                self.session = state;
                Vec::new()
            }
            GatewayCommand::SetExpiry {
                client_order_id,
                deadline,
            } => self.set_expiry(
                request.client_id,
                request.request_id,
                client_order_id,
                deadline,
            ),
            GatewayCommand::Replace {
                client_order_id,
                replacement_client_order_id,
                price_ticks,
                quantity_lots,
                time_in_force,
                post_only,
            } => self.replace(
                request.client_id,
                request.request_id,
                client_order_id,
                replacement_client_order_id,
                price_ticks,
                quantity_lots,
                OrderOptions {
                    time_in_force,
                    post_only,
                },
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

    pub fn session_state(&self) -> SessionState {
        self.session
    }

    pub fn logical_time(&self) -> u64 {
        self.expiry.now()
    }

    pub fn expiry_for_client(&self, client_id: u64, client_order_id: u64) -> Option<u64> {
        let key = ClientOrderKey {
            client_id,
            client_order_id,
        };
        self.orders
            .get(&key)
            .and_then(|order| self.expiry.deadline(order.exchange_order_id))
    }

    fn insert_live_order(&mut self, exchange_id: u64, key: ClientOrderKey) {
        self.live_orders.insert(exchange_id, key);
    }

    fn remove_live_order(&mut self, exchange_id: u64) {
        self.live_orders.remove(&exchange_id);
        self.expiry.remove(exchange_id);
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
                leaves_quantity_lots: if matches!(
                    record.status,
                    OrderStatus::Filled
                        | OrderStatus::Cancelled
                        | OrderStatus::Expired
                        | OrderStatus::Rejected
                ) {
                    0
                } else {
                    record.order_quantity_lots - record.cumulative_quantity_lots
                },
                status: record.status,
                time_in_force: record.time_in_force,
                post_only: record.post_only,
            })
            .collect();
        states.sort_by_key(|state| (state.client_id, state.client_order_id));
        states
    }

    /// Look up an order only within the requesting client's namespace.
    pub fn order_state_for_client(
        &self,
        client_id: u64,
        client_order_id: u64,
    ) -> Option<OrderStateSnapshot> {
        let key = ClientOrderKey {
            client_id,
            client_order_id,
        };
        let record = self.orders.get(&key)?;
        Some(OrderStateSnapshot {
            client_id,
            client_order_id,
            exchange_order_id: record.exchange_order_id,
            symbol: record.symbol.clone(),
            priority_sequence: record.priority_sequence,
            side: record.side,
            price_ticks: record.price_ticks,
            order_quantity_lots: record.order_quantity_lots,
            cumulative_quantity_lots: record.cumulative_quantity_lots,
            leaves_quantity_lots: if matches!(
                record.status,
                OrderStatus::Accepted | OrderStatus::PartiallyFilled
            ) {
                record.order_quantity_lots - record.cumulative_quantity_lots
            } else {
                0
            },
            status: record.status,
            time_in_force: record.time_in_force,
            post_only: record.post_only,
        })
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
                    OrderStatus::Filled
                        | OrderStatus::Cancelled
                        | OrderStatus::Expired
                        | OrderStatus::Rejected
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
            add(
                &mut hash,
                &[
                    time_in_force_code(record.time_in_force),
                    u8::from(record.post_only),
                ],
            );
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
        if self.session == SessionState::Closed {
            add(&mut hash, b"session:closed");
        }
        if self.expiry.now() != 0 || self.expiry.pending().next().is_some() {
            add(&mut hash, b"expiry:v1");
            add(&mut hash, &self.expiry.now().to_le_bytes());
            for (deadline, order_id) in self.expiry.pending() {
                add(&mut hash, &deadline.to_le_bytes());
                add(&mut hash, &order_id.to_le_bytes());
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
        options: OrderOptions,
    ) -> Result<Vec<ExecutionReport>, GatewayError> {
        if !self.session.allows_new_orders() {
            return Ok(vec![self.reject(
                client_id,
                request_id,
                client_order_id,
                None,
                Some(side),
                Some(price_ticks),
                quantity_lots,
                GatewayRejectReason::SessionClosed,
            )]);
        }
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
        let candidate_id = self
            .next_exchange_order_id
            .checked_add(1)
            .ok_or(GatewayError::ExchangeOrderIdExhausted)?;
        let request = OrderRequest {
            order_id: candidate_id,
            side,
            price_ticks,
            quantity_lots,
        };
        // Policy rejections must not consume an exchange ID or engine sequence.
        let request = OrderRequest {
            order_id: candidate_id,
            ..request
        };
        let preflight = self
            .engines
            .entry(symbol.clone())
            .or_default()
            .preflight_for_client(request, options, Some(client_id));
        if let Err(reason) = preflight {
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
        let exchange_order_id = self.allocate_exchange_order_id()?;
        let result = self
            .engines
            .get_mut(&symbol)
            .expect("engine inserted above")
            .submit_with_options_for_client(
                OrderRequest {
                    order_id: exchange_order_id,
                    ..request
                },
                options,
                Some(client_id),
            );
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
                time_in_force: options.time_in_force,
                post_only: options.post_only,
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
            options.time_in_force,
            result.self_trade_prevented,
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
        self.remove_live_order(record.exchange_order_id);
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
            0,
            0,
            None,
            None,
        )]
    }

    fn preflight_expiry_advance(&self, timestamp: u64) -> Result<(), GatewayError> {
        if timestamp < self.expiry.now() {
            return Err(GatewayError::ClockRegression);
        }
        let mut counts: HashMap<&str, u64> = HashMap::new();
        let mut report_count = 0_u128;
        for (_, order_id) in self.expiry.due_at(timestamp) {
            let key = self
                .live_orders
                .get(&order_id)
                .expect("scheduled order is live");
            let order = &self.orders[key];
            let count = counts.entry(&order.symbol).or_default();
            *count = count.saturating_add(1);
            report_count += 1;
        }
        if self
            .next_report_sequence
            .checked_add(report_count)
            .is_none()
        {
            return Err(GatewayError::ReportSequenceExhausted);
        }
        if counts
            .into_iter()
            .any(|(symbol, count)| self.engines[symbol].remaining_sequences() < count)
        {
            return Err(GatewayError::EngineSequenceExhausted);
        }
        Ok(())
    }

    #[allow(dead_code)]
    fn advance_time(&mut self, timestamp: u64) -> Result<Vec<ExecutionReport>, GatewayError> {
        self.preflight_expiry_advance(timestamp)?;
        let due = self
            .expiry
            .advance(timestamp)
            .expect("clock was preflighted");
        let mut reports = Vec::with_capacity(due.len());
        for order_id in due {
            let key = self.live_orders[&order_id];
            let mut report = self
                .cancel(key.client_id, 0, key.client_order_id)
                .pop()
                .expect("one cancel report");
            self.orders.get_mut(&key).expect("order is live").status = OrderStatus::Expired;
            report.status = OrderStatus::Expired;
            reports.push(report);
        }
        Ok(reports)
    }

    fn set_expiry(
        &mut self,
        client_id: u64,
        request_id: u64,
        client_order_id: u64,
        deadline: Option<u64>,
    ) -> Vec<ExecutionReport> {
        let key = ClientOrderKey {
            client_id,
            client_order_id,
        };
        let order = self.orders.get(&key).cloned();
        let reason = match &order {
            None => Some(GatewayRejectReason::UnknownClientOrderId),
            Some(order) if !self.live_orders.contains_key(&order.exchange_order_id) => {
                Some(GatewayRejectReason::OrderNotLive)
            }
            Some(_) if deadline.is_some_and(|value| value <= self.expiry.now()) => {
                Some(GatewayRejectReason::ExpiryNotFuture)
            }
            Some(_) => None,
        };
        if let Some(reason) = reason {
            return vec![self.reject(
                client_id,
                request_id,
                client_order_id,
                order.as_ref().map(|order| order.exchange_order_id),
                order.as_ref().map(|order| order.side),
                order.as_ref().map(|order| order.price_ticks),
                order.as_ref().map_or(0, |order| order.order_quantity_lots),
                reason,
            )];
        }
        let order_id = order.expect("valid order").exchange_order_id;
        self.expiry
            .set(order_id, deadline)
            .expect("deadline validated");
        Vec::new()
    }

    fn cancel_all(&mut self, client_id: u64, request_id: u64) -> Vec<ExecutionReport> {
        let mut orders: Vec<_> = self
            .orders
            .iter()
            .filter(|(key, record)| {
                key.client_id == client_id
                    && self.live_orders.contains_key(&record.exchange_order_id)
            })
            .map(|(key, record)| (record.exchange_order_id, key.client_order_id))
            .collect();
        orders.sort_unstable();
        orders
            .into_iter()
            .flat_map(|(_, order_id)| self.cancel(client_id, request_id, order_id))
            .collect()
    }

    #[allow(clippy::too_many_arguments)]
    fn replace(
        &mut self,
        client_id: u64,
        request_id: u64,
        client_order_id: u64,
        replacement_client_order_id: u64,
        price_ticks: i64,
        quantity_lots: u64,
        options: OrderOptions,
    ) -> Result<Vec<ExecutionReport>, GatewayError> {
        if !self.session.allows_new_orders() {
            return Ok(vec![self.reject(
                client_id,
                request_id,
                replacement_client_order_id,
                None,
                None,
                Some(price_ticks),
                quantity_lots,
                GatewayRejectReason::SessionClosed,
            )]);
        }
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
        let candidate_id = self
            .next_exchange_order_id
            .checked_add(1)
            .ok_or(GatewayError::ExchangeOrderIdExhausted)?;
        let replacement = OrderRequest {
            order_id: candidate_id,
            side: old_record.side,
            price_ticks,
            quantity_lots,
        };
        let preflight = self.engines[&old_record.symbol].preflight_replace_for_client(
            old_record.exchange_order_id,
            replacement,
            options,
            Some(client_id),
        );
        if let Err(reason) = preflight {
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
        let exchange_order_id = self.allocate_exchange_order_id()?;
        let replaced = self
            .engines
            .get_mut(&old_record.symbol)
            .expect("order symbol has a matching engine")
            .replace_with_options_for_client(
                old_record.exchange_order_id,
                OrderRequest {
                    order_id: exchange_order_id,
                    side: old_record.side,
                    price_ticks,
                    quantity_lots,
                },
                options,
                Some(client_id),
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
        self.remove_live_order(old_record.exchange_order_id);
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
            0,
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
                time_in_force: options.time_in_force,
                post_only: options.post_only,
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
            options.time_in_force,
            replaced.replacement.self_trade_prevented,
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
        time_in_force: TimeInForce,
        self_trade_prevented: bool,
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
                self.remove_live_order(trade.maker_order_id);
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
                taker_quantity,
                0,
                None,
                None,
            ));
        }
        if resting_quantity > 0 {
            self.insert_live_order(taker_exchange_id, taker_key);
        }
        if self_trade_prevented || time_in_force == TimeInForce::Ioc {
            let mut taker = self.orders[&taker_key].clone();
            if taker.cumulative_quantity_lots < taker.order_quantity_lots {
                taker.status = OrderStatus::Cancelled;
                self.orders.insert(taker_key, taker.clone());
                reports.push(self.report(
                    client_id,
                    request_id,
                    taker_key.client_order_id,
                    Some(taker_exchange_id),
                    engine_sequence,
                    OrderStatus::Cancelled,
                    Some(taker.side),
                    Some(taker.price_ticks),
                    taker.order_quantity_lots,
                    taker.cumulative_quantity_lots,
                    0,
                    0,
                    None,
                    None,
                ));
            }
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
            session: exchange.session_state(),
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
    #[serde(default)]
    pub session: SessionState,
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
        OrderStatus::Expired => 5,
    }
}

fn time_in_force_code(time_in_force: TimeInForce) -> u8 {
    match time_in_force {
        TimeInForce::Gtc => 0,
        TimeInForce::Ioc => 1,
        TimeInForce::Fok => 2,
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
            time_in_force: TimeInForce::Gtc,
            post_only: false,
        }
    }

    fn new_order_with_options(
        id: u64,
        side: Side,
        price: i64,
        quantity: u64,
        time_in_force: TimeInForce,
        post_only: bool,
    ) -> GatewayCommand {
        GatewayCommand::New {
            client_order_id: id,
            symbol: "DEFAULT".into(),
            side,
            price_ticks: price,
            quantity_lots: quantity,
            time_in_force,
            post_only,
        }
    }

    #[test]
    fn ioc_reports_fills_then_cancels_unfilled_quantity_without_resting() {
        let mut exchange = SimulatedExchange::default();
        exchange
            .handle(request(1, 1, new_order(1, Side::Sell, 100, 3)))
            .unwrap();
        let response = exchange
            .handle(request(
                2,
                1,
                new_order_with_options(2, Side::Buy, 100, 5, TimeInForce::Ioc, false),
            ))
            .unwrap();

        assert_eq!(response.reports.len(), 3);
        assert_eq!(response.reports[1].status, OrderStatus::PartiallyFilled);
        assert_eq!(response.reports[1].cumulative_quantity_lots, 3);
        assert_eq!(response.reports[1].leaves_quantity_lots, 2);
        assert_eq!(response.reports[2].status, OrderStatus::Cancelled);
        assert_eq!(response.reports[2].cumulative_quantity_lots, 3);
        assert_eq!(response.reports[2].leaves_quantity_lots, 0);
        let taker = exchange
            .order_states()
            .into_iter()
            .find(|order| order.client_id == 2)
            .unwrap();
        assert_eq!(taker.status, OrderStatus::Cancelled);
        assert_eq!(taker.leaves_quantity_lots, 0);
        assert_eq!(exchange.live_order_count(), 0);
        exchange.check_invariants().unwrap();
    }

    #[test]
    fn policy_rejections_preserve_order_and_exchange_id_sequences() {
        let mut exchange = SimulatedExchange::default();
        exchange
            .handle(request(1, 1, new_order(1, Side::Sell, 100, 2)))
            .unwrap();
        let before = exchange.book_orders();
        let fok = exchange
            .handle(request(
                2,
                1,
                new_order_with_options(2, Side::Buy, 100, 3, TimeInForce::Fok, false),
            ))
            .unwrap();
        assert_eq!(
            fok.reports[0].reject_reason,
            Some(GatewayRejectReason::Engine(
                RejectReason::FillOrKillUnavailable
            ))
        );
        let post_only = exchange
            .handle(request(
                3,
                1,
                new_order_with_options(3, Side::Buy, 100, 1, TimeInForce::Gtc, true),
            ))
            .unwrap();
        assert_eq!(
            post_only.reports[0].reject_reason,
            Some(GatewayRejectReason::Engine(RejectReason::PostOnlyWouldTake))
        );
        assert_eq!(exchange.book_orders(), before);
        let accepted = exchange
            .handle(request(4, 1, new_order(4, Side::Buy, 99, 1)))
            .unwrap();
        assert_eq!(accepted.reports[0].exchange_order_id, Some(2));
        exchange.check_invariants().unwrap();
    }

    #[test]
    fn rejected_fok_replacement_keeps_old_order_live() {
        let mut exchange = SimulatedExchange::default();
        exchange
            .handle(request(1, 1, new_order(1, Side::Buy, 90, 2)))
            .unwrap();
        exchange
            .handle(request(2, 1, new_order(2, Side::Sell, 100, 1)))
            .unwrap();
        let response = exchange
            .handle(request(
                1,
                2,
                GatewayCommand::Replace {
                    client_order_id: 1,
                    replacement_client_order_id: 3,
                    price_ticks: 100,
                    quantity_lots: 2,
                    time_in_force: TimeInForce::Fok,
                    post_only: false,
                },
            ))
            .unwrap();
        assert_eq!(response.reports.len(), 1);
        assert_eq!(
            response.reports[0].reject_reason,
            Some(GatewayRejectReason::Engine(
                RejectReason::FillOrKillUnavailable
            ))
        );
        assert_eq!(exchange.best_bid(), Some((90, 2)));
        assert_eq!(exchange.live_order_count(), 2);
        exchange.check_invariants().unwrap();
    }

    #[test]
    fn self_trade_cancels_incoming_and_preserves_resting_order() {
        let mut exchange = SimulatedExchange::default();
        exchange
            .handle(request(7, 1, new_order(1, Side::Sell, 100, 4)))
            .unwrap();
        let own_order = exchange.book_orders();
        let response = exchange
            .handle(request(7, 2, new_order(2, Side::Buy, 100, 3)))
            .unwrap();

        assert_eq!(response.reports.len(), 2);
        assert_eq!(response.reports[0].status, OrderStatus::Accepted);
        assert_eq!(response.reports[1].status, OrderStatus::Cancelled);
        assert!(response
            .reports
            .iter()
            .all(|report| report.last_quantity_lots == 0));
        assert_eq!(exchange.book_orders(), own_order);
        assert_eq!(exchange.live_order_count(), 1);
        exchange.check_invariants().unwrap();
    }

    #[test]
    fn self_trade_stops_fifo_walk_after_external_fill() {
        let mut exchange = SimulatedExchange::default();
        exchange
            .handle(request(7, 1, new_order(1, Side::Sell, 101, 4)))
            .unwrap();
        exchange
            .handle(request(8, 1, new_order(1, Side::Sell, 100, 2)))
            .unwrap();
        let own_order = exchange
            .book_orders()
            .into_iter()
            .find(|order| order.client_id == 7)
            .unwrap();
        let response = exchange
            .handle(request(7, 2, new_order(2, Side::Buy, 101, 5)))
            .unwrap();

        assert_eq!(response.reports.len(), 3);
        assert_eq!(response.reports[0].client_id, 8);
        assert_eq!(response.reports[0].status, OrderStatus::Filled);
        assert_eq!(response.reports[1].status, OrderStatus::PartiallyFilled);
        assert_eq!(response.reports[1].last_quantity_lots, 2);
        assert_eq!(response.reports[2].status, OrderStatus::Cancelled);
        assert_eq!(response.reports[2].leaves_quantity_lots, 0);
        let remaining_own_order = exchange
            .book_orders()
            .into_iter()
            .find(|order| order.client_id == 7)
            .unwrap();
        assert_eq!(remaining_own_order, own_order);
        assert_eq!(exchange.live_order_count(), 1);
        exchange.check_invariants().unwrap();
    }

    #[test]
    fn fok_does_not_count_external_liquidity_behind_a_self_order() {
        let mut exchange = SimulatedExchange::default();
        exchange
            .handle(request(7, 1, new_order(1, Side::Sell, 100, 2)))
            .unwrap();
        exchange
            .handle(request(8, 1, new_order(1, Side::Sell, 100, 2)))
            .unwrap();
        let before = exchange.book_orders();
        let response = exchange
            .handle(request(
                7,
                2,
                new_order_with_options(2, Side::Buy, 100, 1, TimeInForce::Fok, false),
            ))
            .unwrap();

        assert_eq!(
            response.reports[0].reject_reason,
            Some(GatewayRejectReason::Engine(
                RejectReason::FillOrKillUnavailable
            ))
        );
        assert_eq!(exchange.book_orders(), before);
        let accepted = exchange
            .handle(request(9, 1, new_order(1, Side::Buy, 90, 1)))
            .unwrap();
        assert_eq!(accepted.reports[0].exchange_order_id, Some(3));
        exchange.check_invariants().unwrap();
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
        assert_eq!(cancel.reports[0].leaves_quantity_lots, 0);
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
                    time_in_force: TimeInForce::Gtc,
                    post_only: false,
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
                    time_in_force: TimeInForce::Gtc,
                    post_only: false,
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
                        time_in_force: TimeInForce::Gtc,
                        post_only: false,
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
                match report.status {
                    OrderStatus::Accepted | OrderStatus::PartiallyFilled => assert_eq!(
                        report.cumulative_quantity_lots + report.leaves_quantity_lots,
                        report.order_quantity_lots
                    ),
                    OrderStatus::Filled => {
                        assert_eq!(report.cumulative_quantity_lots, report.order_quantity_lots);
                        assert_eq!(report.leaves_quantity_lots, 0);
                    }
                    OrderStatus::Cancelled | OrderStatus::Expired => {
                        assert!(report.cumulative_quantity_lots <= report.order_quantity_lots);
                        assert_eq!(report.leaves_quantity_lots, 0);
                    }
                    OrderStatus::Rejected => {}
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
    fn jsonl_requests_support_ioc_and_default_legacy_fields() {
        let input = concat!(
            "{\"client_id\":1,\"request_id\":1,\"command\":{\"type\":\"new\",\"client_order_id\":1,\"side\":\"sell\",\"price_ticks\":100,\"quantity_lots\":2}}\n",
            "{\"client_id\":2,\"request_id\":1,\"command\":{\"type\":\"new\",\"client_order_id\":2,\"side\":\"buy\",\"price_ticks\":100,\"quantity_lots\":3,\"time_in_force\":\"ioc\"}}\n",
            "{\"client_id\":3,\"request_id\":1,\"command\":{\"type\":\"new\",\"client_order_id\":3,\"side\":\"sell\",\"price_ticks\":110,\"quantity_lots\":1}}\n",
            "{\"client_id\":3,\"request_id\":2,\"command\":{\"type\":\"cancel_all\"}}\n"
        );
        let mut output = Vec::new();
        let summary = run_jsonl(input.as_bytes(), &mut output, GatewayConfig::default()).unwrap();
        let responses: Vec<GatewayResponse> = output
            .split(|byte| *byte == b'\n')
            .filter(|line| !line.is_empty())
            .map(|line| serde_json::from_slice(line).unwrap())
            .collect();

        assert_eq!(summary.requests, 4);
        assert_eq!(responses[0].reports[0].status, OrderStatus::Accepted);
        assert_eq!(responses[1].reports.len(), 3);
        assert_eq!(responses[1].reports[2].status, OrderStatus::Cancelled);
        assert_eq!(responses[1].reports[2].leaves_quantity_lots, 0);
        assert_eq!(responses[3].reports[0].status, OrderStatus::Cancelled);
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
                    time_in_force: TimeInForce::Gtc,
                    post_only: false,
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
    fn client_order_lookup_returns_owned_state_only() {
        let mut exchange = SimulatedExchange::default();
        exchange
            .handle(request(100, 1, new_order(55, Side::Sell, 100, 4)))
            .unwrap();

        let state = exchange.order_state_for_client(100, 55).unwrap();
        assert_eq!(state.status, OrderStatus::Accepted);
        assert_eq!(state.leaves_quantity_lots, 4);
        assert_eq!(exchange.order_state_for_client(200, 55), None);
        assert_eq!(exchange.order_state_for_client(100, 56), None);
    }

    #[test]
    fn closed_session_changes_the_exchange_fingerprint() {
        let mut exchange = SimulatedExchange::default();
        let open_fingerprint = exchange.state_fingerprint();
        assert_eq!(exchange.session_state(), SessionState::Open);
        exchange.session = SessionState::Closed;
        assert_ne!(exchange.state_fingerprint(), open_fingerprint);
        exchange.session = SessionState::Open;
        assert_eq!(exchange.state_fingerprint(), open_fingerprint);
    }

    #[test]
    fn older_snapshots_default_to_open_session() {
        let snapshot: ExchangeStateSnapshot =
            serde_json::from_str(r#"{"state_fingerprint":0,"orders":[]}"#).unwrap();
        assert_eq!(snapshot.session, SessionState::Open);
    }

    #[test]
    fn session_control_requires_control_client_and_retries_do_not_reapply() {
        let mut exchange = SimulatedExchange::default();
        let close = GatewayCommand::SetSession {
            state: SessionState::Closed,
        };
        assert_eq!(
            exchange.handle(request(1, 1, close.clone())),
            Err(GatewayError::UnauthorizedSessionControl)
        );
        assert_eq!(exchange.session_state(), SessionState::Open);
        let first = exchange.handle(request(0, 1, close.clone())).unwrap();
        assert!(first.reports.is_empty());
        assert_eq!(exchange.session_state(), SessionState::Closed);
        exchange
            .handle(request(
                0,
                2,
                GatewayCommand::SetSession {
                    state: SessionState::Open,
                },
            ))
            .unwrap();
        assert!(exchange.handle(request(0, 1, close)).unwrap().replayed);
        assert_eq!(exchange.session_state(), SessionState::Open);
    }

    #[test]
    fn closed_session_rejects_new_order_without_reserving_its_id() {
        let mut exchange = SimulatedExchange::default();
        exchange
            .handle(request(
                0,
                1,
                GatewayCommand::SetSession {
                    state: SessionState::Closed,
                },
            ))
            .unwrap();
        let rejected = exchange
            .handle(request(7, 1, new_order(44, Side::Buy, 100, 2)))
            .unwrap();
        assert_eq!(
            rejected.reports[0].reject_reason,
            Some(GatewayRejectReason::SessionClosed)
        );
        assert!(exchange.order_states().is_empty());
        exchange
            .handle(request(
                0,
                2,
                GatewayCommand::SetSession {
                    state: SessionState::Open,
                },
            ))
            .unwrap();
        let accepted = exchange
            .handle(request(7, 2, new_order(44, Side::Buy, 100, 2)))
            .unwrap();
        assert_eq!(accepted.reports[0].status, OrderStatus::Accepted);
        assert_eq!(accepted.reports[0].exchange_order_id, Some(1));
        assert_eq!(accepted.reports[0].engine_sequence, Some(1));
    }

    #[test]
    fn closed_replacement_preserves_order_and_replacement_id() {
        let mut exchange = SimulatedExchange::default();
        exchange
            .handle(request(7, 1, new_order(1, Side::Buy, 100, 2)))
            .unwrap();
        let before = exchange.book_orders();
        exchange
            .handle(request(
                0,
                1,
                GatewayCommand::SetSession {
                    state: SessionState::Closed,
                },
            ))
            .unwrap();
        let replace = GatewayCommand::Replace {
            client_order_id: 1,
            replacement_client_order_id: 2,
            price_ticks: 101,
            quantity_lots: 2,
            time_in_force: TimeInForce::Gtc,
            post_only: false,
        };
        let rejected = exchange.handle(request(7, 2, replace.clone())).unwrap();
        assert_eq!(
            rejected.reports[0].reject_reason,
            Some(GatewayRejectReason::SessionClosed)
        );
        assert_eq!(exchange.book_orders(), before);
        exchange
            .handle(request(
                0,
                2,
                GatewayCommand::SetSession {
                    state: SessionState::Open,
                },
            ))
            .unwrap();
        let accepted = exchange.handle(request(7, 3, replace)).unwrap();
        assert_eq!(accepted.reports[0].status, OrderStatus::Cancelled);
        assert_eq!(accepted.reports[1].exchange_order_id, Some(2));
        assert_eq!(accepted.reports[1].status, OrderStatus::Accepted);
    }

    #[test]
    fn closed_session_allows_cancels_and_preserves_fifo_on_reopen() {
        let mut exchange = SimulatedExchange::default();
        for client in 1..=3 {
            exchange
                .handle(request(client, 1, new_order(1, Side::Buy, 100, 1)))
                .unwrap();
        }
        exchange
            .handle(request(
                0,
                1,
                GatewayCommand::SetSession {
                    state: SessionState::Closed,
                },
            ))
            .unwrap();
        let cancelled = exchange
            .handle(request(3, 2, GatewayCommand::CancelAll))
            .unwrap();
        assert_eq!(cancelled.reports[0].status, OrderStatus::Cancelled);
        exchange
            .handle(request(
                0,
                2,
                GatewayCommand::SetSession {
                    state: SessionState::Open,
                },
            ))
            .unwrap();
        let fill = exchange
            .handle(request(4, 1, new_order(1, Side::Sell, 100, 2)))
            .unwrap();
        assert_eq!(fill.reports[0].client_id, 1);
        assert_eq!(fill.reports[2].client_id, 2);
        assert_eq!(exchange.live_order_count(), 0);
    }

    #[test]
    fn expiry_deadlines_require_owner_and_future_time() {
        let mut exchange = SimulatedExchange::default();
        exchange
            .handle(request(7, 1, new_order(1, Side::Buy, 100, 2)))
            .unwrap();
        let deadline = GatewayCommand::SetExpiry {
            client_order_id: 1,
            deadline: Some(10),
        };
        assert!(exchange
            .handle(request(7, 2, deadline.clone()))
            .unwrap()
            .reports
            .is_empty());
        let foreign = exchange.handle(request(8, 1, deadline)).unwrap();
        assert_eq!(
            foreign.reports[0].reject_reason,
            Some(GatewayRejectReason::UnknownClientOrderId)
        );
        let past = exchange
            .handle(request(
                7,
                3,
                GatewayCommand::SetExpiry {
                    client_order_id: 1,
                    deadline: Some(0),
                },
            ))
            .unwrap();
        assert_eq!(
            past.reports[0].reject_reason,
            Some(GatewayRejectReason::ExpiryNotFuture)
        );
        assert_eq!(exchange.expiry_for_client(7, 1), Some(10));
        assert_eq!(
            exchange.order_state_for_client(7, 1).unwrap().status,
            OrderStatus::Accepted
        );
    }

    #[test]
    fn filling_an_order_removes_its_expiry() {
        let mut exchange = SimulatedExchange::default();
        exchange
            .handle(request(1, 1, new_order(1, Side::Sell, 100, 1)))
            .unwrap();
        exchange
            .handle(request(
                1,
                2,
                GatewayCommand::SetExpiry {
                    client_order_id: 1,
                    deadline: Some(10),
                },
            ))
            .unwrap();
        exchange
            .handle(request(2, 1, new_order(1, Side::Buy, 100, 1)))
            .unwrap();
        assert_eq!(exchange.expiry_for_client(1, 1), None);
        assert_eq!(
            exchange.order_state_for_client(1, 1).unwrap().status,
            OrderStatus::Filled
        );
    }

    #[test]
    fn replacement_does_not_inherit_old_deadline() {
        let mut exchange = SimulatedExchange::default();
        exchange
            .handle(request(1, 1, new_order(1, Side::Buy, 100, 2)))
            .unwrap();
        exchange
            .handle(request(
                1,
                2,
                GatewayCommand::SetExpiry {
                    client_order_id: 1,
                    deadline: Some(10),
                },
            ))
            .unwrap();
        exchange
            .handle(request(
                1,
                3,
                GatewayCommand::Replace {
                    client_order_id: 1,
                    replacement_client_order_id: 2,
                    price_ticks: 101,
                    quantity_lots: 2,
                    time_in_force: TimeInForce::Gtc,
                    post_only: false,
                },
            ))
            .unwrap();
        assert_eq!(exchange.expiry_for_client(1, 1), None);
        assert_eq!(exchange.expiry_for_client(1, 2), None);
        assert_eq!(
            exchange.order_state_for_client(1, 2).unwrap().status,
            OrderStatus::Accepted
        );
    }

    #[test]
    fn retrying_old_expiry_does_not_restore_cleared_deadline() {
        let mut exchange = SimulatedExchange::default();
        exchange
            .handle(request(1, 1, new_order(1, Side::Buy, 100, 2)))
            .unwrap();
        let schedule = GatewayCommand::SetExpiry {
            client_order_id: 1,
            deadline: Some(10),
        };
        exchange.handle(request(1, 2, schedule.clone())).unwrap();
        exchange
            .handle(request(
                1,
                3,
                GatewayCommand::SetExpiry {
                    client_order_id: 1,
                    deadline: None,
                },
            ))
            .unwrap();
        assert!(exchange.handle(request(1, 2, schedule)).unwrap().replayed);
        assert_eq!(exchange.expiry_for_client(1, 1), None);
        exchange
            .handle(request(
                1,
                4,
                GatewayCommand::SetExpiry {
                    client_order_id: 1,
                    deadline: Some(20),
                },
            ))
            .unwrap();
        exchange
            .handle(request(1, 5, GatewayCommand::Cancel { client_order_id: 1 }))
            .unwrap();
        assert_eq!(exchange.expiry_for_client(1, 1), None);
    }

    #[test]
    fn cancel_all_is_client_scoped_ordered_and_idempotent() {
        let mut exchange = SimulatedExchange::default();
        exchange
            .handle(request(1, 1, new_order(11, Side::Sell, 101, 1)))
            .unwrap();
        exchange
            .handle(request(2, 1, new_order(21, Side::Sell, 102, 1)))
            .unwrap();
        exchange
            .handle(request(1, 2, new_order(12, Side::Sell, 103, 1)))
            .unwrap();

        let command = GatewayCommand::CancelAll;
        let first = exchange.handle(request(1, 3, command.clone())).unwrap();
        let retry = exchange.handle(request(1, 3, command)).unwrap();
        assert_eq!(
            first
                .reports
                .iter()
                .map(|report| report.client_order_id)
                .collect::<Vec<_>>(),
            vec![11, 12]
        );
        assert!(first
            .reports
            .iter()
            .all(|report| report.status == OrderStatus::Cancelled));
        assert!(retry.replayed);
        assert_eq!(retry.reports, first.reports);
        assert_eq!(exchange.live_order_count(), 1);
        assert_eq!(
            exchange.order_state_for_client(2, 21).unwrap().status,
            OrderStatus::Accepted
        );
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
