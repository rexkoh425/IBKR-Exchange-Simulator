//! Test-only executable specification with intentionally simple data structures.
//!
//! The oracle stores resting orders in a flat vector and scans it to find each
//! eligible maker. It does not share the production matcher's price maps,
//! linked FIFO queues, or live-order index.

use crate::gateway::{
    ExecutionReport, GatewayCommand, GatewayConfig, GatewayError, GatewayRejectReason,
    GatewayRequest, GatewayResponse, OrderStateSnapshot, OrderStatus,
};
use crate::{
    OrderOptions, OrderRequest, RejectReason, ReplaceResult, RestingOrder, Sequence, Side,
    SubmitResult, TimeInForce, Trade,
};
use proptest::prelude::*;
use std::collections::{HashMap, HashSet};
use std::fs::{self, File};
use std::io::Write;
use std::path::PathBuf;

#[derive(Debug, Default, Clone)]
struct ReferenceMatcher {
    resting: Vec<RestingOrder>,
    used_order_ids: HashSet<u64>,
    owners: HashMap<u64, u64>,
    next_sequence: Sequence,
}

impl ReferenceMatcher {
    fn preflight_order(
        &self,
        request: OrderRequest,
        options: OrderOptions,
        owner_id: Option<u64>,
    ) -> Result<(), RejectReason> {
        if request.price_ticks <= 0 {
            return Err(RejectReason::InvalidPrice);
        }
        if request.quantity_lots == 0 {
            return Err(RejectReason::InvalidQuantity);
        }
        if self.used_order_ids.contains(&request.order_id) {
            return Err(RejectReason::DuplicateOrderId);
        }
        if self.next_sequence == Sequence::MAX {
            return Err(RejectReason::SequenceExhausted);
        }
        if options.post_only && options.time_in_force != TimeInForce::Gtc {
            return Err(RejectReason::InvalidOrderOptions);
        }
        let eligible = |order: &&RestingOrder| match request.side {
            Side::Buy => order.side == Side::Sell && order.price_ticks <= request.price_ticks,
            Side::Sell => order.side == Side::Buy && order.price_ticks >= request.price_ticks,
        };
        if options.post_only && self.resting.iter().find(eligible).is_some() {
            return Err(RejectReason::PostOnlyWouldTake);
        }
        if options.time_in_force == TimeInForce::Fok {
            let mut eligible_orders: Vec<_> = self.resting.iter().filter(eligible).collect();
            eligible_orders.sort_by(|left, right| match request.side {
                Side::Buy => left
                    .price_ticks
                    .cmp(&right.price_ticks)
                    .then(left.sequence.cmp(&right.sequence)),
                Side::Sell => right
                    .price_ticks
                    .cmp(&left.price_ticks)
                    .then(left.sequence.cmp(&right.sequence)),
            });
            let mut available = 0_u128;
            for order in eligible_orders {
                if owner_id.is_some() && self.owners.get(&order.order_id).copied() == owner_id {
                    break;
                }
                available = available.saturating_add(u128::from(order.remaining_lots));
            }
            if available < u128::from(request.quantity_lots) {
                return Err(RejectReason::FillOrKillUnavailable);
            }
        }
        Ok(())
    }

    fn preflight_replace(
        &self,
        old_id: u64,
        replacement: OrderRequest,
        options: OrderOptions,
        owner_id: Option<u64>,
    ) -> Result<(), RejectReason> {
        if !self.resting.iter().any(|order| order.order_id == old_id) {
            return Err(RejectReason::UnknownOrderId);
        }
        if self.next_sequence >= Sequence::MAX - 1 {
            return Err(RejectReason::SequenceExhausted);
        }
        self.preflight_order(replacement, options, owner_id)
    }

    fn submit_with_options_avoiding(
        &mut self,
        request: OrderRequest,
        options: OrderOptions,
        owner_id: Option<u64>,
    ) -> SubmitResult {
        if let Err(reason) = self.preflight_order(request, options, owner_id) {
            return rejected(reason);
        }
        let sequence = self.next_sequence + 1;
        self.next_sequence = sequence;
        self.used_order_ids.insert(request.order_id);

        let mut remaining = request.quantity_lots;
        let mut trades = Vec::new();
        let mut self_trade_prevented = false;
        while remaining > 0 {
            let maker_index = self
                .resting
                .iter()
                .enumerate()
                .filter(|(_, order)| match request.side {
                    Side::Buy => {
                        order.side == Side::Sell && order.price_ticks <= request.price_ticks
                    }
                    Side::Sell => {
                        order.side == Side::Buy && order.price_ticks >= request.price_ticks
                    }
                })
                .min_by(|(_, left), (_, right)| match request.side {
                    Side::Buy => left
                        .price_ticks
                        .cmp(&right.price_ticks)
                        .then(left.sequence.cmp(&right.sequence)),
                    Side::Sell => right
                        .price_ticks
                        .cmp(&left.price_ticks)
                        .then(left.sequence.cmp(&right.sequence)),
                })
                .map(|(index, _)| index);
            let Some(index) = maker_index else { break };
            let maker = self.resting[index];
            if owner_id.is_some() && self.owners.get(&maker.order_id).copied() == owner_id {
                self_trade_prevented = true;
                break;
            }
            let quantity = remaining.min(maker.remaining_lots);
            remaining -= quantity;
            trades.push(Trade {
                sequence,
                maker_order_id: maker.order_id,
                taker_order_id: request.order_id,
                taker_side: request.side,
                price_ticks: maker.price_ticks,
                quantity_lots: quantity,
            });
            if quantity == maker.remaining_lots {
                self.resting.remove(index);
                self.owners.remove(&maker.order_id);
            } else {
                self.resting[index].remaining_lots -= quantity;
            }
        }
        if remaining > 0 && options.time_in_force == TimeInForce::Gtc && !self_trade_prevented {
            self.resting.push(RestingOrder {
                order_id: request.order_id,
                side: request.side,
                price_ticks: request.price_ticks,
                remaining_lots: remaining,
                sequence,
            });
            if let Some(owner_id) = owner_id {
                self.owners.insert(request.order_id, owner_id);
            }
        }
        SubmitResult {
            sequence: Some(sequence),
            trades,
            resting_quantity_lots: if options.time_in_force == TimeInForce::Gtc
                && !self_trade_prevented
            {
                remaining
            } else {
                0
            },
            self_trade_prevented,
            rejection: None,
        }
    }

    fn cancel(&mut self, order_id: u64) -> Result<RestingOrder, RejectReason> {
        let Some(index) = self
            .resting
            .iter()
            .position(|order| order.order_id == order_id)
        else {
            return Err(RejectReason::UnknownOrderId);
        };
        let Some(sequence) = self.next_sequence.checked_add(1) else {
            return Err(RejectReason::SequenceExhausted);
        };
        self.next_sequence = sequence;
        let order = self.resting.remove(index);
        self.owners.remove(&order_id);
        Ok(order)
    }

    fn replace_with_options(
        &mut self,
        old_id: u64,
        replacement: OrderRequest,
        options: OrderOptions,
        owner_id: Option<u64>,
    ) -> ReplaceResult {
        if let Err(reason) = self.preflight_replace(old_id, replacement, options, owner_id) {
            return ReplaceResult {
                cancelled_sequence: None,
                replacement: rejected(reason.clone()),
                rejection: Some(reason),
            };
        }
        self.cancel(old_id)
            .expect("replacement prevalidated old order");
        let cancelled_sequence = Some(self.next_sequence);
        let result = self.submit_with_options_avoiding(replacement, options, owner_id);
        ReplaceResult {
            cancelled_sequence,
            replacement: result,
            rejection: None,
        }
    }

    fn best_bid(&self) -> Option<(i64, u128)> {
        let best = self
            .resting
            .iter()
            .filter(|order| order.side == Side::Buy)
            .map(|order| order.price_ticks)
            .max()?;
        let quantity = self
            .resting
            .iter()
            .filter(|order| order.side == Side::Buy && order.price_ticks == best)
            .map(|order| u128::from(order.remaining_lots))
            .sum();
        Some((best, quantity))
    }

    fn best_ask(&self) -> Option<(i64, u128)> {
        let best = self
            .resting
            .iter()
            .filter(|order| order.side == Side::Sell)
            .map(|order| order.price_ticks)
            .min()?;
        let quantity = self
            .resting
            .iter()
            .filter(|order| order.side == Side::Sell && order.price_ticks == best)
            .map(|order| u128::from(order.remaining_lots))
            .sum();
        Some((best, quantity))
    }

    fn state_fingerprint(&self) -> u64 {
        fn add(hash: &mut u64, bytes: &[u8]) {
            for byte in bytes {
                *hash = (*hash ^ u64::from(*byte)).wrapping_mul(0x100000001b3);
            }
        }
        let mut hash = 0xcbf29ce484222325_u64;
        add(&mut hash, &self.next_sequence.to_le_bytes());
        for side in [Side::Buy, Side::Sell] {
            add(&mut hash, &[if side == Side::Buy { 0 } else { 1 }]);
            let mut orders: Vec<_> = self
                .resting
                .iter()
                .filter(|order| order.side == side)
                .collect();
            orders.sort_by(|left, right| {
                left.price_ticks
                    .cmp(&right.price_ticks)
                    .then(left.sequence.cmp(&right.sequence))
            });
            let mut current_price = None;
            for order in orders {
                if current_price != Some(order.price_ticks) {
                    current_price = Some(order.price_ticks);
                    add(&mut hash, &order.price_ticks.to_le_bytes());
                }
                add(&mut hash, &order.order_id.to_le_bytes());
                add(&mut hash, &order.remaining_lots.to_le_bytes());
                add(&mut hash, &order.sequence.to_le_bytes());
            }
        }
        let mut used: Vec<_> = self.used_order_ids.iter().copied().collect();
        used.sort_unstable();
        for id in used {
            add(&mut hash, &id.to_le_bytes());
        }
        hash
    }
}

fn rejected(reason: RejectReason) -> SubmitResult {
    SubmitResult {
        sequence: None,
        trades: Vec::new(),
        resting_quantity_lots: 0,
        self_trade_prevented: false,
        rejection: Some(reason),
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
struct ReferenceKey {
    client_id: u64,
    client_order_id: u64,
}

#[derive(Debug, Clone)]
struct ReferenceOrder {
    exchange_order_id: u64,
    last_request_id: u64,
    symbol: String,
    priority_sequence: u64,
    side: Side,
    price_ticks: i64,
    quantity_lots: u64,
    cumulative_lots: u64,
    status: OrderStatus,
    time_in_force: TimeInForce,
    post_only: bool,
}

#[derive(Debug, Clone)]
struct ReferenceCache {
    command: GatewayCommand,
    reports: Vec<ExecutionReport>,
}

#[derive(Debug)]
struct ReferenceExchange {
    config: GatewayConfig,
    session: crate::session::SessionState,
    expiry_clock: u64,
    expiries: HashMap<u64, u64>,
    engines: HashMap<String, ReferenceMatcher>,
    next_exchange_id: u64,
    next_report_sequence: u128,
    orders: HashMap<ReferenceKey, ReferenceOrder>,
    used_client_ids: HashSet<ReferenceKey>,
    live: HashMap<u64, ReferenceKey>,
    cache: HashMap<(u64, u64), ReferenceCache>,
}

impl ReferenceExchange {
    fn new(config: GatewayConfig) -> Self {
        Self {
            config,
            session: crate::session::SessionState::Open,
            expiry_clock: 0,
            expiries: HashMap::new(),
            engines: HashMap::new(),
            next_exchange_id: 0,
            next_report_sequence: 0,
            orders: HashMap::new(),
            used_client_ids: HashSet::new(),
            live: HashMap::new(),
            cache: HashMap::new(),
        }
    }

    fn handle(&mut self, request: GatewayRequest) -> Result<GatewayResponse, GatewayError> {
        if request.request_id == 0 {
            return Err(GatewayError::InvalidRequestId);
        }
        let request_key = (request.client_id, request.request_id);
        if let Some(cached) = self.cache.get(&request_key) {
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
            ),
            GatewayCommand::Cancel { client_order_id } => {
                Ok(self.cancel(request.client_id, request.request_id, client_order_id))
            }
            GatewayCommand::CancelAll => Ok(self.cancel_all(request.client_id, request.request_id)),
            GatewayCommand::SetSession { state } => {
                if request.client_id != 0 {
                    return Err(GatewayError::UnauthorizedSessionControl);
                }
                self.session = state;
                Ok(Vec::new())
            }
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
            ),
        }?;
        self.cache.insert(
            request_key,
            ReferenceCache {
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
                None,
                Some(side),
                Some(price_ticks),
                quantity_lots,
                GatewayRejectReason::SessionClosed,
            )]);
        }
        let key = ReferenceKey {
            client_id,
            client_order_id,
        };
        if client_order_id == 0 {
            return Ok(vec![self.reject(
                client_id,
                request_id,
                client_order_id,
                None,
                None,
                Some(side),
                Some(price_ticks),
                quantity_lots,
                GatewayRejectReason::InvalidClientOrderId,
            )]);
        }
        if !self.used_client_ids.insert(key) {
            let exchange_id = self.orders.get(&key).map(|order| order.exchange_order_id);
            return Ok(vec![self.reject(
                client_id,
                request_id,
                client_order_id,
                exchange_id,
                None,
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
                None,
                Some(side),
                Some(price_ticks),
                quantity_lots,
                GatewayRejectReason::InvalidSymbol,
            )]);
        }
        if let Some(reason) = self.risk(price_ticks, quantity_lots) {
            return Ok(vec![self.reject(
                client_id,
                request_id,
                client_order_id,
                None,
                None,
                Some(side),
                Some(price_ticks),
                quantity_lots,
                reason,
            )]);
        }
        let exchange_id = self
            .next_exchange_id
            .checked_add(1)
            .ok_or(GatewayError::ExchangeOrderIdExhausted)?;
        let order_request = OrderRequest {
            order_id: exchange_id,
            side,
            price_ticks,
            quantity_lots,
        };
        let matcher = self.engines.entry(symbol.clone()).or_default();
        if let Err(reason) = matcher.preflight_order(order_request, options, Some(client_id)) {
            return Ok(vec![self.reject(
                client_id,
                request_id,
                client_order_id,
                None,
                None,
                Some(side),
                Some(price_ticks),
                quantity_lots,
                GatewayRejectReason::Engine(reason),
            )]);
        }
        let exchange_id = self.alloc_exchange_id()?;
        let result = self
            .engines
            .get_mut(&symbol)
            .expect("matcher inserted above")
            .submit_with_options_avoiding(order_request, options, Some(client_id));
        if let Some(reason) = result.rejection {
            return Ok(vec![self.reject(
                client_id,
                request_id,
                client_order_id,
                None,
                None,
                Some(side),
                Some(price_ticks),
                quantity_lots,
                GatewayRejectReason::Engine(reason),
            )]);
        }
        let sequence = result.sequence.expect("accepted order has sequence");
        self.orders.insert(
            key,
            ReferenceOrder {
                exchange_order_id: exchange_id,
                last_request_id: request_id,
                symbol,
                priority_sequence: sequence,
                side,
                price_ticks,
                quantity_lots,
                cumulative_lots: 0,
                status: OrderStatus::Accepted,
                time_in_force: options.time_in_force,
                post_only: options.post_only,
            },
        );
        self.reports_for_submit(
            client_id,
            request_id,
            key,
            exchange_id,
            side,
            price_ticks,
            quantity_lots,
            result.trades,
            result.resting_quantity_lots,
            Some(sequence),
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
        let key = ReferenceKey {
            client_id,
            client_order_id,
        };
        let Some(mut order) = self.orders.get(&key).cloned() else {
            return vec![self.reject(
                client_id,
                request_id,
                client_order_id,
                None,
                None,
                None,
                None,
                0,
                GatewayRejectReason::UnknownClientOrderId,
            )];
        };
        if !self.live.contains_key(&order.exchange_order_id) {
            return vec![self.reject(
                client_id,
                request_id,
                client_order_id,
                Some(order.exchange_order_id),
                None,
                Some(order.side),
                Some(order.price_ticks),
                order.quantity_lots,
                GatewayRejectReason::OrderNotLive,
            )];
        }
        let cancelled = self
            .engines
            .get_mut(&order.symbol)
            .expect("symbol has matcher")
            .cancel(order.exchange_order_id)
            .expect("live index agrees");
        order.status = OrderStatus::Cancelled;
        self.orders.insert(key, order.clone());
        self.live.remove(&order.exchange_order_id);
        vec![self.report(
            client_id,
            request_id,
            client_order_id,
            Some(order.exchange_order_id),
            Some(cancelled.sequence),
            OrderStatus::Cancelled,
            Some(order.side),
            Some(order.price_ticks),
            order.quantity_lots,
            order.cumulative_lots,
            0,
            0,
            None,
            None,
        )]
    }

    fn cancel_all(&mut self, client_id: u64, request_id: u64) -> Vec<ExecutionReport> {
        let mut orders: Vec<_> = self
            .orders
            .iter()
            .filter(|(key, order)| {
                key.client_id == client_id && self.live.contains_key(&order.exchange_order_id)
            })
            .map(|(key, order)| (order.exchange_order_id, key.client_order_id))
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
                None,
                Some(price_ticks),
                quantity_lots,
                GatewayRejectReason::SessionClosed,
            )]);
        }
        let old_key = ReferenceKey {
            client_id,
            client_order_id,
        };
        let new_key = ReferenceKey {
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
                None,
                Some(price_ticks),
                quantity_lots,
                GatewayRejectReason::InvalidClientOrderId,
            )]);
        }
        if !self.used_client_ids.insert(new_key) {
            let exchange_id = self
                .orders
                .get(&new_key)
                .map(|order| order.exchange_order_id);
            return Ok(vec![self.reject(
                client_id,
                request_id,
                replacement_client_order_id,
                exchange_id,
                None,
                None,
                Some(price_ticks),
                quantity_lots,
                GatewayRejectReason::DuplicateClientOrderId,
            )]);
        }
        let Some(old) = self.orders.get(&old_key).cloned() else {
            return Ok(vec![self.reject(
                client_id,
                request_id,
                replacement_client_order_id,
                None,
                None,
                None,
                Some(price_ticks),
                quantity_lots,
                GatewayRejectReason::UnknownClientOrderId,
            )]);
        };
        if !self.live.contains_key(&old.exchange_order_id) {
            return Ok(vec![self.reject(
                client_id,
                request_id,
                replacement_client_order_id,
                None,
                None,
                None,
                Some(price_ticks),
                quantity_lots,
                GatewayRejectReason::OrderNotLive,
            )]);
        }
        if let Some(reason) = self.risk(price_ticks, quantity_lots) {
            return Ok(vec![self.reject(
                client_id,
                request_id,
                replacement_client_order_id,
                None,
                None,
                Some(old.side),
                Some(price_ticks),
                quantity_lots,
                reason,
            )]);
        }
        let exchange_id = self
            .next_exchange_id
            .checked_add(1)
            .ok_or(GatewayError::ExchangeOrderIdExhausted)?;
        let replacement_request = OrderRequest {
            order_id: exchange_id,
            side: old.side,
            price_ticks,
            quantity_lots,
        };
        if let Err(reason) = self.engines[&old.symbol].preflight_replace(
            old.exchange_order_id,
            replacement_request,
            options,
            Some(client_id),
        ) {
            return Ok(vec![self.reject(
                client_id,
                request_id,
                replacement_client_order_id,
                None,
                None,
                Some(old.side),
                Some(price_ticks),
                quantity_lots,
                GatewayRejectReason::Engine(reason),
            )]);
        }
        let exchange_id = self.alloc_exchange_id()?;
        let replaced = self
            .engines
            .get_mut(&old.symbol)
            .expect("symbol has matcher")
            .replace_with_options(
                old.exchange_order_id,
                OrderRequest {
                    order_id: exchange_id,
                    side: old.side,
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
                None,
                Some(old.side),
                Some(price_ticks),
                quantity_lots,
                GatewayRejectReason::Engine(reason),
            )]);
        }
        let mut reports = Vec::new();
        let mut cancelled_old = old.clone();
        cancelled_old.status = OrderStatus::Cancelled;
        self.orders.insert(old_key, cancelled_old.clone());
        self.live.remove(&old.exchange_order_id);
        reports.push(self.report(
            client_id,
            request_id,
            client_order_id,
            Some(old.exchange_order_id),
            replaced.cancelled_sequence,
            OrderStatus::Cancelled,
            Some(old.side),
            Some(old.price_ticks),
            old.quantity_lots,
            old.cumulative_lots,
            0,
            0,
            None,
            None,
        ));
        let sequence = replaced
            .replacement
            .sequence
            .expect("accepted replacement has sequence");
        self.orders.insert(
            new_key,
            ReferenceOrder {
                exchange_order_id: exchange_id,
                last_request_id: request_id,
                symbol: old.symbol.clone(),
                priority_sequence: sequence,
                side: old.side,
                price_ticks,
                quantity_lots,
                cumulative_lots: 0,
                status: OrderStatus::Accepted,
                time_in_force: options.time_in_force,
                post_only: options.post_only,
            },
        );
        reports.extend(self.reports_for_submit(
            client_id,
            request_id,
            new_key,
            exchange_id,
            old.side,
            price_ticks,
            quantity_lots,
            replaced.replacement.trades,
            replaced.replacement.resting_quantity_lots,
            Some(sequence),
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
        taker_key: ReferenceKey,
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
        let had_trades = !trades.is_empty();
        let mut reports = Vec::with_capacity(trades.len().saturating_mul(2).max(1));
        for trade in trades {
            let maker_key = *self.live.get(&trade.maker_order_id).expect("maker is live");
            let mut maker = self.orders[&maker_key].clone();
            maker.cumulative_lots += trade.quantity_lots;
            let maker_leaves = maker.quantity_lots - maker.cumulative_lots;
            maker.status = if maker_leaves == 0 {
                OrderStatus::Filled
            } else {
                OrderStatus::PartiallyFilled
            };
            self.orders.insert(maker_key, maker.clone());
            if maker_leaves == 0 {
                self.live.remove(&trade.maker_order_id);
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
                maker.quantity_lots,
                maker.cumulative_lots,
                maker_leaves,
                trade.quantity_lots,
                Some(trade.price_ticks),
                None,
            ));

            let mut taker = self.orders[&taker_key].clone();
            taker.cumulative_lots += trade.quantity_lots;
            let taker_leaves = taker.quantity_lots - taker.cumulative_lots;
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
                taker.cumulative_lots,
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
            self.live.insert(taker_exchange_id, taker_key);
        }
        if self_trade_prevented || time_in_force == TimeInForce::Ioc {
            let mut taker = self.orders[&taker_key].clone();
            if taker.cumulative_lots < taker.quantity_lots {
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
                    taker.quantity_lots,
                    taker.cumulative_lots,
                    0,
                    0,
                    None,
                    None,
                ));
            }
        }
        Ok(reports)
    }

    fn risk(&self, price_ticks: i64, quantity_lots: u64) -> Option<GatewayRejectReason> {
        if price_ticks <= 0 {
            return Some(GatewayRejectReason::Engine(RejectReason::InvalidPrice));
        }
        if quantity_lots == 0 {
            return Some(GatewayRejectReason::Engine(RejectReason::InvalidQuantity));
        }
        if quantity_lots > self.config.max_order_quantity_lots {
            return Some(GatewayRejectReason::QuantityLimit);
        }
        if (price_ticks as u128)
            .checked_mul(u128::from(quantity_lots))
            .is_none_or(|notional| notional > self.config.max_order_notional_ticks_lots)
        {
            return Some(GatewayRejectReason::NotionalLimit);
        }
        None
    }

    fn alloc_exchange_id(&mut self) -> Result<u64, GatewayError> {
        self.next_exchange_id = self
            .next_exchange_id
            .checked_add(1)
            .ok_or(GatewayError::ExchangeOrderIdExhausted)?;
        Ok(self.next_exchange_id)
    }

    #[allow(clippy::too_many_arguments)]
    fn reject(
        &mut self,
        client_id: u64,
        request_id: u64,
        client_order_id: u64,
        exchange_order_id: Option<u64>,
        _symbol: Option<String>,
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
        self.next_report_sequence += 1;
        ExecutionReport {
            report_sequence: self.next_report_sequence,
            client_id,
            request_id,
            client_order_id,
            symbol: self
                .orders
                .get(&ReferenceKey {
                    client_id,
                    client_order_id,
                })
                .map(|order| order.symbol.clone()),
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

    fn order_states(&self) -> Vec<OrderStateSnapshot> {
        let mut states: Vec<_> = self
            .orders
            .iter()
            .map(|(key, order)| OrderStateSnapshot {
                client_id: key.client_id,
                client_order_id: key.client_order_id,
                exchange_order_id: order.exchange_order_id,
                symbol: order.symbol.clone(),
                priority_sequence: order.priority_sequence,
                side: order.side,
                price_ticks: order.price_ticks,
                order_quantity_lots: order.quantity_lots,
                cumulative_quantity_lots: order.cumulative_lots,
                leaves_quantity_lots: if matches!(
                    order.status,
                    OrderStatus::Filled | OrderStatus::Cancelled | OrderStatus::Rejected
                ) {
                    0
                } else {
                    order.quantity_lots - order.cumulative_lots
                },
                status: order.status,
                time_in_force: order.time_in_force,
                post_only: order.post_only,
            })
            .collect();
        states.sort_by_key(|order| (order.client_id, order.client_order_id));
        states
    }

    fn book_orders(&self) -> Vec<OrderStateSnapshot> {
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
                .then_with(|| (left.side == Side::Sell).cmp(&(right.side == Side::Sell)))
                .then_with(|| match left.side {
                    Side::Buy => right.price_ticks.cmp(&left.price_ticks),
                    Side::Sell => left.price_ticks.cmp(&right.price_ticks),
                })
                .then_with(|| left.priority_sequence.cmp(&right.priority_sequence))
        });
        orders
    }

    fn state_fingerprint(&self) -> u64 {
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
        add(&mut hash, &self.next_exchange_id.to_le_bytes());
        add(&mut hash, &self.next_report_sequence.to_le_bytes());
        let mut keys: Vec<_> = self.orders.keys().copied().collect();
        keys.sort_unstable();
        for key in keys {
            let order = &self.orders[&key];
            add(&mut hash, &key.client_id.to_le_bytes());
            add(&mut hash, &key.client_order_id.to_le_bytes());
            add(&mut hash, &order.exchange_order_id.to_le_bytes());
            add(&mut hash, &order.last_request_id.to_le_bytes());
            add(&mut hash, order.symbol.as_bytes());
            add(&mut hash, &order.priority_sequence.to_le_bytes());
            add(
                &mut hash,
                &[side_code(order.side), status_code(order.status)],
            );
            add(&mut hash, &order.price_ticks.to_le_bytes());
            add(&mut hash, &order.quantity_lots.to_le_bytes());
            add(&mut hash, &order.cumulative_lots.to_le_bytes());
            add(
                &mut hash,
                &[
                    match order.time_in_force {
                        TimeInForce::Gtc => 0,
                        TimeInForce::Ioc => 1,
                        TimeInForce::Fok => 2,
                    },
                    u8::from(order.post_only),
                ],
            );
        }
        let mut used: Vec<_> = self.used_client_ids.iter().copied().collect();
        used.sort_unstable();
        for key in used {
            add(&mut hash, &key.client_id.to_le_bytes());
            add(&mut hash, &key.client_order_id.to_le_bytes());
        }
        let mut cached: Vec<_> = self.cache.keys().copied().collect();
        cached.sort_unstable();
        for key in cached {
            let entry = &self.cache[&key];
            add(&mut hash, &key.0.to_le_bytes());
            add(&mut hash, &key.1.to_le_bytes());
            add(&mut hash, &serde_json::to_vec(&entry.command).unwrap());
            for report in &entry.reports {
                add(&mut hash, &serde_json::to_vec(report).unwrap());
            }
        }
        if self.session == crate::session::SessionState::Closed {
            add(&mut hash, b"session:closed");
        }
        if self.expiry_clock != 0 || !self.expiries.is_empty() {
            add(&mut hash, b"expiry:v1");
            add(&mut hash, &self.expiry_clock.to_le_bytes());
            let mut deadlines: Vec<_> = self
                .expiries
                .iter()
                .map(|(&id, &deadline)| (deadline, id))
                .collect();
            deadlines.sort_unstable();
            for (deadline, order_id) in deadlines {
                add(&mut hash, &deadline.to_le_bytes());
                add(&mut hash, &order_id.to_le_bytes());
            }
        }
        hash
    }
}

fn side_code(side: Side) -> u8 {
    if side == Side::Buy {
        0
    } else {
        1
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

fn config() -> GatewayConfig {
    GatewayConfig {
        max_order_quantity_lots: 128,
        max_order_notional_ticks_lots: 10_000,
    }
}

fn random_requests(seed: u64, count: usize) -> Vec<GatewayRequest> {
    let mut state = seed;
    let mut next_request = [0_u64; 9];
    let mut history: Vec<Vec<GatewayRequest>> = vec![Vec::new(); 9];
    let mut requests = Vec::with_capacity(count);
    let next = |state: &mut u64| {
        *state ^= *state << 7;
        *state ^= *state >> 9;
        *state ^= *state << 8;
        *state
    };
    for _ in 0..count {
        let random = next(&mut state);
        let client_id = random % 9;
        let history_index = client_id as usize;
        let choice = (random >> 8) % 100;
        if choice < 8 && !history[history_index].is_empty() {
            let item = (next(&mut state) as usize) % history[history_index].len();
            requests.push(history[history_index][item].clone());
            continue;
        }
        if choice < 15 && !history[history_index].is_empty() {
            let item = (next(&mut state) as usize) % history[history_index].len();
            let mut conflict = history[history_index][item].clone();
            conflict.command = match conflict.command {
                GatewayCommand::New {
                    client_order_id,
                    symbol,
                    side,
                    price_ticks,
                    quantity_lots,
                    ..
                } => GatewayCommand::New {
                    client_order_id: client_order_id ^ (1 << 63),
                    symbol,
                    side,
                    price_ticks,
                    quantity_lots: quantity_lots.wrapping_add(1),
                    time_in_force: TimeInForce::Gtc,
                    post_only: false,
                },
                GatewayCommand::Cancel { client_order_id } => GatewayCommand::Cancel {
                    client_order_id: client_order_id ^ (1 << 63),
                },
                GatewayCommand::CancelAll => GatewayCommand::Cancel { client_order_id: 0 },
                GatewayCommand::SetSession { state } => GatewayCommand::SetSession {
                    state: if state == crate::session::SessionState::Open {
                        crate::session::SessionState::Closed
                    } else {
                        crate::session::SessionState::Open
                    },
                },
                GatewayCommand::Replace {
                    client_order_id,
                    replacement_client_order_id,
                    price_ticks,
                    quantity_lots,
                    ..
                } => GatewayCommand::Replace {
                    client_order_id,
                    replacement_client_order_id: replacement_client_order_id ^ (1 << 63),
                    price_ticks,
                    quantity_lots,
                    time_in_force: TimeInForce::Gtc,
                    post_only: false,
                },
            };
            requests.push(conflict);
            continue;
        }
        next_request[history_index] += 1;
        let request_id = if choice == 15 {
            0
        } else {
            next_request[history_index]
        };
        let command_kind = (random >> 16) % 100;
        let candidate_id = next(&mut state) % 64;
        let command = if command_kind < 58 {
            let price_draw = next(&mut state) % 16;
            let price_ticks = match price_draw {
                0 => -1,
                1 | 2 => 0,
                _ => 98 + (next(&mut state) % 9) as i64,
            };
            let quantity_draw = next(&mut state) % 16;
            let quantity_lots = match quantity_draw {
                0 => 0,
                1 => u64::MAX,
                _ => 1 + (next(&mut state) % 12),
            };
            let symbol = match next(&mut state) % 3 {
                0 => "AAPL",
                1 => "MSFT",
                _ => "NVDA",
            }
            .to_owned();
            GatewayCommand::New {
                client_order_id: candidate_id,
                symbol,
                side: if random & 1 == 0 {
                    Side::Buy
                } else {
                    Side::Sell
                },
                price_ticks,
                quantity_lots,
                time_in_force: TimeInForce::Gtc,
                post_only: false,
            }
        } else if command_kind < 78 {
            GatewayCommand::Cancel {
                client_order_id: candidate_id,
            }
        } else if command_kind < 93 {
            GatewayCommand::Replace {
                client_order_id: candidate_id,
                replacement_client_order_id: next(&mut state) % 64,
                price_ticks: 98 + (next(&mut state) % 9) as i64,
                quantity_lots: 1 + (next(&mut state) % 12),
                time_in_force: TimeInForce::Gtc,
                post_only: false,
            }
        } else if command_kind < 96 {
            GatewayCommand::CancelAll
        } else {
            GatewayCommand::SetSession {
                state: if next(&mut state) & 1 == 0 {
                    crate::session::SessionState::Open
                } else {
                    crate::session::SessionState::Closed
                },
            }
        };
        let request = GatewayRequest {
            client_id,
            request_id,
            command,
        };
        if request_id != 0 {
            history[history_index].push(request.clone());
        }
        requests.push(request);
    }
    requests
}

fn save_minimized_failure(seed: u64, commands: &[GatewayRequest]) -> PathBuf {
    fn fails(commands: &[GatewayRequest]) -> bool {
        let mut production = crate::gateway::SimulatedExchange::new(config());
        let mut reference = ReferenceExchange::new(config());
        for request in commands {
            if production.handle(request.clone()) != reference.handle(request.clone())
                || production.book_orders() != reference.book_orders()
            {
                return true;
            }
        }
        false
    }
    let mut minimized = commands.to_vec();
    let mut granularity = 2_usize;
    while minimized.len() >= 2 {
        let chunk = minimized.len().div_ceil(granularity);
        let mut reduced = false;
        for start in (0..minimized.len()).step_by(chunk) {
            let end = (start + chunk).min(minimized.len());
            let mut candidate = minimized.clone();
            candidate.drain(start..end);
            if fails(&candidate) {
                minimized = candidate;
                granularity = granularity.saturating_sub(1).max(2);
                reduced = true;
                break;
            }
        }
        if !reduced {
            if granularity >= minimized.len() {
                break;
            }
            granularity = (granularity * 2).min(minimized.len());
        }
    }
    let path = PathBuf::from(format!("target/reference-model-failure-seed-{seed}.jsonl"));
    if let Some(parent) = path.parent() {
        let _ = fs::create_dir_all(parent);
    }
    if let Ok(mut file) = File::create(&path) {
        for request in minimized {
            if let Ok(line) = serde_json::to_string(&request) {
                let _ = writeln!(file, "{line}");
            }
        }
    }
    path
}

#[test]
fn production_gateway_matches_independent_reference_for_100k_commands() {
    let seeds = [
        0xACCE_5510,
        0x0B00_B1E5,
        0xF1F0_2026,
        0xDEAD_BEEF,
        0x9876_5432,
    ];
    let mut total_commands = 0_usize;
    for seed in seeds {
        let commands = random_requests(seed, 20_000);
        let mut production = crate::gateway::SimulatedExchange::new(config());
        let mut reference = ReferenceExchange::new(config());
        for (index, request) in commands.iter().enumerate() {
            let actual = production.handle(request.clone());
            let expected = reference.handle(request.clone());
            let failure = actual != expected
                || production.book_orders() != reference.book_orders()
                || production.live_order_count() != reference.live.values().count()
                || ["AAPL", "MSFT", "NVDA"].iter().any(|symbol| {
                    production.best_bid_for(symbol)
                        != reference
                            .engines
                            .get(*symbol)
                            .and_then(ReferenceMatcher::best_bid)
                        || production.best_ask_for(symbol)
                            != reference
                                .engines
                                .get(*symbol)
                                .and_then(ReferenceMatcher::best_ask)
                })
                || (index % 5_000 == 4_999
                    && (production.order_states() != reference.order_states()
                        || production.state_fingerprint() != reference.state_fingerprint()));
            if failure {
                let path = save_minimized_failure(seed, &commands[..=index]);
                panic!("reference mismatch seed={seed:#x} command_index={index}; minimized reproducer: {}", path.display());
            }
            production
                .check_invariants()
                .expect("production invariants hold");
            total_commands += 1;
        }
        assert_eq!(production.order_states(), reference.order_states());
        assert_eq!(
            production.state_fingerprint(),
            reference.state_fingerprint()
        );
        assert!(production.check_invariants().is_ok());
    }
    assert_eq!(total_commands, 100_000);
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(64))]

    #[test]
    fn generated_order_sequences_match_reference_model(
        raw_commands in prop::collection::vec((any::<u8>(), any::<u8>(), any::<i16>(), any::<u16>()), 1..100)
    ) {
        let mut production = crate::gateway::SimulatedExchange::new(config());
        let mut reference = ReferenceExchange::new(config());
        for (index, (kind, flags, raw_price, raw_quantity)) in raw_commands.into_iter().enumerate() {
            let client_id = u64::from(flags % 4);
            let client_order_id = index as u64 + 1;
            let price_ticks = i64::from(raw_price % 120);
            let quantity_lots = u64::from(raw_quantity % 20);
            let time_in_force = match flags % 4 {
                1 => TimeInForce::Ioc,
                2 => TimeInForce::Fok,
                _ => TimeInForce::Gtc,
            };
            let post_only = flags % 4 == 3;
            let command = match kind % 5 {
                0 => GatewayCommand::New {
                    client_order_id,
                    symbol: if flags & 1 == 0 { "AAPL" } else { "MSFT" }.into(),
                    side: if flags & 2 == 0 { Side::Buy } else { Side::Sell },
                    price_ticks,
                    quantity_lots,
                    time_in_force,
                    post_only,
                },
                1 => GatewayCommand::Cancel { client_order_id: u64::from(flags % 16) },
                2 => GatewayCommand::Replace {
                    client_order_id: u64::from(flags % 16),
                    replacement_client_order_id: client_order_id + 1000,
                    price_ticks,
                    quantity_lots,
                    time_in_force,
                    post_only,
                },
                3 => GatewayCommand::CancelAll,
                _ => GatewayCommand::SetSession {
                    state: if flags & 4 == 0 { crate::session::SessionState::Open } else { crate::session::SessionState::Closed },
                },
            };
            let request = GatewayRequest { client_id, request_id: index as u64 + 1, command };
            prop_assert_eq!(production.handle(request.clone()), reference.handle(request));
            prop_assert_eq!(production.book_orders(), reference.book_orders());
            prop_assert_eq!(production.order_states(), reference.order_states());
            prop_assert_eq!(production.state_fingerprint(), reference.state_fingerprint());
            prop_assert!(production.check_invariants().is_ok());
        }
    }
}
