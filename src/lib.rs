//! Deterministic, single-threaded price-time-priority matching for synthetic orders.

use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap, HashSet};

pub type OrderId = u64;
pub type Sequence = u64;
pub type PriceTicks = i64;
pub type QuantityLots = u64;

pub mod benchmark;
pub mod eventlog;
pub mod expiry;
pub mod gateway;
pub mod gateway_benchmark;
pub mod ibkr;
pub mod journal;
pub mod market_benchmark;
pub mod market_data;
pub mod pipeline;
pub mod replay;
pub mod session;

#[cfg(test)]
mod expiry_property_tests;
#[cfg(test)]
mod reference_matcher;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum Side {
    #[serde(alias = "buy")]
    Buy,
    #[serde(alias = "sell")]
    Sell,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct OrderRequest {
    pub order_id: OrderId,
    pub side: Side,
    pub price_ticks: PriceTicks,
    pub quantity_lots: QuantityLots,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TimeInForce {
    #[default]
    Gtc,
    Ioc,
    Fok,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct OrderOptions {
    #[serde(default)]
    pub time_in_force: TimeInForce,
    #[serde(default)]
    pub post_only: bool,
}

impl Default for OrderOptions {
    fn default() -> Self {
        Self {
            time_in_force: TimeInForce::Gtc,
            post_only: false,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Trade {
    pub sequence: Sequence,
    pub maker_order_id: OrderId,
    pub taker_order_id: OrderId,
    pub taker_side: Side,
    pub price_ticks: PriceTicks,
    pub quantity_lots: QuantityLots,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum RejectReason {
    InvalidPrice,
    InvalidQuantity,
    DuplicateOrderId,
    UnknownOrderId,
    SequenceExhausted,
    InvalidOrderOptions,
    PostOnlyWouldTake,
    FillOrKillUnavailable,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SubmitResult {
    pub sequence: Option<Sequence>,
    pub trades: Vec<Trade>,
    pub resting_quantity_lots: QuantityLots,
    pub self_trade_prevented: bool,
    pub rejection: Option<RejectReason>,
}

impl SubmitResult {
    fn rejected(reason: RejectReason) -> Self {
        Self {
            sequence: None,
            trades: Vec::new(),
            resting_quantity_lots: 0,
            self_trade_prevented: false,
            rejection: Some(reason),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RestingOrder {
    pub order_id: OrderId,
    pub side: Side,
    pub price_ticks: PriceTicks,
    pub remaining_lots: QuantityLots,
    pub sequence: Sequence,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplaceResult {
    pub cancelled_sequence: Option<Sequence>,
    pub replacement: SubmitResult,
    pub rejection: Option<RejectReason>,
}

#[derive(Debug, Default)]
struct PriceLevel {
    head: Option<OrderId>,
    tail: Option<OrderId>,
    total_quantity: u128,
}

#[derive(Debug, Clone, Copy)]
struct LiveOrder {
    side: Side,
    price_ticks: PriceTicks,
    remaining_lots: QuantityLots,
    sequence: Sequence,
    owner_id: Option<u64>,
    previous: Option<OrderId>,
    next: Option<OrderId>,
}

#[derive(Debug, Default)]
pub struct MatchingEngine {
    bids: BTreeMap<PriceTicks, PriceLevel>,
    asks: BTreeMap<PriceTicks, PriceLevel>,
    live_orders: HashMap<OrderId, LiveOrder>,
    used_order_ids: HashSet<OrderId>,
    next_sequence: Sequence,
}

impl MatchingEngine {
    pub fn new() -> Self {
        Self::default()
    }

    /// Number of further engine sequence values available for accepted actions.
    pub fn remaining_sequences(&self) -> u64 {
        u64::MAX - self.next_sequence
    }

    /// Match at eligible best prices, with each execution priced at the resting order.
    pub fn submit(&mut self, request: OrderRequest) -> SubmitResult {
        self.submit_with_options(request, OrderOptions::default())
    }

    /// Check order policy without mutating IDs, sequence numbers, or book state.
    pub fn preflight(
        &self,
        request: OrderRequest,
        options: OrderOptions,
    ) -> Result<(), RejectReason> {
        self.preflight_for_client(request, options, None)
    }

    /// Validate matching policy while excluding same-client makers from FOK liquidity.
    pub fn preflight_for_client(
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
        if options.post_only && self.would_cross(request.side, request.price_ticks) {
            return Err(RejectReason::PostOnlyWouldTake);
        }
        if options.time_in_force == TimeInForce::Fok {
            let available = self.available_crossing_quantity_for_client(
                request.side,
                request.price_ticks,
                owner_id,
            );
            if available < u128::from(request.quantity_lots) {
                return Err(RejectReason::FillOrKillUnavailable);
            }
        }
        Ok(())
    }

    /// Apply GTC, IOC or FOK matching while preserving price-time priority.
    pub fn submit_with_options(
        &mut self,
        request: OrderRequest,
        options: OrderOptions,
    ) -> SubmitResult {
        self.submit_with_options_for_client(request, options, None)
    }

    /// Match while preventing executions against same-client resting orders.
    pub fn submit_with_options_for_client(
        &mut self,
        request: OrderRequest,
        options: OrderOptions,
        owner_id: Option<u64>,
    ) -> SubmitResult {
        if let Err(reason) = self.preflight_for_client(request, options, owner_id) {
            return SubmitResult::rejected(reason);
        }
        let Some(sequence) = self.allocate_sequence() else {
            return SubmitResult::rejected(RejectReason::SequenceExhausted);
        };
        self.used_order_ids.insert(request.order_id);

        let mut remaining = request.quantity_lots;
        let mut trades = Vec::new();
        let mut self_trade_prevented = false;
        while remaining > 0 {
            let best_price = match request.side {
                Side::Buy => self
                    .asks
                    .keys()
                    .next()
                    .copied()
                    .filter(|price| *price <= request.price_ticks),
                Side::Sell => self
                    .bids
                    .keys()
                    .next_back()
                    .copied()
                    .filter(|price| *price >= request.price_ticks),
            };
            let Some(price) = best_price else { break };
            let resting_side = match request.side {
                Side::Buy => Side::Sell,
                Side::Sell => Side::Buy,
            };
            let maker_id = self
                .book(resting_side)
                .get(&price)
                .and_then(|level| level.head)
                .expect("price level has a first order");
            let maker = *self.live_orders.get(&maker_id).expect("level head is live");
            if owner_id.is_some() && maker.owner_id == owner_id {
                self_trade_prevented = true;
                break;
            }
            let fill_quantity = remaining.min(maker.remaining_lots);
            remaining -= fill_quantity;
            trades.push(Trade {
                sequence,
                maker_order_id: maker_id,
                taker_order_id: request.order_id,
                taker_side: request.side,
                price_ticks: price,
                quantity_lots: fill_quantity,
            });
            if fill_quantity == maker.remaining_lots {
                self.unlink_live_order(maker_id);
            } else {
                self.live_orders
                    .get_mut(&maker_id)
                    .expect("maker remains live")
                    .remaining_lots -= fill_quantity;
                let level = self
                    .book_mut(resting_side)
                    .get_mut(&price)
                    .expect("price level remains live");
                level.total_quantity = level
                    .total_quantity
                    .checked_sub(u128::from(fill_quantity))
                    .expect("level aggregate includes this fill");
            }
        }

        let resting_quantity_lots = if options.time_in_force == TimeInForce::Gtc {
            if remaining > 0 && !self_trade_prevented {
                self.insert_live_order(
                    RestingOrder {
                        order_id: request.order_id,
                        side: request.side,
                        price_ticks: request.price_ticks,
                        remaining_lots: remaining,
                        sequence,
                    },
                    owner_id,
                );
            }
            if self_trade_prevented {
                0
            } else {
                remaining
            }
        } else {
            0
        };
        SubmitResult {
            sequence: Some(sequence),
            trades,
            resting_quantity_lots,
            self_trade_prevented,
            rejection: None,
        }
    }

    fn would_cross(&self, side: Side, price_ticks: PriceTicks) -> bool {
        match side {
            Side::Buy => self
                .asks
                .keys()
                .next()
                .is_some_and(|ask| *ask <= price_ticks),
            Side::Sell => self
                .bids
                .keys()
                .next_back()
                .is_some_and(|bid| *bid >= price_ticks),
        }
    }

    fn available_crossing_quantity_for_client(
        &self,
        side: Side,
        price_ticks: PriceTicks,
        owner_id: Option<u64>,
    ) -> u128 {
        let mut quantity = 0_u128;
        let mut include_level = |level: &PriceLevel| {
            let mut order_id = level.head;
            while let Some(id) = order_id {
                let order = self.live_orders.get(&id).expect("FIFO node is live");
                if owner_id.is_some() && order.owner_id == owner_id {
                    return false;
                }
                quantity = quantity.saturating_add(u128::from(order.remaining_lots));
                order_id = order.next;
            }
            true
        };
        match side {
            Side::Buy => {
                for level in self.asks.range(..=price_ticks).map(|(_, level)| level) {
                    if !include_level(level) {
                        break;
                    }
                }
            }
            Side::Sell => {
                for level in self.bids.range(price_ticks..).rev().map(|(_, level)| level) {
                    if !include_level(level) {
                        break;
                    }
                }
            }
        }
        quantity
    }

    /// Remove a live order. Rejected cancellations do not advance sequence.
    pub fn cancel(&mut self, order_id: OrderId) -> Result<RestingOrder, RejectReason> {
        if !self.live_orders.contains_key(&order_id) {
            return Err(RejectReason::UnknownOrderId);
        }
        if self.next_sequence == Sequence::MAX {
            return Err(RejectReason::SequenceExhausted);
        }
        self.allocate_sequence()
            .ok_or(RejectReason::SequenceExhausted)?;
        Ok(self.unlink_live_order(order_id))
    }

    /// Replace by canceling the old order and submitting a fresh ID without inherited priority.
    pub fn replace(&mut self, old_order_id: OrderId, replacement: OrderRequest) -> ReplaceResult {
        self.replace_with_options(old_order_id, replacement, OrderOptions::default())
    }

    /// Replace atomically with respect to policy rejection: validate the new order before
    /// cancelling the old one, then apply cancel and submit under the caller's serialization.
    pub fn replace_with_options(
        &mut self,
        old_order_id: OrderId,
        replacement: OrderRequest,
        options: OrderOptions,
    ) -> ReplaceResult {
        self.replace_with_options_for_client(old_order_id, replacement, options, None)
    }

    /// Replace while applying same-client self-trade prevention to the new order.
    pub fn replace_with_options_for_client(
        &mut self,
        old_order_id: OrderId,
        replacement: OrderRequest,
        options: OrderOptions,
        owner_id: Option<u64>,
    ) -> ReplaceResult {
        if let Err(reason) =
            self.preflight_replace_for_client(old_order_id, replacement, options, owner_id)
        {
            return ReplaceResult::rejected(reason);
        }
        self.cancel(old_order_id)
            .expect("preflight confirmed live order and sequence capacity");
        let cancelled_sequence = Some(self.next_sequence);
        let result = self.submit_with_options_for_client(replacement, options, owner_id);
        ReplaceResult {
            cancelled_sequence,
            replacement: result,
            rejection: None,
        }
    }

    /// Check replacement policy without changing the old order or consuming the new ID.
    pub fn preflight_replace(
        &self,
        old_order_id: OrderId,
        replacement: OrderRequest,
        options: OrderOptions,
    ) -> Result<(), RejectReason> {
        self.preflight_replace_for_client(old_order_id, replacement, options, None)
    }

    pub fn preflight_replace_for_client(
        &self,
        old_order_id: OrderId,
        replacement: OrderRequest,
        options: OrderOptions,
        owner_id: Option<u64>,
    ) -> Result<(), RejectReason> {
        if replacement.price_ticks <= 0 {
            return Err(RejectReason::InvalidPrice);
        }
        if replacement.quantity_lots == 0 {
            return Err(RejectReason::InvalidQuantity);
        }
        if self.used_order_ids.contains(&replacement.order_id) {
            return Err(RejectReason::DuplicateOrderId);
        }
        if !self.live_orders.contains_key(&old_order_id) {
            return Err(RejectReason::UnknownOrderId);
        }
        if self.next_sequence >= Sequence::MAX - 1 {
            return Err(RejectReason::SequenceExhausted);
        }
        self.preflight_for_client(replacement, options, owner_id)
    }

    pub fn best_bid(&self) -> Option<(PriceTicks, u128)> {
        self.bids
            .iter()
            .next_back()
            .map(|(price, level)| (*price, level.total_quantity))
    }
    pub fn best_ask(&self) -> Option<(PriceTicks, u128)> {
        self.asks
            .iter()
            .next()
            .map(|(price, level)| (*price, level.total_quantity))
    }
    pub fn resting_orders(&self, side: Side, price: PriceTicks) -> Vec<RestingOrder> {
        let Some(level) = self.book(side).get(&price) else {
            return Vec::new();
        };
        let mut result = Vec::new();
        let mut next_id = level.head;
        while let Some(order_id) = next_id {
            let order = self
                .live_orders
                .get(&order_id)
                .expect("level points to a live order");
            result.push(RestingOrder {
                order_id,
                side: order.side,
                price_ticks: order.price_ticks,
                remaining_lots: order.remaining_lots,
                sequence: order.sequence,
            });
            next_id = order.next;
        }
        result
    }
    pub fn live_order_count(&self) -> usize {
        self.live_orders.len()
    }

    pub fn contains_live_order(&self, order_id: OrderId) -> bool {
        self.live_orders.contains_key(&order_id)
    }

    /// Stable FNV-1a fingerprint of all state that can affect later commands.
    pub fn state_fingerprint(&self) -> u64 {
        let mut hash = 0xcbf29ce484222325_u64;
        fn add(hash: &mut u64, bytes: &[u8]) {
            for byte in bytes {
                *hash ^= u64::from(*byte);
                *hash = hash.wrapping_mul(0x100000001b3);
            }
        }
        add(&mut hash, &self.next_sequence.to_le_bytes());
        for (side, book) in [(Side::Buy, &self.bids), (Side::Sell, &self.asks)] {
            add(
                &mut hash,
                &[match side {
                    Side::Buy => 0,
                    Side::Sell => 1,
                }],
            );
            for (price, level) in book {
                add(&mut hash, &price.to_le_bytes());
                let mut next_id = level.head;
                while let Some(order_id) = next_id {
                    let order = self
                        .live_orders
                        .get(&order_id)
                        .expect("level points to a live order");
                    add(&mut hash, &order_id.to_le_bytes());
                    add(&mut hash, &order.remaining_lots.to_le_bytes());
                    add(&mut hash, &order.sequence.to_le_bytes());
                    next_id = order.next;
                }
            }
        }
        let mut used_ids: Vec<_> = self.used_order_ids.iter().copied().collect();
        used_ids.sort_unstable();
        for order_id in used_ids {
            add(&mut hash, &order_id.to_le_bytes());
        }
        hash
    }

    /// Check book/index agreement, nonzero quantities, and absence of crossed resting prices.
    pub fn check_invariants(&self) -> Result<(), String> {
        let mut indexed = HashSet::new();
        for (side, book) in [(Side::Buy, &self.bids), (Side::Sell, &self.asks)] {
            for (price, level) in book {
                if *price <= 0 || level.head.is_none() || level.tail.is_none() {
                    return Err("book contains an invalid or empty price level".into());
                }
                let mut total_quantity = 0_u128;
                let mut previous = None;
                let mut current = level.head;
                while let Some(order_id) = current {
                    let order = self.live_orders.get(&order_id).ok_or_else(|| {
                        format!("order {order_id} is missing from live-order index")
                    })?;
                    if order.side != side
                        || order.price_ticks != *price
                        || order.remaining_lots == 0
                        || order.previous != previous
                    {
                        return Err(format!("order {order_id} has inconsistent queue fields"));
                    }
                    if !indexed.insert(order_id) {
                        return Err(format!("order {order_id} appears more than once"));
                    }
                    total_quantity = total_quantity
                        .checked_add(u128::from(order.remaining_lots))
                        .ok_or_else(|| "price-level aggregate exceeds u128".to_owned())?;
                    previous = Some(order_id);
                    current = order.next;
                }
                if previous != level.tail || total_quantity != level.total_quantity {
                    return Err(format!(
                        "price level {price} has inconsistent tail or quantity"
                    ));
                }
            }
        }
        if indexed.len() != self.live_orders.len() {
            return Err("live-order index contains an order absent from the book".into());
        }
        if let (Some((bid, _)), Some((ask, _))) = (self.best_bid(), self.best_ask()) {
            if bid >= ask {
                return Err(format!("resting book is crossed: bid {bid} >= ask {ask}"));
            }
        }
        Ok(())
    }

    fn allocate_sequence(&mut self) -> Option<Sequence> {
        self.next_sequence = self.next_sequence.checked_add(1)?;
        Some(self.next_sequence)
    }

    fn book(&self, side: Side) -> &BTreeMap<PriceTicks, PriceLevel> {
        match side {
            Side::Buy => &self.bids,
            Side::Sell => &self.asks,
        }
    }
    fn book_mut(&mut self, side: Side) -> &mut BTreeMap<PriceTicks, PriceLevel> {
        match side {
            Side::Buy => &mut self.bids,
            Side::Sell => &mut self.asks,
        }
    }

    fn insert_live_order(&mut self, order: RestingOrder, owner_id: Option<u64>) {
        let level = self
            .book_mut(order.side)
            .entry(order.price_ticks)
            .or_default();
        let previous = level.tail;
        if let Some(previous_id) = previous {
            self.live_orders
                .get_mut(&previous_id)
                .expect("tail is live")
                .next = Some(order.order_id);
        }
        let level = self
            .book_mut(order.side)
            .get_mut(&order.price_ticks)
            .expect("level was inserted");
        if level.head.is_none() {
            level.head = Some(order.order_id);
        }
        level.tail = Some(order.order_id);
        level.total_quantity = level
            .total_quantity
            .checked_add(u128::from(order.remaining_lots))
            .expect("live orders at one level cannot exceed a u128 aggregate");
        self.live_orders.insert(
            order.order_id,
            LiveOrder {
                side: order.side,
                price_ticks: order.price_ticks,
                remaining_lots: order.remaining_lots,
                sequence: order.sequence,
                owner_id,
                previous,
                next: None,
            },
        );
    }

    fn unlink_live_order(&mut self, order_id: OrderId) -> RestingOrder {
        let order = self.live_orders.remove(&order_id).expect("order is live");
        if let Some(previous_id) = order.previous {
            self.live_orders
                .get_mut(&previous_id)
                .expect("previous order is live")
                .next = order.next;
        } else {
            self.book_mut(order.side)
                .get_mut(&order.price_ticks)
                .expect("price level exists")
                .head = order.next;
        }
        if let Some(next_id) = order.next {
            self.live_orders
                .get_mut(&next_id)
                .expect("next order is live")
                .previous = order.previous;
        } else {
            self.book_mut(order.side)
                .get_mut(&order.price_ticks)
                .expect("price level exists")
                .tail = order.previous;
        }
        let book = self.book_mut(order.side);
        let level = book
            .get_mut(&order.price_ticks)
            .expect("price level exists");
        level.total_quantity = level
            .total_quantity
            .checked_sub(u128::from(order.remaining_lots))
            .expect("aggregate includes this live order");
        if level.head.is_none() {
            book.remove(&order.price_ticks);
        }
        RestingOrder {
            order_id,
            side: order.side,
            price_ticks: order.price_ticks,
            remaining_lots: order.remaining_lots,
            sequence: order.sequence,
        }
    }
}

impl ReplaceResult {
    fn rejected(reason: RejectReason) -> Self {
        Self {
            cancelled_sequence: None,
            replacement: SubmitResult::rejected(reason.clone()),
            rejection: Some(reason),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn remaining_sequence_capacity_tracks_accepted_commands() {
        let mut engine = MatchingEngine::new();
        assert_eq!(engine.remaining_sequences(), u64::MAX);
        engine.submit(order(1, Side::Buy, 100, 1));
        assert_eq!(engine.remaining_sequences(), u64::MAX - 1);
        engine.next_sequence = u64::MAX;
        assert_eq!(engine.remaining_sequences(), 0);
        assert_eq!(engine.cancel(1), Err(RejectReason::SequenceExhausted));
    }
    fn order(order_id: u64, side: Side, price_ticks: i64, quantity_lots: u64) -> OrderRequest {
        OrderRequest {
            order_id,
            side,
            price_ticks,
            quantity_lots,
        }
    }

    #[test]
    fn price_then_time_priority_and_resting_price() {
        let mut engine = MatchingEngine::new();
        engine.submit(order(1, Side::Sell, 102, 3));
        engine.submit(order(2, Side::Sell, 101, 2));
        engine.submit(order(3, Side::Sell, 101, 4));
        let result = engine.submit(order(4, Side::Buy, 103, 5));
        assert_eq!(result.trades.len(), 2);
        assert_eq!(
            (
                result.trades[0].maker_order_id,
                result.trades[0].price_ticks,
                result.trades[0].quantity_lots
            ),
            (2, 101, 2)
        );
        assert_eq!(
            (
                result.trades[1].maker_order_id,
                result.trades[1].price_ticks,
                result.trades[1].quantity_lots
            ),
            (3, 101, 3)
        );
        assert_eq!(engine.best_ask(), Some((101, 1)));
        assert!(engine.check_invariants().is_ok());
    }

    #[test]
    fn partial_and_multilevel_fills_preserve_remainder() {
        let mut engine = MatchingEngine::new();
        engine.submit(order(1, Side::Sell, 100, 2));
        engine.submit(order(2, Side::Sell, 101, 2));
        let result = engine.submit(order(3, Side::Buy, 101, 5));
        assert_eq!(
            result
                .trades
                .iter()
                .map(|trade| trade.quantity_lots)
                .sum::<u64>(),
            4
        );
        assert_eq!(result.resting_quantity_lots, 1);
        assert_eq!(engine.best_bid(), Some((101, 1)));
        assert_eq!(engine.best_ask(), None);
        assert!(engine.check_invariants().is_ok());
    }

    #[test]
    fn ioc_fills_available_quantity_without_resting_remainder() {
        let mut engine = MatchingEngine::new();
        engine.submit(order(1, Side::Sell, 100, 5));
        let result = engine.submit_with_options(
            order(2, Side::Buy, 100, 7),
            OrderOptions {
                time_in_force: TimeInForce::Ioc,
                post_only: false,
            },
        );
        assert_eq!(result.rejection, None);
        assert_eq!(result.resting_quantity_lots, 0);
        assert_eq!(result.trades.len(), 1);
        assert_eq!(result.trades[0].quantity_lots, 5);
        assert_eq!(engine.best_ask(), None);
        assert_eq!(engine.best_bid(), None);
        assert_eq!(engine.live_order_count(), 0);
        assert!(engine.check_invariants().is_ok());
    }

    #[test]
    fn fok_insufficient_depth_rejects_without_mutating_any_state() {
        let mut engine = MatchingEngine::new();
        engine.submit(order(1, Side::Sell, 100, 2));
        let before = engine.state_fingerprint();
        let result = engine.submit_with_options(
            order(2, Side::Buy, 100, 3),
            OrderOptions {
                time_in_force: TimeInForce::Fok,
                post_only: false,
            },
        );
        assert_eq!(result.rejection, Some(RejectReason::FillOrKillUnavailable));
        assert_eq!(result.sequence, None);
        assert_eq!(engine.state_fingerprint(), before);
        assert!(engine.check_invariants().is_ok());
    }

    #[test]
    fn fok_consumes_multiple_levels_only_when_all_quantity_is_available() {
        let mut engine = MatchingEngine::new();
        engine.submit(order(1, Side::Sell, 100, 1));
        engine.submit(order(2, Side::Sell, 101, 2));
        let result = engine.submit_with_options(
            order(3, Side::Buy, 101, 3),
            OrderOptions {
                time_in_force: TimeInForce::Fok,
                post_only: false,
            },
        );
        assert_eq!(result.rejection, None);
        assert_eq!(result.resting_quantity_lots, 0);
        assert_eq!(
            result
                .trades
                .iter()
                .map(|trade| (trade.maker_order_id, trade.price_ticks, trade.quantity_lots))
                .collect::<Vec<_>>(),
            vec![(1, 100, 1), (2, 101, 2)]
        );
        assert_eq!(engine.live_order_count(), 0);
        assert!(engine.check_invariants().is_ok());
    }

    #[test]
    fn post_only_crossing_order_rejects_without_advancing_sequence() {
        let mut engine = MatchingEngine::new();
        engine.submit(order(1, Side::Sell, 100, 2));
        let before = engine.state_fingerprint();
        let result = engine.submit_with_options(
            order(2, Side::Buy, 100, 1),
            OrderOptions {
                time_in_force: TimeInForce::Gtc,
                post_only: true,
            },
        );
        assert_eq!(result.rejection, Some(RejectReason::PostOnlyWouldTake));
        assert_eq!(result.sequence, None);
        assert_eq!(engine.state_fingerprint(), before);
        assert!(engine.check_invariants().is_ok());
    }

    #[test]
    fn post_only_rejects_conflicting_ioc_fok_flags() {
        let mut engine = MatchingEngine::new();
        let result = engine.submit_with_options(
            order(1, Side::Buy, 100, 1),
            OrderOptions {
                time_in_force: TimeInForce::Ioc,
                post_only: true,
            },
        );
        assert_eq!(result.rejection, Some(RejectReason::InvalidOrderOptions));
        assert_eq!(engine.live_order_count(), 0);
        assert_eq!(
            engine.state_fingerprint(),
            MatchingEngine::new().state_fingerprint()
        );
    }

    #[test]
    fn rejects_invalid_and_reused_ids() {
        let mut engine = MatchingEngine::new();
        assert_eq!(
            engine.submit(order(1, Side::Buy, 0, 2)).rejection,
            Some(RejectReason::InvalidPrice)
        );
        assert_eq!(
            engine.submit(order(1, Side::Buy, 10, 0)).rejection,
            Some(RejectReason::InvalidQuantity)
        );
        assert!(engine
            .submit(order(1, Side::Buy, 10, 1))
            .rejection
            .is_none());
        engine.cancel(1).unwrap();
        assert_eq!(
            engine.submit(order(1, Side::Buy, 10, 1)).rejection,
            Some(RejectReason::DuplicateOrderId)
        );
    }

    #[test]
    fn cancel_and_replace_use_new_priority() {
        let mut engine = MatchingEngine::new();
        engine.submit(order(1, Side::Buy, 100, 2));
        engine.submit(order(2, Side::Buy, 100, 2));
        let replaced = engine.replace(1, order(3, Side::Buy, 100, 2));
        assert!(replaced.rejection.is_none());
        let resting = engine.resting_orders(Side::Buy, 100);
        assert_eq!(
            resting
                .iter()
                .map(|entry| entry.order_id)
                .collect::<Vec<_>>(),
            vec![2, 3]
        );
        assert_eq!(engine.cancel(99), Err(RejectReason::UnknownOrderId));
        assert!(engine.check_invariants().is_ok());
    }

    #[test]
    fn cancellation_unlinks_middle_order_and_keeps_fifo_order() {
        let mut engine = MatchingEngine::new();
        engine.submit(order(1, Side::Buy, 100, 1));
        engine.submit(order(2, Side::Buy, 100, 1));
        engine.submit(order(3, Side::Buy, 100, 1));
        engine.cancel(2).unwrap();
        engine.submit(order(4, Side::Buy, 100, 1));

        let result = engine.submit(order(5, Side::Sell, 100, 3));
        assert_eq!(
            result
                .trades
                .iter()
                .map(|trade| trade.maker_order_id)
                .collect::<Vec<_>>(),
            vec![1, 3, 4]
        );
        assert_eq!(engine.live_order_count(), 0);
        assert!(engine.check_invariants().is_ok());
    }

    #[test]
    fn aggregate_level_quantity_exceeds_single_order_u64_boundary() {
        let mut engine = MatchingEngine::new();
        assert!(engine
            .submit(order(1, Side::Buy, 100, u64::MAX))
            .rejection
            .is_none());
        assert!(engine
            .submit(order(2, Side::Buy, 100, 1))
            .rejection
            .is_none());
        assert_eq!(engine.best_bid(), Some((100, u128::from(u64::MAX) + 1)));
        assert_eq!(engine.resting_orders(Side::Buy, 100).len(), 2);
        assert!(engine.check_invariants().is_ok());
    }

    #[derive(Default)]
    struct SlowReference {
        orders: Vec<RestingOrder>,
        used_ids: HashSet<u64>,
        sequence: u64,
    }

    impl SlowReference {
        fn submit(&mut self, request: OrderRequest) -> SubmitResult {
            if request.price_ticks <= 0 {
                return SubmitResult::rejected(RejectReason::InvalidPrice);
            }
            if request.quantity_lots == 0 {
                return SubmitResult::rejected(RejectReason::InvalidQuantity);
            }
            if self.used_ids.contains(&request.order_id) {
                return SubmitResult::rejected(RejectReason::DuplicateOrderId);
            }
            let Some(sequence) = self.sequence.checked_add(1) else {
                return SubmitResult::rejected(RejectReason::SequenceExhausted);
            };
            self.sequence = sequence;
            self.used_ids.insert(request.order_id);
            let mut remaining = request.quantity_lots;
            let mut trades = Vec::new();
            while remaining > 0 {
                let maker_index = self
                    .orders
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
                    .min_by_key(|(_, order)| match request.side {
                        Side::Buy => (order.price_ticks, order.sequence),
                        Side::Sell => (-order.price_ticks, order.sequence),
                    })
                    .map(|(index, _)| index);
                let Some(index) = maker_index else { break };
                let maker = &mut self.orders[index];
                let fill = remaining.min(maker.remaining_lots);
                remaining -= fill;
                trades.push(Trade {
                    sequence,
                    maker_order_id: maker.order_id,
                    taker_order_id: request.order_id,
                    taker_side: request.side,
                    price_ticks: maker.price_ticks,
                    quantity_lots: fill,
                });
                maker.remaining_lots -= fill;
                if maker.remaining_lots == 0 {
                    self.orders.remove(index);
                }
            }
            if remaining > 0 {
                self.orders.push(RestingOrder {
                    order_id: request.order_id,
                    side: request.side,
                    price_ticks: request.price_ticks,
                    remaining_lots: remaining,
                    sequence,
                });
            }
            SubmitResult {
                sequence: Some(sequence),
                trades,
                resting_quantity_lots: remaining,
                self_trade_prevented: false,
                rejection: None,
            }
        }

        fn cancel(&mut self, order_id: u64) -> Result<RestingOrder, RejectReason> {
            let Some(index) = self
                .orders
                .iter()
                .position(|order| order.order_id == order_id)
            else {
                return Err(RejectReason::UnknownOrderId);
            };
            self.sequence = self
                .sequence
                .checked_add(1)
                .ok_or(RejectReason::SequenceExhausted)?;
            Ok(self.orders.remove(index))
        }
    }

    #[test]
    fn seeded_command_sequences_match_independent_slow_reference() {
        for seed in 1..=12_u64 {
            let mut random = seed;
            let mut engine = MatchingEngine::new();
            let mut reference = SlowReference::default();
            let mut next_id = 1_u64;
            for step in 0..2_000 {
                random = random.wrapping_add(0x9e3779b97f4a7c15);
                let choice = splitmix(random);
                if !reference.orders.is_empty() && choice.is_multiple_of(5) {
                    let selected = (splitmix(choice) as usize) % reference.orders.len();
                    let order_id = reference.orders[selected].order_id;
                    assert_eq!(
                        engine.cancel(order_id),
                        reference.cancel(order_id),
                        "seed={seed} step={step}"
                    );
                } else {
                    let side = if choice & 1 == 0 {
                        Side::Buy
                    } else {
                        Side::Sell
                    };
                    let price = 95 + (splitmix(choice) % 11) as i64;
                    let quantity = splitmix(price as u64 ^ choice) % 50 + 1;
                    let request = order(next_id, side, price, quantity);
                    next_id += 1;
                    let actual = engine.submit(request);
                    let expected = reference.submit(request);
                    assert_eq!(actual, expected, "seed={seed} step={step}");
                    let executed: u64 = actual.trades.iter().map(|trade| trade.quantity_lots).sum();
                    assert_eq!(
                        executed + actual.resting_quantity_lots,
                        quantity,
                        "seed={seed} step={step}"
                    );
                    for trade in actual.trades {
                        assert!(trade.quantity_lots > 0);
                        assert_eq!(trade.taker_order_id, request.order_id);
                        match trade.taker_side {
                            Side::Buy => assert!(trade.price_ticks <= request.price_ticks),
                            Side::Sell => assert!(trade.price_ticks >= request.price_ticks),
                        }
                    }
                }
                for side in [Side::Buy, Side::Sell] {
                    for price in 95..=105 {
                        let actual_orders = engine.resting_orders(side, price);
                        let expected_orders: Vec<_> = reference
                            .orders
                            .iter()
                            .copied()
                            .filter(|order| order.side == side && order.price_ticks == price)
                            .collect();
                        assert_eq!(
                            actual_orders, expected_orders,
                            "seed={seed} step={step} side={side:?} price={price}"
                        );
                    }
                }
                assert_eq!(
                    engine.best_bid(),
                    reference
                        .orders
                        .iter()
                        .filter(|o| o.side == Side::Buy)
                        .map(|o| o.price_ticks)
                        .max()
                        .map(|p| (
                            p,
                            reference
                                .orders
                                .iter()
                                .filter(|o| o.side == Side::Buy && o.price_ticks == p)
                                .map(|o| u128::from(o.remaining_lots))
                                .sum()
                        ))
                );
                assert_eq!(
                    engine.best_ask(),
                    reference
                        .orders
                        .iter()
                        .filter(|o| o.side == Side::Sell)
                        .map(|o| o.price_ticks)
                        .min()
                        .map(|p| (
                            p,
                            reference
                                .orders
                                .iter()
                                .filter(|o| o.side == Side::Sell && o.price_ticks == p)
                                .map(|o| u128::from(o.remaining_lots))
                                .sum()
                        ))
                );
                assert!(engine.check_invariants().is_ok(), "seed={seed} step={step}");
            }
        }
    }

    fn splitmix(mut value: u64) -> u64 {
        value = value.wrapping_add(0x9e3779b97f4a7c15);
        value = (value ^ (value >> 30)).wrapping_mul(0xbf58476d1ce4e5b9);
        value = (value ^ (value >> 27)).wrapping_mul(0x94d049bb133111eb);
        value ^ (value >> 31)
    }
}
