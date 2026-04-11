//! Deterministic, single-threaded price-time-priority matching for synthetic orders.

use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap, HashSet};

pub type OrderId = u64;
pub type Sequence = u64;
pub type PriceTicks = i64;
pub type QuantityLots = u64;

pub mod benchmark;
pub mod eventlog;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum Side {
    Buy,
    Sell,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct OrderRequest {
    pub order_id: OrderId,
    pub side: Side,
    pub price_ticks: PriceTicks,
    pub quantity_lots: QuantityLots,
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

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RejectReason {
    InvalidPrice,
    InvalidQuantity,
    DuplicateOrderId,
    UnknownOrderId,
    SequenceExhausted,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SubmitResult {
    pub sequence: Option<Sequence>,
    pub trades: Vec<Trade>,
    pub resting_quantity_lots: QuantityLots,
    pub rejection: Option<RejectReason>,
}

impl SubmitResult {
    fn rejected(reason: RejectReason) -> Self {
        Self {
            sequence: None,
            trades: Vec::new(),
            resting_quantity_lots: 0,
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
    total_quantity: QuantityLots,
}

#[derive(Debug, Clone, Copy)]
struct LiveOrder {
    side: Side,
    price_ticks: PriceTicks,
    remaining_lots: QuantityLots,
    sequence: Sequence,
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

    /// Match at eligible best prices, with each execution priced at the resting order.
    pub fn submit(&mut self, request: OrderRequest) -> SubmitResult {
        if request.price_ticks <= 0 {
            return SubmitResult::rejected(RejectReason::InvalidPrice);
        }
        if request.quantity_lots == 0 {
            return SubmitResult::rejected(RejectReason::InvalidQuantity);
        }
        if self.used_order_ids.contains(&request.order_id) {
            return SubmitResult::rejected(RejectReason::DuplicateOrderId);
        }
        let Some(sequence) = self.allocate_sequence() else {
            return SubmitResult::rejected(RejectReason::SequenceExhausted);
        };
        self.used_order_ids.insert(request.order_id);

        let mut remaining = request.quantity_lots;
        let mut trades = Vec::new();
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
            // Borrow the selected side's level map and the live index as
            // disjoint fields so completed makers can leave the index here.
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
                self.book_mut(resting_side)
                    .get_mut(&price)
                    .expect("price level remains live")
                    .total_quantity -= fill_quantity;
            }
        }

        if remaining > 0 {
            let order = RestingOrder {
                order_id: request.order_id,
                side: request.side,
                price_ticks: request.price_ticks,
                remaining_lots: remaining,
                sequence,
            };
            self.insert_live_order(order);
        }
        SubmitResult {
            sequence: Some(sequence),
            trades,
            resting_quantity_lots: remaining,
            rejection: None,
        }
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
        if replacement.price_ticks <= 0 {
            return ReplaceResult::rejected(RejectReason::InvalidPrice);
        }
        if replacement.quantity_lots == 0 {
            return ReplaceResult::rejected(RejectReason::InvalidQuantity);
        }
        if self.used_order_ids.contains(&replacement.order_id) {
            return ReplaceResult::rejected(RejectReason::DuplicateOrderId);
        }
        if !self.live_orders.contains_key(&old_order_id) {
            return ReplaceResult::rejected(RejectReason::UnknownOrderId);
        }
        if self.next_sequence >= Sequence::MAX - 1 {
            return ReplaceResult::rejected(RejectReason::SequenceExhausted);
        }
        self.cancel(old_order_id)
            .expect("order existence and sequence checked");
        let cancelled_sequence = Some(self.next_sequence);
        let result = self.submit(replacement);
        ReplaceResult {
            cancelled_sequence,
            replacement: result,
            rejection: None,
        }
    }

    pub fn best_bid(&self) -> Option<(PriceTicks, QuantityLots)> {
        self.bids
            .iter()
            .next_back()
            .map(|(price, level)| (*price, level.total_quantity))
    }
    pub fn best_ask(&self) -> Option<(PriceTicks, QuantityLots)> {
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
                let mut total_quantity = 0;
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
                    total_quantity += order.remaining_lots;
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

    fn insert_live_order(&mut self, order: RestingOrder) {
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
        level.total_quantity += order.remaining_lots;
        self.live_orders.insert(
            order.order_id,
            LiveOrder {
                side: order.side,
                price_ticks: order.price_ticks,
                remaining_lots: order.remaining_lots,
                sequence: order.sequence,
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
        level.total_quantity -= order.remaining_lots;
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
}
