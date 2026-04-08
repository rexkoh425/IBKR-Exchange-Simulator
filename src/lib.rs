//! Deterministic, single-threaded price-time-priority matching for synthetic orders.

use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};

pub type OrderId = u64;
pub type Sequence = u64;
pub type PriceTicks = i64;
pub type QuantityLots = u64;

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
pub struct MatchingEngine {
    bids: BTreeMap<PriceTicks, VecDeque<RestingOrder>>,
    asks: BTreeMap<PriceTicks, VecDeque<RestingOrder>>,
    live_orders: HashMap<OrderId, (Side, PriceTicks)>,
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
            let (resting_book, live_orders) = match request.side {
                Side::Buy => (&mut self.asks, &mut self.live_orders),
                Side::Sell => (&mut self.bids, &mut self.live_orders),
            };
            let level = resting_book.get_mut(&price).expect("best price exists");
            let maker = level.front_mut().expect("price level is non-empty");
            let fill_quantity = remaining.min(maker.remaining_lots);
            maker.remaining_lots -= fill_quantity;
            remaining -= fill_quantity;
            trades.push(Trade {
                sequence,
                maker_order_id: maker.order_id,
                taker_order_id: request.order_id,
                taker_side: request.side,
                price_ticks: price,
                quantity_lots: fill_quantity,
            });
            if maker.remaining_lots == 0 {
                let maker_id = maker.order_id;
                level.pop_front();
                live_orders.remove(&maker_id);
            }
            if level.is_empty() {
                resting_book.remove(&price);
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
            self.book_mut(request.side)
                .entry(request.price_ticks)
                .or_default()
                .push_back(order);
            self.live_orders
                .insert(request.order_id, (request.side, request.price_ticks));
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
        let (side, price) = *self
            .live_orders
            .get(&order_id)
            .ok_or(RejectReason::UnknownOrderId)?;
        if self.next_sequence == Sequence::MAX {
            return Err(RejectReason::SequenceExhausted);
        }
        self.allocate_sequence()
            .ok_or(RejectReason::SequenceExhausted)?;
        let book = self.book_mut(side);
        let level = book.get_mut(&price).expect("live order has a price level");
        let index = level
            .iter()
            .position(|order| order.order_id == order_id)
            .expect("live order is in price level");
        let removed = level.remove(index).expect("position was found");
        if level.is_empty() {
            book.remove(&price);
        }
        self.live_orders.remove(&order_id);
        Ok(removed)
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
        self.bids.iter().next_back().map(|(price, orders)| {
            (
                *price,
                orders.iter().map(|order| order.remaining_lots).sum(),
            )
        })
    }
    pub fn best_ask(&self) -> Option<(PriceTicks, QuantityLots)> {
        self.asks.iter().next().map(|(price, orders)| {
            (
                *price,
                orders.iter().map(|order| order.remaining_lots).sum(),
            )
        })
    }
    pub fn resting_orders(&self, side: Side, price: PriceTicks) -> Vec<RestingOrder> {
        self.book(side)
            .get(&price)
            .map(|orders| orders.iter().copied().collect())
            .unwrap_or_default()
    }
    pub fn live_order_count(&self) -> usize {
        self.live_orders.len()
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
            for (price, orders) in book {
                add(&mut hash, &price.to_le_bytes());
                for order in orders {
                    add(&mut hash, &order.order_id.to_le_bytes());
                    add(&mut hash, &order.remaining_lots.to_le_bytes());
                    add(&mut hash, &order.sequence.to_le_bytes());
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
            for (price, orders) in book {
                if *price <= 0 || orders.is_empty() {
                    return Err("book contains an invalid or empty price level".into());
                }
                for order in orders {
                    if order.side != side
                        || order.price_ticks != *price
                        || order.remaining_lots == 0
                    {
                        return Err(format!(
                            "order {} has inconsistent book fields",
                            order.order_id
                        ));
                    }
                    if !indexed.insert(order.order_id) {
                        return Err(format!("order {} appears more than once", order.order_id));
                    }
                    if self.live_orders.get(&order.order_id) != Some(&(side, *price)) {
                        return Err(format!(
                            "order {} is missing from live-order index",
                            order.order_id
                        ));
                    }
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
    fn book(&self, side: Side) -> &BTreeMap<PriceTicks, VecDeque<RestingOrder>> {
        match side {
            Side::Buy => &self.bids,
            Side::Sell => &self.asks,
        }
    }
    fn book_mut(&mut self, side: Side) -> &mut BTreeMap<PriceTicks, VecDeque<RestingOrder>> {
        match side {
            Side::Buy => &mut self.bids,
            Side::Sell => &mut self.asks,
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
}
