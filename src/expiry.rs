//! Logical order deadlines; callers validate ownership and live order status.

use std::collections::{BTreeSet, HashMap};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExpiryError {
    DeadlineNotFuture,
    ClockRegression,
}

#[derive(Debug, Default)]
pub struct ExpirySchedule {
    now: u64,
    by_order: HashMap<u64, u64>,
    by_deadline: BTreeSet<(u64, u64)>,
}

impl ExpirySchedule {
    pub fn now(&self) -> u64 {
        self.now
    }

    pub fn deadline(&self, order_id: u64) -> Option<u64> {
        self.by_order.get(&order_id).copied()
    }

    pub fn set(&mut self, order_id: u64, deadline: Option<u64>) -> Result<(), ExpiryError> {
        if deadline.is_some_and(|deadline| deadline <= self.now) {
            return Err(ExpiryError::DeadlineNotFuture);
        }
        self.remove(order_id);
        if let Some(deadline) = deadline {
            self.by_order.insert(order_id, deadline);
            self.by_deadline.insert((deadline, order_id));
        }
        Ok(())
    }

    pub fn remove(&mut self, order_id: u64) {
        if let Some(deadline) = self.by_order.remove(&order_id) {
            self.by_deadline.remove(&(deadline, order_id));
        }
    }
}
