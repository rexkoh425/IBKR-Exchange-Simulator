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

    pub fn advance(&mut self, now: u64) -> Result<Vec<u64>, ExpiryError> {
        if now < self.now {
            return Err(ExpiryError::ClockRegression);
        }
        self.now = now;
        let due: Vec<_> = self.due_at(now).collect();
        for (_, order_id) in &due {
            self.remove(*order_id);
        }
        Ok(due.into_iter().map(|(_, order_id)| order_id).collect())
    }

    pub fn pending(&self) -> impl Iterator<Item = (u64, u64)> + '_ {
        self.by_deadline.iter().copied()
    }

    /// Inspect the ordered due set without advancing the clock or removing orders.
    pub fn due_at(&self, now: u64) -> impl Iterator<Item = (u64, u64)> + '_ {
        self.by_deadline.range(..=(now, u64::MAX)).copied()
    }
}

#[cfg(test)]
mod tests {
    use super::{ExpiryError, ExpirySchedule};

    #[test]
    fn advancing_expires_by_deadline_then_order_id() {
        let mut schedule = ExpirySchedule::default();
        schedule.set(9, Some(10)).unwrap();
        schedule.set(2, Some(10)).unwrap();
        schedule.set(3, Some(8)).unwrap();
        assert_eq!(schedule.advance(9).unwrap(), vec![3]);
        assert_eq!(schedule.advance(10).unwrap(), vec![2, 9]);
        assert_eq!(schedule.advance(9), Err(ExpiryError::ClockRegression));
        assert_eq!(schedule.now(), 10);
    }

    #[test]
    fn invalid_or_replaced_deadlines_do_not_expire_early() {
        let mut schedule = ExpirySchedule::default();
        schedule.advance(5).unwrap();
        schedule.set(7, Some(12)).unwrap();
        assert_eq!(
            schedule.set(7, Some(5)),
            Err(ExpiryError::DeadlineNotFuture)
        );
        assert_eq!(schedule.deadline(7), Some(12));
        schedule.set(7, Some(20)).unwrap();
        assert!(schedule.advance(12).unwrap().is_empty());
        schedule.set(7, None).unwrap();
        assert_eq!(schedule.deadline(7), None);
        assert!(schedule.advance(20).unwrap().is_empty());
    }

    #[test]
    fn inspecting_due_orders_does_not_change_the_schedule() {
        let mut schedule = ExpirySchedule::default();
        schedule.set(3, Some(10)).unwrap();
        schedule.set(2, Some(10)).unwrap();
        assert_eq!(
            schedule.due_at(10).collect::<Vec<_>>(),
            vec![(10, 2), (10, 3)]
        );
        assert_eq!(schedule.now(), 0);
        assert_eq!(schedule.advance(10).unwrap(), vec![2, 3]);
    }
}
