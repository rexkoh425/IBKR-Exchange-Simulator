//! Conservative outstanding-order exposure in integer ticks and lots.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ClientExposure {
    pub quantity_lots: u128,
    pub notional_ticks_lots: u128,
}

/// Maintains aggregate exposure for clients with active configured limits.
#[derive(Debug, Default)]
pub struct ExposureLedger {
    clients: HashMap<u64, ClientExposure>,
}

impl ExposureLedger {
    pub fn get(&self, client_id: u64) -> ClientExposure {
        self.clients.get(&client_id).copied().unwrap_or_default()
    }

    pub fn add(&mut self, client_id: u64, price: i64, quantity: u64) -> Option<()> {
        let next = self.get(client_id).checked_add_order(price, quantity)?;
        self.clients.insert(client_id, next);
        Some(())
    }

    pub fn subtract(&mut self, client_id: u64, price: i64, quantity: u64) -> Option<()> {
        let next = self.get(client_id).checked_sub_order(price, quantity)?;
        if next == ClientExposure::default() {
            self.clients.remove(&client_id);
        } else {
            self.clients.insert(client_id, next);
        }
        Some(())
    }

    pub fn clients(&self) -> impl Iterator<Item = (u64, ClientExposure)> + '_ {
        self.clients.iter().map(|(&id, &exposure)| (id, exposure))
    }
}

impl ClientExposure {
    pub fn checked_add_order(self, price_ticks: i64, quantity_lots: u64) -> Option<Self> {
        let price = u128::try_from(price_ticks)
            .ok()
            .filter(|price| *price > 0)?;
        let quantity = u128::from(quantity_lots);
        Some(Self {
            quantity_lots: self.quantity_lots.checked_add(quantity)?,
            notional_ticks_lots: self
                .notional_ticks_lots
                .checked_add(price.checked_mul(quantity)?)?,
        })
    }

    pub fn checked_sub_order(self, price_ticks: i64, quantity_lots: u64) -> Option<Self> {
        let price = u128::try_from(price_ticks)
            .ok()
            .filter(|price| *price > 0)?;
        let quantity = u128::from(quantity_lots);
        Some(Self {
            quantity_lots: self.quantity_lots.checked_sub(quantity)?,
            notional_ticks_lots: self
                .notional_ticks_lots
                .checked_sub(price.checked_mul(quantity)?)?,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::{ClientExposure, ExposureLedger};

    #[test]
    fn ledger_keeps_client_totals_separate_and_rejects_invalid_updates() {
        let mut ledger = ExposureLedger::default();
        ledger.add(1, 100, 3).unwrap();
        ledger.add(2, 200, 2).unwrap();
        assert!(ledger.add(1, -1, 1).is_none());
        assert_eq!(ledger.get(1).notional_ticks_lots, 300);
        assert_eq!(ledger.get(2).quantity_lots, 2);
        assert_eq!(ledger.get(3), ClientExposure::default());
    }

    #[test]
    fn ledger_removes_empty_clients_and_rejects_over_release() {
        let mut ledger = ExposureLedger::default();
        ledger.add(1, 100, 3).unwrap();
        assert!(ledger.subtract(1, 100, 4).is_none());
        assert_eq!(ledger.get(1).quantity_lots, 3);
        ledger.subtract(1, 100, 3).unwrap();
        assert_eq!(ledger.clients().count(), 0);
    }

    #[test]
    fn exposure_add_and_release_preserve_quantity_and_notional() {
        let value = ClientExposure::default()
            .checked_add_order(100, 3)
            .unwrap()
            .checked_add_order(120, 2)
            .unwrap();
        assert_eq!(value.quantity_lots, 5);
        assert_eq!(value.notional_ticks_lots, 540);
        let reduced = value.checked_sub_order(100, 1).unwrap();
        assert_eq!(
            (reduced.quantity_lots, reduced.notional_ticks_lots),
            (4, 440)
        );
    }

    #[test]
    fn exposure_arithmetic_rejects_overflow_underflow_and_invalid_prices() {
        let maximum = ClientExposure {
            quantity_lots: u128::MAX,
            notional_ticks_lots: u128::MAX,
        };
        assert!(maximum.checked_add_order(1, 1).is_none());
        assert!(ClientExposure::default().checked_sub_order(1, 1).is_none());
        assert!(ClientExposure::default().checked_add_order(0, 1).is_none());
        assert!(ClientExposure::default().checked_add_order(-1, 1).is_none());
    }
}
