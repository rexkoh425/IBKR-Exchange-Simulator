//! Conservative outstanding-order exposure in integer ticks and lots.

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ClientExposure {
    pub quantity_lots: u128,
    pub notional_ticks_lots: u128,
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
    use super::ClientExposure;

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
