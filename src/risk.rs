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
