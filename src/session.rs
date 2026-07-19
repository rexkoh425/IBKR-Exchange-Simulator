//! Deterministic trading-session state for the simulated exchange.

use serde::{Deserialize, Serialize};

/// Explicit session state; no system clock or market calendar is consulted.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SessionState {
    #[default]
    Open,
    Closed,
}

impl SessionState {
    pub fn allows_new_orders(self) -> bool {
        self == Self::Open
    }
}

#[cfg(test)]
mod tests {
    use super::SessionState;

    #[test]
    fn session_states_have_stable_wire_names_and_open_default() {
        assert_eq!(SessionState::default(), SessionState::Open);
        assert_eq!(
            serde_json::to_string(&SessionState::Closed).unwrap(),
            "\"closed\""
        );
        assert_eq!(
            serde_json::from_str::<SessionState>("\"open\"").unwrap(),
            SessionState::Open
        );
        assert!(!SessionState::Closed.allows_new_orders());
    }
}
