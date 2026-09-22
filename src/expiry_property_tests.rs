use crate::expiry::{ExpiryError, ExpirySchedule};
use proptest::prelude::*;
use std::collections::BTreeMap;

proptest! {
    #[test]
    fn expiry_schedule_matches_simple_map(ops in prop::collection::vec((any::<u8>(), any::<u8>(), any::<u8>()), 1..200)) {
        let mut actual = ExpirySchedule::default();
        let mut expected = BTreeMap::<u64, u64>::new();
        let mut now = 0_u64;
        for (kind, raw_id, raw_time) in ops {
            let id = u64::from(raw_id % 12);
            let time = u64::from(raw_time);
            match kind % 3 {
                0 => {
                    let result = actual.set(id, Some(time));
                    if time > now { expected.insert(id, time); prop_assert!(result.is_ok()); }
                    else { prop_assert_eq!(result, Err(ExpiryError::DeadlineNotFuture)); }
                }
                1 => { actual.set(id, None).unwrap(); expected.remove(&id); }
                _ if time < now => prop_assert_eq!(actual.advance(time), Err(ExpiryError::ClockRegression)),
                _ => {
                    let mut due: Vec<_> = expected.iter().filter(|(_, deadline)| **deadline <= time).map(|(&id, &deadline)| (deadline, id)).collect();
                    due.sort_unstable();
                    prop_assert_eq!(actual.advance(time).unwrap(), due.iter().map(|(_, id)| *id).collect::<Vec<_>>());
                    expected.retain(|_, deadline| *deadline > time);
                    now = time;
                }
            }
            prop_assert_eq!(actual.now(), now);
            for id in 0..12 { prop_assert_eq!(actual.deadline(id), expected.get(&id).copied()); }
            prop_assert!(actual.check_invariants().is_ok());
        }
    }
}
