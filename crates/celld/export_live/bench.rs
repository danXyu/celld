//! Drives the production delivery-state cache at a fixed working-set size.
use super::{DeliveryStates, Position, StreamId};

pub struct DeliveryCache {
    states: DeliveryStates,
    keys: Vec<(String, u64, u64)>,
    next: usize,
}
impl DeliveryCache {
    pub fn new(history: usize, active: usize) -> Self {
        assert!(active > 0 && active <= history);
        let mut states = DeliveryStates::new().unwrap();
        for i in 0..history {
            let cell = format!("Items:{i}");
            let state = states.get((cell.clone(), 1, 1)).unwrap();
            state.stream = Some((
                StreamId {
                    script: "bench".into(),
                    class: "Items".into(),
                    cell,
                    facet: None,
                    incarnation: 1,
                },
                None,
            ));
            state.marked = Some(Position::new(1, 1, 1));
        }
        let mut cache = Self {
            states,
            keys: (0..active).map(|i| (format!("Items:{i}"), 1, 1)).collect(),
            next: 0,
        };
        // Rewarm the active set and verify positions survive a full spill cycle.
        assert_eq!(cache.advance(active), 2 * active as u64);
        assert_eq!(cache.advance(active), 3 * active as u64);
        cache
    }

    /// One acknowledgement batch; the cold store uses the same transaction as delivery.
    pub fn advance(&mut self, records: usize) -> u64 {
        self.states.cold.execute_batch("BEGIN").unwrap();
        let mut sum = 0;
        for _ in 0..records {
            let key = self.keys[self.next].clone();
            self.next = (self.next + 1) % self.keys.len();
            let state = self.states.get(key).unwrap();
            let previous = state.marked.expect("spilled watermark preserved");
            let position = Position::new(1, previous.txid + 1, previous.commit + 1);
            state.position = Some(position);
            state.marked = Some(position);
            sum += position.txid;
        }
        self.states.cold.execute_batch("COMMIT").unwrap();
        sum
    }
}
