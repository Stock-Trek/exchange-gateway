use chrono::Utc;
use serde::{Deserialize, Serialize};
use std::hash::{DefaultHasher, Hash, Hasher};
use uuid::Uuid;

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct TimeOrderedId(pub String);

impl TimeOrderedId {
    pub fn new<T>(hashable: &T) -> Self
    where
        T: Hash,
    {
        let mut state = DefaultHasher::new();
        hashable.hash(&mut state);
        let high_bits = Utc::now().timestamp_millis() as u64;
        let low_bits = state.finish();
        let id = Uuid::from_u64_pair(high_bits, low_bits).to_string();
        Self(id)
    }
}
