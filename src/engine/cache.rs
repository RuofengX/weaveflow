use std::hash::{DefaultHasher, Hasher};

use serde_json::Value;
use tracing::trace;

pub fn compute_cache_key(op_type: &str, data: &Value, config: &Value) -> Vec<u8> {
    trace!(op_type, "computing cache key");
    let mut hasher = DefaultHasher::new();
    hasher.write(op_type.as_bytes());
    hasher.write(b":");
    hasher.write(&serde_json::to_vec(data).unwrap_or_default());
    hasher.write(b":");
    hasher.write(&serde_json::to_vec(config).unwrap_or_default());
    hasher.finish().to_le_bytes().to_vec()
}
