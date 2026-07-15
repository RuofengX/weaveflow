use serde_json::Value;
use sha2::{Digest, Sha256};
use tracing::trace;

pub fn compute_cache_key(op_type: &str, data: &Value, config: &Value) -> Vec<u8> {
    trace!(op_type, "computing cache key");
    let mut hasher = Sha256::new();
    hasher.update(op_type.as_bytes());
    hasher.update(b":");
    hasher.update(serde_json::to_vec(data).unwrap_or_default());
    hasher.update(b":");
    hasher.update(serde_json::to_vec(config).unwrap_or_default());
    hasher.finalize().to_vec()
}
