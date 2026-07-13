use serde_json::Value;
use sha2::{Digest, Sha256};

pub fn compute_cache_key(op_type: &str, data: &[u8], config: &Value) -> Vec<u8> {
    let mut hasher = Sha256::new();
    hasher.update(op_type.as_bytes());
    hasher.update(b":");
    hasher.update(data);
    hasher.update(b":");
    hasher.update(serde_json::to_vec(config).unwrap_or_default());
    hasher.finalize().to_vec()
}
