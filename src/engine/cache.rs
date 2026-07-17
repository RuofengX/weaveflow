use serde_json::Value;
use sha2::{Digest as _, Sha256};
use tracing::trace;

pub fn compute_cache_key(op_type: &str, inputs: &Value) -> Vec<u8> {
    trace!(op_type, "computing cache key");
    let mut hasher = Sha256::new();
    hasher.update(op_type.as_bytes());
    hasher.update(b":");
    hasher.update(serde_json::to_vec(inputs).unwrap_or_default());
    hasher.finalize().to_vec()
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn cache_key_is_sha256_of_full_input() {
        let key = compute_cache_key("noop", &json!({"a": 1}));
        assert_eq!(key.len(), 32);
        let expected = Sha256::digest(b"noop:{\"a\":1}");
        assert_eq!(key, expected.to_vec());
    }

    #[test]
    fn cache_key_differs_by_op_and_inputs() {
        let k1 = compute_cache_key("noop", &json!({"a": 1}));
        let k2 = compute_cache_key("noop", &json!({"a": 2}));
        let k3 = compute_cache_key("var", &json!({"a": 1}));
        assert_ne!(k1, k2);
        assert_ne!(k1, k3);
    }
}
