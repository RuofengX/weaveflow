use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};
use tracing::{debug, trace};

use serde_json::Value;

use crate::dsl::StepId;

const MIN_REDACT_LEN: usize = 4;

#[derive(Debug, Clone)]
pub struct Scope {
    // Arc + make_mut 写时复制：clone 只 bump 引用计数（O(1)），
    // set_output 在独占时原地修改，共享时才复制 HashMap。
    outputs: Arc<HashMap<StepId, Arc<Value>>>,
    slots: Arc<Value>,
    env_values: Arc<Mutex<HashSet<String>>>,
}

impl Scope {
    pub fn new(slots: HashMap<String, Value>) -> Self {
        let value = Value::Object(slots.into_iter().collect());
        Self {
            outputs: Arc::new(HashMap::new()),
            slots: Arc::new(value),
            env_values: Arc::new(Mutex::new(HashSet::new())),
        }
    }

    pub fn record_env_value(&self, value: &str) {
        if value.len() < MIN_REDACT_LEN {
            return;
        }
        self.env_values
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .insert(value.to_string());
    }

    pub fn env_values(&self) -> HashSet<String> {
        self.env_values
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clone()
    }

    pub fn get_output(&self, step_id: &StepId) -> Option<Arc<Value>> {
        let out = self.outputs.get(step_id).cloned();
        trace!(step = %step_id, found = out.is_some(), "scope get_output");
        out
    }

    pub fn slots(&self) -> Arc<Value> {
        self.slots.clone()
    }

    pub fn set_output(&mut self, step_id: &StepId, value: Value) {
        debug!(step = %step_id, "scope set_output");
        Arc::make_mut(&mut self.outputs).insert(step_id.clone(), Arc::new(value));
    }
}

pub fn redact_env_values(value: &mut Value, secrets: &HashSet<String>) {
    match value {
        Value::String(s) => {
            if secrets.contains(s.as_str()) {
                *s = "***".to_string();
            }
        }
        Value::Array(arr) => {
            for v in arr.iter_mut() {
                redact_env_values(v, secrets);
            }
        }
        Value::Object(map) => {
            for v in map.values_mut() {
                redact_env_values(v, secrets);
            }
        }
        _ => {}
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn record_env_value_skips_short_values() {
        let scope = Scope::new(HashMap::new());
        scope.record_env_value("");
        scope.record_env_value("abc");
        assert!(scope.env_values().is_empty());
        scope.record_env_value("abcd");
        assert!(scope.env_values().contains("abcd"));
    }

    #[test]
    fn env_values_shared_across_clones() {
        let scope = Scope::new(HashMap::new());
        let clone = scope.clone();
        clone.record_env_value("shared-secret");
        assert!(scope.env_values().contains("shared-secret"));
    }

    #[test]
    fn redact_exact_matches_recursively() {
        let secrets: HashSet<String> = ["s3cr3t".to_string()].into_iter().collect();
        let mut v = serde_json::json!({
            "a": "s3cr3t",
            "b": ["s3cr3t", "xs3cr3tx", 1],
            "c": {"d": "s3cr3t"},
            "e": null
        });
        redact_env_values(&mut v, &secrets);
        assert_eq!(v["a"], "***");
        assert_eq!(v["b"][0], "***");
        assert_eq!(v["b"][1], "xs3cr3tx");
        assert_eq!(v["b"][2], 1);
        assert_eq!(v["c"]["d"], "***");
        assert_eq!(v["e"], serde_json::Value::Null);
    }
}
