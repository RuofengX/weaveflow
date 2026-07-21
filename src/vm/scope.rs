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
            if secrets.iter().any(|sec| s.contains(sec.as_str())) {
                // 长值优先替换，避免短值先替换破坏长值（子串重叠时结果确定）
                let mut sorted: Vec<&String> = secrets.iter().collect();
                sorted.sort_by_key(|sec| std::cmp::Reverse(sec.len()));
                let mut out = std::mem::take(s);
                for sec in sorted {
                    if out.contains(sec.as_str()) {
                        out = out.replace(sec.as_str(), "***");
                    }
                }
                *s = out;
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
        assert_eq!(v["b"][1], "x***x");
        assert_eq!(v["b"][2], 1);
        assert_eq!(v["c"]["d"], "***");
        assert_eq!(v["e"], serde_json::Value::Null);
    }

    #[test]
    fn redact_substring_matches() {
        let secrets: HashSet<String> = ["tok123".to_string()].into_iter().collect();
        let mut v = serde_json::json!({
            "auth": "Bearer tok123",
            "url": "https://h/api?k=tok123&x=1",
            "plain": "no secret here"
        });
        redact_env_values(&mut v, &secrets);
        assert_eq!(v["auth"], "Bearer ***");
        assert_eq!(v["url"], "https://h/api?k=***&x=1");
        assert_eq!(v["plain"], "no secret here");
    }

    #[test]
    fn redact_overlapping_secrets_longest_first() {
        let secrets: HashSet<String> = ["abcdef".to_string(), "abcd".to_string()]
            .into_iter()
            .collect();
        let mut v = serde_json::json!("xabcdefy");
        redact_env_values(&mut v, &secrets);
        assert_eq!(v, "x***y");
    }
}
