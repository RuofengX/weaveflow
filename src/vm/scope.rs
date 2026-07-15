use std::collections::HashMap;
use std::sync::Arc;

use serde_json::Value;

#[derive(Debug, Clone)]
pub struct Scope {
    outputs: HashMap<String, Arc<Value>>,
    slots: Arc<Value>,
}

impl Scope {
    pub fn new(slots: HashMap<String, Value>) -> Self {
        let value = Value::Object(
            slots.into_iter().collect()
        );
        Self {
            outputs: HashMap::new(),
            slots: Arc::new(value),
        }
    }

    pub fn get_output(&self, step_id: &str) -> Option<Arc<Value>> {
        self.outputs.get(step_id).cloned()
    }

    pub fn slots(&self) -> Arc<Value> {
        self.slots.clone()
    }

    pub fn set_output(&mut self, step_id: &str, value: Value) {
        self.outputs.insert(step_id.to_string(), Arc::new(value));
    }
}
