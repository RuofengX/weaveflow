use std::collections::HashMap;
use std::sync::Arc;
use tracing::{debug, trace};

use serde_json::Value;

use crate::dsl::StepId;

#[derive(Debug, Clone)]
pub struct Scope {
    outputs: HashMap<StepId, Arc<Value>>,
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
        self.outputs.insert(step_id.clone(), Arc::new(value));
    }
}
