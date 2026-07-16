use async_trait::async_trait;
use serde_json::Value;
use tracing::debug;

use crate::operator::builtin::resolve_nested;
use crate::operator::{Operator, OperatorError, OperatorSpec};

pub struct DedupOperator;

#[async_trait]
impl Operator for DedupOperator {
    fn spec(&self) -> OperatorSpec {
        OperatorSpec::new("dedup", "按字段去重数组")
    }

    async fn run(
        &self,
        inputs: &Value,
    ) -> Result<Value, OperatorError> {
        let field = inputs.get("field").and_then(|v| v.as_str()).unwrap_or("");
        debug!(field, "dedup operator");
        let data = inputs.get("data").unwrap_or(&Value::Null);
        let is_array = data.is_array();
        let arr: Vec<Value> = match data {
            Value::Array(a) => a.clone(),
            other => vec![other.clone()],
        };

        let mut seen = std::collections::HashSet::new();
        let mut result = Vec::new();
        for item in arr {
            let key = if field.is_empty() {
                serde_json::to_string(&item).unwrap_or_default()
            } else {
                serde_json::to_string(resolve_nested(&item, field)).unwrap_or_default()
            };
            if seen.insert(key) {
                result.push(item);
            }
        }

        let output = if is_array {
            Value::Array(result)
        } else {
            result.into_iter().next().unwrap_or(Value::Null)
        };
        Ok(output)
    }
}
