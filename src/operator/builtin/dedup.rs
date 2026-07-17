use async_trait::async_trait;
use serde_json::Value;
use tracing::{debug, warn};

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
        inputs: Value,
    ) -> Result<Value, OperatorError> {
        let field = inputs.get("field").and_then(|v| v.as_str()).unwrap_or("").to_string();
        debug!(field, "dedup operator");
        let data = if let Value::Object(mut m) = inputs {
            m.remove("data").unwrap_or(Value::Null)
        } else {
            inputs
        };
        let is_array = data.is_array();
        let arr: Vec<Value> = match data {
            Value::Array(a) => a,
            other => vec![other],
        };

        let mut seen = std::collections::HashSet::new();
        let mut result = Vec::new();
        for item in arr {
            if !field.is_empty() && resolve_nested(&item, &field).is_null() {
                warn!(field = %field, "dedup 字段缺失，元素跳过判重直接保留");
                result.push(item);
                continue;
            }
            let key = if field.is_empty() {
                serde_json::to_string(&item).unwrap_or_default()
            } else {
                serde_json::to_string(resolve_nested(&item, &field)).unwrap_or_default()
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

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[tokio::test]
    async fn missing_field_items_are_kept_not_collapsed() {
        let op = DedupOperator;
        let inputs = json!({
            "data": [
                { "id": 1, "name": "a" },
                { "name": "no_id_1" },
                { "name": "no_id_2" },
                { "id": 1, "name": "a_dup" }
            ],
            "field": "id"
        });
        let out = op.run(inputs).await.expect("run");
        let arr = out.as_array().expect("array");
        assert_eq!(arr.len(), 3);
        assert_eq!(arr[1]["name"], json!("no_id_1"));
        assert_eq!(arr[2]["name"], json!("no_id_2"));
    }

    #[tokio::test]
    async fn present_field_still_dedups() {
        let op = DedupOperator;
        let inputs = json!({
            "data": [
                { "id": 1 },
                { "id": 2 },
                { "id": 1 }
            ],
            "field": "id"
        });
        let out = op.run(inputs).await.expect("run");
        assert_eq!(out.as_array().expect("array").len(), 2);
    }
}
