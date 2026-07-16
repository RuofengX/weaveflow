use async_trait::async_trait;
use rayon::prelude::*;
use serde_json::Value;
use tracing::debug;

use crate::operator::builtin::resolve_nested;
use crate::operator::{Operator, OperatorError, OperatorSpec};

pub struct FilterOperator;

impl FilterOperator {
    fn compare(a: &Value, op: &str, b: &Value) -> Option<bool> {
        match op {
            "eq" => Some(a == b),
            "ne" => Some(a != b),
            "gt" => a.as_f64().zip(b.as_f64()).map(|(x, y)| x > y),
            "gte" => a.as_f64().zip(b.as_f64()).map(|(x, y)| x >= y),
            "lt" => a.as_f64().zip(b.as_f64()).map(|(x, y)| x < y),
            "lte" => a.as_f64().zip(b.as_f64()).map(|(x, y)| x <= y),
            "in" => b.as_array().map(|arr| arr.contains(a)),
            "contains" => {
                let sa = a.as_str()?;
                let sb = b.as_str()?;
                Some(sa.contains(sb))
            }
            _ => None,
        }
    }
}

#[async_trait]
impl Operator for FilterOperator {
    fn spec(&self) -> OperatorSpec {
        OperatorSpec::new("filter", "按条件过滤数组元素")
    }

    async fn run(
        &self,
        inputs: &Value,
    ) -> Result<Value, OperatorError> {
        let field = inputs.get("field").and_then(|v| v.as_str()).unwrap_or("");
        debug!(field, "filter operator");
        let operator = inputs.get("operator").and_then(|v| v.as_str()).unwrap_or("eq");
        let ref_value = inputs.get("value").unwrap_or(&Value::Null);
        let data = inputs.get("data").unwrap_or(&Value::Null);

        let is_array = data.is_array();
        let arr: Vec<Value> = match data {
            Value::Array(a) => a.clone(),
            other => vec![other.clone()],
        };

        let result: Vec<Value> = arr
            .into_par_iter()
            .filter(|item| {
                Self::compare(resolve_nested(item, field), operator, ref_value).unwrap_or(false)
            })
            .collect();

        let output = if is_array {
            Value::Array(result)
        } else {
            result.into_iter().next().unwrap_or(Value::Null)
        };
        Ok(output)
    }
}
