use async_trait::async_trait;
use rayon::prelude::*;
use serde_json::Value;
use tracing::debug;

use crate::operator::builtin::resolve_nested;
use crate::operator::{Operator, OperatorError, OperatorSpec};

pub struct SortOperator;

#[async_trait]
impl Operator for SortOperator {
    fn spec(&self) -> OperatorSpec {
        OperatorSpec::new("sort", "按字段排序数组")
    }

    async fn run(
        &self,
        inputs: &Value,
    ) -> Result<Value, OperatorError> {
        let field = inputs.get("field").and_then(|v| v.as_str()).unwrap_or("");
        debug!(field, "sort operator");
        let order = inputs.get("order").and_then(|v| v.as_str()).unwrap_or("asc");
        let data = inputs.get("data").unwrap_or(&Value::Null);

        let is_array = data.is_array();
        let mut arr: Vec<Value> = match data {
            Value::Array(a) => a.clone(),
            other => vec![other.clone()],
        };

        arr.par_sort_by(|a, b| {
            let va = resolve_nested(a, field);
            let vb = resolve_nested(b, field);
            let cmp = compare_values(va, vb);
            if order == "desc" { cmp.reverse() } else { cmp }
        });

        let output = if is_array {
            Value::Array(arr)
        } else {
            arr.into_iter().next().unwrap_or(Value::Null)
        };
        Ok(output)
    }
}

fn compare_values(a: &Value, b: &Value) -> std::cmp::Ordering {
    use std::cmp::Ordering;
    match (a, b) {
        (Value::Number(x), Value::Number(y)) => {
            x.as_f64().partial_cmp(&y.as_f64()).unwrap_or(Ordering::Equal)
        }
        (Value::String(x), Value::String(y)) => x.cmp(y),
        _ => Ordering::Equal,
    }
}
