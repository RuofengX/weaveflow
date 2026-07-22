use async_trait::async_trait;
use rayon::prelude::*;
use serde_json::Value;
use tracing::debug;

use crate::operator::builtin::{compare_json_numbers, resolve_nested};
use crate::operator::{Operator, OperatorError, OperatorSpec};

pub struct FilterOperator;

impl FilterOperator {
    fn compare(a: &Value, op: &str, b: &Value) -> Option<bool> {
        match op {
            "eq" => Some(a == b),
            "ne" => Some(a != b),
            "gt" => compare_json_numbers(a, b).map(|o| o == std::cmp::Ordering::Greater),
            "gte" => compare_json_numbers(a, b).map(|o| o != std::cmp::Ordering::Less),
            "lt" => compare_json_numbers(a, b).map(|o| o == std::cmp::Ordering::Less),
            "lte" => compare_json_numbers(a, b).map(|o| o != std::cmp::Ordering::Greater),
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

    async fn run(&self, inputs: Value) -> Result<Value, OperatorError> {
        let field = inputs
            .get("field")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        debug!(field, "filter operator");
        let operator = inputs
            .get("operator")
            .and_then(|v| v.as_str())
            .unwrap_or("eq")
            .to_string();
        if !matches!(
            operator.as_str(),
            "eq" | "ne" | "gt" | "gte" | "lt" | "lte" | "in" | "contains"
        ) {
            return Err(OperatorError::Config(format!(
                "filter 不支持 operator: {operator}（可选: eq/ne/gt/gte/lt/lte/in/contains）"
            )));
        }
        let ref_value = inputs.get("value").cloned().unwrap_or(Value::Null);
        // 操作数类型前置校验：in 的 value 必须是数组、contains 的 value 必须是字符串，
        // 否则 compare 恒为 None → 静默返回空数组（配置错误被吞 = 静默丢数据）。
        match operator.as_str() {
            "in" if !ref_value.is_array() => {
                return Err(OperatorError::Config(
                    "filter operator=in 要求 inputs.value 为数组".into(),
                ));
            }
            "contains" if !ref_value.is_string() => {
                return Err(OperatorError::Config(
                    "filter operator=contains 要求 inputs.value 为字符串".into(),
                ));
            }
            _ => {}
        }
        let data = if let Value::Object(mut m) = inputs {
            match m.remove("data") {
                Some(v) if !v.is_null() => v,
                _ => {
                    return Err(OperatorError::Config(
                        "filter 算子 inputs.data 缺失或为 null".into(),
                    ));
                }
            }
        } else {
            inputs
        };

        let is_array = data.is_array();
        let arr: Vec<Value> = match data {
            Value::Array(a) => a,
            other => vec![other],
        };

        let result: Vec<Value> = arr
            .into_par_iter()
            .filter(|item| {
                Self::compare(resolve_nested(item, &field), &operator, &ref_value).unwrap_or(false)
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

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[tokio::test]
    async fn big_integer_gt_is_exact() {
        let op = FilterOperator;
        let inputs = json!({
            "data": [9007199254740992_i64, 9007199254740993_i64],
            "operator": "gt",
            "value": 9007199254740992_i64
        });
        let out = op.run(inputs).await.expect("run");
        assert_eq!(out, json!([9007199254740993_i64]));
    }

    #[tokio::test]
    async fn big_integer_lt_is_exact() {
        let op = FilterOperator;
        let inputs = json!({
            "data": [9007199254740992_i64, 9007199254740993_i64],
            "operator": "lt",
            "value": 9007199254740993_i64
        });
        let out = op.run(inputs).await.expect("run");
        assert_eq!(out, json!([9007199254740992_i64]));
    }

    #[tokio::test]
    async fn float_fallback_still_works() {
        let op = FilterOperator;
        let inputs = json!({
            "data": [1.5, 2.5, 3.5],
            "operator": "gt",
            "value": 2.0
        });
        let out = op.run(inputs).await.expect("run");
        assert_eq!(out, json!([2.5, 3.5]));
    }

    #[tokio::test]
    async fn in_with_non_array_value_returns_config_error() {
        // 配置错误必须显式失败，而非静默返回空数组（ETL 静默丢数据）
        let op = FilterOperator;
        let err = op
            .run(json!({ "data": [1, 2, 3], "operator": "in", "value": 2 }))
            .await
            .expect_err("must fail");
        assert!(matches!(err, OperatorError::Config(_)));
    }

    #[tokio::test]
    async fn contains_with_non_string_value_returns_config_error() {
        let op = FilterOperator;
        let err = op
            .run(json!({ "data": ["ab"], "operator": "contains", "value": 1 }))
            .await
            .expect_err("must fail");
        assert!(matches!(err, OperatorError::Config(_)));
    }

    #[tokio::test]
    async fn unknown_operator_returns_config_error() {
        let op = FilterOperator;
        let inputs = json!({ "data": [1, 2], "operator": "like", "value": 1 });
        let err = op.run(inputs).await.expect_err("must fail");
        assert!(matches!(err, OperatorError::Config(_)));
    }

    #[tokio::test]
    async fn missing_data_returns_config_error() {
        let op = FilterOperator;
        let err = op
            .run(json!({ "operator": "eq", "value": 1 }))
            .await
            .expect_err("must fail");
        assert!(matches!(err, OperatorError::Config(_)));
        let err = op
            .run(json!({ "data": null, "operator": "eq", "value": 1 }))
            .await
            .expect_err("must fail");
        assert!(matches!(err, OperatorError::Config(_)));
    }
}
