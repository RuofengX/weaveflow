use async_trait::async_trait;
use serde_json::Value;

use crate::operator::{Operator, OperatorError, OperatorSpec};

pub struct MergeOperator;

#[async_trait]
impl Operator for MergeOperator {
    fn spec(&self) -> OperatorSpec {
        OperatorSpec::new("merge", "合并两个对象")
    }

    async fn run(
        &self,
        data: &Value,
        config: &Value,
    ) -> Result<Value, OperatorError> {
        let a = if !data.is_null() {
            data.clone()
        } else {
            config.get("a").cloned().unwrap_or(Value::Null)
        };

        let b_val = config.get("b")
            .ok_or_else(|| OperatorError::Config("缺少 b 字段".into()))?;

        match (a.as_object(), b_val.as_object()) {
            (Some(oa), Some(ob)) => {
                let mut merged = oa.clone();
                for (k, v) in ob {
                    merged.insert(k.clone(), v.clone());
                }
                Ok(Value::Object(merged))
            }
            _ => Err(OperatorError::Config("a 和 b 必须是对象".into())),
        }
    }
}
