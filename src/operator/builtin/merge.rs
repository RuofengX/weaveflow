use std::borrow::Cow;

use async_trait::async_trait;
use serde_json::Value;

use crate::operator::{Operator, OperatorError, OperatorSpec};

pub struct MergeOperator;

#[async_trait]
impl Operator for MergeOperator {
    fn spec(&self) -> OperatorSpec {
        OperatorSpec::new("merge", "合并两个对象")
    }

    async fn run<'a>(
        &self,
        data: &'a [u8],
        config: &Value,
    ) -> Result<Cow<'a, [u8]>, OperatorError> {
        let a = if !data.is_empty() {
            serde_json::from_slice(data)
                .map_err(|e| OperatorError::Config(format!("merge parse: {e}")))?
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
                let bytes = serde_json::to_vec(&Value::Object(merged))
                    .map_err(|e| OperatorError::Runtime(format!("merge serialize: {e}")))?;
                Ok(Cow::Owned(bytes))
            }
            _ => Err(OperatorError::Config("a 和 b 必须是对象".into())),
        }
    }
}
