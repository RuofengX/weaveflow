use std::borrow::Cow;

use async_trait::async_trait;
use serde_json::Value;

use crate::operator::{Operator, OperatorError, OperatorSpec};

pub struct SplitOperator;

#[async_trait]
impl Operator for SplitOperator {
    fn spec(&self) -> OperatorSpec {
        OperatorSpec::new("split", "将数组切分为小块")
    }

    async fn run<'a>(
        &self,
        data: &'a [u8],
        config: &Value,
    ) -> Result<Cow<'a, [u8]>, OperatorError> {
        let size = config.get("size").and_then(|v| v.as_u64()).unwrap_or(100) as usize;
        let value: Value = serde_json::from_slice(data)
            .map_err(|e| OperatorError::Config(format!("split parse: {e}")))?;
        let arr: Vec<Value> = match value {
            Value::Array(a) => a,
            other => vec![other],
        };

        let chunks: Vec<Value> = arr
            .chunks(size)
            .map(|c| Value::Array(c.to_vec()))
            .collect();

        let bytes = serde_json::to_vec(&chunks)
            .map_err(|e| OperatorError::Runtime(format!("split serialize: {e}")))?;
        Ok(Cow::Owned(bytes))
    }
}
