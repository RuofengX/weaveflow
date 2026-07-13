use std::borrow::Cow;

use async_trait::async_trait;
use serde_json::Value;

use crate::operator::{Operator, OperatorError, OperatorSpec};

/// var: 将 inputs 序列化为 bytes 输出，供下游引用。
pub struct VarOperator;

#[async_trait]
impl Operator for VarOperator {
    fn spec(&self) -> OperatorSpec {
        OperatorSpec::new("var", "变量占位——将输入序列化输出，供下游引用")
    }

    async fn run<'a>(
        &self,
        _data: &'a [u8],
        config: &Value,
    ) -> Result<Cow<'a, [u8]>, OperatorError> {
        let bytes = serde_json::to_vec(config)
            .map_err(|e| OperatorError::Runtime(format!("var serialize: {e}")))?;
        Ok(Cow::Owned(bytes))
    }
}
