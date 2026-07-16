use async_trait::async_trait;
use serde_json::Value;
use tracing::trace;

use crate::operator::{Operator, OperatorError, OperatorSpec};

/// var: 将 inputs 输出，供下游引用。
pub struct VarOperator;

#[async_trait]
impl Operator for VarOperator {
    fn spec(&self) -> OperatorSpec {
        OperatorSpec::new("var", "变量占位——将输入序列化输出，供下游引用")
    }

    async fn run(
        &self,
        inputs: Value,
    ) -> Result<Value, OperatorError> {
        trace!("var operator (passthrough)");
        Ok(inputs)
    }
}
