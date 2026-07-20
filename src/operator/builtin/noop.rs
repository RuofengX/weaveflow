use async_trait::async_trait;
use serde_json::Value;
use tracing::trace;

use crate::operator::{Operator, OperatorError, OperatorSpec};

pub struct NoopOperator;

#[async_trait]
impl Operator for NoopOperator {
    fn spec(&self) -> OperatorSpec {
        OperatorSpec::new("noop", "直接透传输入")
    }

    async fn run(&self, inputs: Value) -> Result<Value, OperatorError> {
        trace!("noop operator");
        Ok(inputs)
    }
}
