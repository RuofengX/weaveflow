use std::borrow::Cow;

use async_trait::async_trait;
use serde_json::Value;

use crate::operator::{Operator, OperatorError, OperatorSpec};

pub struct NoopOperator;

#[async_trait]
impl Operator for NoopOperator {
    fn spec(&self) -> OperatorSpec {
        OperatorSpec::new("noop", "直接透传输入")
    }

    async fn run<'a>(
        &self,
        data: &'a [u8],
        _config: &Value,
    ) -> Result<Cow<'a, [u8]>, OperatorError> {
        Ok(Cow::Borrowed(data))
    }
}
