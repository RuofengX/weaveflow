use async_trait::async_trait;
use serde_json::Value;
use tracing::debug;

use crate::operator::{Operator, OperatorError, OperatorSpec};

pub struct JsOperator {
    pub name: String,
    pub source: String,
}

#[async_trait]
impl Operator for JsOperator {
    fn spec(&self) -> OperatorSpec {
        OperatorSpec::new(self.name.clone(), "JS 自定义算子 (rquickjs)")
    }

    async fn run(
        &self,
        data: &Value,
        config: &Value,
    ) -> Result<Value, OperatorError> {
        debug!(name = %self.name, "js operator");
        let timeout = config.get("timeout").and_then(|v| v.as_u64()).unwrap_or(30_000);

        let result = tokio::time::timeout(
            std::time::Duration::from_millis(timeout),
            crate::quickjs::run_js(&self.source, "run", data),
        ).await;

        match result {
            Ok(Ok(v)) => Ok(v),
            Ok(Err(e)) => Err(OperatorError::Runtime(format!("JS: {e}"))),
            Err(_) => Err(OperatorError::Timeout),
        }
    }
}

pub struct JsOperatorPlaceholder;
#[async_trait]
impl Operator for JsOperatorPlaceholder {
    fn spec(&self) -> OperatorSpec {
        OperatorSpec::new("js", "JS 自定义算子 (rquickjs)")
    }
    async fn run(&self, _data: &Value, _config: &Value) -> Result<Value, OperatorError> {
        Err(OperatorError::Config("js operator requires inline code".into()))
    }
}
