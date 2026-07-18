use async_trait::async_trait;
use serde_json::Value;
use tracing::debug;

use crate::operator::{Operator, OperatorError, OperatorSpec};

pub struct JsOperator;

#[async_trait]
impl Operator for JsOperator {
    fn spec(&self) -> OperatorSpec {
        OperatorSpec::new("js", "JS 自定义算子 (rquickjs)")
    }

    async fn run(&self, inputs: Value) -> Result<Value, OperatorError> {
        let code = inputs
            .get("code")
            .and_then(|v| v.as_str())
            .ok_or_else(|| OperatorError::Config("JS code 字段缺失或不是字符串".into()))?;

        debug!("js operator");
        let data = inputs.get("data").unwrap_or(&Value::Null);
        let result = crate::quickjs::run_js(code, "run", data).await;

        match result {
            Ok(v) => Ok(v),
            Err(e) => Err(OperatorError::Runtime(format!("JS: {e}"))),
        }
    }
}
