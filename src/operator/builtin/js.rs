use async_trait::async_trait;
use serde_json::Value;
use tracing::debug;

use crate::operator::{Operator, OperatorError, OperatorSpec};

/// JS 沙箱算子。code 字段从 config 中读取，与其他算子输入统一由 resolve_inputs 解析。
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
        let timeout = inputs.get("timeout").and_then(|v| v.as_u64()).unwrap_or(30_000);

        debug!("js operator");
        let data = inputs.get("data").unwrap_or(&Value::Null);
        let result = tokio::time::timeout(
            std::time::Duration::from_millis(timeout),
            crate::quickjs::run_js(code, "run", data),
        )
        .await;

        match result {
            Ok(Ok(v)) => Ok(v),
            Ok(Err(e)) => Err(OperatorError::Runtime(format!("JS: {e}"))),
            Err(_) => Err(OperatorError::Timeout),
        }
    }
}
