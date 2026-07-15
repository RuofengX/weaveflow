use std::borrow::Cow;

use async_trait::async_trait;
use serde_json::Value;

use crate::operator::{Operator, OperatorError, OperatorSpec};

/// JS 自定义算子。通过 rquickjs (QuickJS) 沙箱执行用户 JS。
///
/// `run(data)` — 直接接收上游数据作为入参：
///   - data 为合法 JSON 时 → 解析后的 JSON 值
///   - data 为二进制（非法 JSON）时 → `{ data_base64: "..." }` 对象
///   - data 为空 → `null`
pub struct JsOperator {
    pub name: String,
    pub source: String,
}

#[async_trait]
impl Operator for JsOperator {
    fn spec(&self) -> OperatorSpec {
        OperatorSpec::new(self.name.clone(), "JS 自定义算子 (rquickjs)")
    }

    async fn run<'a>(
        &self,
        data: &'a [u8],
        config: &Value,
    ) -> Result<Cow<'a, [u8]>, OperatorError> {
        let data_val: Value = if data.is_empty() {
            Value::Null
        } else {
            serde_json::from_slice(data).unwrap_or_else(|_| {
                use base64::Engine;
                let b64 =
                    base64::engine::general_purpose::STANDARD.encode(data);
                serde_json::json!({"data_base64": b64})
            })
        };

        let timeout = config.get("timeout").and_then(|v| v.as_u64()).unwrap_or(30_000);

        let result = tokio::time::timeout(
            std::time::Duration::from_millis(timeout),
            crate::quickjs::run_js(&self.source, "run", &data_val),
        ).await;

        match result {
            Ok(Ok(v)) => {
                let bytes = serde_json::to_vec(&v)
                    .map_err(|e| OperatorError::Runtime(format!("JS serialize: {e}")))?;
                Ok(Cow::Owned(bytes))
            }
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
    async fn run<'a>(&self, _data: &'a [u8], _config: &Value) -> Result<Cow<'a, [u8]>, OperatorError> {
        Err(OperatorError::Config("js operator requires inline code".into()))
    }
}
