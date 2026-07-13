use std::borrow::Cow;

use async_trait::async_trait;
use serde_json::Value;

use crate::operator::{Operator, OperatorError, OperatorSpec};

/// JS 自定义算子。通过 rquickjs (QuickJS) 沙箱执行用户 JS。
///
/// 输入处理：
///   - data 为合法 JSON 时 → `input.data` = 解析后的 JSON 值
///   - data 为二进制（非法 JSON）时 → `input.data_base64` = base64 编码的字符串
///   - config 始终映射到 `input.config`
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
        let data_val: Value = serde_json::from_slice(data).unwrap_or(Value::Null);
        let mut input = if let Some(obj) = config.as_object() {
            Value::Object(obj.clone())
        } else {
            serde_json::json!({})
        };

        if data_val == Value::Null && !data.is_empty() {
            use base64::Engine;
            let b64 = base64::engine::general_purpose::STANDARD.encode(data);
            input["data_base64"] = Value::String(b64);
        } else {
            input["data"] = data_val;
        }

        let timeout = config.get("timeout").and_then(|v| v.as_u64()).unwrap_or(30_000);

        let result = tokio::time::timeout(
            std::time::Duration::from_millis(timeout),
            crate::quickjs::run_js(&self.source, "run", &input),
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
