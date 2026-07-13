use std::borrow::Cow;

use async_trait::async_trait;
use serde_json::Value;
use tracing::warn;

use crate::operator::{Operator, OperatorError, OperatorSpec};

/// LLM 算子：调用 OpenAI 兼容的 chat completions API，直接返回文本内容。
///
/// # Config 字段
/// - `url` (string, 必需): API endpoint，如 `http://spark:8000/v1/chat/completions`
/// - `model` (string, 必需): 模型名
/// - `prompt` (string, 必需): 用户提示词
/// - `system` (string, 可选): 系统提示词
/// - `images_b64` 或 `image_base64` (string 或 string[], 可选): base64 图片列表
/// - `image_type` (string, 可选, 默认 "image/jpeg"): data URI 的 MIME type，如 "application/pdf"
/// - `max_tokens` (u64, 可选, 默认 4096)
/// - `temperature` (f64, 可选, 默认 0.0)
/// - `skip_vision_check` (bool, 可选, 默认 false): 跳过视觉模型能力预检
///
/// # 输出
/// 返回 choices[0].message.content 文本的 JSON 字符串。
pub struct LlmOperator;

/// 提取图片 base64 列表，兼容 `images_b64` 和 `image_base64` 两个字段名。
fn extract_images(config: &Value) -> (Vec<&str>, bool) {
    let mut found = false;

    let images: Vec<&str> = match config.get("images_b64").or_else(|| config.get("image_base64")) {
        Some(Value::String(s)) => { found = true; vec![s.as_str()] }
        Some(Value::Array(arr)) => {
            let imgs: Vec<&str> = arr.iter().filter_map(|v| v.as_str()).collect();
            found = !imgs.is_empty();
            imgs
        }
        _ => Vec::new(),
    };

    (images, found)
}

#[async_trait]
impl Operator for LlmOperator {
    fn spec(&self) -> OperatorSpec {
        OperatorSpec::new("llm", "OpenAI 兼容的 LLM 推理，返回文本内容").with_cache(false)
    }

    async fn run<'a>(
        &self,
        _data: &'a [u8],
        config: &Value,
    ) -> Result<Cow<'a, [u8]>, OperatorError> {
        let url = config
            .get("url")
            .and_then(|v| v.as_str())
            .ok_or_else(|| OperatorError::Config("llm 缺少 url".into()))?;

        let model = config
            .get("model")
            .and_then(|v| v.as_str())
            .ok_or_else(|| OperatorError::Config("llm 缺少 model".into()))?;

        let prompt = config
            .get("prompt")
            .and_then(|v| v.as_str())
            .ok_or_else(|| OperatorError::Config("llm 缺少 prompt".into()))?;

        let system = config.get("system").and_then(|v| v.as_str());

        // images_b64 / image_base64: string → 单张，array → 多张
        let (images, has_images) = extract_images(config);
        let skip_vision_check = config
            .get("skip_vision_check")
            .and_then(|v| v.as_bool())
            .unwrap_or(false);

        if has_images && !skip_vision_check {
            warn!(
                model = %model,
                image_count = images.len(),
                "images provided to llm operator — ensure model '{model}' supports vision/image inputs. Set skip_vision_check=true to suppress this warning."
            );
        }
        let mime_type = config
            .get("image_type")
            .and_then(|v| v.as_str())
            .unwrap_or("image/jpeg");
        let max_tokens = config
            .get("max_tokens")
            .and_then(|v| v.as_u64())
            .unwrap_or(4096);
        let temperature = config
            .get("temperature")
            .and_then(|v| v.as_f64())
            .unwrap_or(0.0);

        // ── 构造 messages ──
        let mut messages: Vec<Value> = Vec::new();

        if let Some(sys) = system {
            messages.push(serde_json::json!({ "role": "system", "content": sys }));
        }

        // user message: text + optional images
        let mut user_content: Vec<Value> =
            vec![serde_json::json!({ "type": "text", "text": prompt })];

        for b64 in &images {
            user_content.push(serde_json::json!({
                "type": "image_url",
                "image_url": { "url": format!("data:{mime_type};base64,{b64}") }
            }));
        }

        messages.push(serde_json::json!({
            "role": "user",
            "content": user_content
        }));

        // ── 构造请求体 ──
        let body = serde_json::json!({
            "model": model,
            "messages": messages,
            "max_tokens": max_tokens,
            "temperature": temperature,
        });

        let body_bytes = serde_json::to_vec(&body)
            .map_err(|e| OperatorError::Runtime(format!("llm serialize body: {e}")))?;

        // ── HTTP POST ──
        let client = reqwest::Client::new();
        let resp = client
            .post(url)
            .header("Content-Type", "application/json")
            .body(body_bytes)
            .send()
            .await
            .map_err(|e| OperatorError::Runtime(format!("llm request: {e}")))?;

        let status = resp.status();
        let body_text = resp
            .text()
            .await
            .map_err(|e| OperatorError::Runtime(format!("llm read response: {e}")))?;

        if !status.is_success() {
            return Err(OperatorError::Runtime(format!(
                "llm HTTP {status}: {}",
                &body_text[..body_text.len().min(500)]
            )));
        }

        // ── 解析响应 ──
        let resp_json: Value = serde_json::from_str(&body_text).map_err(|e| {
            OperatorError::Runtime(format!("llm parse response: {e}"))
        })?;

        if let Some(err) = resp_json.get("error") {
            let msg = err
                .get("message")
                .and_then(|v| v.as_str())
                .unwrap_or("unknown error");
            let code = err.get("code").and_then(|v| v.as_str()).unwrap_or("");

            let hint = if has_images
                && (msg.contains("image")
                    || msg.contains("vision")
                    || msg.contains("multimodal")
                    || code.contains("invalid_request"))
            {
                format!(
                    " — hint: model '{}' may not support vision/image inputs. "
                    , model
                )
            } else {
                String::new()
            };

            return Err(OperatorError::Runtime(format!(
                "llm API error: {msg}{hint}"
            )));
        }

        let content = resp_json
            .get("choices")
            .and_then(|c| c.get(0))
            .and_then(|c| c.get("message"))
            .and_then(|m| m.get("content"))
            .and_then(|c| c.as_str())
            .ok_or_else(|| {
                OperatorError::Runtime("llm response missing choices[0].message.content".into())
            })?;

        let result = serde_json::Value::String(content.to_string());
        let bytes = serde_json::to_vec(&result)
            .map_err(|e| OperatorError::Runtime(format!("llm serialize output: {e}")))?;

        Ok(Cow::Owned(bytes))
    }
}
