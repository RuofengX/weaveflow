use async_trait::async_trait;
use serde_json::Value;
use tracing::{debug, warn};

use crate::operator::{Operator, OperatorError, OperatorSpec};

/// LLM 算子：调用 OpenAI 兼容的 chat completions API，直接返回文本内容。
pub struct LlmOperator;

fn extract_images(inputs: &Value) -> (Vec<&str>, bool) {
    let mut found = false;

    let images: Vec<&str> = match inputs.get("images_b64").or_else(|| inputs.get("image_base64")) {
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

    async fn run(
        &self,
        inputs: &Value,
    ) -> Result<Value, OperatorError> {
        debug!("llm request");
        let url = inputs
            .get("url")
            .and_then(|v| v.as_str())
            .ok_or_else(|| OperatorError::Config("llm 缺少 url".into()))?;

        let model = inputs
            .get("model")
            .and_then(|v| v.as_str())
            .ok_or_else(|| OperatorError::Config("llm 缺少 model".into()))?;

        let prompt = inputs
            .get("prompt")
            .and_then(|v| v.as_str())
            .ok_or_else(|| OperatorError::Config("llm 缺少 prompt".into()))?;

        let system = inputs.get("system").and_then(|v| v.as_str());

        let (images, has_images) = extract_images(inputs);
        let skip_vision_check = inputs
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
        let mime_type = inputs
            .get("image_type")
            .and_then(|v| v.as_str())
            .unwrap_or("image/jpeg");
        let max_tokens = inputs
            .get("max_tokens")
            .and_then(|v| v.as_u64())
            .unwrap_or(4096);
        let temperature = inputs
            .get("temperature")
            .and_then(|v| v.as_f64())
            .unwrap_or(0.0);

        let mut messages: Vec<Value> = Vec::new();

        if let Some(sys) = system {
            messages.push(serde_json::json!({ "role": "system", "content": sys }));
        }

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

        let body = serde_json::json!({
            "model": model,
            "messages": messages,
            "max_tokens": max_tokens,
            "temperature": temperature,
        });

        let body_bytes = serde_json::to_vec(&body)
            .map_err(|e| OperatorError::Runtime(format!("llm serialize body: {e}")))?;

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
                format!(" — hint: model '{}' may not support vision/image inputs. ", model)
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

        Ok(Value::String(content.to_string()))
    }
}
