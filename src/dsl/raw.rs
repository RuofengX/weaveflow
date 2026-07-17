use std::collections::HashMap;

use serde::Deserialize;
use serde_json::Value;

use super::pipeline::PipelineDef;
use super::retry::RetryDef;
use super::step::{BatchConfig, IterateConfig, StepDef, StepId};
use super::step_op::{self, StepOp};
use super::storage::StorageDef;
use super::variable::{parse_string_to_refvalue, RefValue, VariablePath};

// ---------------------------------------------------------------------------
// Raw pipeline — no RefValue, no catch-all HashMap on steps
// ---------------------------------------------------------------------------

#[derive(Deserialize)]
pub struct RawPipelineDef {
    pub name: String,
    #[serde(default)]
    pub description: Option<String>,
    #[serde(default)]
    pub storage: Option<StorageDef>,
    #[serde(default)]
    pub slots: Vec<super::pipeline::SlotDef>,
    #[serde(default)]
    pub steps: Vec<RawStepDef>,
    pub output: String,
}

#[derive(Deserialize)]
pub struct RawStepDef {
    pub id: String,
    #[serde(default)]
    pub after: Option<Vec<String>>,
    pub iterate: Option<RawIterateConfig>,
    pub cache: Option<bool>,
    pub retry: Option<RetryDef>,
    #[serde(default, alias = "timeout")]
    pub timeout_sec: Option<f64>,

    #[serde(flatten)]
    pub op: RawStepOp,
    #[serde(flatten)]
    pub extra: HashMap<String, Value>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RawIterateConfig {
    pub over: String,
    #[serde(rename = "as")]
    pub as_name: String,
    #[serde(default)]
    pub max_workers: Option<u32>,
    #[serde(default)]
    pub batch: Option<BatchConfig>,
}
// ---------------------------------------------------------------------------
// Raw step operators — one struct per operator, plain types only
// ---------------------------------------------------------------------------

#[derive(Deserialize)]
#[serde(tag = "type", content = "inputs", rename_all = "lowercase")]
pub enum RawStepOp {
    Http(RawHttpInputs),
    Js(RawJsInputs),
    Filter(RawFilterInputs),
    Sort(RawSortInputs),
    Dedup(RawDedupInputs),
    Merge(RawMergeInputs),
    Base64(RawBase64Inputs),
    Noop,
    Var(RawVarInputs),
    File(RawFileInputs),
    Command(RawCommandInputs),
    Llm(RawLlmInputs),
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RawHttpInputs {
    pub url: Value,
    #[serde(default)]
    pub method: Option<String>,
    #[serde(default)]
    pub headers: Option<HashMap<String, Value>>,
    #[serde(default)]
    pub body: Option<Value>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RawJsInputs {
    pub code: Value,
    #[serde(default)]
    pub data: Option<Value>,
    #[serde(default, alias = "timeout")]
    pub timeout_sec: Option<f64>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RawFilterInputs {
    #[serde(default)]
    pub data: Option<Value>,
    #[serde(default)]
    pub operator: Option<String>,
    #[serde(default)]
    pub field: Option<String>,
    #[serde(default)]
    pub value: Option<Value>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RawSortInputs {
    #[serde(default)]
    pub data: Option<Value>,
    #[serde(default)]
    pub field: Option<String>,
    #[serde(default)]
    pub order: Option<String>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RawDedupInputs {
    #[serde(default)]
    pub data: Option<Value>,
    #[serde(default)]
    pub field: Option<String>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RawMergeInputs {
    pub b: Value,
    #[serde(default)]
    pub a: Option<Value>,
    #[serde(default)]
    pub deep: Option<bool>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RawBase64Inputs {
    #[serde(default)]
    pub data: Option<Value>,
    #[serde(default)]
    pub mode: Option<String>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RawFileInputs {
    #[serde(default)]
    pub path: Option<Value>,
    #[serde(default)]
    pub url: Option<Value>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RawCommandInputs {
    pub command: Value,
    #[serde(default)]
    pub shell: Option<String>,
    #[serde(default)]
    pub stdin: Option<Value>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RawLlmInputs {
    pub url: Value,
    pub model: String,
    pub prompt: Value,
    #[serde(default)]
    pub system: Option<Value>,
    #[serde(default)]
    pub images_b64: Option<Value>,
    #[serde(default)]
    pub image_type: Option<String>,
    #[serde(default)]
    pub max_tokens: Option<u64>,
    #[serde(default)]
    pub temperature: Option<f64>,
    #[serde(default)]
    pub skip_vision_check: Option<bool>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RawVarInputs {
    #[serde(default)]
    pub value: Option<Value>,
}

// ---------------------------------------------------------------------------
// Raw → PipelineDef / StepDef conversion
// ---------------------------------------------------------------------------

impl TryFrom<RawPipelineDef> for PipelineDef {
    type Error = ParseError;

    fn try_from(raw: RawPipelineDef) -> Result<Self, Self::Error> {
        Ok(PipelineDef {
            name: raw.name,
            description: raw.description,
            storage: raw.storage,
            slots: raw.slots,
            steps: raw
                .steps
                .into_iter()
                .map(StepDef::try_from)
                .collect::<Result<Vec<_>, _>>()?,
            output: parse_string_to_refvalue(&raw.output),
        })
    }
}

impl TryFrom<RawStepDef> for StepDef {
    type Error = ParseError;

    fn try_from(raw: RawStepDef) -> Result<Self, Self::Error> {
        if !raw.extra.is_empty() {
            let mut keys: Vec<String> = raw.extra.into_keys().collect();
            keys.sort();
            return Err(ParseError::UnknownStepFields(raw.id, keys.join(", ")));
        }
        Ok(StepDef {
            id: StepId::from(raw.id),
            after: raw.after.map(|a| a.into_iter().map(StepId::from).collect()),
            iterate: raw.iterate.map(IterateConfig::try_from).transpose()?,
            cache: raw.cache,
            retry: raw.retry,
            timeout_sec: raw.timeout_sec,
            op: raw.op.into(),
        })
    }
}

impl From<RawStepOp> for StepOp {
    fn from(raw: RawStepOp) -> Self {
        match raw {
            RawStepOp::Http(r) => StepOp::Http(step_op::HttpInputs {
                url: yaml_to_refvalue(&r.url),
                method: r.method,
                headers: r.headers.map(|m| m.into_iter().map(|(k, v)| (k, yaml_to_refvalue(&v))).collect()),
                body: r.body.as_ref().map(yaml_to_refvalue),
            }),
            RawStepOp::Js(r) => StepOp::Js(step_op::JsInputs {
                code: yaml_to_refvalue(&r.code),
                data: r.data.as_ref().map(yaml_to_refvalue),
                timeout_sec: r.timeout_sec,
            }),
            RawStepOp::Filter(r) => StepOp::Filter(step_op::FilterInputs {
                data: r.data.as_ref().map(yaml_to_refvalue),
                operator: r.operator.unwrap_or_else(|| "eq".into()),
                field: r.field,
                value: r.value.as_ref().map(yaml_to_refvalue),
            }),
            RawStepOp::Sort(r) => StepOp::Sort(step_op::SortInputs {
                data: r.data.as_ref().map(yaml_to_refvalue),
                field: r.field,
                order: r.order.unwrap_or_else(|| "asc".into()),
            }),
            RawStepOp::Dedup(r) => StepOp::Dedup(step_op::DedupInputs {
                data: r.data.as_ref().map(yaml_to_refvalue),
                field: r.field,
            }),
            RawStepOp::Merge(r) => StepOp::Merge(step_op::MergeInputs {
                b: yaml_to_refvalue(&r.b),
                a: r.a.as_ref().map(yaml_to_refvalue),
                deep: r.deep,
            }),
            RawStepOp::Base64(r) => StepOp::Base64(step_op::Base64Inputs {
                data: r.data.as_ref().map(yaml_to_refvalue),
                mode: r.mode,
            }),
            RawStepOp::Noop => StepOp::Noop,
            RawStepOp::Var(r) => StepOp::Var(step_op::VarInputs {
                value: r.value.as_ref().map(yaml_to_refvalue),
            }),
            RawStepOp::File(r) => StepOp::File(step_op::FileInputs {
                path: r.path.as_ref().map(yaml_to_refvalue),
                url: r.url.as_ref().map(yaml_to_refvalue),
            }),
            RawStepOp::Command(r) => StepOp::Command(step_op::CommandInputs {
                command: yaml_to_refvalue(&r.command),
                shell: r.shell,
                stdin: r.stdin.as_ref().map(yaml_to_refvalue),
            }),
            RawStepOp::Llm(r) => StepOp::Llm(step_op::LlmInputs {
                url: yaml_to_refvalue(&r.url),
                model: r.model,
                prompt: yaml_to_refvalue(&r.prompt),
                system: r.system.as_ref().map(yaml_to_refvalue),
                images_b64: r.images_b64.as_ref().map(yaml_to_refvalue),
                image_type: r.image_type,
                max_tokens: r.max_tokens.unwrap_or(4096),
                temperature: r.temperature,
                skip_vision_check: r.skip_vision_check,
            }),
        }
    }
}

impl TryFrom<RawIterateConfig> for IterateConfig {
    type Error = ParseError;

    fn try_from(raw: RawIterateConfig) -> Result<Self, Self::Error> {
        Ok(IterateConfig {
            over: VariablePath::parse(&raw.over)
                .ok_or_else(|| ParseError::InvalidIterateOver(raw.over.clone()))?,
            as_name: raw.as_name,
            max_workers: raw.max_workers,
            batch: raw.batch,
        })
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Convert a YAML `Value` to `RefValue`.
/// String `"{slots.x}"` → `Ref`, other scalars → `Literal`.
/// Objects/arrays are recursively walked: nested strings converted to
/// `{"Ref": {"parts": [...]}}` inline tags.
fn yaml_to_refvalue(v: &Value) -> RefValue {
    match v {
        Value::String(s) => {
            if let Some(path) = VariablePath::parse(s) {
                RefValue::Ref(path)
            } else {
                RefValue::Literal(v.clone())
            }
        }
        Value::Object(map) => {
            RefValue::Literal(Value::Object(
                map.iter().map(|(k, v)| (k.clone(), replace_template_strings(v))).collect(),
            ))
        }
        Value::Array(arr) => {
            RefValue::Literal(Value::Array(
                arr.iter().map(replace_template_strings).collect(),
            ))
        }
        _ => RefValue::Literal(v.clone()),
    }
}

/// Replace `"{...}"` strings in a Value tree with `{"Ref": {"parts": [...]}}`.
/// Used for converting nested data inside a `RefValue::Literal`.
fn replace_template_strings(v: &Value) -> Value {
    match v {
        Value::String(s) => {
            if let Some(path) = VariablePath::parse(s) {
                let mut map = serde_json::Map::new();
                map.insert(
                    "Ref".into(),
                    serde_json::json!({"parts": path.parts}),
                );
                Value::Object(map)
            } else {
                v.clone()
            }
        }
        Value::Object(map) => {
            Value::Object(
                map.iter().map(|(k, v)| (k.clone(), replace_template_strings(v))).collect(),
            )
        }
        Value::Array(arr) => {
            Value::Array(arr.iter().map(replace_template_strings).collect())
        }
        _ => v.clone(),
    }
}
// ---------------------------------------------------------------------------
// Errors
// ---------------------------------------------------------------------------

#[derive(Debug, thiserror::Error)]
pub enum ParseError {
    #[error("YAML 解析失败: {0}")]
    Yaml(String),
    #[error("iterate.over 必须是 {{...}} 形式的变量路径: {0}")]
    InvalidIterateOver(String),
    #[error("步骤 {0} 存在未知字段: {1}")]
    UnknownStepFields(String, String),
}

impl From<rust_yaml::Error> for ParseError {
    fn from(e: rust_yaml::Error) -> Self {
        ParseError::Yaml(e.to_string())
    }
}

impl From<serde_json::Error> for ParseError {
    fn from(e: serde_json::Error) -> Self {
        ParseError::Yaml(e.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unknown_inputs_field_rejected() {
        let yaml = r#"
name: t
steps:
  - id: s
    type: http
    inputs:
      url: "http://example.com"
      methd: post
output: "{s.output}"
"#;
        let r: Result<RawPipelineDef, _> = rust_yaml::from_str(yaml);
        assert!(r.is_err(), "typo field `methd` should be rejected");
    }

    #[test]
    fn unknown_iterate_field_rejected() {
        let yaml = r#"
name: t
steps:
  - id: s
    type: noop
    iterate:
      over: "{slots.items}"
      as: item
      max_worker: 4
output: "{s.output}"
"#;
        let r: Result<RawPipelineDef, _> = rust_yaml::from_str(yaml);
        assert!(r.is_err(), "typo field `max_worker` should be rejected");
    }

    #[test]
    fn unknown_step_field_rejected() {
        let yaml = r#"
name: t
steps:
  - id: s
    type: noop
    cach: true
output: "{s.output}"
"#;
        let raw: RawPipelineDef = rust_yaml::from_str(yaml).unwrap();
        let err = PipelineDef::try_from(raw).unwrap_err();
        assert!(matches!(err, ParseError::UnknownStepFields(_, _)));
        assert!(err.to_string().contains("cach"));
    }

    #[test]
    fn all_known_step_fields_accepted() {
        let yaml = r#"
name: t
steps:
  - id: s
    type: http
    after: [a]
    iterate:
      over: "{slots.items}"
      as: item
      max_workers: 4
      batch:
        size: 10
    cache: false
    retry:
      max_attempts: 2
      backoff: fixed
      delay_ms: 100
    timeout_sec: 30
    inputs:
      url: "http://example.com"
      method: get
      headers:
        X-Key: v
      body: "{}"
output: "{s.output}"
"#;
        let raw: RawPipelineDef = rust_yaml::from_str(yaml).unwrap();
        assert!(PipelineDef::try_from(raw).is_ok());
    }
}
