use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use super::variable::RefValue;

// ---------------------------------------------------------------------------
// StepOp — adjacently tagged enum dispatched by `type` + `inputs` fields
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", content = "inputs", rename_all = "lowercase")]
pub enum StepOp {
    Http(HttpInputs),
    Js(JsInputs),
    Filter(FilterInputs),
    Sort(SortInputs),
    Dedup(DedupInputs),
    Merge(MergeInputs),
    Base64(Base64Inputs),
    Noop,
    Var(VarInputs),
    File(FileInputs),
    Command(CommandInputs),
    Llm(LlmInputs),
}

impl StepOp {
    pub fn op_type(&self) -> &'static str {
        match self {
            StepOp::Http(_) => "http",
            StepOp::Js(_) => "js",
            StepOp::Filter(_) => "filter",
            StepOp::Sort(_) => "sort",
            StepOp::Dedup(_) => "dedup",
            StepOp::Merge(_) => "merge",
            StepOp::Base64(_) => "base64",
            StepOp::Noop => "noop",
            StepOp::Var(_) => "var",
            StepOp::File(_) => "file",
            StepOp::Command(_) => "command",
            StepOp::Llm(_) => "llm",
        }
    }
}

// ---------------------------------------------------------------------------
// Per-operator Inputs structs — 零 HashMap catch-all
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HttpInputs {
    pub url: RefValue,
    #[serde(default)]
    pub method: Option<String>,
    #[serde(default)]
    pub headers: Option<HashMap<String, RefValue>>,
    #[serde(default)]
    pub body: Option<RefValue>,
}

/// JS 算子的 code 字段在 inputs.code 中。
/// `{{step.output}}` 双花括号在运行时由 `resolve_code_templates` 处理。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JsInputs {
    pub code: String,
    #[serde(default)]
    pub data: Option<RefValue>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FilterInputs {
    #[serde(default)]
    pub data: Option<RefValue>,
    #[serde(default = "default_filter_operator")]
    pub operator: String,
    #[serde(default)]
    pub field: Option<String>,
    #[serde(default)]
    pub value: Option<RefValue>,
}

fn default_filter_operator() -> String {
    "eq".to_string()
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SortInputs {
    #[serde(default)]
    pub data: Option<RefValue>,
    #[serde(default)]
    pub field: Option<String>,
    #[serde(default = "default_sort_order")]
    pub order: String,
}

fn default_sort_order() -> String {
    "asc".to_string()
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DedupInputs {
    #[serde(default)]
    pub data: Option<RefValue>,
    #[serde(default)]
    pub field: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MergeInputs {
    pub b: RefValue,
    #[serde(default)]
    pub a: Option<RefValue>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Base64Inputs {
    #[serde(default)]
    pub data: Option<RefValue>,
    #[serde(default)]
    pub mode: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FileInputs {
    #[serde(default)]
    pub path: Option<RefValue>,
    #[serde(default)]
    pub url: Option<RefValue>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CommandInputs {
    pub command: RefValue,
    #[serde(default)]
    pub shell: Option<String>,
    #[serde(default)]
    pub stdin: Option<RefValue>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LlmInputs {
    pub url: RefValue,
    pub model: String,
    pub prompt: RefValue,
    #[serde(default)]
    pub system: Option<RefValue>,
    #[serde(default)]
    pub images_b64: Option<RefValue>,
    #[serde(default)]
    pub image_type: Option<String>,
    #[serde(default = "default_max_tokens")]
    pub max_tokens: u64,
    #[serde(default)]
    pub temperature: Option<f64>,
    #[serde(default)]
    pub skip_vision_check: Option<bool>,
}

fn default_max_tokens() -> u64 {
    4096
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VarInputs {
    #[serde(default)]
    pub value: Option<RefValue>,
}
