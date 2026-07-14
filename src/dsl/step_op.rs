use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use super::variable::RefValue;

// ---------------------------------------------------------------------------
// StepOp — adjacently tagged enum dispatched by `type` + `inputs` fields
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", content = "inputs")]
pub enum StepOp {
    #[serde(rename = "http")]
    HttpClient(HttpInputs),
    #[serde(rename = "js")]
    JsScript(JsInputs),
    #[serde(rename = "filter")]
    FilterData(FilterInputs),
    #[serde(rename = "sort")]
    SortData(SortInputs),
    #[serde(rename = "dedup")]
    DedupData(DedupInputs),
    #[serde(rename = "merge")]
    MergeData(MergeInputs),
    #[serde(rename = "split")]
    SplitData(SplitInputs),
    #[serde(rename = "base64")]
    Base64Data(Base64Inputs),
    #[serde(rename = "noop")]
    Noop,
    #[serde(rename = "var")]
    VarOutput(VarInputs),
    #[serde(rename = "file")]
    FileReader(FileInputs),
    #[serde(rename = "command")]
    CommandRun(CommandInputs),
    #[serde(rename = "llm")]
    LlmClient(LlmInputs),
    #[serde(rename = "fork")]
    ForkFlow(ForkInputs),
}

impl StepOp {
    pub fn op_type(&self) -> &'static str {
        match self {
            StepOp::HttpClient(_) => "http",
            StepOp::JsScript(_) => "js",
            StepOp::FilterData(_) => "filter",
            StepOp::SortData(_) => "sort",
            StepOp::DedupData(_) => "dedup",
            StepOp::MergeData(_) => "merge",
            StepOp::SplitData(_) => "split",
            StepOp::Base64Data(_) => "base64",
            StepOp::Noop => "noop",
            StepOp::VarOutput(_) => "var",
            StepOp::FileReader(_) => "file",
            StepOp::CommandRun(_) => "command",
            StepOp::LlmClient(_) => "llm",
            StepOp::ForkFlow(_) => "fork",
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
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FilterInputs {
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
    pub field: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MergeInputs {
    pub b: RefValue,
    #[serde(default)]
    pub a: Option<RefValue>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SplitInputs {
    #[serde(default = "default_split_size")]
    pub size: u32,
}

fn default_split_size() -> u32 {
    100
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Base64Inputs {
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

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ForkInputs {
    pub branches: Vec<ForkBranch>,
    #[serde(default = "default_join")]
    pub join: String,
}

fn default_join() -> String {
    "object".to_string()
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ForkBranch {
    #[serde(default)]
    pub id: Option<String>,
    #[serde(rename = "type")]
    pub op_type: String,
    #[serde(default)]
    pub inputs: HashMap<String, RefValue>,
}
