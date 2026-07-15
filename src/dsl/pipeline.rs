use serde::{Deserialize, Serialize};
use serde_json::Value;

use super::step::StepDef;
use super::storage::StorageDef;
use super::variable::RefValue;

/// Pipeline 顶层定义，由 YAML 反序列化而来。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PipelineDef {
    pub name: String,
    pub description: Option<String>,
    pub storage: Option<StorageDef>,
    #[serde(default)]
    pub slots: Vec<SlotDef>,
    #[serde(default)]
    pub steps: Vec<StepDef>,
    pub output: RefValue,
}

/// Pipeline 输入槽位声明。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SlotDef {
    pub name: String,
    /// 该槽位的 JSON Schema（如 `{"type": "string", "format": "uri"}`）。
    pub schema: Value,
}
