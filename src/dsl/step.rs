use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use super::retry::RetryDef;
use super::variable::{RefValue, VariablePath};

/// 单个执行步骤的定义。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StepDef {
    pub id: String,
    #[serde(rename = "type")]
    pub r#type: String,
    pub after: Option<Vec<String>>,
    #[serde(default)]
    pub iterate: Option<IterateConfig>,
    /// 传给算子的输入参数。值已在反序列化时解析为 `RefValue`。
    pub inputs: Option<HashMap<String, RefValue>>,
    pub cache: Option<bool>,
    pub retry: Option<RetryDef>,
    pub timeout: Option<u64>,
    #[serde(default)]
    pub code: Option<String>,
}

/// 迭代配置，放在 step 的 `iterate` 字段。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IterateConfig {
    /// 模板引用，指向要遍历的数组，如 `{extract.output}`。
    pub over: VariablePath,
    #[serde(rename = "as")]
    pub as_name: String,
    #[serde(default)]
    pub max_workers: Option<u32>,
    #[serde(default)]
    pub batch: Option<BatchConfig>,
}

/// 迭代批量配置。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BatchConfig {
    pub size: u32,
}
