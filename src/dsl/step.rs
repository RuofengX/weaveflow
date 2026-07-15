use serde::{Deserialize, Serialize};

use super::retry::RetryDef;
use super::step_op::StepOp;
use super::variable::VariablePath;

#[derive(Debug, Clone, Serialize, Deserialize)]
 pub struct StepDef {
    pub id: String,
    #[serde(default)]
    pub after: Option<Vec<String>>,
    pub iterate: Option<IterateConfig>,
    pub cache: Option<bool>,
    pub retry: Option<RetryDef>,
    pub timeout: Option<u64>,

    #[serde(flatten)]
    pub op: StepOp,
}

/// 迭代配置，放在 step 的 `iterate` 字段。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IterateConfig {
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
