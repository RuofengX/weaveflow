use serde::{Deserialize, Serialize};

use super::retry::RetryDef;
use super::step_op::StepOp;
use super::variable::VariablePath;

/// Pipeline 步骤标识符，newtype over String。
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct StepId(pub String);

impl std::fmt::Display for StepId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl std::borrow::Borrow<str> for StepId {
    fn borrow(&self) -> &str {
        &self.0
    }
}

impl AsRef<str> for StepId {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

impl From<String> for StepId {
    fn from(s: String) -> Self {
        StepId(s)
    }
}

impl From<&str> for StepId {
    fn from(s: &str) -> Self {
        StepId(s.to_string())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StepDef {
    pub id: StepId,
    #[serde(default)]
    pub after: Option<Vec<StepId>>,
    pub iterate: Option<IterateConfig>,
    pub cache: Option<bool>,
    pub retry: Option<RetryDef>,
    #[serde(default, alias = "timeout")]
    pub timeout_sec: Option<f64>,

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
#[serde(deny_unknown_fields)]
pub struct BatchConfig {
    pub size: u32,
}
