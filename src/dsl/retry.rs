use serde::{Deserialize, Serialize};

/// 重试配置。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RetryDef {
    #[serde(default = "default_max_attempts")]
    pub max_attempts: u32,
    #[serde(default)]
    pub backoff: BackoffStrategy,
    #[serde(default = "default_delay_ms")]
    pub delay_ms: u64,
}

fn default_max_attempts() -> u32 {
    1
}
fn default_delay_ms() -> u64 {
    1000
}

/// 重试退避策略。
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub enum BackoffStrategy {
    #[serde(rename = "fixed")]
    #[default]
    Fixed,
    #[serde(rename = "exponential")]
    Exponential,
}
