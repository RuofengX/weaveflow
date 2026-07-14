use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use super::variable::RefValue;

/// 校验/守卫规则，在 apply 阶段或执行前评估。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RuleDef {
    pub id: String,
    #[serde(rename = "type")]
    pub r#type: String,
    pub inputs: Option<HashMap<String, RefValue>>,
    #[serde(default)]
    pub code: Option<String>,
}
