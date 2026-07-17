use serde::{Deserialize, Serialize};

/// Task ID（UUID v4，redb 表 key）。
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct TaskId(pub uuid::Uuid);

impl Default for TaskId {
    fn default() -> Self {
        Self::new()
    }
}

impl TaskId {
    pub fn new() -> Self { TaskId(uuid::Uuid::new_v4()) }
}

impl From<uuid::Uuid> for TaskId {
    fn from(u: uuid::Uuid) -> Self { TaskId(u) }
}

impl std::fmt::Display for TaskId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// Pipeline ID（UUID v4，redb 表 key）。
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct PipelineId(pub uuid::Uuid);

impl Default for PipelineId {
    fn default() -> Self {
        Self::new()
    }
}

impl PipelineId {
    pub fn new() -> Self { PipelineId(uuid::Uuid::new_v4()) }
}

impl From<uuid::Uuid> for PipelineId {
    fn from(u: uuid::Uuid) -> Self { PipelineId(u) }
}

impl std::fmt::Display for PipelineId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

use chrono::{DateTime, Utc};

/// Task 元数据（存储在 redb task 表中）。创建后仅 status 字段可更新。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskMeta {
    pub task_id: TaskId,
    pub pipeline_name: String,
    pub created_at: DateTime<Utc>,
    pub result_ttl_secs: i64,
    pub inputs: serde_json::Value,
    #[serde(default = "default_task_status")]
    pub status: String,
}

fn default_task_status() -> String {
    "unknown".to_string()
}

pub const TASK_STATUS_RUNNING: &str = "running";
pub const TASK_STATUS_COMPLETED: &str = "completed";
pub const TASK_STATUS_FAILED: &str = "failed";
pub const TASK_STATUS_INTERRUPTED: &str = "failed_interrupted";
