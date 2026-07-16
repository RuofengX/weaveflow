use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

/// 步骤执行进度。状态无关的元数据在外层，状态相关的数据在 StepState 中。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StepProgress {
    pub step_id: String,
    pub timeout: Option<u64>,
    pub state: StepState,
}

impl StepProgress {
    pub fn new(step_id: &str, timeout: Option<u64>) -> Self {
        StepProgress {
            step_id: step_id.into(),
            timeout,
            state: StepState::Pending,
        }
    }
}

/// 步骤状态机 — 不同变体携带不同数据。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum StepState {
    Pending,
    Running {
        started_at: DateTime<Utc>,
        attempts: u32,
    },
    Iterating {
        started_at: DateTime<Utc>,
        progress: IterateProgress,
    },
    Completed {
        started_at: DateTime<Utc>,
        completed_at: DateTime<Utc>,
        attempts: u32,
        cached: bool,
        duration_ms: u64,
    },
    Failed {
        started_at: Option<DateTime<Utc>>,
        completed_at: DateTime<Utc>,
        error: String,
        attempts: u32,
    },
}

/// iterate 模式进度。
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct IterateProgress {
    pub total: u64,
    pub done: u64,
    pub errors: u64,
    pub skip: u64,
}

/// 步骤级执行进度容器。
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct Progress {
    pub steps: Vec<StepProgress>,
}

impl Progress {
    pub fn from_step_ids(ids: &[String]) -> Self {
        Progress {
            steps: ids.iter().map(|id| StepProgress::new(id, None)).collect(),
        }
    }

    pub fn step_mut(&mut self, step_id: &str) -> Option<&mut StepProgress> {
        self.steps.iter_mut().find(|s| s.step_id == step_id)
    }

    pub fn step(&self, step_id: &str) -> Option<&StepProgress> {
        self.steps.iter().find(|s| s.step_id == step_id)
    }
}

/// Task 整体状态——Running 变体携带步骤级进度。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum TaskStatus {
    Pending,
    Running(Progress),
    Completed(serde_json::Value),
    Failed(String),
}

/// DAG 层的结构信息（用于前端渲染并行括号）。
#[derive(Debug, Clone, serde::Serialize)]
pub struct LayerInfo {
    pub index: usize,
    pub step_ids: Vec<String>,
}
