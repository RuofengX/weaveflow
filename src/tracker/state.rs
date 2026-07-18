use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::dsl::StepId;

/// 步骤执行进度。状态无关的元数据在外层，状态相关的数据在 StepState 中。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StepProgress {
    pub step_id: StepId,
    pub timeout_sec: Option<f64>,
    pub state: StepState,
}

impl StepProgress {
    pub fn new(step_id: &StepId, timeout_sec: Option<f64>) -> Self {
        StepProgress {
            step_id: step_id.clone(),
            timeout_sec,
            state: StepState::Pending,
        }
    }
}

/// 步骤状态机 — 不同变体携带不同数据。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum StepState {
    Pending,
    Skipped,
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
}

/// 步骤级执行进度容器。
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct Progress {
    pub steps: Vec<StepProgress>,
}

impl Progress {
    pub fn from_step_ids(steps: &[(StepId, Option<f64>)]) -> Self {
        Progress {
            steps: steps
                .iter()
                .map(|(id, timeout_sec)| StepProgress::new(id, *timeout_sec))
                .collect(),
        }
    }

    pub fn step_mut(&mut self, step_id: &StepId) -> Option<&mut StepProgress> {
        self.steps.iter_mut().find(|s| &s.step_id == step_id)
    }

    pub fn step(&self, step_id: &StepId) -> Option<&StepProgress> {
        self.steps.iter().find(|s| &s.step_id == step_id)
    }
}

/// Task 整体状态——Running 变体携带步骤级进度。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum TaskStatus {
    Running(Progress),
    Completed(serde_json::Value),
    Failed(String),
}

/// DAG 层的结构信息（用于前端渲染并行括号）。
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct LayerInfo {
    pub index: usize,
    pub step_ids: Vec<StepId>,
}
