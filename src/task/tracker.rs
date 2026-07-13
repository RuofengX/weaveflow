use chrono::{DateTime, Utc};
use std::collections::HashMap;
use std::sync::Mutex;

use crate::task::progress::{Progress, StepState, TaskStatus};
use crate::task::TaskId;

/// DAG 层的结构信息（用于前端渲染并行括号）。
#[derive(Debug, Clone, serde::Serialize)]
pub struct LayerInfo {
    pub index: usize,
    pub step_ids: Vec<String>,
}

/// 对外暴露的 task 状态快照。
#[derive(Debug, Clone, serde::Serialize)]
pub struct TaskSnapshot {
    pub task_id: String,
    pub pipeline_name: String,
    pub status: TaskStatus,
    pub layers: Vec<LayerInfo>,
    /// Per-step progress — always present so TUI can render step states
    /// even after the task has completed or failed.
    pub steps: Vec<crate::task::progress::StepProgress>,
    pub started_at: Option<DateTime<Utc>>,
    pub completed_at: Option<DateTime<Utc>>,
    pub total_duration_ms: Option<u64>,
}

/// 内部 run 状态。
struct RunState {
    task_id: TaskId,
    pipeline_name: String,
    layers: Vec<LayerInfo>,
    progress: Progress,
    status: TaskStatus,
    started_at: Option<DateTime<Utc>>,
    completed_at: Option<DateTime<Utc>>,
    tx: tokio::sync::broadcast::Sender<Vec<u8>>,
}

/// 内存中的 task 运行时状态追踪器。
///
/// - 执行器每步更新状态 + iterate 进度
/// - WS 订阅 broadcast channel 获取 push
/// - HTTP GET /runs/:task_id 直接查
pub struct TaskTracker {
    runs: Mutex<HashMap<TaskId, RunState>>,
}

impl Default for TaskTracker {
    fn default() -> Self {
        Self::new()
    }
}

impl TaskTracker {
    pub fn new() -> Self {
        TaskTracker {
            runs: Mutex::new(HashMap::new()),
        }
    }

    /// 注册新 task，返回 broadcast receiver + 初始快照。
    pub async fn create(
        &self,
        task_id: TaskId,
        pipeline_name: String,
        step_ids: Vec<String>,
        layers: Vec<LayerInfo>,
    ) -> (tokio::sync::broadcast::Receiver<Vec<u8>>, TaskSnapshot) {
        let progress = Progress::from_step_ids(&step_ids);
        let status = TaskStatus::Running(progress.clone());
        let (tx, rx) = tokio::sync::broadcast::channel(64);

        // Set initial state as Running with Undefined progress
        let state = RunState {
            task_id,
            pipeline_name: pipeline_name.clone(),
            layers: layers.clone(),
            progress,
            status,
            started_at: Some(Utc::now()),
            completed_at: None,
            tx,
        };

        let progress = Progress::from_step_ids(&step_ids);
        let snapshot = TaskSnapshot {
            task_id: task_id.to_string(),
            pipeline_name,
            status: TaskStatus::Running(progress.clone()),
            layers,
            steps: progress.steps.clone(),
            started_at: Some(Utc::now()),
            completed_at: None,
            total_duration_ms: None,
        };

        self.runs.lock().unwrap().insert(task_id, state);
        (rx, snapshot)
    }

    /// 更新单个 step 的状态，并广播。
    pub async fn update_step(&self, task_id: &TaskId, step_id: &str, state: StepState) {
        let mut runs = self.runs.lock().unwrap();
        if let Some(run) = runs.get_mut(task_id) {
            if let Some(step) = run.progress.step_mut(step_id) {
                step.state = state;
            }
            self.broadcast(run);
        }
    }

    /// 更新 iterate 进度（done/total），每个 chunk 完成时调用，并广播。
    pub async fn update_iterate(&self, task_id: &TaskId, step_id: &str, done: u64, total: u64) {
        let mut runs = self.runs.lock().unwrap();
        if let Some(run) = runs.get_mut(task_id) {
            if let Some(step) = run.progress.step_mut(step_id)
                && let StepState::Iterating { progress, .. } = &mut step.state {
                    progress.done = done;
                    progress.total = total;
                }
            self.broadcast(run);
        }
    }

    /// 标记 task 完成。
    pub async fn complete(&self, task_id: &TaskId, output: serde_json::Value) {
        let mut runs = self.runs.lock().unwrap();
        if let Some(run) = runs.get_mut(task_id) {
            run.status = TaskStatus::Completed(output);
            run.completed_at = Some(Utc::now());
            self.broadcast(run);
        }
    }

    /// 标记 task 失败。
    pub async fn fail(&self, task_id: &TaskId, error: String) {
        let mut runs = self.runs.lock().unwrap();
        if let Some(run) = runs.get_mut(task_id) {
            run.status = TaskStatus::Failed(error);
            run.completed_at = Some(Utc::now());
            self.broadcast(run);
        }
    }

    /// 查询当前快照。
    pub async fn get(&self, task_id: &TaskId) -> Option<TaskSnapshot> {
        let runs = self.runs.lock().unwrap();
        runs.get(task_id).map(|r| self.build_snapshot(r))
    }

    /// 获取 broadcast receiver（用于 WS 订阅）。
    pub async fn subscribe(
        &self,
        task_id: &TaskId,
    ) -> Option<tokio::sync::broadcast::Receiver<Vec<u8>>> {
        let runs = self.runs.lock().unwrap();
        runs.get(task_id).map(|r| r.tx.subscribe())
    }

    // ── internal ──

    fn broadcast(&self, run: &RunState) {
        let bytes =
            serde_json::to_vec(&self.build_snapshot(run)).unwrap_or_default();
        let _ = run.tx.send(bytes);
    }

    fn build_snapshot(&self, run: &RunState) -> TaskSnapshot {
        let total_duration_ms = match (run.started_at, run.completed_at) {
            (Some(start), Some(end)) => {
                Some((end - start).num_milliseconds() as u64)
            }
            _ => None,
        };
        TaskSnapshot {
            task_id: run.task_id.to_string(),
            pipeline_name: run.pipeline_name.clone(),
            status: run.status.clone(),
            layers: run.layers.clone(),
            steps: run.progress.steps.clone(),
            started_at: run.started_at,
            completed_at: run.completed_at,
            total_duration_ms,
        }
    }
}
