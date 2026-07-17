use chrono::{DateTime, Utc};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use tracing::{debug, info};

use crate::dsl::StepId;

use super::meta::TaskId;
use super::state::{Progress, StepProgress, StepState, TaskStatus, LayerInfo};

/// 对外暴露的 task 状态快照。
#[derive(Debug, Clone, serde::Serialize)]
pub struct TaskSnapshot {
    pub task_id: String,
    pub pipeline_name: String,
    pub status: TaskStatus,
    pub layers: Vec<LayerInfo>,
    /// Per-step progress — always present so TUI can render step states
    /// even after the task has completed or failed.
    pub steps: Vec<StepProgress>,
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
    status: RunStatus,
    started_at: Option<DateTime<Utc>>,
    completed_at: Option<DateTime<Utc>>,
    tx: tokio::sync::broadcast::Sender<Vec<u8>>,
}

#[derive(Debug, Clone)]
enum RunStatus {
    Running,
    Completed(serde_json::Value),
    Failed(String),
}

/// 内存中的 task 运行时状态追踪器。
///
/// - 执行器每步更新状态 + iterate 进度
/// - WS 订阅 broadcast channel 获取 push
/// - HTTP GET /runs/:task_id 直接查
#[derive(Clone, Default)]
pub struct TaskTracker {
    runs: Arc<Mutex<HashMap<TaskId, RunState>>>,
}

impl TaskTracker {
    pub fn new() -> Self {
        Self::default()
    }

    /// 注册新 task，返回 broadcast receiver + 初始快照。
    pub async fn create(
        &self,
        task_id: TaskId,
        pipeline_name: String,
        step_ids: Vec<StepId>,
        layers: Vec<LayerInfo>,
    ) -> (tokio::sync::broadcast::Receiver<Vec<u8>>, TaskSnapshot) {
        debug!(task_id = %task_id, pipeline = %pipeline_name, steps = step_ids.len(), "tracker create");
        let progress = Progress::from_step_ids(&step_ids);
        let (tx, rx) = tokio::sync::broadcast::channel(64);

        // Set initial state as Running with Undefined progress
        let state = RunState {
            task_id,
            pipeline_name: pipeline_name.clone(),
            layers: layers.clone(),
            progress,
            status: RunStatus::Running,
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
    pub async fn update_step(&self, task_id: &TaskId, step_id: &StepId, state: StepState) {
        debug!(task_id = %task_id, step = %step_id, state = ?state, "tracker update_step");
        let mut runs = self.runs.lock().unwrap();
        if let Some(run) = runs.get_mut(task_id) {
            if let Some(step) = run.progress.step_mut(step_id) {
                step.state = state;
            }
            self.broadcast(run);
        }
    }

    /// 更新 iterate 进度（done/total），每个 chunk 完成时调用，并广播。
    pub async fn update_iterate(&self, task_id: &TaskId, step_id: &StepId, done: u64, total: u64) {
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
        info!(task_id = %task_id, "tracker complete");
        let mut runs = self.runs.lock().unwrap();
        if let Some(run) = runs.get_mut(task_id) {
            run.status = RunStatus::Completed(output);
            run.completed_at = Some(Utc::now());
            self.broadcast(run);
        }
    }

    /// 标记 task 失败。
    pub async fn fail(&self, task_id: &TaskId, error: String) {
        info!(task_id = %task_id, %error, "tracker fail");
        let mut runs = self.runs.lock().unwrap();
        if let Some(run) = runs.get_mut(task_id) {
            run.status = RunStatus::Failed(error);
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
        let status = match &run.status {
            RunStatus::Running => TaskStatus::Running(run.progress.clone()),
            RunStatus::Completed(output) => TaskStatus::Completed(output.clone()),
            RunStatus::Failed(error) => TaskStatus::Failed(error.clone()),
        };
        TaskSnapshot {
            task_id: run.task_id.to_string(),
            pipeline_name: run.pipeline_name.clone(),
            status,
            layers: run.layers.clone(),
            steps: run.progress.steps.clone(),
            started_at: run.started_at,
            completed_at: run.completed_at,
            total_duration_ms,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn step_id(s: &str) -> StepId {
        StepId(s.to_string())
    }

    fn running_progress(snapshot: &TaskSnapshot) -> &Progress {
        match &snapshot.status {
            TaskStatus::Running(progress) => progress,
            other => panic!("expected Running status, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn snapshot_progress_reflects_step_updates() {
        let tracker = TaskTracker::new();
        let task_id = TaskId::new();
        let (_rx, initial) = tracker
            .create(task_id, "p".to_string(), vec![step_id("a"), step_id("b")], vec![])
            .await;

        let progress = running_progress(&initial);
        assert!(progress
            .steps
            .iter()
            .all(|s| matches!(s.state, StepState::Pending)));

        tracker
            .update_step(
                &task_id,
                &step_id("a"),
                StepState::Running {
                    started_at: Utc::now(),
                    attempts: 1,
                },
            )
            .await;

        let snapshot = tracker.get(&task_id).await.unwrap();
        let progress = running_progress(&snapshot);
        assert!(matches!(
            progress.step(&step_id("a")).unwrap().state,
            StepState::Running { .. }
        ));
        assert!(matches!(
            progress.step(&step_id("b")).unwrap().state,
            StepState::Pending
        ));

        tracker
            .update_step(
                &task_id,
                &step_id("a"),
                StepState::Completed {
                    started_at: Utc::now(),
                    completed_at: Utc::now(),
                    attempts: 1,
                    cached: false,
                    duration_ms: 1,
                },
            )
            .await;

        let snapshot = tracker.get(&task_id).await.unwrap();
        let progress = running_progress(&snapshot);
        assert!(matches!(
            progress.step(&step_id("a")).unwrap().state,
            StepState::Completed { .. }
        ));
        assert_eq!(
            serde_json::to_value(&snapshot.steps).unwrap(),
            serde_json::to_value(&progress.steps).unwrap()
        );
    }

    #[tokio::test]
    async fn broadcast_payload_progress_matches_get() {
        let tracker = TaskTracker::new();
        let task_id = TaskId::new();
        let (mut rx, _initial) = tracker
            .create(task_id, "p".to_string(), vec![step_id("a")], vec![])
            .await;

        tracker
            .update_step(
                &task_id,
                &step_id("a"),
                StepState::Running {
                    started_at: Utc::now(),
                    attempts: 1,
                },
            )
            .await;

        let bytes = rx.recv().await.unwrap();
        let pushed: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        let snapshot = tracker.get(&task_id).await.unwrap();

        let live_steps = serde_json::to_value(&snapshot.steps).unwrap();
        assert_eq!(pushed["steps"], live_steps);
        assert_eq!(pushed["status"]["Running"]["steps"], live_steps);
        assert!(
            pushed["status"]["Running"]["steps"][0]["state"]
                .get("Running")
                .is_some()
        );
    }

    #[tokio::test]
    async fn completed_status_keeps_live_steps() {
        let tracker = TaskTracker::new();
        let task_id = TaskId::new();
        let (_rx, _initial) = tracker
            .create(task_id, "p".to_string(), vec![step_id("a")], vec![])
            .await;

        tracker
            .update_step(
                &task_id,
                &step_id("a"),
                StepState::Completed {
                    started_at: Utc::now(),
                    completed_at: Utc::now(),
                    attempts: 1,
                    cached: false,
                    duration_ms: 1,
                },
            )
            .await;
        tracker.complete(&task_id, serde_json::json!({"ok": true})).await;

        let snapshot = tracker.get(&task_id).await.unwrap();
        assert!(matches!(snapshot.status, TaskStatus::Completed(_)));
        assert!(matches!(
            snapshot.steps[0].state,
            StepState::Completed { .. }
        ));
    }
}
