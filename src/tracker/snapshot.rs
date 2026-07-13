use serde::{Deserialize, Serialize};

/// Snapshot key = task_id + seq.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct SnapshotKey {
    pub task_id: uuid::Uuid,
    pub seq: u64,
}

impl std::fmt::Display for SnapshotKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}/{}", self.task_id, self.seq)
    }
}

/// 增量步骤快照——只存当前步骤输出 bytes。
/// 全量状态可通过重放 snapshot 序列重建。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Snapshot {
    pub seq: u64,
    pub step_id: String,
    pub output: Vec<u8>,
}
