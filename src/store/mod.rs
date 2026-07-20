pub(super) mod database;
pub(super) mod object;

pub use object::{ObjectDigest, ObjectValue};
pub(crate) use crate::store::database::CacheKey;

use chrono::Utc;
use redb::{Database as RedbDb, ReadableTable};
use std::path::Path;
use tracing::debug;

use crate::dsl::{PipelineDef, StepId};
use crate::error::{WeaveflowError, WeaveflowResult};
use crate::store::database::{
    CACHE, OBJECT, PIPELINE, SNAPSHOT, SNAPSHOT_HEADER, TASK, V0_PIPELINE, V0_TASK, V1_PIPELINE,
    V1_TASK,
};
use crate::tracker::meta::TASK_STATUS_RUNNING;
use crate::tracker::snapshot::{Snapshot, SnapshotKey};
use crate::tracker::{PipelineId, TaskId, TaskMeta};

/// weaveflow 数据层入口。封装 redb。
#[derive(Debug)]
pub struct Database {
    pub(crate) db: std::sync::RwLock<redb::Database>,
}

/// open 失败的分类：schema 版本不匹配（触发自动迁移）或其他错误。
enum OpenFailure {
    SchemaMismatch,
    Other(WeaveflowError),
}

fn init_table<K, V>(
    txn: &redb::WriteTransaction,
    def: redb::TableDefinition<K, V>,
    op: &'static str,
) -> Result<(), OpenFailure>
where
    K: redb::Key + 'static,
    V: redb::Value + 'static,
{
    txn.open_table(def).map(|_| ()).map_err(|e| match e {
        redb::TableError::TableTypeMismatch { .. } => OpenFailure::SchemaMismatch,
        other => OpenFailure::Other(WeaveflowError::Database {
            operation: op,
            source: Box::new(other.into()),
        }),
    })
}

// ── 构造 ───────────────────────────────────────────────────────────────────

impl Database {
    pub fn open(db_path: impl AsRef<Path>) -> WeaveflowResult<Self> {
        let path = db_path.as_ref();
        match Self::open_current(path) {
            Ok(db) => {
                debug!(path = %path.display(), "database opened");
                Ok(db)
            }
            Err(OpenFailure::SchemaMismatch) => Self::migrate_v0(path),
            Err(OpenFailure::Other(e)) => Err(e),
        }
    }

    fn open_current(path: &Path) -> Result<Self, OpenFailure> {
        let db = RedbDb::create(path).map_err(|e| OpenFailure::Other(WeaveflowError::Database {
            operation: "create",
            source: Box::new(e.into()),
        }))?;
        let txn = db.begin_write().map_err(|e| OpenFailure::Other(WeaveflowError::Database {
            operation: "init_tables begin_write",
            source: Box::new(e.into()),
        }))?;
        init_table(&txn, PIPELINE, "init_tables pipeline")?;
        init_table(&txn, TASK, "init_tables task")?;
        init_table(&txn, SNAPSHOT, "init_tables snapshot")?;
        init_table(&txn, OBJECT, "init_tables object")?;
        init_table(&txn, CACHE, "init_tables cache")?;
        txn.commit().map_err(|e| OpenFailure::Other(WeaveflowError::Database {
            operation: "init_tables commit",
            source: Box::new(e.into()),
        }))?;
        Ok(Database {
            db: std::sync::RwLock::new(db),
        })
    }

    /// v0 → 当前 schema 自动迁移：旧文件改名备份，PIPELINE/TASK 逐条拷贝，
    /// SNAPSHOT/OBJECT/CACHE 丢弃（缓存可重算、历史快照可弃）。
    fn migrate_v0(path: &Path) -> WeaveflowResult<Self> {
        let mut bak_name = path
            .file_name()
            .map(std::ffi::OsString::from)
            .unwrap_or_default();
        bak_name.push(".v0.bak");
        let bak = path.with_file_name(bak_name);

        let _ = std::fs::remove_file(&bak);
        std::fs::rename(path, &bak).map_err(|e| {
            WeaveflowError::Internal(format!(
                "旧版本数据库 schema 不兼容，且备份 {} -> {} 失败: {e}",
                path.display(),
                bak.display()
            ))
        })?;

        match Self::migrate_v0_inner(&bak, path) {
            Ok(db) => {
                tracing::warn!(
                    backup = %bak.display(),
                    "旧版本 (v0) 数据库已自动迁移：PIPELINE/TASK 已保留，SNAPSHOT/OBJECT/CACHE 已丢弃"
                );
                Ok(db)
            }
            Err(e) => Err(WeaveflowError::Internal(format!(
                "旧版本数据库已备份到 {}，请手动处理后重试（自动迁移失败: {e}）",
                bak.display()
            ))),
        }
    }

    fn migrate_v0_inner(bak: &Path, path: &Path) -> WeaveflowResult<Self> {
        let old = RedbDb::open(bak).map_err(|e| WeaveflowError::Database {
            operation: "migrate open_v0",
            source: Box::new(e.into()),
        })?;
        let read = old.begin_read().map_err(|e| WeaveflowError::Database {
            operation: "migrate v0 begin_read",
            source: Box::new(e.into()),
        })?;

        // 依次尝试 v0 / v1 类型名读取旧表（v1 与 v0 同为 serde_json 行格式）
        macro_rules! read_rows {
            ($def:expr, $op:literal) => {
                match read.open_table($def) {
                    Ok(table) => {
                        let mut rows = Vec::new();
                        for r in table.iter().map_err(|e| WeaveflowError::Database {
                            operation: concat!($op, " iter"),
                            source: Box::new(e.into()),
                        })? {
                            let (k, v) = r.map_err(|e| WeaveflowError::Database {
                                operation: concat!($op, " row"),
                                source: Box::new(e.into()),
                            })?;
                            rows.push((k.value(), v.value().0));
                        }
                        Some(rows)
                    }
                    Err(redb::TableError::TableTypeMismatch { .. })
                    | Err(redb::TableError::TableDoesNotExist(_)) => None,
                    Err(e) => {
                        return Err(WeaveflowError::Database {
                            operation: $op,
                            source: Box::new(e.into()),
                        })
                    }
                }
            };
        }

        let pipeline_rows = match read_rows!(V0_PIPELINE, "migrate v0 pipeline") {
            Some(rows) => rows,
            None => read_rows!(V1_PIPELINE, "migrate v1 pipeline").unwrap_or_default(),
        };
        let task_rows = match read_rows!(V0_TASK, "migrate v0 task") {
            Some(rows) => rows,
            None => read_rows!(V1_TASK, "migrate v1 task").unwrap_or_default(),
        };
        drop(read);
        drop(old);

        let mut pipelines: Vec<(PipelineId, PipelineDef)> = Vec::new();
        for (pid, mut raw) in pipeline_rows {
            // snapshot_ttl 字段已移除，剥离以免 deny_unknown_fields 报错
            if let Some(storage) = raw.get_mut("storage").and_then(|s| s.as_object_mut()) {
                storage.remove("snapshot_ttl");
            }
            let def: PipelineDef = serde_json::from_value(raw).map_err(|e| {
                WeaveflowError::Internal(format!("v0 pipeline 反序列化失败: {e}"))
            })?;
            pipelines.push((pid, def));
        }
        let mut tasks: Vec<(TaskId, TaskMeta)> = Vec::new();
        for (tid, raw) in task_rows {
            // v0 TaskMeta 可能没有 status 字段（serde default 兜底）
            let meta: TaskMeta = serde_json::from_value(raw).map_err(|e| {
                WeaveflowError::Internal(format!("v0 task 反序列化失败: {e}"))
            })?;
            tasks.push((tid, meta));
        }

        let db = Self::open_current(path).map_err(|e| match e {
            OpenFailure::SchemaMismatch => {
                WeaveflowError::Internal("迁移后 schema 仍不匹配".to_string())
            }
            OpenFailure::Other(e) => e,
        })?;
        let g = db.db.read().unwrap_or_else(|e| e.into_inner());
        let txn = g.begin_write().map_err(|e| WeaveflowError::Database {
            operation: "migrate begin_write",
            source: Box::new(e.into()),
        })?;
        {
            let mut table = txn.open_table(PIPELINE).map_err(|e| WeaveflowError::Database {
                operation: "migrate open pipeline",
                source: Box::new(e.into()),
            })?;
            for (pid, def) in &pipelines {
                table.insert(*pid, def).map_err(|e| WeaveflowError::Database {
                    operation: "migrate insert pipeline",
                    source: Box::new(e.into()),
                })?;
            }
        }
        {
            let mut table = txn.open_table(TASK).map_err(|e| WeaveflowError::Database {
                operation: "migrate open task",
                source: Box::new(e.into()),
            })?;
            for (tid, meta) in &tasks {
                table.insert(*tid, meta).map_err(|e| WeaveflowError::Database {
                    operation: "migrate insert task",
                    source: Box::new(e.into()),
                })?;
            }
        }
        txn.commit().map_err(|e| WeaveflowError::Database {
            operation: "migrate commit",
            source: Box::new(e.into()),
        })?;
        drop(g);
        debug!(
            pipelines = pipelines.len(),
            tasks = tasks.len(),
            "v0 database migrated"
        );
        Ok(db)
    }

    // ── Pipeline ────────────────────────────────────────────────────────

    pub fn save_pipeline_upsert(&self, def: &PipelineDef) -> WeaveflowResult<PipelineId> {
        debug!(name = %def.name, steps = def.steps.len(), "save_pipeline_upsert");
        // 名称查找与插入必须在同一写事务内完成（redb 写事务全局串行），
        // 否则并发 apply 同名 pipeline 会出现 check-then-act 双插。
        let g = self.db.read().unwrap_or_else(|e| e.into_inner());
        let txn = g.begin_write().map_err(|e| WeaveflowError::Database {
            operation: "save_pipeline_upsert begin_write",
            source: Box::new(e.into()),
        })?;
        let existing: Option<PipelineId> = {
            let table = txn.open_table(PIPELINE).map_err(|e| WeaveflowError::Database {
                operation: "save_pipeline_upsert open_table",
                source: Box::new(e.into()),
            })?;
            let mut found = None;
            for result in table.iter().map_err(|e| WeaveflowError::Database {
                operation: "save_pipeline_upsert iter",
                source: Box::new(e.into()),
            })? {
                let (k, v) = result.map_err(|e| WeaveflowError::Database {
                    operation: "save_pipeline_upsert read_row",
                    source: Box::new(e.into()),
                })?;
                let stored: PipelineDef = v.value();
                if stored.name == def.name {
                    found = Some(k.value());
                    break;
                }
            }
            found
        };
        let pid = existing.unwrap_or_default();
        let mut table = txn.open_table(PIPELINE).map_err(|e| WeaveflowError::Database {
            operation: "save_pipeline_upsert open_table",
            source: Box::new(e.into()),
        })?;
        table.insert(pid, def).map_err(|e| WeaveflowError::Database {
            operation: "save_pipeline_upsert insert",
            source: Box::new(e.into()),
        })?;
        drop(table);
        txn.commit().map_err(|e| WeaveflowError::Database {
            operation: "save_pipeline_upsert commit",
            source: Box::new(e.into()),
        })?;
        Ok(pid)
    }

    pub fn load_pipeline(&self, pid: &PipelineId) -> WeaveflowResult<Option<PipelineDef>> {
        let g = self.db.read().unwrap_or_else(|e| e.into_inner());
        let txn = g.begin_read().map_err(|e| WeaveflowError::Database {
            operation: "load_pipeline begin_read",
            source: Box::new(e.into()),
        })?;
        let table = txn.open_table(PIPELINE).map_err(|e| WeaveflowError::Database {
            operation: "load_pipeline open_table",
            source: Box::new(e.into()),
        })?;
        let result = match table.get(*pid).map_err(|e| WeaveflowError::Database {
            operation: "load_pipeline get",
            source: Box::new(e.into()),
        })? {
            Some(guard) => Ok(Some(guard.value())),
            None => Ok(None),
        };
        drop(table);
        result
    }

    pub fn list_pipelines(&self) -> WeaveflowResult<Vec<(PipelineId, PipelineDef)>> {
        let g = self.db.read().unwrap_or_else(|e| e.into_inner());
        let txn = g.begin_read().map_err(|e| WeaveflowError::Database {
            operation: "list_pipelines begin_read",
            source: Box::new(e.into()),
        })?;
        let table = match txn.open_table(PIPELINE) {
            Ok(t) => t,
            Err(_) => return Ok(Vec::new()), // table not created yet
        };
        let mut items = Vec::new();
        for result in table.iter().map_err(|e| WeaveflowError::Database {
            operation: "list_pipelines iter",
            source: Box::new(e.into()),
        })? {
            let (k, v) = result.map_err(|e| WeaveflowError::Database {
                operation: "list_pipelines read_row",
                source: Box::new(e.into()),
            })?;
            items.push((k.value(), v.value()));
        }
        Ok(items)
    }

    pub fn delete_pipeline(&self, pid: &PipelineId) -> WeaveflowResult<bool> {
        let g = self.db.read().unwrap_or_else(|e| e.into_inner());
        let txn = g.begin_write().map_err(|e| WeaveflowError::Database {
            operation: "delete_pipeline begin_write",
            source: Box::new(e.into()),
        })?;
        let mut table = txn.open_table(PIPELINE).map_err(|e| WeaveflowError::Database {
            operation: "delete_pipeline open_table",
            source: Box::new(e.into()),
        })?;
        let removed = table.remove(*pid).map_err(|e| WeaveflowError::Database {
            operation: "delete_pipeline remove",
            source: Box::new(e.into()),
        })?
        .is_some();
        drop(table);
        txn.commit().map_err(|e| WeaveflowError::Database {
            operation: "delete_pipeline commit",
            source: Box::new(e.into()),
        })?;
        Ok(removed)
    }

    pub fn find_pipeline_by_name(&self, name: &str) -> WeaveflowResult<Option<(PipelineId, PipelineDef)>> {
        for (pid, def) in self.list_pipelines()? {
            if &*def.name == name {
                return Ok(Some((pid, def)));
            }
        }
        Ok(None)
    }

    // ── Task ────────────────────────────────────────────────────────────

    /// 更新 TaskMeta（由 executor 内部调用）。
    pub fn save_task(&self, meta: &TaskMeta) -> WeaveflowResult<()> {
        let g = self.db.read().unwrap_or_else(|e| e.into_inner());
        let txn = g.begin_write().map_err(|e| WeaveflowError::Database {
            operation: "save_task begin_write",
            source: Box::new(e.into()),
        })?;
        let mut table = txn.open_table(TASK).map_err(|e| WeaveflowError::Database {
            operation: "save_task open_table",
            source: Box::new(e.into()),
        })?;
        table.insert(meta.task_id, meta).map_err(|e| WeaveflowError::Database {
            operation: "save_task insert",
            source: Box::new(e.into()),
        })?;
        drop(table);
        txn.commit().map_err(|e| WeaveflowError::Database {
            operation: "save_task commit",
            source: Box::new(e.into()),
        })?;
        Ok(())
    }

    /// 创建新 Task。内部自动分配 TaskId，确保数据完备。
    pub fn create_task(
        &self,
        pipeline_name: &str,
        inputs: serde_json::Value,
        result_ttl_secs: i64,
    ) -> WeaveflowResult<TaskId> {
        debug!(pipeline = %pipeline_name, "create_task");
        let task_id = TaskId::new();
        let meta = TaskMeta {
            task_id,
            pipeline_name: pipeline_name.to_string(),
            created_at: chrono::Utc::now(),
            result_ttl_secs,
            inputs,
            status: crate::tracker::meta::TASK_STATUS_RUNNING.to_string(),
        };
        let g = self.db.read().unwrap_or_else(|e| e.into_inner());
        let txn = g.begin_write().map_err(|e| WeaveflowError::Database {
            operation: "create_task begin_write",
            source: Box::new(e.into()),
        })?;
        let mut table = txn.open_table(TASK).map_err(|e| WeaveflowError::Database {
            operation: "create_task open_table",
            source: Box::new(e.into()),
        })?;
        table.insert(task_id, &meta).map_err(|e| WeaveflowError::Database {
            operation: "create_task insert",
            source: Box::new(e.into()),
        })?;
        drop(table);
        txn.commit().map_err(|e| WeaveflowError::Database {
            operation: "create_task commit",
            source: Box::new(e.into()),
        })?;
        Ok(task_id)
    }

    pub fn load_task(&self, task_id: &TaskId) -> WeaveflowResult<Option<TaskMeta>> {
        let g = self.db.read().unwrap_or_else(|e| e.into_inner());
        let txn = g.begin_read().map_err(|e| WeaveflowError::Database {
            operation: "load_task begin_read",
            source: Box::new(e.into()),
        })?;
        let table = txn.open_table(TASK).map_err(|e| WeaveflowError::Database {
            operation: "load_task open_table",
            source: Box::new(e.into()),
        })?;
        let result = match table.get(*task_id).map_err(|e| WeaveflowError::Database {
            operation: "load_task get",
            source: Box::new(e.into()),
        })? {
            Some(guard) => Ok(Some(guard.value())),
            None => Ok(None),
        };
        drop(table);
        result
    }

    /// 更新 task 的 status 字段（其他字段保持不变）。
    pub fn set_task_status(&self, task_id: &TaskId, status: &str) -> WeaveflowResult<()> {
        let g = self.db.read().unwrap_or_else(|e| e.into_inner());
        let txn = g.begin_write().map_err(|e| WeaveflowError::Database {
            operation: "set_task_status begin_write",
            source: Box::new(e.into()),
        })?;
        let mut table = txn.open_table(TASK).map_err(|e| WeaveflowError::Database {
            operation: "set_task_status open_table",
            source: Box::new(e.into()),
        })?;
        let existing: Option<TaskMeta> = table
            .get(*task_id)
            .map_err(|e| WeaveflowError::Database {
                operation: "set_task_status get",
                source: Box::new(e.into()),
            })?
            .map(|g| g.value());
        if let Some(mut meta) = existing {
            meta.status = status.to_string();
            table.insert(*task_id, &meta).map_err(|e| WeaveflowError::Database {
                operation: "set_task_status insert",
                source: Box::new(e.into()),
            })?;
        }
        drop(table);
        txn.commit().map_err(|e| WeaveflowError::Database {
            operation: "set_task_status commit",
            source: Box::new(e.into()),
        })?;
        Ok(())
    }

    /// 启动时恢复：把残留的 running 状态 task 标记为 interrupted。返回标记数量。
    pub fn mark_interrupted_tasks(&self) -> WeaveflowResult<u64> {
        let g = self.db.read().unwrap_or_else(|e| e.into_inner());
        let txn = g.begin_write().map_err(|e| WeaveflowError::Database {
            operation: "mark_interrupted begin_write",
            source: Box::new(e.into()),
        })?;
        let mut count = 0u64;
        {
            let mut table = match txn.open_table(TASK) {
                Ok(t) => t,
                Err(_) => return Ok(0),
            };
            let stale: Vec<TaskId> = table
                .iter()
                .map_err(|e| WeaveflowError::Database {
                    operation: "mark_interrupted iter",
                    source: Box::new(e.into()),
                })?
                .filter_map(|r| r.ok())
                .filter(|(_, v)| {
                    v.value().status == crate::tracker::meta::TASK_STATUS_RUNNING
                })
                .map(|(k, _)| k.value())
                .collect();
            for task_id in stale {
                let existing: Option<TaskMeta> = table
                    .get(task_id)
                    .map_err(|e| WeaveflowError::Database {
                        operation: "mark_interrupted get",
                        source: Box::new(e.into()),
                    })?
                    .map(|g| g.value());
                if let Some(mut meta) = existing {
                    meta.status = crate::tracker::meta::TASK_STATUS_INTERRUPTED.to_string();
                    table.insert(task_id, &meta).map_err(|e| WeaveflowError::Database {
                        operation: "mark_interrupted insert",
                        source: Box::new(e.into()),
                    })?;
                    count += 1;
                }
            }
        }
        txn.commit().map_err(|e| WeaveflowError::Database {
            operation: "mark_interrupted commit",
            source: Box::new(e.into()),
        })?;
        Ok(count)
    }

    pub fn list_tasks(&self) -> WeaveflowResult<Vec<TaskMeta>> {
        let g = self.db.read().unwrap_or_else(|e| e.into_inner());
        let txn = g.begin_read().map_err(|e| WeaveflowError::Database {
            operation: "list_tasks begin_read",
            source: Box::new(e.into()),
        })?;
        let table = match txn.open_table(TASK) {
            Ok(t) => t,
            Err(_) => return Ok(Vec::new()),
        };
        let mut items = Vec::new();
        for result in table.iter().map_err(|e| WeaveflowError::Database {
            operation: "list_tasks iter",
            source: Box::new(e.into()),
        })? {
            let (_, v) = result.map_err(|e| WeaveflowError::Database {
                operation: "list_tasks read_row",
                source: Box::new(e.into()),
            })?;
            items.push(v.value());
        }
        Ok(items)
    }

    // ── Snapshot ────────────────────────────────────────────────────────

    /// 普通快照（Durability::None — 写 OS 页缓存，不 fsync）。
    pub fn save_snapshot(&self, task_id: &TaskId, snap: Snapshot) -> WeaveflowResult<u64> {
        debug!(task_id = %task_id, step = %snap.step_id, "save_snapshot");
        self.save_snapshot_with_durability(task_id, snap, redb::Durability::None)
    }

    /// 持久化快照（Durability::Immediate — fsync 到磁盘, 同时redb还会将之前的非持久数据全部flush到磁盘）。
    pub fn save_snapshot_durable(&self, task_id: &TaskId, snap: Snapshot) -> WeaveflowResult<u64> {
        self.save_snapshot_with_durability(task_id, snap, redb::Durability::Immediate)
    }

    fn save_snapshot_with_durability(&self, task_id: &TaskId, mut snap: Snapshot, durability: redb::Durability) -> WeaveflowResult<u64> {
        // 自动递增 seq + 写入（单次写事务）
        let g = self.db.read().unwrap_or_else(|e| e.into_inner());
        let mut write_txn = g.begin_write().map_err(|e| WeaveflowError::Database {
            operation: "save_snapshot begin_write",
            source: Box::new(e.into()),
        })?;
        let max_seq = {
            let table = write_txn.open_table(SNAPSHOT).map_err(|e| WeaveflowError::Database {
                operation: "save_snapshot open_table",
                source: Box::new(e.into()),
            })?;
            let start = SnapshotKey { task_id: task_id.0, seq: 0 };
            let end   = SnapshotKey { task_id: task_id.0, seq: u64::MAX };
            let mut max = 0u64;
            let iter = table.range(start..=end).map_err(|e| WeaveflowError::Database {
                operation: "save_snapshot range",
                source: Box::new(e.into()),
            })?;
            for result in iter {
                let (k, _) = result.map_err(|e| WeaveflowError::Database {
                    operation: "save_snapshot read_row",
                    source: Box::new(e.into()),
                })?;
                if k.value().seq > max {
                    max = k.value().seq;
                }
            }
            max
        };
        let next_seq = max_seq + 1;
        snap.seq = next_seq;
        let key = SnapshotKey { task_id: task_id.0, seq: next_seq };
        let mut snap_table = write_txn.open_table(SNAPSHOT).map_err(|e| WeaveflowError::Database {
            operation: "save_snapshot open_snap_table",
            source: Box::new(e.into()),
        })?;
        snap_table.insert(key, &snap).map_err(|e| WeaveflowError::Database {
            operation: "save_snapshot insert",
            source: Box::new(e.into()),
        })?;
        drop(snap_table);
        write_txn.set_durability(durability);
        write_txn.commit().map_err(|e| WeaveflowError::Database {
            operation: "save_snapshot commit",
            source: Box::new(e.into()),
        })?;
        Ok(next_seq)
    }

    pub fn load_snapshots(&self, task_id: &TaskId) -> WeaveflowResult<Vec<(u64, Snapshot)>> {
        let g = self.db.read().unwrap_or_else(|e| e.into_inner());
        let txn = g.begin_read().map_err(|e| WeaveflowError::Database {
            operation: "load_snapshots begin_read",
            source: Box::new(e.into()),
        })?;
        let table = txn.open_table(SNAPSHOT).map_err(|e| WeaveflowError::Database {
            operation: "load_snapshots open_table",
            source: Box::new(e.into()),
        })?;
        let start = SnapshotKey { task_id: task_id.0, seq: 0 };
        let end   = SnapshotKey { task_id: task_id.0, seq: u64::MAX };
        let mut items = Vec::new();
        for result in table.range(start..=end).map_err(|e| WeaveflowError::Database {
            operation: "load_snapshots range",
            source: Box::new(e.into()),
        })? {
            let (k, v) = result.map_err(|e| WeaveflowError::Database {
                operation: "load_snapshots read_row",
                source: Box::new(e.into()),
            })?;
            items.push((k.value().seq, v.value()));
        }
        Ok(items)
    }

    /// 按 seq 点查单条 snapshot，避免全量加载。
    pub fn load_snapshot_by_seq(
        &self,
        task_id: &TaskId,
        seq: u64,
    ) -> WeaveflowResult<Option<Snapshot>> {
        let g = self.db.read().unwrap_or_else(|e| e.into_inner());
        let txn = g.begin_read().map_err(|e| WeaveflowError::Database {
            operation: "load_snapshot_by_seq begin_read",
            source: Box::new(e.into()),
        })?;
        let table = txn.open_table(SNAPSHOT).map_err(|e| WeaveflowError::Database {
            operation: "load_snapshot_by_seq open_table",
            source: Box::new(e.into()),
        })?;
        let key = SnapshotKey { task_id: task_id.0, seq };
        let result = table.get(key).map_err(|e| WeaveflowError::Database {
            operation: "load_snapshot_by_seq get",
            source: Box::new(e.into()),
        })?;
        Ok(result.map(|g| g.value()))
    }

    /// 只遍历 key 统计 snapshot 数量，不反序列化 value。
    pub fn count_snapshots(&self, task_id: &TaskId) -> WeaveflowResult<u64> {
        let g = self.db.read().unwrap_or_else(|e| e.into_inner());
        let txn = g.begin_read().map_err(|e| WeaveflowError::Database {
            operation: "count_snapshots begin_read",
            source: Box::new(e.into()),
        })?;
        let table = txn.open_table(SNAPSHOT).map_err(|e| WeaveflowError::Database {
            operation: "count_snapshots open_table",
            source: Box::new(e.into()),
        })?;
        let start = SnapshotKey { task_id: task_id.0, seq: 0 };
        let end   = SnapshotKey { task_id: task_id.0, seq: u64::MAX };
        let mut n = 0u64;
        for result in table.range(start..=end).map_err(|e| WeaveflowError::Database {
            operation: "count_snapshots range",
            source: Box::new(e.into()),
        })? {
            result.map_err(|e| WeaveflowError::Database {
                operation: "count_snapshots read_row",
                source: Box::new(e.into()),
            })?;
            n += 1;
        }
        Ok(n)
    }

    /// 列出 task 的全部 snapshot key（经 header-only 视图读取，跳过 output 字节拷贝）。
    pub fn list_snapshot_keys(&self, task_id: &TaskId) -> WeaveflowResult<Vec<(u64, StepId)>> {
        let g = self.db.read().unwrap_or_else(|e| e.into_inner());
        let txn = g.begin_read().map_err(|e| WeaveflowError::Database {
            operation: "list_snapshot_keys begin_read",
            source: Box::new(e.into()),
        })?;
        let table = txn.open_table(SNAPSHOT_HEADER).map_err(|e| WeaveflowError::Database {
            operation: "list_snapshot_keys open_table",
            source: Box::new(e.into()),
        })?;
        let start = SnapshotKey { task_id: task_id.0, seq: 0 };
        let end   = SnapshotKey { task_id: task_id.0, seq: u64::MAX };
        let mut items = Vec::new();
        for result in table.range(start..=end).map_err(|e| WeaveflowError::Database {
            operation: "list_snapshot_keys range",
            source: Box::new(e.into()),
        })? {
            let (k, v) = result.map_err(|e| WeaveflowError::Database {
                operation: "list_snapshot_keys read_row",
                source: Box::new(e.into()),
            })?;
            items.push((k.value().seq, v.value().step_id));
        }
        Ok(items)
    }

    // ── Object ──────────────────────────────────────────────────────────

    pub fn store_object(&self, value: &serde_json::Value) -> WeaveflowResult<ObjectDigest> {
        let json = serde_json::to_vec(value)
            .map_err(|e| WeaveflowError::Internal(format!("serialize: {e}")))?;
        let digest = ObjectDigest::compute(&json);
        let obj_value = ObjectValue::new(value.clone());
        let g = self.db.read().unwrap_or_else(|e| e.into_inner());
        let txn = g.begin_write().map_err(|e| WeaveflowError::Database {
            operation: "store_object begin_write",
            source: Box::new(e.into()),
        })?;
        {
            let mut table = txn.open_table(OBJECT).map_err(|e| WeaveflowError::Database {
                operation: "store_object open_table",
                source: Box::new(e.into()),
            })?;
            table.insert(digest, obj_value).map_err(|e| WeaveflowError::Database {
                operation: "store_object insert",
                source: Box::new(e.into()),
            })?;
        }
        txn.commit().map_err(|e| WeaveflowError::Database {
            operation: "store_object commit",
            source: Box::new(e.into()),
        })?;
        Ok(digest)
    }

    pub fn check_cache_bytes(&self, key: &[u8]) -> WeaveflowResult<Option<serde_json::Value>> {
        let digest = ObjectDigest::compute(key);
        let ck = CacheKey(digest);
        let g = self.db.read().unwrap_or_else(|e| e.into_inner());
        let txn = g.begin_read().map_err(|e| WeaveflowError::Database {
            operation: "check_cache_bytes begin_read",
            source: Box::new(e.into()),
        })?;
        match txn.open_table(CACHE) {
            Ok(table) => {
                match table.get(ck).map_err(|e| WeaveflowError::Database {
                    operation: "check_cache_bytes get",
                    source: Box::new(e.into()),
                })? {
                    Some(guard) => {
                        let output_digest = guard.value();
                        debug!(digest = %output_digest, "cache hit");
                        self.load_object(&output_digest)
                    }
                    None => {
                        debug!("cache miss");
                        Ok(None)
                    }
                }
            }
            Err(_) => Ok(None),
        }
    }

    /// Value-based cache: store output Value with cache key.
    pub fn set_cache_bytes(&self, key: &[u8], output: &serde_json::Value) -> WeaveflowResult<()> {
        let output_digest = self.store_object(output)?;
        debug!(digest = %output_digest, "cache set");
        let ck = CacheKey(ObjectDigest::compute(key));
        let g = self.db.read().unwrap_or_else(|e| e.into_inner());
        let txn = g.begin_write().map_err(|e| WeaveflowError::Database {
            operation: "set_cache_bytes begin_write",
            source: Box::new(e.into()),
        })?;
        {
            let mut table = txn.open_table(CACHE).map_err(|e| WeaveflowError::Database {
                operation: "set_cache_bytes open_table",
                source: Box::new(e.into()),
            })?;
            table.insert(ck, output_digest).map_err(|e| WeaveflowError::Database {
                operation: "set_cache_bytes insert",
                source: Box::new(e.into()),
            })?;
        }
        txn.commit().map_err(|e| WeaveflowError::Database {
            operation: "set_cache_bytes commit",
            source: Box::new(e.into()),
        })?;
        Ok(())
    }

    pub fn load_object(&self, digest: &ObjectDigest) -> WeaveflowResult<Option<serde_json::Value>> {
        let g = self.db.read().unwrap_or_else(|e| e.into_inner());
        let txn = g.begin_read().map_err(|e| WeaveflowError::Database {
            operation: "load_object begin_read",
            source: Box::new(e.into()),
        })?;
        let table = txn.open_table(OBJECT).map_err(|e| WeaveflowError::Database {
            operation: "load_object open_table",
            source: Box::new(e.into()),
        })?;
        let result = match table.get(*digest).map_err(|e| WeaveflowError::Database {
            operation: "load_object get",
            source: Box::new(e.into()),
        })? {
            Some(guard) => {
                let obj = guard.value();
                Ok(Some(obj.data))
            }
            None => Ok(None),
        };
        drop(table);
        result
    }

    // ── Prune / Sweep ───────────────────────────────────────────────────

    /// 扫描阶段（只读事务）：选出待删 task 及其判定时刻的 snapshot max_seq、
    /// 以及未被引用的 OBJECT / 悬空 CACHE 行。
    pub fn prune_scan(&self, options: &PruneOptions) -> WeaveflowResult<PrunePlan> {
        let g = self.db.read().unwrap_or_else(|e| e.into_inner());
        let read_txn = g.begin_read().map_err(|e| WeaveflowError::Database {
            operation: "prune_scan begin_read",
            source: Box::new(e.into()),
        })?;
        let table = read_txn.open_table(TASK).map_err(|e| WeaveflowError::Database {
            operation: "prune_scan open_table",
            source: Box::new(e.into()),
        })?;
        let now = Utc::now();
        let mut tasks: Vec<(TaskMeta, u64)> = Vec::new();
        for result in table.iter().map_err(|e| WeaveflowError::Database {
            operation: "prune_scan iter",
            source: Box::new(e.into()),
        })? {
            let (_, value) = result.map_err(|e| WeaveflowError::Database {
                operation: "prune_scan read_row",
                source: Box::new(e.into()),
            })?;
            let task: TaskMeta = value.value();
            if options.skip_tasks.contains(&task.task_id) {
                continue;
            }
            // 无 snapshot 的 task：status 仍为 running 的可能是新建/在跑任务，
            // 跳过；非 running（启动即失败、interrupted）且无 snapshot 的允许
            // 清理（max_seq=0 → 不删任何 snapshot），否则这类 TASK 行永久累积。
            let max_seq = match max_snapshot_seq(&read_txn, &task.task_id)? {
                Some(seq) => seq,
                None if task.status == TASK_STATUS_RUNNING => continue,
                None => 0,
            };
            if let Some(ref name) = options.pipeline
                && &task.pipeline_name != name { continue; }
            if !options.force {
                let age = now.signed_duration_since(task.created_at).num_seconds();
                if age < task.result_ttl_secs { continue; }
            }
            tasks.push((task, max_seq));
        }
        drop(table);
        drop(read_txn);

        let (dead_objects, freed_bytes, dangling_cache) = self.collect_object_garbage()?;
        Ok(PrunePlan {
            tasks,
            dead_objects,
            dangling_cache,
            freed_bytes,
        })
    }

    /// 执行阶段（写事务）：按 plan 删除。snapshot 只删 seq ≤ 判定时 max_seq 的，
    /// 判定后新写入的快照不受影响（读-写间隙竞态防护）。
    pub fn prune_execute(&self, plan: &PrunePlan, dry_run: bool) -> WeaveflowResult<PruneReport> {
        let mut report = PruneReport {
            objects_removed: plan.dead_objects.len() as u64,
            bytes_freed: plan.freed_bytes,
            cache_entries_removed: plan.dangling_cache.len() as u64,
            ..Default::default()
        };

        if dry_run {
            report.tasks_removed = plan.tasks.len() as u64;
            report.snapshots_removed = 0;
            return Ok(report);
        }

        if plan.tasks.is_empty() && plan.dead_objects.is_empty() && plan.dangling_cache.is_empty() {
            return Ok(report);
        }

        {
            let g = self.db.read().unwrap_or_else(|e| e.into_inner());
            let write_txn = g.begin_write().map_err(|e| WeaveflowError::Database {
                operation: "prune_execute begin_write",
                source: Box::new(e.into()),
            })?;
            let mut snap_table = write_txn.open_table(SNAPSHOT).map_err(|e| WeaveflowError::Database {
            operation: "prune_execute open_snap_table",
            source: Box::new(e.into()),
        })?;
        let mut task_table = write_txn.open_table(TASK).map_err(|e| WeaveflowError::Database {
            operation: "prune_execute open_task_table",
            source: Box::new(e.into()),
        })?;
        let mut obj_table = write_txn.open_table(OBJECT).map_err(|e| WeaveflowError::Database {
            operation: "prune_execute open_obj_table",
            source: Box::new(e.into()),
        })?;
        let mut cache_table = write_txn.open_table(CACHE).map_err(|e| WeaveflowError::Database {
            operation: "prune_execute open_cache_table",
            source: Box::new(e.into()),
        })?;

        for (task, max_seq) in &plan.tasks {
            let task_id = task.task_id;
            let start = SnapshotKey { task_id: task_id.0, seq: 0 };
            let end = SnapshotKey { task_id: task_id.0, seq: *max_seq };
            let snap_keys: Vec<SnapshotKey> = snap_table
                .range(start..=end).map_err(|e| WeaveflowError::Database {
                    operation: "prune_execute range",
                    source: Box::new(e.into()),
                })?
                .filter_map(|r| r.ok())
                .map(|(k, _)| k.value())
                .collect();

            report.snapshots_removed += snap_keys.len() as u64;

            for sk in &snap_keys {
                snap_table.remove(*sk).map_err(|e| WeaveflowError::Database {
                    operation: "prune_execute snap_remove",
                    source: Box::new(e.into()),
                })?;
            }

            task_table.remove(task_id).map_err(|e| WeaveflowError::Database {
                operation: "prune_execute task_remove",
                source: Box::new(e.into()),
            })?;
            report.tasks_removed += 1;
        }

        for digest in &plan.dead_objects {
            obj_table.remove(*digest).map_err(|e| WeaveflowError::Database {
                operation: "prune_execute obj_remove",
                source: Box::new(e.into()),
            })?;
        }
        for key in &plan.dangling_cache {
            cache_table.remove(*key).map_err(|e| WeaveflowError::Database {
                operation: "prune_execute cache_remove",
                source: Box::new(e.into()),
            })?;
        }

        drop((snap_table, task_table, obj_table, cache_table));
        write_txn.commit().map_err(|e| WeaveflowError::Database {
            operation: "prune_execute commit",
            source: Box::new(e.into()),
        })?;
        }
        let mut wg = self.db.write().unwrap_or_else(|e| e.into_inner());
        wg.compact().map_err(|e| WeaveflowError::Database {
            operation: "prune_execute compact",
            source: Box::new(e.into()),
        })?;

        Ok(report)
    }

    pub fn prune(&self, options: &PruneOptions) -> WeaveflowResult<PruneReport> {
        let plan = self.prune_scan(options)?;
        self.prune_execute(&plan, options.dry_run)
    }

    fn collect_object_garbage(&self) -> WeaveflowResult<(Vec<ObjectDigest>, u64, Vec<CacheKey>)> {
        let g = self.db.read().unwrap_or_else(|e| e.into_inner());
        let txn = g.begin_read().map_err(|e| WeaveflowError::Database {
            operation: "gc begin_read",
            source: Box::new(e.into()),
        })?;
        let cache_table = txn.open_table(CACHE).map_err(|e| WeaveflowError::Database {
            operation: "gc open_cache_table",
            source: Box::new(e.into()),
        })?;
        let obj_table = txn.open_table(OBJECT).map_err(|e| WeaveflowError::Database {
            operation: "gc open_obj_table",
            source: Box::new(e.into()),
        })?;
        let mut referenced = std::collections::HashSet::new();
        let mut dangling = Vec::new();
        for result in cache_table.iter().map_err(|e| WeaveflowError::Database {
            operation: "gc cache_iter",
            source: Box::new(e.into()),
        })? {
            let (k, v) = result.map_err(|e| WeaveflowError::Database {
                operation: "gc cache_read_row",
                source: Box::new(e.into()),
            })?;
            let digest = v.value();
            if obj_table.get(digest).map_err(|e| WeaveflowError::Database {
                operation: "gc obj_get",
                source: Box::new(e.into()),
            })?.is_some() {
                referenced.insert(digest);
            } else {
                dangling.push(k.value());
            }
        }
        let mut dead = Vec::new();
        let mut freed = 0u64;
        for result in obj_table.iter().map_err(|e| WeaveflowError::Database {
            operation: "gc obj_iter",
            source: Box::new(e.into()),
        })? {
            let (k, v) = result.map_err(|e| WeaveflowError::Database {
                operation: "gc obj_read_row",
                source: Box::new(e.into()),
            })?;
            if !referenced.contains(&k.value()) {
                freed += serde_json::to_vec(&v.value())
                    .map(|b| b.len() as u64)
                    .unwrap_or(0);
                dead.push(k.value());
            }
        }
        Ok((dead, freed, dangling))
    }
}

/// 检查 task 是否已有 snapshot。
fn max_snapshot_seq(txn: &redb::ReadTransaction, task_id: &TaskId) -> WeaveflowResult<Option<u64>> {
    let table = txn.open_table(SNAPSHOT).map_err(|e| WeaveflowError::Database {
        operation: "max_snapshot_seq open_table",
        source: Box::new(e.into()),
    })?;
    let start = SnapshotKey { task_id: task_id.0, seq: 0 };
    let end = SnapshotKey { task_id: task_id.0, seq: u64::MAX };
    let iter = table.range(start..=end).map_err(|e| WeaveflowError::Database {
        operation: "max_snapshot_seq range",
        source: Box::new(e.into()),
    })?;
    let mut max: Option<u64> = None;
    for result in iter {
        let (k, _) = result.map_err(|e| WeaveflowError::Database {
            operation: "max_snapshot_seq read_row",
            source: Box::new(e.into()),
        })?;
        let seq = k.value().seq;
        max = Some(max.map_or(seq, |m: u64| m.max(seq)));
    }
    Ok(max)
}

/// Prune 选项。
#[derive(Debug, Clone, Default)]
pub struct PruneOptions {
    pub dry_run: bool,
    pub force: bool,
    pub pipeline: Option<String>,
    pub skip_tasks: std::collections::HashSet<TaskId>,
}

/// prune_scan 的产出：待删 task 及判定时刻的 snapshot max_seq、待回收对象。
pub struct PrunePlan {
    pub tasks: Vec<(TaskMeta, u64)>,
    pub dead_objects: Vec<ObjectDigest>,
    pub dangling_cache: Vec<CacheKey>,
    pub freed_bytes: u64,
}

/// Prune 报告。
#[derive(Debug, Default)]
pub struct PruneReport {
    pub tasks_removed: u64,
    pub snapshots_removed: u64,
    pub objects_removed: u64,
    pub cache_entries_removed: u64,
    pub bytes_freed: u64,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dsl::StepId;

    fn temp_db() -> (Database, tempfile::TempDir) {
        let dir = tempfile::tempdir().expect("tempdir");
        let db = Database::open(dir.path().join("weaveflow.redb")).expect("open db");
        (db, dir)
    }

    fn save_one_snapshot(db: &Database, task_id: &TaskId) {
        let snap = Snapshot {
            seq: 0,
            step_id: StepId("s".to_string()),
            output: serde_json::to_vec(&serde_json::json!({"ok": true})).unwrap(),
        };
        db.save_snapshot(task_id, snap).expect("save snapshot");
    }

    #[test]
    fn fresh_db_reads_return_empty_not_error() {
        let (db, _dir) = temp_db();
        let tid = TaskId::new();
        assert!(db.load_task(&tid).unwrap().is_none());
        assert!(db.load_snapshots(&tid).unwrap().is_empty());
        assert_eq!(db.count_snapshots(&tid).unwrap(), 0);
        assert!(db.list_snapshot_keys(&tid).unwrap().is_empty());
        assert!(db
            .load_object(&ObjectDigest::compute(b"missing"))
            .unwrap()
            .is_none());
        assert!(db.check_cache_bytes(b"missing").unwrap().is_none());
        assert!(db.list_tasks().unwrap().is_empty());
        let report = db.prune(&PruneOptions::default()).unwrap();
        assert_eq!(report.tasks_removed, 0);
        assert_eq!(report.objects_removed, 0);
        assert_eq!(report.cache_entries_removed, 0);
    }

    #[test]
    fn save_pipeline_upsert_same_name_keeps_pid_and_overwrites() {
        let (db, _dir) = temp_db();
        let mut def = PipelineDef {
            name: "p".into(),
            description: None,
            storage: None,
            slots: vec![],
            steps: vec![crate::dsl::StepDef {
                id: StepId("s".into()),
                after: None,
                iterate: None,
                retry: None,
                cache: None,
                timeout_sec: None,
                op: crate::dsl::StepOp::Noop,
            }],
            output: crate::dsl::RefValue::Literal(serde_json::json!(null)),
        };
        let pid1 = db.save_pipeline_upsert(&def).unwrap();
        def.description = Some("v2".into());
        let pid2 = db.save_pipeline_upsert(&def).unwrap();
        assert_eq!(pid1, pid2, "同名 upsert 必须复用同一 PipelineId");
        let loaded = db.load_pipeline(&pid1).unwrap().unwrap();
        assert_eq!(loaded.description.as_deref(), Some("v2"));
        let all = db.list_pipelines().unwrap();
        assert_eq!(all.iter().filter(|(_, d)| d.name == "p").count(), 1);
    }

    #[test]
    fn prune_removes_terminal_task_without_snapshots() {
        let (db, _dir) = temp_db();
        let failed = db.create_task("p", serde_json::json!({}), 0).unwrap();
        db.set_task_status(&failed, crate::tracker::meta::TASK_STATUS_FAILED)
            .unwrap();
        let running = db.create_task("p", serde_json::json!({}), 0).unwrap();

        let report = db
            .prune(&PruneOptions { force: true, ..Default::default() })
            .unwrap();

        assert_eq!(report.tasks_removed, 1);
        assert!(db.load_task(&failed).unwrap().is_none());
        assert!(db.load_task(&running).unwrap().is_some());
    }

    #[test]
    fn prune_skips_tasks_in_skip_set() {
        let (db, _dir) = temp_db();
        let running = db.create_task("p", serde_json::json!({}), 0).unwrap();
        let finished = db.create_task("p", serde_json::json!({}), 0).unwrap();
        save_one_snapshot(&db, &running);
        save_one_snapshot(&db, &finished);

        let mut options = PruneOptions {
            force: true,
            ..Default::default()
        };
        options.skip_tasks.insert(running);
        let report = db.prune(&options).unwrap();

        assert_eq!(report.tasks_removed, 1);
        assert!(db.load_task(&running).unwrap().is_some());
        assert_eq!(db.load_snapshots(&running).unwrap().len(), 1);
        assert!(db.load_task(&finished).unwrap().is_none());
    }

    #[test]
    fn prune_gc_removes_unreferenced_objects() {
        let (db, _dir) = temp_db();
        db.set_cache_bytes(b"key-live", &serde_json::json!({"v": 1}))
            .unwrap();
        let dead = db.store_object(&serde_json::json!({"v": 2})).unwrap();

        let report = db.prune(&PruneOptions::default()).unwrap();

        assert_eq!(report.objects_removed, 1);
        assert!(report.bytes_freed > 0);
        assert!(db.load_object(&dead).unwrap().is_none());
        assert!(db.check_cache_bytes(b"key-live").unwrap().is_some());
    }

    #[test]
    fn prune_gc_removes_dangling_cache_entries() {
        let (db, _dir) = temp_db();
        let orphan_digest = ObjectDigest::compute(b"phantom-object-never-stored");
        {
            let ck = CacheKey(ObjectDigest::compute(b"dangling-cache-entry"));
            let g = db.db.read().unwrap_or_else(|e| e.into_inner());
            let txn = g.begin_write().unwrap();
            {
                let mut table = txn.open_table(CACHE).unwrap();
                table.insert(ck, orphan_digest).unwrap();
            }
            txn.commit().unwrap();
        }
        let alive_digest = db.store_object(&serde_json::json!({"alive": 1})).unwrap();

        let report = db.prune(&PruneOptions::default()).unwrap();

        assert_eq!(report.objects_removed, 1);
        assert_eq!(report.cache_entries_removed, 1);
        assert!(db.load_object(&alive_digest).unwrap().is_none());
    }

    #[test]
    fn prune_gc_clears_all_objects_when_cache_empty() {
        let (db, _dir) = temp_db();
        let d1 = db.store_object(&serde_json::json!({"v": 1})).unwrap();
        let d2 = db.store_object(&serde_json::json!({"v": 2})).unwrap();

        let report = db.prune(&PruneOptions::default()).unwrap();

        assert_eq!(report.objects_removed, 2);
        assert!(db.load_object(&d1).unwrap().is_none());
        assert!(db.load_object(&d2).unwrap().is_none());
    }

    #[test]
    fn prune_dry_run_reports_gc_without_deleting() {
        let (db, _dir) = temp_db();
        let dead = db.store_object(&serde_json::json!({"v": 1})).unwrap();

        let report = db
            .prune(&PruneOptions {
                dry_run: true,
                ..Default::default()
            })
            .unwrap();

        assert_eq!(report.objects_removed, 1);
        assert!(db.load_object(&dead).unwrap().is_some());
    }

    #[test]
    fn mark_interrupted_tasks_marks_running_as_failed_interrupted() {
        let (db, _dir) = temp_db();
        let running = db.create_task("p", serde_json::json!({}), 3600).unwrap();
        let finished = db.create_task("p", serde_json::json!({}), 3600).unwrap();
        db.set_task_status(&finished, crate::tracker::meta::TASK_STATUS_COMPLETED)
            .unwrap();

        let n = db.mark_interrupted_tasks().unwrap();

        assert_eq!(n, 1);
        assert_eq!(
            db.load_task(&running).unwrap().unwrap().status,
            crate::tracker::meta::TASK_STATUS_INTERRUPTED
        );
        assert_eq!(
            db.load_task(&finished).unwrap().unwrap().status,
            crate::tracker::meta::TASK_STATUS_COMPLETED
        );
    }

    #[test]
    fn count_and_list_snapshot_keys() {
        let (db, _dir) = temp_db();
        let tid = TaskId::new();
        save_one_snapshot(&db, &tid);
        save_one_snapshot(&db, &tid);

        assert_eq!(db.count_snapshots(&tid).unwrap(), 2);
        let keys = db.list_snapshot_keys(&tid).unwrap();
        assert_eq!(keys.len(), 2);
        assert_eq!(keys[0].0, 1);
        assert_eq!(keys[1].0, 2);
        assert_eq!(keys[0].1, StepId("s".to_string()));
    }

    #[test]
    fn prune_execute_preserves_snapshots_written_after_scan() {
        let (db, _dir) = temp_db();
        let task = db.create_task("p", serde_json::json!({}), 0).unwrap();
        save_one_snapshot(&db, &task);
        let plan = db
            .prune_scan(&PruneOptions {
                force: true,
                ..Default::default()
            })
            .unwrap();
        // scan 之后写入的新 snapshot（seq 2）不在判定 max_seq（seq 1）范围内
        save_one_snapshot(&db, &task);

        let report = db.prune_execute(&plan, false).unwrap();

        assert_eq!(report.snapshots_removed, 1);
        assert!(db.load_task(&task).unwrap().is_none());
        let remaining = db.load_snapshots(&task).unwrap();
        assert_eq!(remaining.len(), 1);
        assert_eq!(remaining[0].0, 2);
    }

    #[test]
    fn v0_database_auto_migrates_on_open() {
        use crate::store::database::{V0PipelineDef, V0TaskMeta};

        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("weaveflow.redb");
        let bak = dir.path().join("weaveflow.redb.v0.bak");

        let yaml = r#"
name: legacy_pipe
storage:
  result_ttl: "2h"
steps:
  - id: s
    type: noop
output: "{s.output}"
"#;
        let def = crate::dsl::parser::parse(yaml).expect("parse");
        let mut pipeline_json = serde_json::to_value(&def).expect("pipeline json");
        // 塞回已移除的 snapshot_ttl 字段，模拟 v0 时代的行
        pipeline_json["storage"]["snapshot_ttl"] = serde_json::json!("7d");

        let task_id = TaskId::new();
        let meta = TaskMeta {
            task_id,
            pipeline_name: "legacy_pipe".to_string(),
            created_at: chrono::Utc::now(),
            result_ttl_secs: 3600,
            inputs: serde_json::json!({}),
            status: crate::tracker::meta::TASK_STATUS_RUNNING.to_string(),
        };
        let mut task_json = serde_json::to_value(&meta).expect("task json");
        // v0 TaskMeta 可能没有 status 字段（serde default 应兜底为 "unknown"）
        task_json.as_object_mut().unwrap().remove("status");

        let pipeline_id = PipelineId::new();

        // 用 v0 类型名手工造库：1 条 pipeline + 1 条 task
        {
            let raw = RedbDb::create(&path).expect("create v0 db");
            let txn = raw.begin_write().unwrap();
            {
                let mut t = txn.open_table(V0_PIPELINE).unwrap();
                t.insert(pipeline_id, V0PipelineDef(pipeline_json)).unwrap();
            }
            {
                let mut t = txn.open_table(V0_TASK).unwrap();
                t.insert(task_id, V0TaskMeta(task_json)).unwrap();
            }
            txn.commit().unwrap();
        }

        let db = Database::open(&path).expect("open should auto-migrate");

        assert!(bak.exists(), "备份文件 {bak:?} 应存在");
        let pipelines = db.list_pipelines().unwrap();
        assert_eq!(pipelines.len(), 1);
        assert_eq!(&*pipelines[0].1.name, "legacy_pipe");
        assert_eq!(
            pipelines[0]
                .1
                .storage
                .as_ref()
                .and_then(|s| s.result_ttl)
                .map(|t| t.0.num_seconds()),
            Some(7200)
        );
        let tasks = db.list_tasks().unwrap();
        assert_eq!(tasks.len(), 1);
        assert_eq!(tasks[0].task_id, task_id);
        assert_eq!(tasks[0].status, "unknown");
        assert_eq!(tasks[0].result_ttl_secs, 3600);
    }
}
