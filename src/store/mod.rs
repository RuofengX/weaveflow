pub(super) mod database;
pub(super) mod object;

pub use object::{ObjectDigest, ObjectValue};
pub(crate) use crate::store::database::CacheKey;

use chrono::Utc;
use redb::{Database as RedbDb, ReadableTable};
use std::path::Path;

use crate::dsl::PipelineDef;
use crate::error::{WeaveError, WeaveResult};
use crate::store::database::{CACHE, OBJECT, PIPELINE, SNAPSHOT, TASK};
use crate::tracker::snapshot::{Snapshot, SnapshotKey};
use crate::tracker::{PipelineId, TaskId, TaskMeta};

/// weave 数据层入口。封装 redb。
#[derive(Debug)]
pub struct Database {
    pub(crate) db: redb::Database,
}

// ── 构造 ───────────────────────────────────────────────────────────────────

impl Database {
    pub fn open(db_path: impl AsRef<Path>) -> WeaveResult<Self> {
        let db = RedbDb::create(db_path).map_err(|e| WeaveError::Database {
            operation: "create",
            source: Box::new(e.into()),
        })?;
        Ok(Database { db })
    }

    // ── Pipeline ────────────────────────────────────────────────────────

    pub fn save_pipeline_upsert(&self, def: &PipelineDef) -> WeaveResult<PipelineId> {
        if let Some((pid, _)) = self.find_pipeline_by_name(&def.name)? {
            let txn = self.db.begin_write().map_err(|e| WeaveError::Database {
                operation: "save_pipeline_upsert begin_write",
                source: Box::new(e.into()),
            })?;
            let mut table = txn.open_table(PIPELINE).map_err(|e| WeaveError::Database {
                operation: "save_pipeline_upsert open_table",
                source: Box::new(e.into()),
            })?;
            table.insert(pid, def).map_err(|e| WeaveError::Database {
                operation: "save_pipeline_upsert insert",
                source: Box::new(e.into()),
            })?;
            drop(table);
            txn.commit().map_err(|e| WeaveError::Database {
                operation: "save_pipeline_upsert commit",
                source: Box::new(e.into()),
            })?;
            Ok(pid)
        } else {
            let pid = PipelineId::new();
            let txn = self.db.begin_write().map_err(|e| WeaveError::Database {
                operation: "save_pipeline begin_write",
                source: Box::new(e.into()),
            })?;
            let mut table = txn.open_table(PIPELINE).map_err(|e| WeaveError::Database {
                operation: "save_pipeline open_table",
                source: Box::new(e.into()),
            })?;
            table.insert(pid, def).map_err(|e| WeaveError::Database {
                operation: "save_pipeline insert",
                source: Box::new(e.into()),
            })?;
            drop(table);
            txn.commit().map_err(|e| WeaveError::Database {
                operation: "save_pipeline commit",
                source: Box::new(e.into()),
            })?;
            Ok(pid)
        }
    }

    pub fn load_pipeline(&self, pid: &PipelineId) -> WeaveResult<Option<PipelineDef>> {
        let txn = self.db.begin_read().map_err(|e| WeaveError::Database {
            operation: "load_pipeline begin_read",
            source: Box::new(e.into()),
        })?;
        let table = txn.open_table(PIPELINE).map_err(|e| WeaveError::Database {
            operation: "load_pipeline open_table",
            source: Box::new(e.into()),
        })?;
        let result = match table.get(*pid).map_err(|e| WeaveError::Database {
            operation: "load_pipeline get",
            source: Box::new(e.into()),
        })? {
            Some(guard) => Ok(Some(guard.value())),
            None => Ok(None),
        };
        drop(table);
        result
    }

    pub fn list_pipelines(&self) -> WeaveResult<Vec<(PipelineId, PipelineDef)>> {
        let txn = self.db.begin_read().map_err(|e| WeaveError::Database {
            operation: "list_pipelines begin_read",
            source: Box::new(e.into()),
        })?;
        let table = match txn.open_table(PIPELINE) {
            Ok(t) => t,
            Err(_) => return Ok(Vec::new()), // table not created yet
        };
        let mut items = Vec::new();
        for result in table.iter().map_err(|e| WeaveError::Database {
            operation: "list_pipelines iter",
            source: Box::new(e.into()),
        })? {
            let (k, v) = result.map_err(|e| WeaveError::Database {
                operation: "list_pipelines read_row",
                source: Box::new(e.into()),
            })?;
            items.push((k.value(), v.value()));
        }
        Ok(items)
    }

    pub fn delete_pipeline(&self, pid: &PipelineId) -> WeaveResult<bool> {
        let txn = self.db.begin_write().map_err(|e| WeaveError::Database {
            operation: "delete_pipeline begin_write",
            source: Box::new(e.into()),
        })?;
        let mut table = txn.open_table(PIPELINE).map_err(|e| WeaveError::Database {
            operation: "delete_pipeline open_table",
            source: Box::new(e.into()),
        })?;
        let removed = table.remove(*pid).map_err(|e| WeaveError::Database {
            operation: "delete_pipeline remove",
            source: Box::new(e.into()),
        })?
        .is_some();
        drop(table);
        txn.commit().map_err(|e| WeaveError::Database {
            operation: "delete_pipeline commit",
            source: Box::new(e.into()),
        })?;
        Ok(removed)
    }

    pub fn find_pipeline_by_name(&self, name: &str) -> WeaveResult<Option<(PipelineId, PipelineDef)>> {
        for (pid, def) in self.list_pipelines()? {
            if &*def.name == name {
                return Ok(Some((pid, def)));
            }
        }
        Ok(None)
    }

    // ── Task ────────────────────────────────────────────────────────────

    /// 更新 TaskMeta（由 executor 内部调用）。
    pub fn save_task(&self, meta: &TaskMeta) -> WeaveResult<()> {
        let txn = self.db.begin_write().map_err(|e| WeaveError::Database {
            operation: "save_task begin_write",
            source: Box::new(e.into()),
        })?;
        let mut table = txn.open_table(TASK).map_err(|e| WeaveError::Database {
            operation: "save_task open_table",
            source: Box::new(e.into()),
        })?;
        table.insert(meta.task_id, meta).map_err(|e| WeaveError::Database {
            operation: "save_task insert",
            source: Box::new(e.into()),
        })?;
        drop(table);
        txn.commit().map_err(|e| WeaveError::Database {
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
    ) -> WeaveResult<TaskId> {
        let task_id = TaskId::new();
        let meta = TaskMeta {
            task_id,
            pipeline_name: pipeline_name.to_string(),
            created_at: chrono::Utc::now(),
            result_ttl_secs,
            inputs,
        };
        let txn = self.db.begin_write().map_err(|e| WeaveError::Database {
            operation: "create_task begin_write",
            source: Box::new(e.into()),
        })?;
        let mut table = txn.open_table(TASK).map_err(|e| WeaveError::Database {
            operation: "create_task open_table",
            source: Box::new(e.into()),
        })?;
        table.insert(task_id, &meta).map_err(|e| WeaveError::Database {
            operation: "create_task insert",
            source: Box::new(e.into()),
        })?;
        drop(table);
        txn.commit().map_err(|e| WeaveError::Database {
            operation: "create_task commit",
            source: Box::new(e.into()),
        })?;
        Ok(task_id)
    }

    pub fn load_task(&self, task_id: &TaskId) -> WeaveResult<Option<TaskMeta>> {
        let txn = self.db.begin_read().map_err(|e| WeaveError::Database {
            operation: "load_task begin_read",
            source: Box::new(e.into()),
        })?;
        let table = txn.open_table(TASK).map_err(|e| WeaveError::Database {
            operation: "load_task open_table",
            source: Box::new(e.into()),
        })?;
        let result = match table.get(*task_id).map_err(|e| WeaveError::Database {
            operation: "load_task get",
            source: Box::new(e.into()),
        })? {
            Some(guard) => Ok(Some(guard.value())),
            None => Ok(None),
        };
        drop(table);
        result
    }

    pub fn list_tasks(&self) -> WeaveResult<Vec<TaskMeta>> {
        let txn = self.db.begin_read().map_err(|e| WeaveError::Database {
            operation: "list_tasks begin_read",
            source: Box::new(e.into()),
        })?;
        let table = match txn.open_table(TASK) {
            Ok(t) => t,
            Err(_) => return Ok(Vec::new()),
        };
        let mut items = Vec::new();
        for result in table.iter().map_err(|e| WeaveError::Database {
            operation: "list_tasks iter",
            source: Box::new(e.into()),
        })? {
            let (_, v) = result.map_err(|e| WeaveError::Database {
                operation: "list_tasks read_row",
                source: Box::new(e.into()),
            })?;
            items.push(v.value());
        }
        Ok(items)
    }

    // ── Snapshot ────────────────────────────────────────────────────────

    /// 普通快照（Durability::None — 写 OS 页缓存，不 fsync）。
    pub fn save_snapshot(&self, task_id: &TaskId, snap: Snapshot) -> WeaveResult<u64> {
        self.save_snapshot_with_durability(task_id, snap, redb::Durability::None)
    }

    /// 持久化快照（Durability::Immediate — fsync 到磁盘, 同时redb还会将之前的非持久数据全部flush到磁盘）。
    pub fn save_snapshot_durable(&self, task_id: &TaskId, snap: Snapshot) -> WeaveResult<u64> {
        self.save_snapshot_with_durability(task_id, snap, redb::Durability::Immediate)
    }

    fn save_snapshot_with_durability(&self, task_id: &TaskId, mut snap: Snapshot, durability: redb::Durability) -> WeaveResult<u64> {
        // 自动递增 seq + 写入（单次写事务）
        let mut write_txn = self.db.begin_write().map_err(|e| WeaveError::Database {
            operation: "save_snapshot begin_write",
            source: Box::new(e.into()),
        })?;
        let max_seq = {
            let table = write_txn.open_table(SNAPSHOT).map_err(|e| WeaveError::Database {
                operation: "save_snapshot open_table",
                source: Box::new(e.into()),
            })?;
            let start = SnapshotKey { task_id: task_id.0, seq: 0 };
            let end   = SnapshotKey { task_id: task_id.0, seq: u64::MAX };
            let mut max = 0u64;
            if let Ok(iter) = table.range(start..=end) {
                for result in iter.flatten() {
                    let (k, _) = result;
                    if k.value().seq > max {
                        max = k.value().seq;
                    }
                }
            }
            max
        };
        let next_seq = max_seq + 1;
        snap.seq = next_seq;
        let key = SnapshotKey { task_id: task_id.0, seq: next_seq };
        let mut snap_table = write_txn.open_table(SNAPSHOT).map_err(|e| WeaveError::Database {
            operation: "save_snapshot open_snap_table",
            source: Box::new(e.into()),
        })?;
        snap_table.insert(key, &snap).map_err(|e| WeaveError::Database {
            operation: "save_snapshot insert",
            source: Box::new(e.into()),
        })?;
        drop(snap_table);
        write_txn.set_durability(durability);
        write_txn.commit().map_err(|e| WeaveError::Database {
            operation: "save_snapshot commit",
            source: Box::new(e.into()),
        })?;
        Ok(next_seq)
    }

    pub fn load_snapshots(&self, task_id: &TaskId) -> WeaveResult<Vec<(u64, Snapshot)>> {
        let txn = self.db.begin_read().map_err(|e| WeaveError::Database {
            operation: "load_snapshots begin_read",
            source: Box::new(e.into()),
        })?;
        let table = txn.open_table(SNAPSHOT).map_err(|e| WeaveError::Database {
            operation: "load_snapshots open_table",
            source: Box::new(e.into()),
        })?;
        let start = SnapshotKey { task_id: task_id.0, seq: 0 };
        let end   = SnapshotKey { task_id: task_id.0, seq: u64::MAX };
        let mut items = Vec::new();
        for result in table.range(start..=end).map_err(|e| WeaveError::Database {
            operation: "load_snapshots range",
            source: Box::new(e.into()),
        })? {
            let (k, v) = result.map_err(|e| WeaveError::Database {
                operation: "load_snapshots read_row",
                source: Box::new(e.into()),
            })?;
            items.push((k.value().seq, v.value()));
        }
        Ok(items)
    }

    // ── Object ──────────────────────────────────────────────────────────

    pub fn store_object(&self, value: &serde_json::Value) -> WeaveResult<ObjectDigest> {
        let json = serde_json::to_vec(value)
            .map_err(|e| WeaveError::Internal(format!("serialize: {e}")))?;
        let digest = ObjectDigest::compute(&json);
        let obj_value = ObjectValue::new(value.clone());
        let txn = self.db.begin_write().map_err(|e| WeaveError::Database {
            operation: "store_object begin_write",
            source: Box::new(e.into()),
        })?;
        {
            let mut table = txn.open_table(OBJECT).map_err(|e| WeaveError::Database {
                operation: "store_object open_table",
                source: Box::new(e.into()),
            })?;
            let existing = table.get(&digest).map_err(|e| WeaveError::Database {
                operation: "store_object get",
                source: Box::new(e.into()),
            })?.map(|g| g.value());
            match existing {
                Some(mut obj) => {
                    obj.ref_count += 1;
                    table.insert(digest, obj).map_err(|e| WeaveError::Database {
                        operation: "store_object update",
                        source: Box::new(e.into()),
                    })?;
                }
                None => {
                    table.insert(digest, obj_value).map_err(|e| WeaveError::Database {
                        operation: "store_object insert",
                        source: Box::new(e.into()),
                    })?;
                }
            }
        }
        txn.commit().map_err(|e| WeaveError::Database {
            operation: "store_object commit",
            source: Box::new(e.into()),
        })?;
        Ok(digest)
    }

    /// 查询缓存。返回 (output_digest, output_value) 或 None。
    pub fn check_cache(
        &self,
        inputs_val: &serde_json::Value,
    ) -> WeaveResult<Option<(ObjectDigest, serde_json::Value)>> {
        let ck = CacheKey(ObjectDigest::compute(
            &serde_json::to_vec(inputs_val).unwrap_or_default(),
        ));
        let txn = self.db.begin_read().map_err(|e| WeaveError::Database {
            operation: "check_cache begin_read",
            source: Box::new(e.into()),
        })?;
        let table_result = txn.open_table(CACHE);
        match table_result {
            Ok(table) => {
                match table.get(ck).map_err(|e| WeaveError::Database {
                    operation: "check_cache get",
                    source: Box::new(e.into()),
                })? {
                    Some(guard) => {
                        let digest = guard.value();
                        let value = self.load_object(&digest)?;
                        Ok(value.map(|v| (digest, v)))
                    }
                    None => Ok(None),
                }
            }
            Err(_) => Ok(None),
        }
    }

    /// 写入缓存。
    pub fn set_cache(
        &self,
        inputs_val: &serde_json::Value,
        output_digest: &ObjectDigest,
    ) -> WeaveResult<()> {
        let ck = CacheKey(ObjectDigest::compute(
            &serde_json::to_vec(inputs_val).unwrap_or_default(),
        ));
        let txn = self.db.begin_write().map_err(|e| WeaveError::Database {
            operation: "set_cache begin_write",
            source: Box::new(e.into()),
        })?;
        let mut table = txn.open_table(CACHE).map_err(|e| WeaveError::Database {
            operation: "set_cache open_table",
            source: Box::new(e.into()),
        })?;
        table.insert(ck, *output_digest).map_err(|e| WeaveError::Database {
            operation: "set_cache insert",
            source: Box::new(e.into()),
        })?;
        drop(table);
        txn.commit().map_err(|e| WeaveError::Database {
            operation: "set_cache commit",
            source: Box::new(e.into()),
        })?;
        Ok(())
    }

    /// Value-based cache: check if output is cached. Returns the output Value.
    pub fn check_cache_bytes(&self, key: &[u8]) -> WeaveResult<Option<serde_json::Value>> {
        let digest = ObjectDigest::compute(key);
        let ck = CacheKey(digest);
        let txn = self.db.begin_read().map_err(|e| WeaveError::Database {
            operation: "check_cache_bytes begin_read",
            source: Box::new(e.into()),
        })?;
        match txn.open_table(CACHE) {
            Ok(table) => {
                match table.get(ck).map_err(|e| WeaveError::Database {
                    operation: "check_cache_bytes get",
                    source: Box::new(e.into()),
                })? {
                    Some(guard) => {
                        let output_digest = guard.value();
                        self.load_object(&output_digest)
                    }
                    None => Ok(None),
                }
            }
            Err(_) => Ok(None),
        }
    }

    /// Value-based cache: store output Value with cache key.
    pub fn set_cache_bytes(&self, key: &[u8], output: &serde_json::Value) -> WeaveResult<()> {
        let output_digest = self.store_object(output)?;
        let ck = CacheKey(ObjectDigest::compute(key));
        let txn = self.db.begin_write().map_err(|e| WeaveError::Database {
            operation: "set_cache_bytes begin_write",
            source: Box::new(e.into()),
        })?;
        {
            let mut table = txn.open_table(CACHE).map_err(|e| WeaveError::Database {
                operation: "set_cache_bytes open_table",
                source: Box::new(e.into()),
            })?;
            table.insert(ck, output_digest).map_err(|e| WeaveError::Database {
                operation: "set_cache_bytes insert",
                source: Box::new(e.into()),
            })?;
        }
        txn.commit().map_err(|e| WeaveError::Database {
            operation: "set_cache_bytes commit",
            source: Box::new(e.into()),
        })?;
        Ok(())
    }

    pub fn load_object(&self, digest: &ObjectDigest) -> WeaveResult<Option<serde_json::Value>> {
        let txn = self.db.begin_read().map_err(|e| WeaveError::Database {
            operation: "load_object begin_read",
            source: Box::new(e.into()),
        })?;
        let table = txn.open_table(OBJECT).map_err(|e| WeaveError::Database {
            operation: "load_object open_table",
            source: Box::new(e.into()),
        })?;
        let result = match table.get(*digest).map_err(|e| WeaveError::Database {
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

    pub fn prune(&mut self, options: &PruneOptions) -> WeaveResult<PruneReport> {
        let mut report = PruneReport::default();

        let read_txn = self.db.begin_read().map_err(|e| WeaveError::Database {
            operation: "prune begin_read",
            source: Box::new(e.into()),
        })?;
        let table = read_txn.open_table(TASK).map_err(|e| WeaveError::Database {
            operation: "prune open_table",
            source: Box::new(e.into()),
        })?;
        let now = Utc::now();
        let mut to_delete: Vec<TaskMeta> = Vec::new();
        for result in table.iter().map_err(|e| WeaveError::Database {
            operation: "prune iter",
            source: Box::new(e.into()),
        })? {
            let (_, value) = result.map_err(|e| WeaveError::Database {
                operation: "prune read_row",
                source: Box::new(e.into()),
            })?;
            let task: TaskMeta = value.value();
            // 跳过没有 snapshot 的 task（尚在运行或新建）
            if !has_snapshots(&read_txn, &task.task_id)? {
                continue;
            }
            if let Some(ref name) = options.pipeline
                && &task.pipeline_name != name { continue; }
            if !options.force {
                let age = now.signed_duration_since(task.created_at).num_seconds();
                if age < task.result_ttl_secs { continue; }
            }
            to_delete.push(task);
        }
        drop(table);
        drop(read_txn);

        if to_delete.is_empty() { return Ok(report); }

        if !options.dry_run {
            let write_txn = self.db.begin_write().map_err(|e| WeaveError::Database {
                operation: "prune begin_write",
                source: Box::new(e.into()),
            })?;
            let mut snap_table = write_txn.open_table(SNAPSHOT).map_err(|e| WeaveError::Database {
                operation: "prune open_snap_table",
                source: Box::new(e.into()),
            })?;
            let mut task_table = write_txn.open_table(TASK).map_err(|e| WeaveError::Database {
                operation: "prune open_task_table",
                source: Box::new(e.into()),
            })?;
            let obj_table = write_txn.open_table(OBJECT).map_err(|e| WeaveError::Database {
                operation: "prune open_obj_table",
                source: Box::new(e.into()),
            })?;

            for task in &to_delete {
                let task_id = task.task_id;
                let start = SnapshotKey { task_id: task_id.0, seq: 0 };
                let end = SnapshotKey { task_id: task_id.0, seq: u64::MAX };
                let snap_entries: Vec<(SnapshotKey, Snapshot)> = snap_table
                    .range(start..=end).map_err(|e| WeaveError::Database {
                        operation: "prune range",
                        source: Box::new(e.into()),
                    })?
                    .filter_map(|r| r.ok())
                    .map(|(k, v)| (k.value(), v.value()))
                    .collect();

                report.snapshots_removed += snap_entries.len() as u64;

                for (sk, _snap) in &snap_entries {
                    snap_table.remove(*sk).map_err(|e| WeaveError::Database {
                        operation: "prune snap_remove",
                        source: Box::new(e.into()),
                    })?;
                }

                task_table.remove(task_id).map_err(|e| WeaveError::Database {
                    operation: "prune task_remove",
                    source: Box::new(e.into()),
                })?;
                report.tasks_removed += 1;
            }

            drop((snap_table, task_table, obj_table));
            write_txn.commit().map_err(|e| WeaveError::Database {
                operation: "prune commit",
                source: Box::new(e.into()),
            })?;
            self.db.compact().map_err(|e| WeaveError::Database {
                operation: "prune compact",
                source: Box::new(e.into()),
            })?;
        } else {
            report.tasks_removed = to_delete.len() as u64;
        }

        Ok(report)
    }
}

/// 检查 task 是否已有 snapshot。
fn has_snapshots(txn: &redb::ReadTransaction, task_id: &TaskId) -> WeaveResult<bool> {
    let table = txn.open_table(SNAPSHOT).map_err(|e| WeaveError::Database {
        operation: "has_snapshots open_table",
        source: Box::new(e.into()),
    })?;
    let start = SnapshotKey { task_id: task_id.0, seq: 0 };
    let end = SnapshotKey { task_id: task_id.0, seq: u64::MAX };
    if let Ok(iter) = table.range(start..=end) {
        Ok(iter.flatten().next().is_some())
    } else {
        Ok(false)
    }
}

/// Prune 选项。
#[derive(Debug, Clone, Default)]
pub struct PruneOptions {
    pub dry_run: bool,
    pub force: bool,
    pub pipeline: Option<String>,
}

/// Prune 报告。
#[derive(Debug, Default)]
pub struct PruneReport {
    pub tasks_removed: u64,
    pub snapshots_removed: u64,
    pub objects_removed: u64,
    pub bytes_freed: u64,
}
