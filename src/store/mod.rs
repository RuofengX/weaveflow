pub(super) mod database;
pub(super) mod object;

pub use object::{ObjectDigest, ObjectValue};
pub(crate) use crate::store::database::CacheKey;

use chrono::Utc;
use redb::{Database as RedbDb, ReadableTable};
use std::path::Path;
use tracing::debug;

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
        let path = db_path.as_ref();
        let db = RedbDb::create(path).map_err(|e| WeaveError::Database {
            operation: "create",
            source: Box::new(e.into()),
        })?;
        let txn = db.begin_write().map_err(|e| WeaveError::Database {
            operation: "init_tables begin_write",
            source: Box::new(e.into()),
        })?;
        let _ = txn.open_table(PIPELINE).map_err(|e| WeaveError::Database {
            operation: "init_tables pipeline",
            source: Box::new(e.into()),
        })?;
        let _ = txn.open_table(TASK).map_err(|e| WeaveError::Database {
            operation: "init_tables task",
            source: Box::new(e.into()),
        })?;
        let _ = txn.open_table(SNAPSHOT).map_err(|e| WeaveError::Database {
            operation: "init_tables snapshot",
            source: Box::new(e.into()),
        })?;
        let _ = txn.open_table(OBJECT).map_err(|e| WeaveError::Database {
            operation: "init_tables object",
            source: Box::new(e.into()),
        })?;
        let _ = txn.open_table(CACHE).map_err(|e| WeaveError::Database {
            operation: "init_tables cache",
            source: Box::new(e.into()),
        })?;
        txn.commit().map_err(|e| WeaveError::Database {
            operation: "init_tables commit",
            source: Box::new(e.into()),
        })?;
        debug!(path = %path.display(), "database opened");
        Ok(Database { db })
    }

    // ── Pipeline ────────────────────────────────────────────────────────

    pub fn save_pipeline_upsert(&self, def: &PipelineDef) -> WeaveResult<PipelineId> {
        debug!(name = %def.name, steps = def.steps.len(), "save_pipeline_upsert");
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

    /// 更新 task 的 status 字段（其他字段保持不变）。
    pub fn set_task_status(&self, task_id: &TaskId, status: &str) -> WeaveResult<()> {
        let txn = self.db.begin_write().map_err(|e| WeaveError::Database {
            operation: "set_task_status begin_write",
            source: Box::new(e.into()),
        })?;
        let mut table = txn.open_table(TASK).map_err(|e| WeaveError::Database {
            operation: "set_task_status open_table",
            source: Box::new(e.into()),
        })?;
        let existing: Option<TaskMeta> = table
            .get(*task_id)
            .map_err(|e| WeaveError::Database {
                operation: "set_task_status get",
                source: Box::new(e.into()),
            })?
            .map(|g| g.value());
        if let Some(mut meta) = existing {
            meta.status = status.to_string();
            table.insert(*task_id, &meta).map_err(|e| WeaveError::Database {
                operation: "set_task_status insert",
                source: Box::new(e.into()),
            })?;
        }
        drop(table);
        txn.commit().map_err(|e| WeaveError::Database {
            operation: "set_task_status commit",
            source: Box::new(e.into()),
        })?;
        Ok(())
    }

    /// 启动时恢复：把残留的 running 状态 task 标记为 interrupted。返回标记数量。
    pub fn mark_interrupted_tasks(&self) -> WeaveResult<u64> {
        let txn = self.db.begin_write().map_err(|e| WeaveError::Database {
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
                .map_err(|e| WeaveError::Database {
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
                    .map_err(|e| WeaveError::Database {
                        operation: "mark_interrupted get",
                        source: Box::new(e.into()),
                    })?
                    .map(|g| g.value());
                if let Some(mut meta) = existing {
                    meta.status = crate::tracker::meta::TASK_STATUS_INTERRUPTED.to_string();
                    table.insert(task_id, &meta).map_err(|e| WeaveError::Database {
                        operation: "mark_interrupted insert",
                        source: Box::new(e.into()),
                    })?;
                    count += 1;
                }
            }
        }
        txn.commit().map_err(|e| WeaveError::Database {
            operation: "mark_interrupted commit",
            source: Box::new(e.into()),
        })?;
        Ok(count)
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
        debug!(task_id = %task_id, step = %snap.step_id, "save_snapshot");
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
            let iter = table.range(start..=end).map_err(|e| WeaveError::Database {
                operation: "save_snapshot range",
                source: Box::new(e.into()),
            })?;
            for result in iter {
                let (k, _) = result.map_err(|e| WeaveError::Database {
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

    /// 按 seq 点查单条 snapshot，避免全量加载。
    pub fn load_snapshot_by_seq(
        &self,
        task_id: &TaskId,
        seq: u64,
    ) -> WeaveResult<Option<Snapshot>> {
        let txn = self.db.begin_read().map_err(|e| WeaveError::Database {
            operation: "load_snapshot_by_seq begin_read",
            source: Box::new(e.into()),
        })?;
        let table = txn.open_table(SNAPSHOT).map_err(|e| WeaveError::Database {
            operation: "load_snapshot_by_seq open_table",
            source: Box::new(e.into()),
        })?;
        let key = SnapshotKey { task_id: task_id.0, seq };
        let result = table.get(key).map_err(|e| WeaveError::Database {
            operation: "load_snapshot_by_seq get",
            source: Box::new(e.into()),
        })?;
        Ok(result.map(|g| g.value()))
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
    pub fn set_cache_bytes(&self, key: &[u8], output: &serde_json::Value) -> WeaveResult<()> {
        let output_digest = self.store_object(output)?;
        debug!(digest = %output_digest, "cache set");
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

    /// 扫描阶段（只读事务）：选出待删 task 及其判定时刻的 snapshot max_seq、
    /// 以及未被引用的 OBJECT / 悬空 CACHE 行。
    pub fn prune_scan(&self, options: &PruneOptions) -> WeaveResult<PrunePlan> {
        let read_txn = self.db.begin_read().map_err(|e| WeaveError::Database {
            operation: "prune_scan begin_read",
            source: Box::new(e.into()),
        })?;
        let table = read_txn.open_table(TASK).map_err(|e| WeaveError::Database {
            operation: "prune_scan open_table",
            source: Box::new(e.into()),
        })?;
        let now = Utc::now();
        let mut tasks: Vec<(TaskMeta, u64)> = Vec::new();
        for result in table.iter().map_err(|e| WeaveError::Database {
            operation: "prune_scan iter",
            source: Box::new(e.into()),
        })? {
            let (_, value) = result.map_err(|e| WeaveError::Database {
                operation: "prune_scan read_row",
                source: Box::new(e.into()),
            })?;
            let task: TaskMeta = value.value();
            if options.skip_tasks.contains(&task.task_id) {
                continue;
            }
            // 跳过没有 snapshot 的 task（尚在运行或新建）
            let max_seq = match max_snapshot_seq(&read_txn, &task.task_id)? {
                Some(seq) => seq,
                None => continue,
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
    pub fn prune_execute(&mut self, plan: &PrunePlan, dry_run: bool) -> WeaveResult<PruneReport> {
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

        let write_txn = self.db.begin_write().map_err(|e| WeaveError::Database {
            operation: "prune_execute begin_write",
            source: Box::new(e.into()),
        })?;
        let mut snap_table = write_txn.open_table(SNAPSHOT).map_err(|e| WeaveError::Database {
            operation: "prune_execute open_snap_table",
            source: Box::new(e.into()),
        })?;
        let mut task_table = write_txn.open_table(TASK).map_err(|e| WeaveError::Database {
            operation: "prune_execute open_task_table",
            source: Box::new(e.into()),
        })?;
        let mut obj_table = write_txn.open_table(OBJECT).map_err(|e| WeaveError::Database {
            operation: "prune_execute open_obj_table",
            source: Box::new(e.into()),
        })?;
        let mut cache_table = write_txn.open_table(CACHE).map_err(|e| WeaveError::Database {
            operation: "prune_execute open_cache_table",
            source: Box::new(e.into()),
        })?;

        for (task, max_seq) in &plan.tasks {
            let task_id = task.task_id;
            let start = SnapshotKey { task_id: task_id.0, seq: 0 };
            let end = SnapshotKey { task_id: task_id.0, seq: *max_seq };
            let snap_keys: Vec<SnapshotKey> = snap_table
                .range(start..=end).map_err(|e| WeaveError::Database {
                    operation: "prune_execute range",
                    source: Box::new(e.into()),
                })?
                .filter_map(|r| r.ok())
                .map(|(k, _)| k.value())
                .collect();

            report.snapshots_removed += snap_keys.len() as u64;

            for sk in &snap_keys {
                snap_table.remove(*sk).map_err(|e| WeaveError::Database {
                    operation: "prune_execute snap_remove",
                    source: Box::new(e.into()),
                })?;
            }

            task_table.remove(task_id).map_err(|e| WeaveError::Database {
                operation: "prune_execute task_remove",
                source: Box::new(e.into()),
            })?;
            report.tasks_removed += 1;
        }

        for digest in &plan.dead_objects {
            obj_table.remove(*digest).map_err(|e| WeaveError::Database {
                operation: "prune_execute obj_remove",
                source: Box::new(e.into()),
            })?;
        }
        for key in &plan.dangling_cache {
            cache_table.remove(*key).map_err(|e| WeaveError::Database {
                operation: "prune_execute cache_remove",
                source: Box::new(e.into()),
            })?;
        }

        drop((snap_table, task_table, obj_table, cache_table));
        write_txn.commit().map_err(|e| WeaveError::Database {
            operation: "prune_execute commit",
            source: Box::new(e.into()),
        })?;
        self.db.compact().map_err(|e| WeaveError::Database {
            operation: "prune_execute compact",
            source: Box::new(e.into()),
        })?;

        Ok(report)
    }

    pub fn prune(&mut self, options: &PruneOptions) -> WeaveResult<PruneReport> {
        let plan = self.prune_scan(options)?;
        self.prune_execute(&plan, options.dry_run)
    }

    fn collect_object_garbage(&self) -> WeaveResult<(Vec<ObjectDigest>, u64, Vec<CacheKey>)> {
        let txn = self.db.begin_read().map_err(|e| WeaveError::Database {
            operation: "gc begin_read",
            source: Box::new(e.into()),
        })?;
        let cache_table = txn.open_table(CACHE).map_err(|e| WeaveError::Database {
            operation: "gc open_cache_table",
            source: Box::new(e.into()),
        })?;
        let obj_table = txn.open_table(OBJECT).map_err(|e| WeaveError::Database {
            operation: "gc open_obj_table",
            source: Box::new(e.into()),
        })?;
        let mut referenced = std::collections::HashSet::new();
        let mut dangling = Vec::new();
        for result in cache_table.iter().map_err(|e| WeaveError::Database {
            operation: "gc cache_iter",
            source: Box::new(e.into()),
        })? {
            let (k, v) = result.map_err(|e| WeaveError::Database {
                operation: "gc cache_read_row",
                source: Box::new(e.into()),
            })?;
            let digest = v.value();
            if obj_table.get(digest).map_err(|e| WeaveError::Database {
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
        for result in obj_table.iter().map_err(|e| WeaveError::Database {
            operation: "gc obj_iter",
            source: Box::new(e.into()),
        })? {
            let (k, v) = result.map_err(|e| WeaveError::Database {
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
fn max_snapshot_seq(txn: &redb::ReadTransaction, task_id: &TaskId) -> WeaveResult<Option<u64>> {
    let table = txn.open_table(SNAPSHOT).map_err(|e| WeaveError::Database {
        operation: "max_snapshot_seq open_table",
        source: Box::new(e.into()),
    })?;
    let start = SnapshotKey { task_id: task_id.0, seq: 0 };
    let end = SnapshotKey { task_id: task_id.0, seq: u64::MAX };
    let iter = table.range(start..=end).map_err(|e| WeaveError::Database {
        operation: "max_snapshot_seq range",
        source: Box::new(e.into()),
    })?;
    let mut max: Option<u64> = None;
    for result in iter {
        let (k, _) = result.map_err(|e| WeaveError::Database {
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
        let db = Database::open(dir.path().join("weave.redb")).expect("open db");
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
        let (mut db, _dir) = temp_db();
        let tid = TaskId::new();
        assert!(db.load_task(&tid).unwrap().is_none());
        assert!(db.load_snapshots(&tid).unwrap().is_empty());
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
    fn prune_skips_tasks_in_skip_set() {
        let (mut db, _dir) = temp_db();
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
        let (mut db, _dir) = temp_db();
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
        let (mut db, _dir) = temp_db();
        let missing = ObjectDigest::compute(b"missing-object");
        db.set_cache(&serde_json::json!({"op": "noop"}), &missing)
            .unwrap();

        let report = db.prune(&PruneOptions::default()).unwrap();

        assert_eq!(report.cache_entries_removed, 1);
        assert_eq!(report.objects_removed, 0);
        assert!(db
            .check_cache(&serde_json::json!({"op": "noop"}))
            .unwrap()
            .is_none());
    }

    #[test]
    fn prune_gc_clears_all_objects_when_cache_empty() {
        let (mut db, _dir) = temp_db();
        let d1 = db.store_object(&serde_json::json!({"v": 1})).unwrap();
        let d2 = db.store_object(&serde_json::json!({"v": 2})).unwrap();

        let report = db.prune(&PruneOptions::default()).unwrap();

        assert_eq!(report.objects_removed, 2);
        assert!(db.load_object(&d1).unwrap().is_none());
        assert!(db.load_object(&d2).unwrap().is_none());
    }

    #[test]
    fn prune_dry_run_reports_gc_without_deleting() {
        let (mut db, _dir) = temp_db();
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
}
