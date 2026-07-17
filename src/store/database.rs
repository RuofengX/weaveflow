
use redb::{Key as RedbKey, TableDefinition, TypeName};
use redb::Value as RedbValue;
use std::cmp::Ordering;

use crate::dsl::PipelineDef;
use crate::store::object::ObjectDigest;
use crate::store::object::ObjectValue;
use crate::tracker::snapshot::{Snapshot, SnapshotKey};
use crate::tracker::{PipelineId, TaskId, TaskMeta};
use uuid::Uuid;

// ── TaskId（UUID v4，定长 16 字节） ───────────────────────────────────

impl RedbValue for TaskId {
    type SelfType<'a> = TaskId where Self: 'a;
    type AsBytes<'a> = Vec<u8> where Self: 'a;
    fn fixed_width() -> Option<usize> { Some(16) }
    fn from_bytes<'a>(data: &'a [u8]) -> Self::SelfType<'a> where Self: 'a {
        let bytes: [u8; 16] = data[..16].try_into().expect("TaskId: 需要 16 字节");
        TaskId(Uuid::from_bytes(bytes))
    }
    fn as_bytes<'a, 'b: 'a>(value: &'a Self::SelfType<'b>) -> Self::AsBytes<'a> where Self: 'b {
        value.0.as_bytes().to_vec()
    }
    fn type_name() -> TypeName { TypeName::new("weave::TaskId") }
}

impl RedbKey for TaskId {
    fn compare(data1: &[u8], data2: &[u8]) -> Ordering { data1.cmp(data2) }
}

// ── PipelineId（UUID v4，定长 16 字节）

impl RedbValue for PipelineId {
    type SelfType<'a> = PipelineId where Self: 'a;
    type AsBytes<'a> = Vec<u8> where Self: 'a;
    fn fixed_width() -> Option<usize> { Some(16) }
    fn from_bytes<'a>(data: &'a [u8]) -> Self::SelfType<'a> where Self: 'a {
        let bytes: [u8; 16] = data[..16].try_into().expect("PipelineId: 需要 16 字节");
        PipelineId(Uuid::from_bytes(bytes))
    }
    fn as_bytes<'a, 'b: 'a>(value: &'a Self::SelfType<'b>) -> Self::AsBytes<'a> where Self: 'b {
        value.0.as_bytes().to_vec()
    }
    fn type_name() -> TypeName { TypeName::new("weave::PipelineId") }
}

impl RedbKey for PipelineId {
    fn compare(data1: &[u8], data2: &[u8]) -> Ordering { data1.cmp(data2) }
}


// ── SnapshotKey（16 字节 UUID + 8 字节 u64 BE = 24 字节） ─────────────

impl RedbValue for SnapshotKey {
    type SelfType<'a> = SnapshotKey where Self: 'a;
    type AsBytes<'a> = Vec<u8> where Self: 'a;
    fn fixed_width() -> Option<usize> { Some(24) }
    fn from_bytes<'a>(data: &'a [u8]) -> Self::SelfType<'a> where Self: 'a {
        let uuid_bytes: [u8; 16] = data[..16].try_into().expect("SnapshotKey UUID: 需要 16 字节");
        let seq = u64::from_be_bytes(data[16..24].try_into().expect("SnapshotKey seq: 需要 8 字节"));
        SnapshotKey { task_id: Uuid::from_bytes(uuid_bytes), seq }
    }
    fn as_bytes<'a, 'b: 'a>(value: &'a Self::SelfType<'b>) -> Self::AsBytes<'a> where Self: 'b {
        let mut buf = Vec::with_capacity(24);
        buf.extend_from_slice(value.task_id.as_bytes());
        buf.extend_from_slice(&value.seq.to_be_bytes());
        buf
    }
    fn type_name() -> TypeName { TypeName::new("weave::SnapshotKey") }
}

impl RedbKey for SnapshotKey {
    fn compare(data1: &[u8], data2: &[u8]) -> Ordering { data1.cmp(data2) }
}

// ── ObjectDigest（定长 32 字节 SHA256） ───────────────────────────────────

impl RedbValue for ObjectDigest {
    type SelfType<'a> = ObjectDigest where Self: 'a;
    type AsBytes<'a> = Vec<u8> where Self: 'a;
    fn fixed_width() -> Option<usize> { Some(32) }
    fn from_bytes<'a>(data: &'a [u8]) -> Self::SelfType<'a> where Self: 'a {
        let bytes: [u8; 32] = data
            .try_into()
            .expect("ObjectDigest: 需要 32 字节");
        ObjectDigest(bytes)
    }
    fn as_bytes<'a, 'b: 'a>(value: &'a Self::SelfType<'b>) -> Self::AsBytes<'a> where Self: 'b {
        value.as_bytes().to_vec()
    }
    fn type_name() -> TypeName { TypeName::new("weave::ObjectDigest::v1") }
}

impl RedbKey for ObjectDigest {
    fn compare(data1: &[u8], data2: &[u8]) -> Ordering { data1.cmp(data2) }
}

// ── Value 类型：serde_json 序列化 ──────────────────────────────────────

impl RedbValue for PipelineDef {
    type SelfType<'a> = PipelineDef where Self: 'a;
    type AsBytes<'a> = Vec<u8> where Self: 'a;
    fn fixed_width() -> Option<usize> { None }
    fn from_bytes<'a>(data: &'a [u8]) -> Self::SelfType<'a> where Self: 'a {
        serde_json::from_slice(data)
            .expect("反序列化 PipelineDef 失败")
    }
    fn as_bytes<'a, 'b: 'a>(value: &'a Self::SelfType<'b>) -> Self::AsBytes<'a> where Self: 'b {
        serde_json::to_vec(value).expect("序列化 PipelineDef 失败")
    }
    fn type_name() -> TypeName { TypeName::new("weave::PipelineDef::v1") }
}

impl RedbValue for TaskMeta {
    type SelfType<'a> = TaskMeta where Self: 'a;
    type AsBytes<'a> = Vec<u8> where Self: 'a;
    fn fixed_width() -> Option<usize> { None }
    fn from_bytes<'a>(data: &'a [u8]) -> Self::SelfType<'a> where Self: 'a {
        serde_json::from_slice(data)
            .expect("反序列化 TaskMeta 失败")
    }
    fn as_bytes<'a, 'b: 'a>(value: &'a Self::SelfType<'b>) -> Self::AsBytes<'a> where Self: 'b {
        serde_json::to_vec(value).expect("序列化 TaskMeta 失败")
    }
    fn type_name() -> TypeName { TypeName::new("weave::TaskMeta::v1") }
}

impl RedbValue for Snapshot {
    type SelfType<'a> = Snapshot where Self: 'a;
    type AsBytes<'a> = Vec<u8> where Self: 'a;
    fn fixed_width() -> Option<usize> { None }
    fn from_bytes<'a>(data: &'a [u8]) -> Self::SelfType<'a> where Self: 'a {
        serde_json::from_slice(data)
            .expect("反序列化 Snapshot 失败")
    }
    fn as_bytes<'a, 'b: 'a>(value: &'a Self::SelfType<'b>) -> Self::AsBytes<'a> where Self: 'b {
        serde_json::to_vec(value).expect("序列化 Snapshot 失败")
    }
    fn type_name() -> TypeName { TypeName::new("weave::Snapshot::v1") }
}

impl RedbValue for ObjectValue {
    type SelfType<'a> = ObjectValue where Self: 'a;
    type AsBytes<'a> = Vec<u8> where Self: 'a;
    fn fixed_width() -> Option<usize> { None }
    fn from_bytes<'a>(data: &'a [u8]) -> Self::SelfType<'a> where Self: 'a {
        serde_json::from_slice(data)
            .expect("反序列化 ObjectValue 失败")
    }
    fn as_bytes<'a, 'b: 'a>(value: &'a Self::SelfType<'b>) -> Self::AsBytes<'a> where Self: 'b {
        serde_json::to_vec(value).expect("序列化 ObjectValue 失败")
    }
    fn type_name() -> TypeName { TypeName::new("weave::Object::v1") }
}

pub const PIPELINE: TableDefinition<PipelineId, PipelineDef> = TableDefinition::new("pipeline");
pub const TASK: TableDefinition<TaskId, TaskMeta> = TableDefinition::new("task");
pub const SNAPSHOT: TableDefinition<SnapshotKey, Snapshot> = TableDefinition::new("snapshot");
pub const OBJECT: TableDefinition<ObjectDigest, ObjectValue> = TableDefinition::new("object");

// ── CacheKey（单 ObjectDigest，32 字节固定大小） ─────────────────

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CacheKey(pub ObjectDigest);

impl RedbValue for CacheKey {
    type SelfType<'a> = CacheKey where Self: 'a;
    type AsBytes<'a> = Vec<u8> where Self: 'a;
    fn fixed_width() -> Option<usize> { Some(32) }
    fn from_bytes<'a>(data: &'a [u8]) -> Self::SelfType<'a> where Self: 'a {
        let bytes: [u8; 32] = data
            .try_into()
            .expect("CacheKey: 需要 32 字节");
        CacheKey(ObjectDigest(bytes))
    }
    fn as_bytes<'a, 'b: 'a>(value: &'a Self::SelfType<'b>) -> Self::AsBytes<'a> where Self: 'b {
        value.0.as_bytes().to_vec()
    }
    fn type_name() -> TypeName { TypeName::new("weave::CacheKey") }
}

impl RedbKey for CacheKey {
    fn compare(data1: &[u8], data2: &[u8]) -> Ordering { data1.cmp(data2) }
}

pub const CACHE: TableDefinition<CacheKey, ObjectDigest> = TableDefinition::new("cache");

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn value_type_names_carry_v1_schema_version() {
        let names = [
            <PipelineDef as RedbValue>::type_name(),
            <TaskMeta as RedbValue>::type_name(),
            <Snapshot as RedbValue>::type_name(),
            <ObjectValue as RedbValue>::type_name(),
            <ObjectDigest as RedbValue>::type_name(),
        ];
        for name in names {
            let dbg = format!("{name:?}");
            assert!(
                dbg.contains("::v1"),
                "type name {dbg} missing ::v1 suffix"
            );
        }
    }

    #[test]
    fn object_digest_from_bytes_roundtrip() {
        let digest = ObjectDigest::compute(b"hello");
        let bytes = <ObjectDigest as RedbValue>::as_bytes(&digest);
        let back = <ObjectDigest as RedbValue>::from_bytes(&bytes);
        assert_eq!(digest.as_bytes(), back.as_bytes());
    }

    #[test]
    #[should_panic(expected = "32")]
    fn object_digest_from_bytes_rejects_short_data() {
        let _ = <ObjectDigest as RedbValue>::from_bytes(&[0u8; 8]);
    }

    #[test]
    #[should_panic(expected = "32")]
    fn cache_key_from_bytes_rejects_short_data() {
        let _ = <CacheKey as RedbValue>::from_bytes(&[0u8; 8]);
    }
}
