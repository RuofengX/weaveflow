use redb::Value as RedbValue;
use redb::{Key as RedbKey, TableDefinition, TypeName};
use std::cmp::Ordering;

use crate::dsl::PipelineDef;
use crate::dsl::StepId;
use crate::routine::{RoutineEventRecord, RoutineRow};
use crate::store::object::ObjectDigest;
use crate::store::object::ObjectValue;
use crate::tracker::snapshot::{Snapshot, SnapshotKey};
use crate::tracker::{PipelineId, TaskId, TaskMeta};
use uuid::Uuid;

// ── TaskId（UUID v4，定长 16 字节） ───────────────────────────────────

impl RedbValue for TaskId {
    type SelfType<'a>
        = TaskId
    where
        Self: 'a;
    type AsBytes<'a>
        = Vec<u8>
    where
        Self: 'a;
    fn fixed_width() -> Option<usize> {
        Some(16)
    }
    fn from_bytes<'a>(data: &'a [u8]) -> Self::SelfType<'a>
    where
        Self: 'a,
    {
        let bytes: [u8; 16] = data[..16].try_into().expect("TaskId: 需要 16 字节");
        TaskId(Uuid::from_bytes(bytes))
    }
    fn as_bytes<'a, 'b: 'a>(value: &'a Self::SelfType<'b>) -> Self::AsBytes<'a>
    where
        Self: 'b,
    {
        value.0.as_bytes().to_vec()
    }
    fn type_name() -> TypeName {
        TypeName::new("weaveflow::TaskId")
    }
}

impl RedbKey for TaskId {
    fn compare(data1: &[u8], data2: &[u8]) -> Ordering {
        data1.cmp(data2)
    }
}

// ── PipelineId（UUID v4，定长 16 字节）

impl RedbValue for PipelineId {
    type SelfType<'a>
        = PipelineId
    where
        Self: 'a;
    type AsBytes<'a>
        = Vec<u8>
    where
        Self: 'a;
    fn fixed_width() -> Option<usize> {
        Some(16)
    }
    fn from_bytes<'a>(data: &'a [u8]) -> Self::SelfType<'a>
    where
        Self: 'a,
    {
        let bytes: [u8; 16] = data[..16].try_into().expect("PipelineId: 需要 16 字节");
        PipelineId(Uuid::from_bytes(bytes))
    }
    fn as_bytes<'a, 'b: 'a>(value: &'a Self::SelfType<'b>) -> Self::AsBytes<'a>
    where
        Self: 'b,
    {
        value.0.as_bytes().to_vec()
    }
    fn type_name() -> TypeName {
        TypeName::new("weaveflow::PipelineId")
    }
}

impl RedbKey for PipelineId {
    fn compare(data1: &[u8], data2: &[u8]) -> Ordering {
        data1.cmp(data2)
    }
}

// ── SnapshotKey（16 字节 UUID + 8 字节 u64 BE = 24 字节） ─────────────

impl RedbValue for SnapshotKey {
    type SelfType<'a>
        = SnapshotKey
    where
        Self: 'a;
    type AsBytes<'a>
        = Vec<u8>
    where
        Self: 'a;
    fn fixed_width() -> Option<usize> {
        Some(24)
    }
    fn from_bytes<'a>(data: &'a [u8]) -> Self::SelfType<'a>
    where
        Self: 'a,
    {
        let uuid_bytes: [u8; 16] = data[..16]
            .try_into()
            .expect("SnapshotKey UUID: 需要 16 字节");
        let seq = u64::from_be_bytes(
            data[16..24]
                .try_into()
                .expect("SnapshotKey seq: 需要 8 字节"),
        );
        SnapshotKey {
            task_id: Uuid::from_bytes(uuid_bytes),
            seq,
        }
    }
    fn as_bytes<'a, 'b: 'a>(value: &'a Self::SelfType<'b>) -> Self::AsBytes<'a>
    where
        Self: 'b,
    {
        let mut buf = Vec::with_capacity(24);
        buf.extend_from_slice(value.task_id.as_bytes());
        buf.extend_from_slice(&value.seq.to_be_bytes());
        buf
    }
    fn type_name() -> TypeName {
        TypeName::new("weaveflow::SnapshotKey")
    }
}

impl RedbKey for SnapshotKey {
    fn compare(data1: &[u8], data2: &[u8]) -> Ordering {
        data1.cmp(data2)
    }
}

// ── ObjectDigest（定长 32 字节 SHA256） ───────────────────────────────────

impl RedbValue for ObjectDigest {
    type SelfType<'a>
        = ObjectDigest
    where
        Self: 'a;
    type AsBytes<'a>
        = Vec<u8>
    where
        Self: 'a;
    fn fixed_width() -> Option<usize> {
        Some(32)
    }
    fn from_bytes<'a>(data: &'a [u8]) -> Self::SelfType<'a>
    where
        Self: 'a,
    {
        let bytes: [u8; 32] = data.try_into().expect("ObjectDigest: 需要 32 字节");
        ObjectDigest(bytes)
    }
    fn as_bytes<'a, 'b: 'a>(value: &'a Self::SelfType<'b>) -> Self::AsBytes<'a>
    where
        Self: 'b,
    {
        value.as_bytes().to_vec()
    }
    fn type_name() -> TypeName {
        TypeName::new("weaveflow::ObjectDigest::v1")
    }
}

impl RedbKey for ObjectDigest {
    fn compare(data1: &[u8], data2: &[u8]) -> Ordering {
        data1.cmp(data2)
    }
}

// ── Value 类型：serde_json 序列化 ──────────────────────────────────────

impl RedbValue for PipelineDef {
    type SelfType<'a>
        = PipelineDef
    where
        Self: 'a;
    type AsBytes<'a>
        = Vec<u8>
    where
        Self: 'a;
    fn fixed_width() -> Option<usize> {
        None
    }
    fn from_bytes<'a>(data: &'a [u8]) -> Self::SelfType<'a>
    where
        Self: 'a,
    {
        serde_json::from_slice(data).expect("反序列化 PipelineDef 失败")
    }
    fn as_bytes<'a, 'b: 'a>(value: &'a Self::SelfType<'b>) -> Self::AsBytes<'a>
    where
        Self: 'b,
    {
        serde_json::to_vec(value).expect("序列化 PipelineDef 失败")
    }
    fn type_name() -> TypeName {
        TypeName::new("weaveflow::PipelineDef::v1")
    }
}

impl RedbValue for TaskMeta {
    type SelfType<'a>
        = TaskMeta
    where
        Self: 'a;
    type AsBytes<'a>
        = Vec<u8>
    where
        Self: 'a;
    fn fixed_width() -> Option<usize> {
        None
    }
    fn from_bytes<'a>(data: &'a [u8]) -> Self::SelfType<'a>
    where
        Self: 'a,
    {
        serde_json::from_slice(data).expect("反序列化 TaskMeta 失败")
    }
    fn as_bytes<'a, 'b: 'a>(value: &'a Self::SelfType<'b>) -> Self::AsBytes<'a>
    where
        Self: 'b,
    {
        serde_json::to_vec(value).expect("序列化 TaskMeta 失败")
    }
    fn type_name() -> TypeName {
        TypeName::new("weaveflow::TaskMeta::v1")
    }
}

// ── Snapshot：自定义二进制布局（避免 serde_json 序列化 Vec<u8> 的 4 倍膨胀）──
// 布局: seq(8B BE) | step_id_len(4B BE) | step_id(UTF-8) | output(raw bytes)

fn snapshot_encode(seq: u64, step_id: &str, output: &[u8]) -> Vec<u8> {
    let step_bytes = step_id.as_bytes();
    let mut buf = Vec::with_capacity(12 + step_bytes.len() + output.len());
    buf.extend_from_slice(&seq.to_be_bytes());
    buf.extend_from_slice(&(step_bytes.len() as u32).to_be_bytes());
    buf.extend_from_slice(step_bytes);
    buf.extend_from_slice(output);
    buf
}

fn snapshot_decode_header(data: &[u8]) -> (u64, StepId) {
    assert!(data.len() >= 12, "Snapshot: 数据过短 ({} 字节)", data.len());
    let seq = u64::from_be_bytes(data[..8].try_into().expect("seq 8 字节"));
    let step_len = u32::from_be_bytes(data[8..12].try_into().expect("len 4 字节")) as usize;
    assert!(
        data.len() >= 12 + step_len,
        "Snapshot: step_id 长度 {step_len} 越界 (总长 {})",
        data.len()
    );
    let step_id =
        String::from_utf8(data[12..12 + step_len].to_vec()).expect("Snapshot: step_id 非 UTF-8");
    (seq, StepId(step_id))
}

impl RedbValue for Snapshot {
    type SelfType<'a>
        = Snapshot
    where
        Self: 'a;
    type AsBytes<'a>
        = Vec<u8>
    where
        Self: 'a;
    fn fixed_width() -> Option<usize> {
        None
    }
    fn from_bytes<'a>(data: &'a [u8]) -> Self::SelfType<'a>
    where
        Self: 'a,
    {
        let (seq, step_id) = snapshot_decode_header(data);
        let step_len = u32::from_be_bytes(data[8..12].try_into().expect("len 4 字节")) as usize;
        Snapshot {
            seq,
            step_id,
            output: data[12 + step_len..].to_vec(),
        }
    }
    fn as_bytes<'a, 'b: 'a>(value: &'a Self::SelfType<'b>) -> Self::AsBytes<'a>
    where
        Self: 'b,
    {
        snapshot_encode(value.seq, &value.step_id.0, &value.output)
    }
    fn type_name() -> TypeName {
        TypeName::new("weaveflow::Snapshot::v2")
    }
}

/// 只读 header 的 Snapshot 视图（同表同类型名），用于列表/统计时跳过 output 拷贝。
#[derive(Debug, Clone)]
pub(crate) struct SnapshotHeader {
    pub seq: u64,
    pub step_id: StepId,
}

impl RedbValue for SnapshotHeader {
    type SelfType<'a>
        = SnapshotHeader
    where
        Self: 'a;
    type AsBytes<'a>
        = Vec<u8>
    where
        Self: 'a;
    fn fixed_width() -> Option<usize> {
        None
    }
    fn from_bytes<'a>(data: &'a [u8]) -> Self::SelfType<'a>
    where
        Self: 'a,
    {
        let (seq, step_id) = snapshot_decode_header(data);
        SnapshotHeader { seq, step_id }
    }
    fn as_bytes<'a, 'b: 'a>(value: &'a Self::SelfType<'b>) -> Self::AsBytes<'a>
    where
        Self: 'b,
    {
        snapshot_encode(value.seq, &value.step_id.0, &[])
    }
    fn type_name() -> TypeName {
        TypeName::new("weaveflow::Snapshot::v2")
    }
}

impl RedbValue for ObjectValue {
    type SelfType<'a>
        = ObjectValue
    where
        Self: 'a;
    type AsBytes<'a>
        = Vec<u8>
    where
        Self: 'a;
    fn fixed_width() -> Option<usize> {
        None
    }
    fn from_bytes<'a>(data: &'a [u8]) -> Self::SelfType<'a>
    where
        Self: 'a,
    {
        serde_json::from_slice(data).expect("反序列化 ObjectValue 失败")
    }
    fn as_bytes<'a, 'b: 'a>(value: &'a Self::SelfType<'b>) -> Self::AsBytes<'a>
    where
        Self: 'b,
    {
        serde_json::to_vec(value).expect("序列化 ObjectValue 失败")
    }
    fn type_name() -> TypeName {
        TypeName::new("weaveflow::Object::v1")
    }
}

pub const PIPELINE: TableDefinition<PipelineId, PipelineDef> = TableDefinition::new("pipeline");
pub const TASK: TableDefinition<TaskId, TaskMeta> = TableDefinition::new("task");
pub const SNAPSHOT: TableDefinition<SnapshotKey, Snapshot> = TableDefinition::new("snapshot");
pub const OBJECT: TableDefinition<ObjectDigest, ObjectValue> = TableDefinition::new("object");

// ── CacheKey（单 ObjectDigest，32 字节固定大小） ─────────────────

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CacheKey(pub ObjectDigest);

impl RedbValue for CacheKey {
    type SelfType<'a>
        = CacheKey
    where
        Self: 'a;
    type AsBytes<'a>
        = Vec<u8>
    where
        Self: 'a;
    fn fixed_width() -> Option<usize> {
        Some(32)
    }
    fn from_bytes<'a>(data: &'a [u8]) -> Self::SelfType<'a>
    where
        Self: 'a,
    {
        let bytes: [u8; 32] = data.try_into().expect("CacheKey: 需要 32 字节");
        CacheKey(ObjectDigest(bytes))
    }
    fn as_bytes<'a, 'b: 'a>(value: &'a Self::SelfType<'b>) -> Self::AsBytes<'a>
    where
        Self: 'b,
    {
        value.0.as_bytes().to_vec()
    }
    fn type_name() -> TypeName {
        TypeName::new("weaveflow::CacheKey")
    }
}

impl RedbKey for CacheKey {
    fn compare(data1: &[u8], data2: &[u8]) -> Ordering {
        data1.cmp(data2)
    }
}

pub const CACHE: TableDefinition<CacheKey, ObjectDigest> = TableDefinition::new("cache");

/// snapshot 表的 header-only 视图（与 SNAPSHOT 同表名、同类型名）。
pub(crate) const SNAPSHOT_HEADER: TableDefinition<SnapshotKey, SnapshotHeader> =
    TableDefinition::new("snapshot");

impl RedbValue for RoutineRow {
    type SelfType<'a>
        = RoutineRow
    where
        Self: 'a;
    type AsBytes<'a>
        = Vec<u8>
    where
        Self: 'a;
    fn fixed_width() -> Option<usize> {
        None
    }
    fn from_bytes<'a>(data: &'a [u8]) -> Self::SelfType<'a>
    where
        Self: 'a,
    {
        serde_json::from_slice(data).expect("反序列化 RoutineRow 失败")
    }
    fn as_bytes<'a, 'b: 'a>(value: &'a Self::SelfType<'b>) -> Self::AsBytes<'a>
    where
        Self: 'b,
    {
        serde_json::to_vec(value).expect("序列化 RoutineRow 失败")
    }
    fn type_name() -> TypeName {
        TypeName::new("weaveflow::RoutineRow::v1")
    }
}

pub const ROUTINE: TableDefinition<&str, RoutineRow> = TableDefinition::new("routine");

impl RedbValue for RoutineEventRecord {
    type SelfType<'a>
        = RoutineEventRecord
    where
        Self: 'a;
    type AsBytes<'a>
        = Vec<u8>
    where
        Self: 'a;
    fn fixed_width() -> Option<usize> {
        None
    }
    fn from_bytes<'a>(data: &'a [u8]) -> Self::SelfType<'a>
    where
        Self: 'a,
    {
        serde_json::from_slice(data).expect("反序列化 RoutineEventRecord 失败")
    }
    fn as_bytes<'a, 'b: 'a>(value: &'a Self::SelfType<'b>) -> Self::AsBytes<'a>
    where
        Self: 'b,
    {
        serde_json::to_vec(value).expect("序列化 RoutineEventRecord 失败")
    }
    fn type_name() -> TypeName {
        TypeName::new("weaveflow::RoutineEventRecord::v1")
    }
}

/// routine_event 表 key：[name_len u32 BE][name bytes][seq u64 BE]，
/// 字节序比较即 (routine, seq) 字典序 —— 同一 routine 的事件连续且按 seq 排序。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EventKey {
    pub routine: String,
    pub seq: u64,
}

impl RedbValue for EventKey {
    type SelfType<'a>
        = EventKey
    where
        Self: 'a;
    type AsBytes<'a>
        = Vec<u8>
    where
        Self: 'a;
    fn fixed_width() -> Option<usize> {
        None
    }
    fn from_bytes<'a>(data: &'a [u8]) -> Self::SelfType<'a>
    where
        Self: 'a,
    {
        let name_len =
            u32::from_be_bytes(data[..4].try_into().expect("EventKey: 需要 4 字节长度前缀"))
                as usize;
        let name_end = 4 + name_len;
        let name =
            String::from_utf8(data[4..name_end].to_vec()).expect("EventKey: routine 名不是 UTF-8");
        let seq = u64::from_be_bytes(
            data[name_end..name_end + 8]
                .try_into()
                .expect("EventKey: 需要 8 字节 seq"),
        );
        EventKey { routine: name, seq }
    }
    fn as_bytes<'a, 'b: 'a>(value: &'a Self::SelfType<'b>) -> Self::AsBytes<'a>
    where
        Self: 'b,
    {
        let mut buf = Vec::with_capacity(12 + value.routine.len());
        buf.extend_from_slice(&(value.routine.len() as u32).to_be_bytes());
        buf.extend_from_slice(value.routine.as_bytes());
        buf.extend_from_slice(&value.seq.to_be_bytes());
        buf
    }
    fn type_name() -> TypeName {
        TypeName::new("weaveflow::EventKey")
    }
}

impl RedbKey for EventKey {
    fn compare(data1: &[u8], data2: &[u8]) -> Ordering {
        data1.cmp(data2)
    }
}

pub const ROUTINE_EVENT: TableDefinition<EventKey, RoutineEventRecord> =
    TableDefinition::new("routine_event");

// ── v0 schema（自动迁移用）：类型名不带 ::vN 后缀 ─────────────────────
// v0 行的序列化格式同为 serde_json，这里以 Value 暂存，由迁移代码做字段兼容处理。

macro_rules! v0_value {
    ($name:ident, $type_name:literal) => {
        #[derive(Debug, Clone)]
        pub(crate) struct $name(pub serde_json::Value);

        impl RedbValue for $name {
            type SelfType<'a>
                = $name
            where
                Self: 'a;
            type AsBytes<'a>
                = Vec<u8>
            where
                Self: 'a;
            fn fixed_width() -> Option<usize> {
                None
            }
            fn from_bytes<'a>(data: &'a [u8]) -> Self::SelfType<'a>
            where
                Self: 'a,
            {
                $name(serde_json::from_slice(data).expect("反序列化 v0 行失败"))
            }
            fn as_bytes<'a, 'b: 'a>(value: &'a Self::SelfType<'b>) -> Self::AsBytes<'a>
            where
                Self: 'b,
            {
                serde_json::to_vec(&value.0).expect("序列化 v0 行失败")
            }
            fn type_name() -> TypeName {
                TypeName::new($type_name)
            }
        }
    };
}

v0_value!(V0PipelineDef, "weaveflow::PipelineDef");
v0_value!(V0TaskMeta, "weaveflow::TaskMeta");
v0_value!(V1PipelineDef, "weaveflow::PipelineDef::v1");
v0_value!(V1TaskMeta, "weaveflow::TaskMeta::v1");
// 2.0 之前的 trigger 表行（serde_json 格式；RoutineRow 是其超集，缺失字段有 default）。
v0_value!(LegacyTriggerRowJson, "weaveflow::TriggerRow::v1");

pub(crate) const V0_PIPELINE: TableDefinition<PipelineId, V0PipelineDef> =
    TableDefinition::new("pipeline");
pub(crate) const V0_TASK: TableDefinition<TaskId, V0TaskMeta> = TableDefinition::new("task");
pub(crate) const V1_PIPELINE: TableDefinition<PipelineId, V1PipelineDef> =
    TableDefinition::new("pipeline");
pub(crate) const V1_TASK: TableDefinition<TaskId, V1TaskMeta> = TableDefinition::new("task");
pub(crate) const LEGACY_TRIGGER: TableDefinition<&str, LegacyTriggerRowJson> =
    TableDefinition::new("trigger");

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn value_type_names_carry_schema_version() {
        let v1_names = [
            <PipelineDef as RedbValue>::type_name(),
            <TaskMeta as RedbValue>::type_name(),
            <ObjectValue as RedbValue>::type_name(),
            <ObjectDigest as RedbValue>::type_name(),
        ];
        for name in v1_names {
            let dbg = format!("{name:?}");
            assert!(dbg.contains("::v1"), "type name {dbg} missing ::v1 suffix");
        }
        let snap = format!("{:?}", <Snapshot as RedbValue>::type_name());
        assert!(
            snap.contains("::v2"),
            "type name {snap} missing ::v2 suffix"
        );
        let snap_header = format!("{:?}", <SnapshotHeader as RedbValue>::type_name());
        assert_eq!(snap, snap_header, "SnapshotHeader 必须与 Snapshot 同类型名");
    }

    #[test]
    fn snapshot_binary_layout_roundtrip() {
        let snap = Snapshot {
            seq: 42,
            step_id: StepId("步骤-α".to_string()),
            output: vec![0, 1, 2, 255, 254],
        };
        let bytes = <Snapshot as RedbValue>::as_bytes(&snap);
        assert_eq!(&bytes[..8], &42u64.to_be_bytes());
        let back = <Snapshot as RedbValue>::from_bytes(&bytes);
        assert_eq!(back.seq, 42);
        assert_eq!(back.step_id.0, "步骤-α");
        assert_eq!(back.output, vec![0, 1, 2, 255, 254]);

        let header = <SnapshotHeader as RedbValue>::from_bytes(&bytes);
        assert_eq!(header.seq, 42);
        assert_eq!(header.step_id.0, "步骤-α");
    }

    #[test]
    fn snapshot_binary_layout_empty_output_and_step() {
        let snap = Snapshot {
            seq: 0,
            step_id: StepId(String::new()),
            output: Vec::new(),
        };
        let bytes = <Snapshot as RedbValue>::as_bytes(&snap);
        assert_eq!(bytes.len(), 12);
        let back = <Snapshot as RedbValue>::from_bytes(&bytes);
        assert_eq!(back.seq, 0);
        assert!(back.step_id.0.is_empty());
        assert!(back.output.is_empty());
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
