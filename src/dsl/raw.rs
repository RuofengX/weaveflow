use std::collections::HashMap;

use serde::Deserialize;
use serde_json::Value;

use super::pipeline::PipelineDef;
use super::retry::RetryDef;
use super::rule::RuleDef;
use super::step::{BatchConfig, IterateConfig, StepDef};
use super::storage::StorageDef;
use super::variable::{parse_string_to_refvalue, RefValue, VariablePath};

// ---------------------------------------------------------------------------
// YAML 中间层类型 — 仅用于 YAML 字符串 ↔ PipelineDef 的转换，
// 不参与 redb 持久化。
// ---------------------------------------------------------------------------

#[derive(Deserialize)]
pub struct RawPipelineDef {
    pub name: String,
    #[serde(default)]
    pub description: Option<String>,
    #[serde(default)]
    pub storage: Option<StorageDef>,
    #[serde(default)]
    pub slots: Vec<super::pipeline::SlotDef>,
    #[serde(default)]
    pub steps: Vec<RawStepDef>,
    pub output: String,
    #[serde(default)]
    pub rules: Vec<RawRuleDef>,
}

#[derive(Deserialize)]
pub struct RawStepDef {
    pub id: String,
    #[serde(rename = "type")]
    pub r#type: String,
    #[serde(default)]
    pub after: Option<Vec<String>>,
    #[serde(default)]
    pub iterate: Option<RawIterateConfig>,
    pub inputs: Option<Value>,
    pub cache: Option<bool>,
    pub retry: Option<RetryDef>,
    pub timeout: Option<u64>,
    #[serde(default)]
    pub code: Option<String>,
}

#[derive(Deserialize)]
pub struct RawIterateConfig {
    pub over: String,
    #[serde(rename = "as")]
    pub as_name: String,
    #[serde(default)]
    pub max_workers: Option<u32>,
    #[serde(default)]
    pub batch: Option<BatchConfig>,
}

#[derive(Deserialize)]
pub struct RawRuleDef {
    pub id: String,
    #[serde(rename = "type")]
    pub r#type: String,
    #[serde(default)]
    pub inputs: Option<Value>,
    #[serde(default)]
    pub code: Option<String>,
}

// ---------------------------------------------------------------------------
// Raw → Pipeline 转换：把 YAML 字符串中的模板语法 `{...}` 解析为 RefValue
// ---------------------------------------------------------------------------

impl From<RawPipelineDef> for PipelineDef {
    fn from(raw: RawPipelineDef) -> Self {
        PipelineDef {
            name: raw.name,
            description: raw.description,
            storage: raw.storage,
            slots: raw.slots,
            steps: raw.steps.into_iter().map(StepDef::from).collect(),
            output: parse_string_to_refvalue(&raw.output),
            rules: raw.rules.into_iter().map(RuleDef::from).collect(),
        }
    }
}

impl From<RawStepDef> for StepDef {
    fn from(raw: RawStepDef) -> Self {
        StepDef {
            id: raw.id,
            r#type: raw.r#type,
            after: raw.after,
            iterate: raw.iterate.map(IterateConfig::from),
            inputs: raw.inputs.map(value_to_inputs),
            cache: raw.cache,
            retry: raw.retry,
            timeout: raw.timeout,
            code: raw.code,
        }
    }
}

impl From<RawIterateConfig> for IterateConfig {
    fn from(raw: RawIterateConfig) -> Self {
        IterateConfig {
            over: VariablePath::parse(&raw.over)
                .expect("iterate.over must be a valid variable path"),
            as_name: raw.as_name,
            max_workers: raw.max_workers,
            batch: raw.batch,
        }
    }
}

impl From<RawRuleDef> for RuleDef {
    fn from(raw: RawRuleDef) -> Self {
        RuleDef {
            id: raw.id,
            r#type: raw.r#type,
            inputs: raw.inputs.map(value_to_inputs),
            code: raw.code,
        }
    }
}

/// 将 YAML 反序列化出的 `Value` 对象转为 `HashMap<String, RefValue>`，
/// 字符串值会检测是否为模板 `{...}`。
fn value_to_inputs(val: Value) -> HashMap<String, RefValue> {
    match val {
        Value::Object(map) => {
            let mut result = HashMap::new();
            for (k, v) in map {
                result.insert(k, value_to_refvalue(v));
            }
            result
        }
        _ => HashMap::new(),
    }
}

fn value_to_refvalue(v: Value) -> RefValue {
    match v {
        Value::String(s) => parse_string_to_refvalue(&s),
        other => RefValue::Literal(other),
    }
}
