use serde::Deserialize;
use serde_json::Value;

use super::pipeline::PipelineDef;
use super::retry::RetryDef;
use super::rule::RuleDef;
use super::step::{BatchConfig, IterateConfig, StepDef};
use super::step_op::StepOp;
use super::storage::StorageDef;
use super::variable::{parse_string_to_refvalue, RefValue, VariablePath};

// ---------------------------------------------------------------------------
// YAML 中间层类型 — 仅用于 YAML → PipelineDef 转换，不参与 redb 持久化
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
    pub op_type: String,
    #[serde(default)]
    pub after: Option<Vec<String>>,
    #[serde(default)]
    pub iterate: Option<RawIterateConfig>,
    pub cache: Option<bool>,
    pub retry: Option<RetryDef>,
    pub timeout: Option<u64>,
    #[serde(default)]
    pub inputs: Option<Value>,
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
// Raw → PipelineDef / StepDef 转换
// ---------------------------------------------------------------------------

impl TryFrom<RawPipelineDef> for PipelineDef {
    type Error = ParseError;

    fn try_from(raw: RawPipelineDef) -> Result<Self, Self::Error> {
        Ok(PipelineDef {
            name: raw.name,
            description: raw.description,
            storage: raw.storage,
            slots: raw.slots,
            steps: raw
                .steps
                .into_iter()
                .map(StepDef::try_from)
                .collect::<Result<Vec<_>, _>>()?,
            output: parse_string_to_refvalue(&raw.output),
            rules: raw
                .rules
                .into_iter()
                .map(|r| {
                    Ok(RuleDef {
                        id: r.id,
                        r#type: r.r#type,
                        inputs: r.inputs.map(value_to_inputs),
                        code: r.code,
                    })
                })
                .collect::<Result<Vec<_>, Self::Error>>()?,
        })
    }
}

impl TryFrom<RawStepDef> for StepDef {
    type Error = ParseError;

    fn try_from(raw: RawStepDef) -> Result<Self, Self::Error> {
        let op: StepOp = match raw.op_type.as_str() {
            "http" => StepOp::HttpClient(from_inputs(&raw.inputs, "http")?),
            "js" => StepOp::JsScript(from_inputs(&raw.inputs, "js")?),
            "filter" => StepOp::FilterData(from_inputs(&raw.inputs, "filter")?),
            "sort" => StepOp::SortData(from_inputs(&raw.inputs, "sort")?),
            "dedup" => StepOp::DedupData(from_inputs(&raw.inputs, "dedup")?),
            "merge" => StepOp::MergeData(from_inputs(&raw.inputs, "merge")?),
            "split" => StepOp::SplitData(from_inputs(&raw.inputs, "split")?),
            "base64" => StepOp::Base64Data(from_inputs(&raw.inputs, "base64")?),
            "noop" => StepOp::Noop,
            "var" => StepOp::VarOutput(from_inputs(&raw.inputs, "var")?),
            "file" => StepOp::FileReader(from_inputs(&raw.inputs, "file")?),
            "command" => StepOp::CommandRun(from_inputs(&raw.inputs, "command")?),
            "llm" => StepOp::LlmClient(from_inputs(&raw.inputs, "llm")?),
            "fork" => StepOp::ForkFlow(from_inputs(&raw.inputs, "fork")?),
            other => {
                return Err(ParseError::Yaml(format!(
                    "未注册的步骤类型: {other}（步骤: {}）",
                    raw.id
                )))
            }
        };

        Ok(StepDef {
            id: raw.id,
            after: raw.after,
            iterate: raw.iterate.map(IterateConfig::from),
            cache: raw.cache,
            retry: raw.retry,
            timeout: raw.timeout,
            op,
        })
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

// ---------------------------------------------------------------------------
// 输入转换：Value → 带模板检测 + 类型校验
// ---------------------------------------------------------------------------

use serde::de::DeserializeOwned;

#[derive(Debug, thiserror::Error)]
pub enum ParseError {
    #[error("YAML 解析失败: {0}")]
    Yaml(String),
}

impl From<rust_yaml::Error> for ParseError {
    fn from(e: rust_yaml::Error) -> Self {
        ParseError::Yaml(e.to_string())
    }
}

impl From<serde_json::Error> for ParseError {
    fn from(e: serde_json::Error) -> Self {
        ParseError::Yaml(e.to_string())
    }
}

/// 将 YAML 反序列化出的原始 `Value` 直接转为带模板检测 + 类型校验的 `T`。
/// `RefValue` 的自定义 Deserialize 会在 `from_value` 中自动处理字符串 → 模板解析。
fn from_inputs<T: DeserializeOwned>(val: &Option<Value>, op_name: &str) -> Result<T, ParseError> {
    let raw = val.clone().unwrap_or(Value::Null);
    serde_json::from_value(raw).map_err(|e| {
        ParseError::Yaml(format!(
            "算子 {} 的 inputs 配置错误: {e}",
            op_name
        ))
    })
}

fn value_to_inputs(val: Value) -> std::collections::HashMap<String, RefValue> {
    match val {
        Value::Object(map) => {
            let mut result = std::collections::HashMap::new();
            for (k, v) in map {
                result.insert(k, value_to_refvalue(v));
            }
            result
        }
        _ => std::collections::HashMap::new(),
    }
}

fn value_to_refvalue(v: Value) -> RefValue {
    match v {
        Value::String(s) => parse_string_to_refvalue(&s),
        other => RefValue::Literal(other),
    }
}
