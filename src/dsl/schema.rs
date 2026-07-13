use serde::{Deserialize, Serialize};
use serde_json::Value;

// ---------------------------------------------------------------------------

/// 编译期解析后的变量引用，如 `{slots.name}` → `["slots", "name"]`。
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct VariableRef {
    pub parts: Vec<String>,
}

impl VariableRef {
    pub fn is_step_output(&self) -> bool {
        self.parts.len() >= 3 && self.parts[0] == "steps"
    }

    pub fn step_id(&self) -> Option<&str> {
        if self.is_step_output() {
            Some(self.parts[1].as_str())
        } else {
            None
        }
    }
}

/// 参数值：在 DSL 解析时区分字面量和变量引用。
#[derive(Debug, Clone, Serialize)]
pub enum RefValue {
    Literal(Value),
    Ref(VariableRef),
}

impl From<Value> for RefValue {
    fn from(v: Value) -> Self {
        match v {
            Value::String(s) => parse_template(&s),
            other => RefValue::Literal(other),
        }
    }
}

impl RefValue {
    /// 转换为 serde_json::Value（Ref → 字符串，Literal → 原值）。
    pub fn to_value(&self) -> Value {
        match self {
            RefValue::Literal(v) => v.clone(),
            RefValue::Ref(var) => Value::String(format!("{{{}}}", var.parts.join("."))),
        }
    }
}

impl<'de> serde::Deserialize<'de> for RefValue {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        struct PV;
        impl<'de> serde::de::Visitor<'de> for PV {
            type Value = RefValue;
            fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
                write!(f, "a JSON value or variable reference like {{greet.output}}")
            }
            fn visit_str<E: serde::de::Error>(self, s: &str) -> Result<RefValue, E> {
                Ok(parse_template(s))
            }
            fn visit_string<E: serde::de::Error>(self, s: String) -> Result<RefValue, E> {
                Ok(parse_template(&s))
            }
            fn visit_i64<E: serde::de::Error>(self, v: i64) -> Result<RefValue, E> {
                Ok(RefValue::Literal(serde_json::json!(v)))
            }
            fn visit_f64<E: serde::de::Error>(self, v: f64) -> Result<RefValue, E> {
                Ok(RefValue::Literal(serde_json::json!(v)))
            }
            fn visit_bool<E: serde::de::Error>(self, v: bool) -> Result<RefValue, E> {
                Ok(RefValue::Literal(serde_json::json!(v)))
            }
        }
        deserializer.deserialize_any(PV)
    }
}

pub fn parse_variable_ref(s: &str) -> Option<VariableRef> {
    let s = s.trim();
    if s.starts_with('{') && s.ends_with('}') {
        let inner = &s[1..s.len() - 1];
        if inner.is_empty() { return None; }
        let parts: Vec<String> = inner.split('.').map(|p| p.trim().to_string()).collect();
        if parts.is_empty() { return None; }
        if parts.iter().any(|p| p.is_empty() || p.contains(char::is_whitespace)) {
            return None;
        }
        Some(VariableRef { parts })
    } else {
        None
    }
}

pub fn parse_template(s: &str) -> RefValue {
    if let Some(var) = parse_variable_ref(s) {
        RefValue::Ref(var)
    } else {
        RefValue::Literal(Value::String(s.to_string()))
    }
}

// ---------------------------------------------------------------------------
// Pipeline 顶层
// ---------------------------------------------------------------------------

/// 完整的 Pipeline 定义，对应 DSL YAML 的根结构。
///
/// ```yaml
/// name: my_pipeline
/// slots:
///   - name: url
///     schema: { type: string }
/// steps:
///   - id: fetch
///     type: http
///     inputs:
///       url: "{slots.url}"
/// output: "{fetch.output}"
/// ```
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PipelineDef {
    pub name: String,
    pub description: Option<String>,

    /// 中间结果和最终结果的保留策略。
    pub storage: Option<StorageDef>,

    /// Pipeline 级输入占位符，调用方在 POST /runs 时注入值。
    #[serde(default)]
    pub slots: Vec<SlotDef>,

    #[serde(default)]
    pub steps: Vec<StepDef>,

    pub output: String,
}

// ---------------------------------------------------------------------------
// Pipeline 级占位符
// ---------------------------------------------------------------------------

/// Pipeline 级输入占位符声明。
///
/// ```yaml
/// slots:
///   - name: source_url
///     schema: { type: string, pattern: "^https?://" }
/// ```
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SlotDef {
    pub name: String,
    pub schema: Value,
}

// ---------------------------------------------------------------------------
// 存储配置
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Ttl(pub chrono::TimeDelta);

impl serde::Serialize for Ttl {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let total_seconds = self.0.num_seconds();
        let s = if total_seconds % 86400 == 0 {
            format!("{}d", total_seconds / 86400)
        } else if total_seconds % 3600 == 0 {
            format!("{}h", total_seconds / 3600)
        } else if total_seconds % 60 == 0 {
            format!("{}m", total_seconds / 60)
        } else {
            format!("{}s", total_seconds)
        };
        serializer.serialize_str(&s)
    }
}

impl<'de> serde::Deserialize<'de> for Ttl {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        Ttl::parse(&s).map_err(serde::de::Error::custom)
    }
}

impl Ttl {
    fn parse(s: &str) -> Result<Self, String> {
        if s.len() < 2 {
            return Err(format!("无效的时间格式: {}", s));
        }
        let (num_str, unit) = s.split_at(s.len() - 1);
        let num: i64 = num_str
            .parse()
            .map_err(|_| format!("无效的数字: {}", num_str))?;
        match unit {
            "s" => Ok(Ttl(chrono::TimeDelta::seconds(num))),
            "m" => Ok(Ttl(chrono::TimeDelta::minutes(num))),
            "h" => Ok(Ttl(chrono::TimeDelta::hours(num))),
            "d" => Ok(Ttl(chrono::TimeDelta::days(num))),
            _ => Err(format!("无效的时间单位: {}，仅支持 s/m/h/d", unit)),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StorageDef {
    pub snapshot_ttl: Option<Ttl>,
    pub result_ttl: Option<Ttl>,
}

// ---------------------------------------------------------------------------
// 步骤定义
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StepDef {
    pub id: String,

    #[serde(rename = "type")]
    pub r#type: String,

    pub after: Option<Vec<String>>,

    /// 迭代配置（step 根级，非 inputs 内）。
    #[serde(default)]
    pub iterate: Option<IterateConfig>,

    /// 算子输入（纯参数，不再包含 iterate）。
    pub inputs: Option<Value>,

    pub cache: Option<bool>,
    pub retry: Option<RetryDef>,
    pub timeout: Option<u64>,

    /// 内联代码（type=\"js\" 时使用）。DSL 直接写 JS 源码，无需预注册。
    /// 支持 `{{step_id.output}}` 模板引用，运行时从 scope 解析后内联。
    #[serde(default)]
    pub code: Option<String>,
}

// ---------------------------------------------------------------------------
// 重试策略
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RetryDef {
    #[serde(default = "default_max_attempts")]
    pub max_attempts: u32,

    #[serde(default)]
    pub backoff: BackoffStrategy,

    #[serde(default = "default_delay_ms")]
    pub delay_ms: u64,

    pub validator: Option<String>,
}

fn default_max_attempts() -> u32 { 1 }
fn default_delay_ms() -> u64 { 1000 }

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub enum BackoffStrategy {
    #[serde(rename = "fixed")]
    #[default]
    Fixed,

    #[serde(rename = "exponential")]
    Exponential,
}

// ---------------------------------------------------------------------------
// 迭代配置
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IterateConfig {
    pub over: String,

    #[serde(rename = "as")]
    pub as_name: String,

    /// 并发数。省缺 → rayon 自动选择。
    #[serde(default)]
    pub max_workers: Option<u32>,

    /// 批量模式：每批 size 个元素打包传给算子。
    #[serde(default)]
    pub batch: Option<BatchConfig>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BatchConfig {
    pub size: u32,
}
