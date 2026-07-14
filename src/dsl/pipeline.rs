use serde::{Deserialize, Serialize};
use serde_json::Value;

// ---------------------------------------------------------------------------
// 变量引用
// ---------------------------------------------------------------------------

/// 变量引用，从 DSL 模板语法 `{路径.字段}` 解析而来。
///
/// 每个点号分隔的段成为 `parts` 的一个元素。
/// 例如 `{extract.output.body}` 解析为 `["extract", "output", "body"]`。
///
/// 支持的变量前缀：
/// - `{slots.name}` — 引用 pipeline 输入槽位
/// - `{env.KEY}` — 引用环境变量
/// - `{step_id.output}` — 引用其他 step 的完整输出
/// - `{step_id.output.field}` — 引用 step 输出中的嵌套字段
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct VariableRef {
    pub parts: Vec<String>,
}

/// 表示一个 JSON 字面量或变量引用。
/// DSL 中任何可以填静态常量或动态引用的地方都使用此枚举。
#[derive(Debug, Clone, Serialize)]
pub enum RefValue {
    /// 具体的 JSON 值（数字、布尔、null、对象、数组，或非模板的普通字符串）。
    Literal(Value),
    /// 模板引用，如 `{extract.output}`，运行时解析。
    Ref(VariableRef),
}

/// 尝试将字符串解析为变量引用 `{...}`。
///
/// 如果字符串是合法的模板表达式则返回 `Some(VariableRef)`：
/// - 必须以 `{` 开头、`}` 结尾
/// - 点号之间至少有一个非空段
/// - 各段不能包含空白字符
///
/// 普通字符串（非模板值）返回 `None`。
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

/// 解析字符串：符合 `{...}` 模板语法则返回 `RefValue::Ref`，否则包装为 `RefValue::Literal`。
pub fn parse_template(s: &str) -> RefValue {
    if let Some(var) = parse_variable_ref(s) {
        RefValue::Ref(var)
    } else {
        RefValue::Literal(Value::String(s.to_string()))
    }
}

/// 将 JSON `Value` 转为 `RefValue`。
///
/// - `Value::String` 会检查是否含 `{...}` 模板语法，是则转为 `RefValue::Ref`。
/// - 其他 JSON 类型（数字、布尔、null、对象、数组）直接变为 `RefValue::Literal`。
impl From<Value> for RefValue {
    fn from(v: Value) -> Self {
        match v {
            Value::String(s) => parse_template(&s),
            other => RefValue::Literal(other),
        }
    }
}

impl RefValue {
    /// 将 `RefValue` 转回普通 `serde_json::Value`。
    ///
    /// - `Literal` 原样返回内部值。
    /// - `Ref` 重构为 `{parts.join(".")}` 格式的字符串。
    pub fn to_value(&self) -> Value {
        match self {
            RefValue::Literal(v) => v.clone(),
            RefValue::Ref(var) => Value::String(format!("{{{}}}", var.parts.join("."))),
        }
    }
}

/// `RefValue` 的自定义反序列化实现。
///
/// 处理的 JSON 类型：
/// - 字符串 → 通过 `parse_template` 判断是否为模板引用
/// - 整数 → `RefValue::Literal(json!(v))`
/// - 浮点数 → `RefValue::Literal(json!(v))`
/// - 布尔值 → `RefValue::Literal(json!(v))`
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

// ---------------------------------------------------------------------------
// Pipeline DSL 结构体
// ---------------------------------------------------------------------------

/// Pipeline 顶层定义，由 YAML 反序列化而来。
///
/// # 示例 (YAML)
///
/// ```yaml
/// name: my_pipeline
/// description: 示例 pipeline
/// slots:
///   - name: input_url
///     schema:
///       type: string
///       format: uri
/// steps:
///   - id: fetch
///     type: http
///     inputs:
///       url: "{slots.input_url}"
///   - id: transform
///     type: filter
///     after: [fetch]
///     inputs:
///       data: "{fetch.output}"
/// output: "{transform.output}"
/// ```
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PipelineDef {
    /// Pipeline 的逻辑名称，用于引用。
    pub name: String,
    /// 可选的人工可读描述。
    pub description: Option<String>,
    /// 快照和结果在 redb 中的 TTL 配置。
    pub storage: Option<StorageDef>,
    /// 输入槽位声明。每个槽位声明一个 `name` 和用于校验运行时输入的 JSON Schema。
    #[serde(default)]
    pub slots: Vec<SlotDef>,
    /// 步骤列表，通过 `after` 字段约束 DAG 执行顺序。
    #[serde(default)]
    pub steps: Vec<StepDef>,
    /// 模板表达式，指定 pipeline 的最终输出，例如 `{step_id.output}`。
    pub output: String,
    /// 校验/守卫规则，在 apply 或执行前评估。
    #[serde(default)]
    pub rules: Vec<RuleDef>,
}

/// Pipeline 输入槽位的声明。
///
/// 每个槽位暴露一个命名输入，调用方必须在运行时提供。
/// `schema` 字段存放 JSON Schema，用于校验输入值。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SlotDef {
    /// 槽位名称。
    pub name: String,
    /// 该槽位的 JSON Schema（如 `{"type": "string", "format": "uri"}`）。
    pub schema: Value,
}

/// 校验/守卫规则，在 apply 阶段或执行前评估。
///
/// 规则是轻量级断言，检查 `inputs` 并可执行内联 JS `code` 实现自定义逻辑。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RuleDef {
    /// 规则的唯一标识。
    pub id: String,
    /// 规则类型（如 `"js"`、`"validate"`）。
    #[serde(rename = "type")]
    pub r#type: String,
    /// 规则的输入参数，可包含模板引用。
    pub inputs: Option<Value>,
    /// 当 `type` 为 `"js"` 时的内联 JS 源码。
    #[serde(default)]
    pub code: Option<String>,
}

/// 人类可读的 TTL（生存时间）值，从 `"30s"`、`"5m"`、`"2h"`、`"7d"` 等字符串解析。
///
/// 内部存储为 `chrono::TimeDelta`。
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Ttl(pub chrono::TimeDelta);

impl serde::Serialize for Ttl {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where S: serde::Serializer {
        let secs = self.0.num_seconds();
        let s = if secs % 86400 == 0 { format!("{}d", secs / 86400) }
               else if secs % 3600 == 0 { format!("{}h", secs / 3600) }
               else if secs % 60 == 0 { format!("{}m", secs / 60) }
               else { format!("{}s", secs) };
        serializer.serialize_str(&s)
    }
}

impl<'de> serde::Deserialize<'de> for Ttl {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where D: serde::Deserializer<'de> {
        let s = String::deserialize(deserializer)?;
        if s.len() < 2 { return Err(serde::de::Error::custom(format!("无效: {}", s))); }
        let (num_str, unit) = s.split_at(s.len() - 1);
        let num: i64 = num_str.parse().map_err(serde::de::Error::custom)?;
        match unit {
            "s" => Ok(Ttl(chrono::TimeDelta::seconds(num))),
            "m" => Ok(Ttl(chrono::TimeDelta::minutes(num))),
            "h" => Ok(Ttl(chrono::TimeDelta::hours(num))),
            "d" => Ok(Ttl(chrono::TimeDelta::days(num))),
            _ => Err(serde::de::Error::custom(format!("无效单位: {}", unit))),
        }
    }
}

/// 存储 TTL 配置。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StorageDef {
    /// 快照的生存时间。
    pub snapshot_ttl: Option<Ttl>,
    /// 最终结果的生存时间。
    pub result_ttl: Option<Ttl>,
}

/// 单个执行步骤的定义。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StepDef {
    /// 步骤唯一标识。
    pub id: String,
    /// 算子类型名，例如 `"http"`、`"filter"`、`"js"`。
    #[serde(rename = "type")]
    pub r#type: String,
    /// 显式依赖的其他 step ID 列表，用于约束 DAG 执行顺序。
    /// 若未指定则按 DSL 声明顺序执行。
    pub after: Option<Vec<String>>,
    /// 迭代配置。设置后该 step 会对数组中每个元素执行一次算子调用。
    #[serde(default)]
    pub iterate: Option<IterateConfig>,
    /// 该 step 传给算子的输入参数，可包含模板引用。
    pub inputs: Option<Value>,
    /// 是否启用缓存。启用时以 SHA256(解析后的输入) 为 key 进行内容寻址去重。
    pub cache: Option<bool>,
    /// 重试配置。
    pub retry: Option<RetryDef>,
    /// 超时时间（毫秒）。
    pub timeout: Option<u64>,
    /// 当 `type` 为 `"js"` 时的内联 JS 源码。
    #[serde(default)]
    pub code: Option<String>,
}

/// 重试配置。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RetryDef {
    /// 最大重试次数（含首次执行），默认为 1（不重试）。
    #[serde(default = "default_max_attempts")]
    pub max_attempts: u32,
    /// 退避策略：`fixed`（固定间隔）或 `exponential`（指数递增）。
    #[serde(default)]
    pub backoff: BackoffStrategy,
    /// 每次重试之间的基础延迟（毫秒），默认 1000。
    #[serde(default = "default_delay_ms")]
    pub delay_ms: u64,
    /// 可选的 validator 表达式，用于判断执行是否成功。
    pub validator: Option<String>,
}
fn default_max_attempts() -> u32 { 1 }
fn default_delay_ms() -> u64 { 1000 }

/// 重试退避策略。
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub enum BackoffStrategy {
    /// 固定间隔：每次重试之间等待相同的时间。
    #[serde(rename = "fixed")] #[default] Fixed,
    /// 指数递增：每次重试的等待时间翻倍。
    #[serde(rename = "exponential")] Exponential,
}

/// 迭代配置，放在 step 的 `iterate` 字段。
///
/// 当设置后，执行器会将 `over` 引用的数组展开，对每个元素（或每批元素）调用该算子。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IterateConfig {
    /// 模板引用，指向要遍历的数组，如 `{extract.output}`。
    pub over: String,
    /// 迭代元素的变量名，在 step 的 `inputs` 中可通过 `{变量名}` 引用。
    #[serde(rename = "as")]
    pub as_name: String,
    /// 最大并发数。省略时由 rayon 自动决定。
    #[serde(default)]
    pub max_workers: Option<u32>,
    /// 批量配置。若设置则每次送一批元素而非单个元素给算子。
    #[serde(default)]
    pub batch: Option<BatchConfig>,
}

/// 迭代批量配置。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BatchConfig {
    /// 每批包含的元素数量。
    pub size: u32,
}
