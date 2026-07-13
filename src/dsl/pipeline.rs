use serde::{Deserialize, Serialize};
use serde_json::Value;

// ---------------------------------------------------------------------------
// Variable references
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct VariableRef {
    pub parts: Vec<String>,
}

#[derive(Debug, Clone, Serialize)]
pub enum RefValue {
    Literal(Value),
    Ref(VariableRef),
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

impl From<Value> for RefValue {
    fn from(v: Value) -> Self {
        match v {
            Value::String(s) => parse_template(&s),
            other => RefValue::Literal(other),
        }
    }
}

impl RefValue {
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

// ---------------------------------------------------------------------------
// Pipeline DSL schema
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PipelineDef {
    pub name: String,
    pub description: Option<String>,
    pub storage: Option<StorageDef>,
    #[serde(default)]
    pub slots: Vec<SlotDef>,
    #[serde(default)]
    pub steps: Vec<StepDef>,
    pub output: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SlotDef {
    pub name: String,
    pub schema: Value,
}

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

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StorageDef {
    pub snapshot_ttl: Option<Ttl>,
    pub result_ttl: Option<Ttl>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StepDef {
    pub id: String,
    #[serde(rename = "type")]
    pub r#type: String,
    pub after: Option<Vec<String>>,
    #[serde(default)]
    pub iterate: Option<IterateConfig>,
    pub inputs: Option<Value>,
    pub cache: Option<bool>,
    pub retry: Option<RetryDef>,
    pub timeout: Option<u64>,
    #[serde(default)]
    pub code: Option<String>,
}

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
    #[serde(rename = "fixed")] #[default] Fixed,
    #[serde(rename = "exponential")] Exponential,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IterateConfig {
    pub over: String,
    #[serde(rename = "as")]
    pub as_name: String,
    #[serde(default)]
    pub max_workers: Option<u32>,
    #[serde(default)]
    pub batch: Option<BatchConfig>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BatchConfig {
    pub size: u32,
}
