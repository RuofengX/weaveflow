use serde::{Deserialize, Serialize};
use serde_json::Value;

/// 变量路径，从 DSL 模板语法 `{路径.字段}` 解析而来。
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
pub struct VariablePath {
    pub parts: Vec<String>,
}

/// 表示一个 JSON 字面量或变量路径引用。
///
/// `Literal` — 普通的 JSON 值（数字、布尔、null、对象、数组、字符串）。
/// `Ref` — 运行时从 scope 中解析的变量引用路径。
///
/// # 反序列化
/// Redb JSON 存储使用 externally tagged 格式 `{"Literal": ...}` / `{"Ref": {...}}`，
/// YAML 转换路径以 plain string 输入，内部自动检测 `{...}` 模板语法。
#[derive(Debug, Clone, Serialize)]
pub enum RefValue {
    Literal(Value),
    Ref(VariablePath),
}

impl VariablePath {
    /// 尝试将字符串解析为变量路径 `{...}`。
    ///
    /// 如果字符串是合法的模板表达式则返回 `Some(VariablePath)`：
    /// - 必须以 `{` 开头、`}` 结尾
    /// - 点号之间至少有一个非空段
    /// - 各段不能包含空白字符
    ///
    /// 普通字符串（非模板值）返回 `None`。
    pub fn parse(s: &str) -> Option<VariablePath> {
        let s = s.trim();
        if s.starts_with('{') && s.ends_with('}') {
            let inner = &s[1..s.len() - 1];
            if inner.is_empty() {
                return None;
            }
            let parts: Vec<String> = inner.split('.').map(|p| p.trim().to_string()).collect();
            if parts.is_empty() {
                return None;
            }
            if parts.iter().any(|p| p.is_empty() || p.contains(char::is_whitespace)) {
                return None;
            }
            Some(VariablePath { parts })
        } else {
            None
        }
    }
}

/// 把 YAML 字符串解析为 `RefValue`：模板字符串 `{...}` → `Ref`，其他 → `Literal`。
pub fn parse_string_to_refvalue(s: &str) -> RefValue {
    if let Some(path) = VariablePath::parse(s) {
        RefValue::Ref(path)
    } else {
        RefValue::Literal(Value::String(s.to_owned()))
    }
}

impl RefValue {
    /// 将 `RefValue` 转回普通 `serde_json::Value`。
    /// `Literal` 原样返回，`Ref` 重构为 `{parts.join(".")}` 格式的字符串。
    pub fn to_value(&self) -> Value {
        match self {
            RefValue::Literal(v) => v.clone(),
            RefValue::Ref(path) => Value::String(format!("{{{}}}", path.parts.join("."))),
        }
    }
}

/// 自定义反序列化：兼容 redb JSON（externally tagged enum）和 YAML 转换路径（plain string/values）。
impl<'de> Deserialize<'de> for RefValue {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        struct Rfv;
        impl<'de> serde::de::Visitor<'de> for Rfv {
            type Value = RefValue;

            fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
                write!(f, "JSON 值或变量引用如 {{greet.output}}")
            }

            fn visit_str<E: serde::de::Error>(self, s: &str) -> Result<RefValue, E> {
                if let Some(path) = VariablePath::parse(s) {
                    Ok(RefValue::Ref(path))
                } else {
                    Ok(RefValue::Literal(Value::String(s.to_owned())))
                }
            }

            fn visit_string<E: serde::de::Error>(self, s: String) -> Result<RefValue, E> {
                self.visit_str(&s)
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

            fn visit_unit<E: serde::de::Error>(self) -> Result<RefValue, E> {
                Ok(RefValue::Literal(Value::Null))
            }

            fn visit_seq<S: serde::de::SeqAccess<'de>>(
                self,
                mut access: S,
            ) -> Result<RefValue, S::Error> {
                let mut seq = Vec::new();
                while let Some(elem) = access.next_element::<Value>()? {
                    seq.push(elem);
                }
                Ok(RefValue::Literal(Value::Array(seq)))
            }

            fn visit_map<M: serde::de::MapAccess<'de>>(
                self,
                mut access: M,
            ) -> Result<RefValue, M::Error> {
                let first_key: Option<String> = access.next_key()?;
                let Some(first_key) = first_key else {
                    return Ok(RefValue::Literal(Value::Object(Default::default())));
                };
                match first_key.as_str() {
                    "Ref" => {
                        let path: VariablePath = access.next_value()?;
                        Ok(RefValue::Ref(path))
                    }
                    "Literal" => {
                        let val: Value = access.next_value()?;
                        Ok(RefValue::Literal(val))
                    }
                    _ => {
                        let mut map = serde_json::Map::new();
                        let first_val: Value = access.next_value()?;
                        map.insert(first_key, first_val);
                        while let Some((key, value)) =
                            access.next_entry::<String, Value>()?
                        {
                            map.insert(key, value);
                        }
                        Ok(RefValue::Literal(Value::Object(map)))
                    }
                }
            }
        }
        deserializer.deserialize_any(Rfv)
    }
}
