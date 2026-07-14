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
#[derive(Debug, Clone, Serialize, Deserialize)]
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
