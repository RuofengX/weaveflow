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
    pub fn parse(s: &str) -> Option<VariablePath> {
        let s = s.trim();
        let inner = if s.starts_with("{{") && s.ends_with("}}") {
            &s[2..s.len() - 2]
        } else if s.starts_with('{') && s.ends_with('}') {
            &s[1..s.len() - 1]
        } else {
            return None;
        };
        if inner.is_empty() {
            return None;
        }
        let parts: Vec<String> = inner.split('.').map(|p| p.trim().to_string()).collect();
        if parts.is_empty() {
            return None;
        }
        if parts.iter().any(|p| {
            p.is_empty()
                || !p
                    .chars()
                    .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-')
        }) {
            return None;
        }
        Some(VariablePath { parts })
    }
}

impl RefValue {
    /// 将 `RefValue` 转回普通 `serde_json::Value`。
    pub fn to_value(&self) -> Value {
        match self {
            RefValue::Literal(v) => v.clone(),
            RefValue::Ref(path) => Value::String(format!("{{{}}}", path.parts.join("."))),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_valid_paths() {
        assert_eq!(
            VariablePath::parse("{step.output.field}").unwrap().parts,
            vec!["step", "output", "field"]
        );
        assert_eq!(
            VariablePath::parse("{env.API_KEY}").unwrap().parts,
            vec!["env", "API_KEY"]
        );
        assert_eq!(
            VariablePath::parse("{{a-b_c.d}}").unwrap().parts,
            vec!["a-b_c", "d"]
        );
    }

    #[test]
    fn parse_rejects_adjacent_refs() {
        assert!(VariablePath::parse("{a.b}{c.d}").is_none());
    }

    #[test]
    fn parse_rejects_illegal_part_chars() {
        assert!(VariablePath::parse("{a.b c}").is_none());
        assert!(VariablePath::parse("{a.b/c}").is_none());
        assert!(VariablePath::parse("{}").is_none());
    }

    #[test]
    fn parse_rejects_non_template() {
        assert!(VariablePath::parse("plain").is_none());
        assert!(VariablePath::parse("prefix {a.b}").is_none());
    }
}
