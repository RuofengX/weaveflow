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

/// f-string 模板的一个片段：字面量文本或变量引用。
///
/// 由 `f"..."` 语法解析而来（仅小写 `f` 前缀），例如
/// `f"https://{env.HOST}/api?q={slots.q}"` 解析为
/// `[Lit("https://"), Ref(env.HOST), Lit("/api?q="), Ref(slots.q)]`。
/// 模板解析结果恒为 String；字面量中的 `{`/`}` 需转义为 `\{`/`\}`。
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum TemplatePart {
    Lit(String),
    Ref(VariablePath),
}

/// 表示一个 JSON 字面量、变量路径引用或 f-string 模板。
///
/// `Literal` — 普通的 JSON 值（数字、布尔、null、对象、数组、字符串）。
/// `Ref` — 运行时从 scope 中解析的变量引用路径。
/// `Template` — `f"..."` 模板，解析时拼接各片段为一个 String。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum RefValue {
    Literal(Value),
    Ref(VariablePath),
    Template(Vec<TemplatePart>),
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

impl TemplatePart {
    /// 解析 `f"..."` 模板字符串。
    ///
    /// - 不以 `f"` 开头 → `Ok(None)`（不是模板，调用方按普通字符串处理）。
    /// - 以 `f"` 开头但缺少配对结尾引号 / 引用未闭合 / 路径非法 / 裸 `}` → `Err`。
    /// - 否则 → `Ok(Some(parts))`；`\{` / `\}` 转义为字面花括号，其余 `\x` 原样保留。
    pub fn parse_fstring(s: &str) -> Result<Option<Vec<TemplatePart>>, String> {
        if !s.starts_with("f\"") {
            return Ok(None);
        }
        if s.len() < 3 || !s.ends_with('"') {
            return Err(format!("f-string 缺少结尾引号: {s:?}"));
        }
        let inner = &s[2..s.len() - 1];
        let mut parts = Vec::new();
        let mut lit = String::new();
        let mut chars = inner.chars();
        while let Some(c) = chars.next() {
            match c {
                '\\' => match chars.next() {
                    Some('{') => lit.push('{'),
                    Some('}') => lit.push('}'),
                    Some(other) => {
                        lit.push('\\');
                        lit.push(other);
                    }
                    None => lit.push('\\'),
                },
                '{' => {
                    let mut content = String::new();
                    let mut closed = false;
                    for c in chars.by_ref() {
                        if c == '}' {
                            closed = true;
                            break;
                        }
                        content.push(c);
                    }
                    if !closed {
                        return Err(format!("f-string 中的引用缺少闭合 '}}': {s:?}"));
                    }
                    let path = VariablePath::parse(&format!("{{{content}}}"))
                        .ok_or_else(|| format!("f-string 中非法变量路径 '{{{content}}}': {s:?}"))?;
                    if !lit.is_empty() {
                        parts.push(TemplatePart::Lit(std::mem::take(&mut lit)));
                    }
                    parts.push(TemplatePart::Ref(path));
                }
                '}' => {
                    return Err(format!("f-string 中的 '}}' 需转义为 '\\}}': {s:?}"));
                }
                _ => lit.push(c),
            }
        }
        if !lit.is_empty() {
            parts.push(TemplatePart::Lit(lit));
        }
        Ok(Some(parts))
    }
}

impl RefValue {
    /// 将 `RefValue` 转回普通 `serde_json::Value`。
    pub fn to_value(&self) -> Value {
        match self {
            RefValue::Literal(v) => v.clone(),
            RefValue::Ref(path) => Value::String(format!("{{{}}}", path.parts.join("."))),
            RefValue::Template(parts) => {
                let mut out = String::from("f\"");
                for part in parts {
                    match part {
                        TemplatePart::Lit(s) => {
                            out.push_str(&s.replace('{', "\\{").replace('}', "\\}"));
                        }
                        TemplatePart::Ref(path) => {
                            out.push_str(&format!("{{{}}}", path.parts.join(".")));
                        }
                    }
                }
                out.push('"');
                Value::String(out)
            }
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

    #[test]
    fn fstring_basic_parsing() {
        let parts = TemplatePart::parse_fstring("f\"https://{env.HOST}/api?q={slots.q}\"")
            .unwrap()
            .unwrap();
        assert_eq!(
            parts,
            vec![
                TemplatePart::Lit("https://".into()),
                TemplatePart::Ref(VariablePath::parse("{env.HOST}").unwrap()),
                TemplatePart::Lit("/api?q=".into()),
                TemplatePart::Ref(VariablePath::parse("{slots.q}").unwrap()),
            ]
        );
    }

    #[test]
    fn fstring_not_template() {
        assert!(TemplatePart::parse_fstring("plain").unwrap().is_none());
        assert!(TemplatePart::parse_fstring("F\"{a.b}\"").unwrap().is_none());
        assert!(
            TemplatePart::parse_fstring("\"f{ a.b }\"")
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn fstring_malformed_is_error() {
        assert!(TemplatePart::parse_fstring("f\"abc").is_err());
        assert!(TemplatePart::parse_fstring("f\"").is_err());
        assert!(TemplatePart::parse_fstring("f\"{a.b\"").is_err());
        assert!(TemplatePart::parse_fstring("f\"{a b}\"").is_err());
        assert!(TemplatePart::parse_fstring("f\"a}b\"").is_err());
    }

    #[test]
    fn fstring_escapes() {
        let parts = TemplatePart::parse_fstring("f\"a\\{b\\}c\"")
            .unwrap()
            .unwrap();
        assert_eq!(parts, vec![TemplatePart::Lit("a{b}c".into())]);
        let parts = TemplatePart::parse_fstring("f\"c:\\path\\{x\"")
            .unwrap()
            .unwrap();
        assert_eq!(parts, vec![TemplatePart::Lit("c:\\path{x".into())]);
    }

    #[test]
    fn fstring_empty_and_no_ref() {
        assert_eq!(
            TemplatePart::parse_fstring("f\"\"").unwrap().unwrap(),
            vec![]
        );
        assert_eq!(
            TemplatePart::parse_fstring("f\"plain\"").unwrap().unwrap(),
            vec![TemplatePart::Lit("plain".into())]
        );
    }
}
