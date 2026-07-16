// ---------------------------------------------------------------------------
// DSL 解析器：YAML 字符串 → PipelineDef
// ---------------------------------------------------------------------------

use crate::dsl::PipelineDef;
use crate::dsl::raw::RawPipelineDef;
use tracing::debug;

/// 将 YAML 字符串解析为 `PipelineDef`。
///
/// 解析分两步：
/// 1. YAML → `RawPipelineDef`（所有字段保持原样，模板字符串尚未解析）
/// 2. `RawPipelineDef` → `PipelineDef`（把 `{slots.x}` / `{step.output}` 等模板字符串
///    转换为 `RefValue::Ref`）
///
/// `rust_yaml` 的解析错误（含行号、上下文）直接透传到 `ParseError` 返回给调用方。
pub fn parse(yaml: &str) -> Result<PipelineDef, ParseError> {
    let raw: RawPipelineDef = rust_yaml::from_str(yaml)?;
    let pipeline = PipelineDef::try_from(raw).map_err(|e| ParseError::Yaml(e.to_string()))?;
    debug!(name = %pipeline.name, steps = pipeline.steps.len(), "pipeline parsed");
    Ok(pipeline)
}

#[derive(Debug, thiserror::Error)]
pub enum ParseError {
    #[error("YAML 解析失败: {0}")]
    Yaml(String),
}

impl From<rust_yaml::Error> for ParseError {
    fn from(e: rust_yaml::Error) -> Self {
        Self::Yaml(e.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dsl::{RefValue, StepId, StepOp};

    fn minimal_yaml() -> &'static str {
        r#"
name: minimal
steps:
  - id: fetch
    type: http
    inputs:
      url: "{slots.url}"
output: "{fetch.output}"
"#
    }

    #[test]
    fn parse_minimal() {
        let def = parse(minimal_yaml()).unwrap();
        assert_eq!(def.name, "minimal");
        assert_eq!(def.steps.len(), 1);
        assert_eq!(def.steps[0].id, StepId::from("fetch"));
        assert_eq!(def.steps[0].op.op_type(), "http");
        assert!(matches!(def.output, RefValue::Ref(_)));
        let StepOp::Http(ref inputs) = def.steps[0].op else { panic!("expected http") };
        assert!(matches!(inputs.url, RefValue::Ref(_)));
    }

    #[test]
    fn parse_with_iterate() {
        let yaml = r#"
name: iterate_demo
steps:
  - id: process
    type: http
    iterate:
      over: "{slots.data}"
      as: "item"
      max_workers: 8
    inputs:
      url: "{slots.data}"
output: "{process.output}"
"#;
        let def = parse(yaml).unwrap();
        let iter = def.steps[0].iterate.as_ref().unwrap();
        assert_eq!(iter.over.parts, vec!["slots", "data"]);
        assert_eq!(iter.as_name, "item");
        assert_eq!(iter.max_workers, Some(8));
    }

    #[test]
    fn parse_with_retry() {
        let yaml = r#"
name: retry_test
steps:
  - id: s1
    type: http
    inputs:
      url: "https://api.example.com"
    retry:
      max_attempts: 3
      backoff: exponential
      delay_ms: 2000
    timeout: 60
output: "{s1.output}"
"#;
        let def = parse(yaml).unwrap();
        let step = &def.steps[0];
        assert_eq!(step.timeout, Some(60));
        let retry = step.retry.as_ref().unwrap();
        assert_eq!(retry.max_attempts, 3);
    }

    #[test]
    fn parse_invalid_yaml() {
        let result = parse("name: \nsteps: [invalid");
        assert!(result.is_err());
    }

    #[test]
    fn parse_with_slots() {
        let yaml = r#"
name: with_slots
slots:
  - name: url
    schema:
      type: string
      pattern: "^https?://"
steps:
  - id: fetch
    type: http
    inputs:
      url: "{slots.url}"
output: "{fetch.output}"
"#;
        let def = parse(yaml).unwrap();
        assert_eq!(def.slots.len(), 1);
        assert_eq!(def.slots[0].name, "url");
    }

    #[test]
    fn parse_literal_inputs() {
        let yaml = r#"
name: literal_test
steps:
  - id: s
    type: var
    inputs:
      value: "static_value"
output: done
"#;
        let def = parse(yaml).unwrap();
        let StepOp::Var(ref v) = def.steps[0].op else { panic!("expected var") };
        assert!(matches!(v.value, Some(RefValue::Literal(_))));
        assert!(matches!(def.output, RefValue::Literal(_)));
    }
}
