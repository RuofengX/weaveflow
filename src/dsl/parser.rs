// ---------------------------------------------------------------------------
// DSL 解析器：YAML 字符串 → PipelineDef
// ---------------------------------------------------------------------------

use crate::dsl::schema::PipelineDef;

/// 将 YAML 字符串解析为 `PipelineDef`。
pub fn parse(yaml: &str) -> Result<PipelineDef, ParseError> {
    let def: PipelineDef = serde_yaml::from_str(yaml)?;
    Ok(def)
}

#[derive(Debug, thiserror::Error)]
pub enum ParseError {
    #[error("YAML 解析失败: {0}")]
    Yaml(String),
}

impl From<serde_yaml::Error> for ParseError {
    fn from(e: serde_yaml::Error) -> Self {
        Self::Yaml(e.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
        assert_eq!(def.steps[0].id, "fetch");
        assert_eq!(def.steps[0].r#type, "http");
    }

    #[test]
    fn parse_with_iterate() {
        let yaml = r#"
name: iterate_demo
steps:
  - id: process
    type: http
    inputs:
      iterate:
        over: "{slots.data}"
        as: "item"
        max_workers: 8
output: "{process.output}"
"#;
        let def = parse(yaml).unwrap();
        let inputs = def.steps[0].inputs.as_ref().unwrap();
        let iterate = inputs.get("iterate").unwrap();
        assert_eq!(iterate.get("over").unwrap(), "{slots.data}");
        assert_eq!(iterate.get("as").unwrap(), "item");
        assert_eq!(iterate.get("max_workers").unwrap(), 8);
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
}
