use std::collections::HashMap;

mod common;

fn run_yaml(
    yaml: &str,
    slots: HashMap<String, serde_json::Value>,
) -> weave::error::WeaveResult<serde_json::Value> {
    common::run_yaml(yaml, slots)
}

#[test]
fn retry_exponential_backoff_does_not_panic() {
    let yaml = r#"
name: retry-exp
slots: []
steps:
  - id: a
    type: noop
    retry:
      max_attempts: 3
      backoff: exponential
      delay_ms: 10
output: "{a.output}"
"#;
    let result = run_yaml(yaml, HashMap::new());
    assert!(
        result.is_ok(),
        "exponential backoff with noop should succeed: {:?}",
        result.err()
    );
}

#[test]
fn iterate_with_retry_does_not_panic() {
    let yaml = r#"
name: it-retry
slots:
  - name: items
    schema: { type: array }
steps:
  - id: a
    type: noop
    iterate:
      over: "{slots.items}"
      as: item
    retry:
      max_attempts: 2
      delay_ms: 10
output: "{a.output}"
"#;
    let mut slots = HashMap::new();
    slots.insert("items".into(), serde_json::json!([1, 2, 3]));
    let result = run_yaml(yaml, slots);
    assert!(
        result.is_ok(),
        "iterate with retry config should complete: {:?}",
        result.err()
    );
}
