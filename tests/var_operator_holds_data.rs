
// var_operator_holds_data

#[path = "common/mod.rs"]
mod common;
use common::run_yaml;
use serde_json::json;
use std::collections::HashMap;

#[test]
fn var_operator_holds_data() {
    let yaml = r#"
name: var_test
steps:
  - id: conf
    type: var
    inputs:
      value:
        api_key: "sk-xxx"
        timeout: 30
        endpoint: "https://api.example.com"
output: "{conf.output.value}"
"#;
    let result = run_yaml(yaml, HashMap::new()).expect("run");
    assert_eq!(result["api_key"], json!("sk-xxx"));
    assert_eq!(result["timeout"], json!(30));
}
