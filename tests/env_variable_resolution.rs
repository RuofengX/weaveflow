
// env_variable_resolution

#[path = "common/mod.rs"]
mod common;
use common::run_yaml;
use serde_json::json;
use std::collections::HashMap;

#[test]
fn env_variable_resolution() {
    unsafe { std::env::set_var("WEAVE_TEST_ENV", "env_val"); }
    let yaml = r#"
name: env_test
steps:
  - id: s1
    type: var
    inputs:
      key: "{env.WEAVE_TEST_ENV}"
output: "{s1.output}"
"#;
    let result = run_yaml(yaml, HashMap::new()).expect("run");
    assert_eq!(result["key"], json!("env_val"));
}
