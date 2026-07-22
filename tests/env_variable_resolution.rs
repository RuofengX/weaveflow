// env_variable_resolution

#[path = "common/mod.rs"]
mod common;
use common::run_yaml;
use serde_json::json;
use std::collections::HashMap;

#[test]
fn env_variable_resolution() {
    unsafe {
        std::env::set_var("WEAVEFLOW_TEST_ENV", "env_val");
    }
    let yaml = r#"
name: env_test
steps:
  - id: s1
    type: var
    inputs:
      value:
        key: "{env.WEAVEFLOW_TEST_ENV}"
output: "{s1.output.value}"
"#;
    let result = run_yaml(yaml, HashMap::new()).expect("run");
    // env 引用正常解析；最终 output 与快照同规脱敏（≥4 字符的 env 值替换为 ***）
    assert_eq!(result["key"], json!("***"));
}
