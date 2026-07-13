
// file_operator — 读本地文件 + 作为二进制 data 传给下游

#[path = "common/mod.rs"]
mod common;
use common::run_yaml;
use serde_json::json;
use std::collections::HashMap;
use std::io::Write;

#[test]
fn file_operator_local_path() {
    let mut f = tempfile::NamedTempFile::new().expect("temp file");
    f.write_all(b"hello from file operator").expect("write");
    let path = f.path().to_string_lossy().to_string();

    let yaml = format!(
        r#"
name: file_local
steps:
  - id: read
    type: file
    inputs:
      path: "{path}"
output: "{{read.output}}"
"#
    );
    let result = run_yaml(&yaml, HashMap::new()).expect("run");
    // file 算子输出的原始 bytes 不是合法 JSON，serde_json 解析为 Null
    assert!(result.is_null() || result.is_string());
}

#[test]
fn file_operator_output_flows_to_js() {
    let mut f = tempfile::NamedTempFile::new().expect("temp file");
    f.write_all(b"hello from file operator").expect("write");
    let path = f.path().to_string_lossy().to_string();

    let yaml = format!(
        r#"
name: file_to_js
steps:
  - id: read
    type: file
    inputs:
      path: "{path}"
  - id: check
    type: js
    inputs:
      data: "{{read.output}}"
    code: |
      function run(input) {{
        return {{
          has_base64: typeof input.data_base64 === "string",
          length: input.data_base64 ? input.data_base64.length : 0
        }};
      }}
output: "{{check.output}}"
"#
    );
    let result = run_yaml(&yaml, HashMap::new()).expect("run");
    assert_eq!(result["has_base64"], json!(true));
    assert!(result["length"].as_u64().unwrap() > 0);
}

#[test]
fn file_operator_missing_config() {
    let yaml = r#"
name: file_missing
steps:
  - id: read
    type: file
    inputs:
      something_else: "x"
output: "{read.output}"
"#;
    let result = run_yaml(yaml, HashMap::new());
    assert!(result.is_err(), "expected error for missing path/url");
}

#[test]
fn file_operator_not_found() {
    let yaml = r#"
name: file_not_found
steps:
  - id: read
    type: file
    inputs:
      path: "/tmp/__does_not_exist_xyz"
output: "{read.output}"
"#;
    let result = run_yaml(yaml, HashMap::new());
    assert!(result.is_err(), "expected error for non-existent file");
}
