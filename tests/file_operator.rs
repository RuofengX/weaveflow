
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
    assert!(result.is_object(), "expected object, got {}", result);
    assert!(result["content"].is_string());
    assert!(result["mimetype"].is_string());
    assert!(result["size"].as_u64().unwrap() > 0);
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
        function run(data) {{
          return {{
            has_content: typeof data.content === "string",
            length: data.content ? data.content.length : 0,
            mimetype: data.mimetype,
            size: data.size
          }};
        }}
output: "{{check.output}}"
"#
    );
    let result = run_yaml(&yaml, HashMap::new()).expect("run");
    assert_eq!(result["has_content"], json!(true));
    assert!(result["length"].as_u64().unwrap() > 0);
    assert!(result["mimetype"].is_string());
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
