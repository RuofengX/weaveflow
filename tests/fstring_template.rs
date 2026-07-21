//! f-string 模板（`f"..."`）端到端：解析 → 校验 → DAG 隐式依赖 → resolver 拼接。

mod common;

use std::collections::HashMap;

use common::run_yaml;
use weaveflow::dsl::parser::parse;

#[test]
fn fstring_concatenates_slots_and_step_outputs() {
    let yaml = r#"
name: fstring_demo
slots:
  - name: min
    schema: { type: integer, default: 2 }
steps:
  - id: build
    type: var
    inputs:
      value: f"https://host/api?q={slots.min}&n={count.output}"
  - id: count
    type: js
    inputs:
      code: "function run(data) { return 40 + 2; }"
output: "{build.output.value}"
"#;
    let out = run_yaml(yaml, HashMap::new()).expect("run");
    assert_eq!(out, "https://host/api?q=2&n=42");
}

#[test]
fn fstring_escapes_and_non_string_stringify() {
    let yaml = r#"
name: fstring_types
steps:
  - id: s
    type: var
    inputs:
      value:
        obj: { a: 1 }
        arr: [1, 2]
        nothing: null
  - id: t
    type: var
    inputs:
      value: f"\{o\}={s.output.value.obj} arr={s.output.value.arr} null=[{s.output.value.nothing}] bool=[{s.output.value.missing}]"
output: "{t.output.value}"
"#;
    let out = run_yaml(yaml, HashMap::new()).expect("run");
    assert_eq!(out, r#"{o}={"a":1} arr=[1,2] null=[] bool=[]"#);
}

#[test]
fn fstring_iterate_as_name_ref() {
    let yaml = r#"
name: fstring_iterate
slots:
  - name: items
    schema: { type: array }
steps:
  - id: each
    type: var
    iterate:
      over: "{slots.items}"
      as: item
    inputs:
      value: f"user:{item.name}#{item.id}"
  - id: join
    type: js
    inputs:
      code: "function run(data) { return data.map(x => x.value).join(','); }"
      data: "{each.output}"
output: "{join.output}"
"#;
    let mut slots = HashMap::new();
    slots.insert(
        "items".to_string(),
        serde_json::json!([{ "id": 1, "name": "ann" }, { "id": 2, "name": "bob" }]),
    );
    let out = run_yaml(yaml, slots).expect("run");
    assert_eq!(out, "user:ann#1,user:bob#2");
}

#[test]
fn fstring_in_pipeline_output() {
    let yaml = r#"
name: fstring_output
slots:
  - name: who
    schema: { type: string, default: "world" }
steps:
  - id: s
    type: js
    inputs:
      code: "function run(data) { return 'hello'; }"
output: f"{s.output}, {slots.who}!"
"#;
    let out = run_yaml(yaml, HashMap::new()).expect("run");
    assert_eq!(out, "hello, world!");
}

#[test]
fn fstring_malformed_is_parse_error() {
    for bad in ["f\"{slots.x}", "f\"abc", "f\"a}b\"", "f\"{a b}\""] {
        let yaml = format!(
            "name: bad\nsteps:\n  - id: s\n    type: var\n    inputs:\n      value: '{bad}'\noutput: \"{{s.output}}\"\n"
        );
        assert!(parse(&yaml).is_err(), "must reject: {bad}");
    }
}

#[test]
fn fstring_ghost_step_ref_rejected_by_validator() {
    let yaml = r#"
name: fstring_ghost
steps:
  - id: s
    type: var
    inputs:
      value: f"x={ghost.output}"
output: "{s.output}"
"#;
    let def = parse(yaml).expect("parse");
    let report = weaveflow::dsl::validator::validate(&def);
    assert!(
        report
            .errors
            .iter()
            .any(|e| e.code == "variable_ref_not_found"),
        "errors: {:?}",
        report.errors
    );
}

#[test]
fn fstring_creates_implicit_dag_dependency() {
    // count 在 YAML 中排在 build 之后且无 after，但 build 的模板引用了它
    // → 必须先执行（若依赖缺失，build 会因 scope 无 count 而失败）。
    let yaml = r#"
name: fstring_dep
steps:
  - id: build
    type: var
    inputs:
      value: f"n={count.output}"
  - id: count
    type: js
    inputs:
      code: "function run(data) { return 7; }"
output: "{build.output.value}"
"#;
    let out = run_yaml(yaml, HashMap::new()).expect("run");
    assert_eq!(out, "n=7");
}
