// iterate_as_binding: iterate.as 真实绑定 — {item}/{item.field}/{item.0.x} 在任意
// 算子字段解析为当前 chunk 元素（等同于注入 op 级 scope 根），替代旧的 "data" 键注入

#[path = "common/mod.rs"]
mod common;
use common::run_yaml;
use serde_json::json;
use std::collections::HashMap;

fn slots_with_items(items: serde_json::Value) -> HashMap<String, serde_json::Value> {
    let mut slots = HashMap::new();
    slots.insert("items".into(), items);
    slots
}

#[test]
fn as_binding_whole_element_in_any_field() {
    // var.value 是任意 JSON 字段：{item} 解析为整个元素
    let yaml = r#"
name: as_whole
slots:
  - name: items
    schema:
      type: array
steps:
  - id: echo
    type: var
    iterate:
      over: "{slots.items}"
      as: "item"
    inputs:
      value: "{item}"
output: "{echo.output}"
"#;
    let result = run_yaml(yaml, slots_with_items(json!(["a", "b", "c"]))).expect("run");
    assert_eq!(
        result,
        json!([{"value": "a"}, {"value": "b"}, {"value": "c"}])
    );
}

#[test]
fn as_binding_field_drill_down() {
    let yaml = r#"
name: as_field
slots:
  - name: items
    schema:
      type: array
steps:
  - id: names
    type: var
    iterate:
      over: "{slots.items}"
      as: "item"
    inputs:
      value: "{item.user.name}"
output: "{names.output}"
"#;
    let items = json!([
        {"user": {"name": "ann"}},
        {"user": {"name": "bob"}}
    ]);
    let result = run_yaml(yaml, slots_with_items(items)).expect("run");
    assert_eq!(result, json!([{"value": "ann"}, {"value": "bob"}]));
}

#[test]
fn as_binding_array_index_strict() {
    let yaml = r#"
name: as_index
slots:
  - name: items
    schema:
      type: array
steps:
  - id: first
    type: var
    iterate:
      over: "{slots.items}"
      as: "item"
    inputs:
      value: "{item.0}"
output: "{first.output}"
"#;
    let result = run_yaml(yaml, slots_with_items(json!([[10, 20], [30]]))).expect("run");
    assert_eq!(result, json!([{"value": 10}, {"value": 30}]));
}

#[test]
fn as_binding_array_index_out_of_bounds_is_hard_error() {
    let yaml = r#"
name: as_index_oob
slots:
  - name: items
    schema:
      type: array
steps:
  - id: bad
    type: var
    iterate:
      over: "{slots.items}"
      as: "item"
    inputs:
      value: "{item.5}"
output: "{bad.output}"
"#;
    let err = run_yaml(yaml, slots_with_items(json!([[10]]))).expect_err("must fail");
    assert!(err.to_string().contains("item.5"), "err: {err}");
}

#[test]
fn as_binding_missing_field_resolves_null() {
    let yaml = r#"
name: as_missing
slots:
  - name: items
    schema:
      type: array
steps:
  - id: m
    type: var
    iterate:
      over: "{slots.items}"
      as: "item"
    inputs:
      value: "{item.missing}"
output: "{m.output}"
"#;
    let result = run_yaml(yaml, slots_with_items(json!([{"a": 1}]))).expect("run");
    assert_eq!(result, json!([{"value": null}]));
}

#[test]
fn as_binding_inside_literal_object() {
    // 顶层 object 字面量中的 {item.x} 深解析（内联 Ref tag）
    let yaml = r#"
name: as_literal_obj
slots:
  - name: items
    schema:
      type: array
steps:
  - id: packed
    type: var
    iterate:
      over: "{slots.items}"
      as: "item"
    inputs:
      value:
        id: "{item.id}"
        static_field: 1
output: "{packed.output}"
"#;
    let result = run_yaml(yaml, slots_with_items(json!([{"id": 7}, {"id": 8}]))).expect("run");
    assert_eq!(
        result,
        json!([
            {"value": {"id": 7, "static_field": 1}},
            {"value": {"id": 8, "static_field": 1}}
        ])
    );
}

#[test]
fn as_binding_batch_mode_element_is_slice_array() {
    let yaml = r#"
name: as_batch
slots:
  - name: items
    schema:
      type: array
steps:
  - id: lens
    type: js
    iterate:
      over: "{slots.items}"
      as: "item"
      batch:
        size: 2
    inputs:
      data: "{item}"
      code: |
        function run(data) { return [data.length]; }
output: "{lens.output}"
"#;
    let result = run_yaml(yaml, slots_with_items(json!([1, 2, 3, 4, 5]))).expect("run");
    // 3 个 chunk（大小 2/2/1），每个返回 [chunk_len]，batch 聚合展平
    assert_eq!(result, json!([2, 2, 1]));
}

#[test]
fn as_binding_js_data_field() {
    let yaml = r#"
name: as_js
slots:
  - name: items
    schema:
      type: array
steps:
  - id: doubled
    type: js
    iterate:
      over: "{slots.items}"
      as: "item"
    inputs:
      data: "{item}"
      code: |
        function run(data) { return data * 2; }
output: "{doubled.output}"
"#;
    let result = run_yaml(yaml, slots_with_items(json!([1, 2, 3]))).expect("run");
    assert_eq!(result, json!([2, 4, 6]));
}

#[test]
fn as_binding_mixed_with_upstream_refs() {
    // as 引用与 slots/上游引用在同一步共存
    let yaml = r#"
name: as_mixed
slots:
  - name: items
    schema:
      type: array
  - name: suffix
    schema:
      type: string
steps:
  - id: packed
    type: var
    iterate:
      over: "{slots.items}"
      as: "item"
    inputs:
      value:
        element: "{item}"
        suffix: "{slots.suffix}"
output: "{packed.output}"
"#;
    let mut slots = slots_with_items(json!(["x"]));
    slots.insert("suffix".into(), json!("!"));
    let result = run_yaml(yaml, slots).expect("run");
    assert_eq!(result, json!([{"value": {"element": "x", "suffix": "!"}}]));
}
