use std::collections::HashMap;
use std::sync::Arc;

use serde_json::json;
use tokio::sync::Mutex;
use weave::dsl::parser::parse;
use weave::engine::dag::Dag;
use weave::engine::runner::Runner;
use weave::store::Database;
use weave::tracker::{LayerInfo, TaskTracker};

#[test]
fn verify_iterate_batch_pipeline() {
    let yaml = r#"
name: etl_test
steps:
  - id: f
    type: filter
    inputs:
      iterate:
        over: "{slots.orders}"
        as: "item"
        batch:
          size: 100
      data: "{item}"
      field: status
      operator: eq
      value: "paid"
output: "{f.output}"
"#;

    let orders: Vec<serde_json::Value> = (0..200).map(|i| json!({
        "status": if i % 3 == 0 { "paid" } else { "pending" },
        "total": i,
    })).collect();

    let def = parse(yaml).unwrap();
    let tmp = tempfile::tempdir().unwrap();
    let db = Database::open(tmp.path().join("w.redb")).unwrap();
    let db = Arc::new(Mutex::new(db));
    let tracker = Arc::new(TaskTracker::new());

    let dag = Dag::from_pipeline(&def).unwrap();
    let layers = dag.topological_sort().unwrap();
    let all_step_ids: Vec<String> = layers.iter().flatten().cloned().collect();
    let layer_infos: Vec<LayerInfo> = layers
        .iter()
        .enumerate()
        .map(|(i, step_ids)| LayerInfo {
            index: i,
            step_ids: step_ids.clone(),
        })
        .collect();

    let mut slots = HashMap::new();
    slots.insert("orders".to_string(), json!(orders));

    let task_id = {
        let db_guard = db.try_lock().unwrap();
        db_guard
            .create_task(&def.name, serde_json::json!(slots), 3600)
            .unwrap()
    };
    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(tracker.create(
        task_id,
        def.name.to_string(),
        all_step_ids,
        layer_infos,
    ));

    let runner = Runner::new(def, db.clone(), tracker);

    let result = rt.block_on(runner.run(task_id, slots)).unwrap();
    let v: serde_json::Value = serde_json::from_slice(&result).unwrap();
    let count = v.as_array().map(|a| a.len()).unwrap_or(0);
    assert!(
        count > 0,
        "batch iterate should produce filtered results, got {count}"
    );
}
