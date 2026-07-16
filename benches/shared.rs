#![allow(unused)]

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use serde_json::json;
use tokio::sync::Mutex;
use weave::engine::dag::Dag;
use weave::engine::runner::Runner;
use weave::store::Database;
use weave::tracker::{LayerInfo, TaskId, TaskTracker};

pub const DATA_SIZE: usize = 2_000;

pub struct TempDir(PathBuf);

impl TempDir {
    pub fn new(prefix: &str) -> Self {
        let n = std::process::id();
        let path = std::env::temp_dir().join(format!("weave-bench-{prefix}-{n}"));
        let _ = std::fs::create_dir_all(&path);
        TempDir(path)
    }
}

impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

pub fn generate_orders(n: usize) -> Vec<serde_json::Value> {
    let statuses = ["paid", "pending", "paid", "cancelled", "paid"];
    let cities = [
        "Beijing", "Shanghai", "Shenzhen", "Hangzhou", "Chengdu",
        "Guangzhou", "Nanjing", "Wuhan", "Xian", "Chongqing",
    ];
    (0..n)
        .map(|i| json!({
            "order_id": format!("ORD-{:06}", i),
            "user_id": format!("U{}", i % 1000),
            "city": cities[i % cities.len()],
            "status": statuses[i % statuses.len()],
            "total": (100.0 + ((i * 7) % 9000) as f64) / 10.0,
        }))
        .collect()
}

pub fn generate_nested_data(n: usize) -> Vec<serde_json::Value> {
    let tags = ["urgent", "normal", "low", "urgent", "normal"];
    (0..n)
        .map(|i| json!({
            "id": i,
            "priority": tags[i % tags.len()],
            "metadata": {
                "source": format!("src-{}", i % 5),
                "version": i as u32 % 3 + 1,
            },
            "values": (0..(i % 10 + 1)).map(|j| j as f64 * 1.5).collect::<Vec<_>>(),
        }))
        .collect()
}

pub fn build_iterate_pipeline(batch_size: Option<usize>) -> String {
    let batch = batch_size.map(|n| format!("batch:\n        size: {n}\n")).unwrap_or_default();
    format!(
        r#"name: etl_iterate
steps:
  - id: filter_paid
    type: filter
    inputs:
      data: "{{slots.orders}}"
      field: status
      operator: eq
      value: "paid"
  - id: sort_by_total
    type: sort
    inputs:
      data: "{{filter_paid.output}}"
      field: total
      order: desc
  - id: iterate_filter
    type: filter
    iterate:
      over: "{{sort_by_total.output}}"
      as: "item"
      {batch}
    inputs:
      data: "{{item}}"
      field: status
      operator: eq
      value: "paid"
output: "{{iterate_filter.output}}""#
    )
}

pub fn build_single_filter_pipeline() -> String {
    r#"name: single_filter
steps:
  - id: f
    type: filter
    inputs:
      data: "{slots.orders}"
      field: status
      operator: eq
      value: "paid"
output: "{f.output}""#
        .to_string()
}

pub fn build_inline_js_pipeline() -> String {
    r#"name: inline_js
steps:
  - id: f
    type: js
    inputs:
      code: |
        function run(data) {
          return data.filter(function(o) { return o.status === 'paid'; });
        }
      data: "{slots.orders}"
output: "{f.output}""#
        .to_string()
}

pub fn build_parallel_pipeline() -> String {
    r#"name: parallel_ops
steps:
  - id: obj_a
    type: var
    inputs:
      value:
        x: 1
        name: "alpha"
  - id: obj_b
    type: var
    inputs:
      value:
        y: 2
        version: 3
  - id: merged
    type: merge
    inputs:
      a: "{obj_a.output}"
      b: "{obj_b.output}"
output: "{merged.output}""#
        .to_string()
}

pub fn build_chained_pipeline() -> String {
    r#"name: op_chain
steps:
  - id: f
    type: filter
    inputs:
      data: "{slots.items}"
      field: priority
      operator: eq
      value: "urgent"
  - id: s
    type: sort
    inputs:
      data: "{f.output}"
      field: id
      order: asc
  - id: d
    type: dedup
    inputs:
      data: "{s.output}"
      field: "metadata.source"
output: "{d.output}""#
        .to_string()
}

pub fn build_var_passthrough_pipeline() -> String {
    r#"name: var_passthrough
steps:
  - id: v
    type: var
    inputs:
      value: "{slots.orders}"
output: "{v.output}""#
        .to_string()
}

pub fn setup(
    rt: &tokio::runtime::Runtime,
    def: &weave::dsl::PipelineDef,
    db: Arc<Mutex<Database>>,
    slots: HashMap<String, serde_json::Value>,
) -> (Runner, TaskId, HashMap<String, serde_json::Value>) {
    let tracker = TaskTracker::new();

    let dag = Dag::from_pipeline(def).expect("dag");
    let layers = dag.topological_sort().expect("topo");
    let all_step_ids: Vec<String> = layers.iter().flatten().cloned().collect();
    let layer_infos: Vec<LayerInfo> = layers
        .iter()
        .enumerate()
        .map(|(i, step_ids)| LayerInfo { index: i, step_ids: step_ids.clone() })
        .collect();

    let task_id = {
        let db_guard = db.try_lock().expect("db lock");
        db_guard.create_task(&def.name, json!(slots), 3600).expect("create task")
    };

    rt.block_on(tracker.create(task_id, def.name.clone(), all_step_ids, layer_infos));

    let runner = Runner::new(def.clone(), db.clone(), tracker);
    (runner, task_id, slots)
}

pub fn run_once(
    rt: &tokio::runtime::Runtime,
    def: &weave::dsl::PipelineDef,
    db: &Arc<Mutex<Database>>,
    slots: HashMap<String, serde_json::Value>,
) -> usize {
    let (runner, task_id, slots) = setup(rt, def, db.clone(), slots);
    let result = rt.block_on(runner.run(task_id, slots)).expect("run");
    result.len()
}

pub fn fresh_db(tmpdir: &TempDir, prefix: &str, counter: &AtomicUsize) -> (Arc<Mutex<Database>>, PathBuf) {
    let n = counter.fetch_add(1, Ordering::Relaxed);
    let dir = tmpdir.0.join(format!("{prefix}-{n}"));
    let _ = std::fs::create_dir_all(&dir);
    (Arc::new(Mutex::new(Database::open(dir.join("weave.redb")).expect("open db"))), dir)
}
