//! weave ETL benchmark suite.
//!
//! Benchmarks: iterate batch sizes, builtin vs inline JS,
//! parallel layer execution, operator stress test.
//!
//! Run: cargo bench

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use criterion::{Criterion, black_box, criterion_group, criterion_main};
use serde_json::json;
use tokio::sync::Mutex;
use weave::dsl::parser::parse;
use weave::engine::dag::Dag;
use weave::engine::runner::Runner;
use weave::store::Database;
use weave::tracker::{LayerInfo, TaskId, TaskTracker};

const DATA_SIZE: usize = 2_000;

fn generate_orders(n: usize) -> Vec<serde_json::Value> {
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

fn generate_nested_data(n: usize) -> Vec<serde_json::Value> {
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

// ── Pipeline builders ────────────────────────────────────────────────────

fn build_iterate_pipeline(batch_size: Option<usize>) -> String {
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

fn build_single_filter_pipeline() -> String {
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

fn build_inline_js_pipeline() -> String {
    r#"name: inline_js
steps:
  - id: f
    type: js
    code: |
      function run(input) {
        return input.data.filter(function(o) { return o.status === 'paid'; });
      }
    inputs:
      data: "{slots.orders}"
output: "{f.output}""#
    .to_string()
}

fn build_parallel_pipeline() -> String {
    r#"name: parallel_ops
steps:
  - id: f1
    type: filter
    inputs:
      data: "{slots.orders}"
      field: status
      operator: eq
      value: "paid"
  - id: sorted
    type: sort
    inputs:
      data: "{slots.orders}"
      field: total
      order: desc
  - id: merged
    type: merge
    inputs:
      a: "{f1.output}"
      b: "{sorted.output}"
output: "{merged.output}""#
    .to_string()
}

fn build_chained_pipeline() -> String {
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

fn build_var_passthrough_pipeline() -> String {
    r#"name: var_passthrough
steps:
  - id: v
    type: var
    inputs:
      payload: "{slots.orders}"
output: "{v.output}""#
    .to_string()
}

// ── Helpers ──────────────────────────────────────────────────────────────

fn setup(
    rt: &tokio::runtime::Runtime,
    def: &weave::dsl::PipelineDef,
    db: Arc<Mutex<Database>>,
    slots: HashMap<String, serde_json::Value>,
) -> (Runner, TaskId, HashMap<String, serde_json::Value>) {
    let tracker = Arc::new(TaskTracker::new());

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

fn run_once(
    rt: &tokio::runtime::Runtime,
    def: &weave::dsl::PipelineDef,
    db: Arc<Mutex<Database>>,
    slots: HashMap<String, serde_json::Value>,
) -> usize {
    let (runner, task_id, slots) = setup(rt, def, db, slots);
    let result = rt.block_on(runner.run(task_id, slots)).expect("run");
    result.len()
}

fn fresh_db(prefix: &str, counter: &AtomicUsize) -> Arc<Mutex<Database>> {
    let n = counter.fetch_add(1, Ordering::Relaxed);
    let tmp = std::env::temp_dir().join(format!("weave-bench-{prefix}-{n}"));
    let _ = std::fs::create_dir_all(&tmp);
    Arc::new(Mutex::new(Database::open(tmp.join("weave.redb")).expect("open db")))
}

// ── Benchmarks ───────────────────────────────────────────────────────────

fn bench_iterate_batch(c: &mut Criterion) {
    let mut group = c.benchmark_group("iterate_batch");
    group.sample_size(10);
    group.warm_up_time(Duration::from_secs(10));
    group.measurement_time(Duration::from_secs(20));
    let orders = generate_orders(DATA_SIZE);
    let rt = tokio::runtime::Runtime::new().unwrap();
    let mut slots = HashMap::new();
    slots.insert("orders".into(), json!(orders));

    for &bs in &[Some(100), Some(200), Some(500), None] {
        let label = bs.map(|n| format!("batch_size_{n}")).unwrap_or_else(|| "batch_none".into());
        let yaml = build_iterate_pipeline(bs);
        let def = parse(&yaml).expect("parse");

        let counter = AtomicUsize::new(0);
        group.bench_function(format!("{label}_miss"), |b| {
            b.iter(|| {
                let db = fresh_db(&format!("m-{label}"), &counter);
                let n = run_once(&rt, &def, db, slots.clone());
                black_box(n);
            })
        });

        let db = fresh_db(&format!("h-{label}"), &AtomicUsize::new(0));
        run_once(&rt, &def, db.clone(), slots.clone());
        group.bench_function(format!("{label}_hit"), |b| {
            b.iter(|| {
                let n = run_once(&rt, &def, db.clone(), slots.clone());
                black_box(n);
            })
        });
    }
    group.finish();
}

fn bench_builtin_vs_js(c: &mut Criterion) {
    let mut group = c.benchmark_group("builtin_vs_js");
    group.sample_size(20);
    group.warm_up_time(Duration::from_secs(1));
    group.measurement_time(Duration::from_secs(5));
    let orders = generate_orders(DATA_SIZE);
    let rt = tokio::runtime::Runtime::new().unwrap();
    let mut slots = HashMap::new();
    slots.insert("orders".into(), json!(orders));

    let yaml_builtin = build_single_filter_pipeline();
    let def_builtin = parse(&yaml_builtin).expect("parse");
    let counter = AtomicUsize::new(0);

    group.bench_function("filter_builtin_miss", |b| {
        b.iter(|| {
            let db = fresh_db("bm", &counter);
            let n = run_once(&rt, &def_builtin, db, slots.clone());
            black_box(n);
        })
    });

    let db = fresh_db("bh", &AtomicUsize::new(0));
    run_once(&rt, &def_builtin, db.clone(), slots.clone());
    group.bench_function("filter_builtin_hit", |b| {
        b.iter(|| {
            let n = run_once(&rt, &def_builtin, db.clone(), slots.clone());
            black_box(n);
        })
    });

    let yaml_js = build_inline_js_pipeline();
    let def_js = parse(&yaml_js).expect("parse js");
    let counter2 = AtomicUsize::new(0);

    group.bench_function("filter_inline_js_miss", |b| {
        b.iter(|| {
            let db = fresh_db("jm", &counter2);
            let n = run_once(&rt, &def_js, db, slots.clone());
            black_box(n);
        })
    });

    let db_js = fresh_db("jh", &AtomicUsize::new(0));
    run_once(&rt, &def_js, db_js.clone(), slots.clone());
    group.bench_function("filter_inline_js_hit", |b| {
        b.iter(|| {
            let n = run_once(&rt, &def_js, db_js.clone(), slots.clone());
            black_box(n);
        })
    });

    group.finish();
}

fn bench_operator_chain(c: &mut Criterion) {
    let mut group = c.benchmark_group("operator_chain");
    group.sample_size(20);
    group.warm_up_time(Duration::from_secs(1));
    group.measurement_time(Duration::from_secs(5));
    let items = generate_nested_data(DATA_SIZE);
    let rt = tokio::runtime::Runtime::new().unwrap();
    let mut slots = HashMap::new();
    slots.insert("items".into(), json!(items));

    let yaml = build_chained_pipeline();
    let def = parse(&yaml).expect("parse");

    let counter = AtomicUsize::new(0);
    group.bench_function("chain_filter_sort_dedup", |b| {
        b.iter(|| {
            let db = fresh_db("chain", &counter);
            let n = run_once(&rt, &def, db, slots.clone());
            black_box(n);
        })
    });

    group.finish();
}

fn bench_parallel_layer(c: &mut Criterion) {
    let mut group = c.benchmark_group("parallel_layer");
    group.sample_size(20);
    group.warm_up_time(Duration::from_secs(1));
    group.measurement_time(Duration::from_secs(5));
    let orders = generate_orders(DATA_SIZE);
    let rt = tokio::runtime::Runtime::new().unwrap();
    let mut slots = HashMap::new();
    slots.insert("orders".into(), json!(orders));

    let yaml = build_parallel_pipeline();
    let def = parse(&yaml).expect("parse");

    let counter = AtomicUsize::new(0);
    group.bench_function("parallel_filter_sort_merge", |b| {
        b.iter(|| {
            let db = fresh_db("par", &counter);
            let n = run_once(&rt, &def, db, slots.clone());
            black_box(n);
        })
    });

    group.finish();
}

fn bench_var_passthrough(c: &mut Criterion) {
    let mut group = c.benchmark_group("var_passthrough");
    group.sample_size(30);
    group.warm_up_time(Duration::from_secs(1));
    let orders = generate_orders(DATA_SIZE);
    let rt = tokio::runtime::Runtime::new().unwrap();
    let mut slots = HashMap::new();
    slots.insert("orders".into(), json!(orders));

    let yaml = build_var_passthrough_pipeline();
    let def = parse(&yaml).expect("parse");
    let counter = AtomicUsize::new(0);

    group.bench_function("var_json_roundtrip", |b| {
        b.iter(|| {
            let db = fresh_db("var", &counter);
            let n = run_once(&rt, &def, db, slots.clone());
            black_box(n);
        })
    });

    group.finish();
}

criterion_group!(
    benches,
    bench_iterate_batch,
    bench_builtin_vs_js,
    bench_operator_chain,
    bench_parallel_layer,
    bench_var_passthrough,
);
criterion_main!(benches);
