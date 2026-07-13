//! weave ETL benchmark.
//!
//! Tests: batch size, cache miss vs hit, built-in vs inline JS.
//! E-commerce scenario: 2,000 orders.
//!
//! Run: cargo bench
//! Quick: cargo bench -- --quick

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use criterion::{Criterion, black_box, criterion_group, criterion_main};
use serde_json::json;
use tokio::sync::Mutex;
use weave::dsl::parser::parse;
use weave::runtime::Executor;
use weave::runtime::dag::Dag;
use weave::store::Database;
use weave::task::{LayerInfo, TaskTracker};

// ── Data Generation ───────────────────────────────────────────────────────

const DATA_SIZE: usize = 2_000;

fn generate_orders(n: usize) -> Vec<serde_json::Value> {
    let statuses = ["paid", "pending", "paid", "cancelled", "paid"];
    let cities = [
        "Beijing",
        "Shanghai",
        "Shenzhen",
        "Hangzhou",
        "Chengdu",
        "Guangzhou",
        "Nanjing",
        "Wuhan",
        "Xian",
        "Chongqing",
    ];

    (0..n)
        .map(|i| {
            json!({
                "order_id": format!("ORD-{:06}", i),
                "user_id": format!("U{}", i % 1000),
                "city": cities[i % cities.len()],
                "status": statuses[i % statuses.len()],
                "total": (100.0 + ((i * 7) % 9000) as f64) / 10.0,
            })
        })
        .collect()
}

// ── Pipeline Builders ─────────────────────────────────────────────────────

fn build_iterate_pipeline(batch_size: Option<usize>) -> String {
    let batch = match batch_size {
        Some(n) => format!("batch:\n        size: {n}\n"),
        None => String::new(),
    };
    format!(
        r#"
name: etl_iterate
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
    type: {op_type}
    iterate:
      over: "{{sort_by_total.output}}"
      as: "item"
      {batch}
    inputs:
      data: "{{item}}"
      field: status
      operator: eq
      value: "paid"
output: "{{iterate_filter.output}}"
"#,
        op_type = "%OP_TYPE%"
    )
}

fn build_single_filter_pipeline() -> String {
    r#"
name: single_filter
steps:
  - id: f
    type: filter
    inputs:
      data: "{slots.orders}"
      field: status
      operator: eq
      value: "paid"
output: "{f.output}"
"#
    .to_string()
}

fn build_inline_js_pipeline() -> String {
    r#"name: inline_js_filter
steps:
  - id: f
    type: js
    code: |
      function run(input) {
        return input.data.filter(function(o) { return o.status === 'paid'; });
      }
    inputs:
      data: "{slots.orders}"
output: "{f.output}"
"#
    .to_string()
}

// ── Helpers ────────────────────────────────────────────────────────────────

fn make_executor(
    rt: &tokio::runtime::Runtime,
    def: &weave::dsl::schema::PipelineDef,
    db: Arc<Mutex<Database>>,
    orders: &[serde_json::Value],
) -> (
    Executor,
    weave::task::TaskId,
    HashMap<String, serde_json::Value>,
) {
    let tracker = Arc::new(TaskTracker::new());

    let dag = Dag::from_pipeline(def).expect("dag");
    let layers = dag.topological_sort().expect("topo");
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
        let db_guard = db.try_lock().expect("db lock");
        db_guard
            .create_task(&def.name, json!(slots), 3600)
            .expect("create task")
    };

    rt.block_on(tracker.create(task_id, def.name.to_string(), all_step_ids, layer_infos));

    let executor = Executor::new(def.clone(), db.clone(), tracker);
    (executor, task_id, slots)
}

fn run_once(
    rt: &tokio::runtime::Runtime,
    def: &weave::dsl::schema::PipelineDef,
    db: Arc<Mutex<Database>>,
    orders: &[serde_json::Value],
) -> usize {
    let (executor, task_id, slots) = make_executor(rt, def, db, orders);
    let result = rt
        .block_on(executor.run(task_id, slots, 3600))
        .expect("run");
    result.len()
}

/// Open a fresh DB at a unique path (counter suffix, no dir delete).
fn fresh_db(prefix: &str, counter: &AtomicUsize) -> Arc<Mutex<Database>> {
    let n = counter.fetch_add(1, Ordering::Relaxed);
    let tmp = std::env::temp_dir().join(format!("weave-bench-{prefix}-{n}"));
    let _ = std::fs::create_dir_all(&tmp);
    Arc::new(Mutex::new(
        Database::open(tmp.join("weave.redb")).expect("open db"),
    ))
}

// ── Benchmarks ────────────────────────────────────────────────────────────

fn bench_iterate_batch(c: &mut Criterion) {
    let mut group = c.benchmark_group("iterate_batch");
    group.sample_size(10);
    group.warm_up_time(Duration::from_secs(10));
    group.measurement_time(Duration::from_secs(20));
    let orders = generate_orders(DATA_SIZE);
    let rt = tokio::runtime::Runtime::new().unwrap();

    let sizes = &[Some(100), Some(200), Some(500), None];
    for &bs in sizes {
        let label = match bs {
            Some(n) => format!("batch_size_{n}"),
            None => "batch_none".to_string(),
        };
        let yaml = build_iterate_pipeline(bs).replace("%OP_TYPE%", "filter");
        let def = parse(&yaml).expect("parse");

        // ── cache MISS: sequential fresh DBs ──
        let counter = AtomicUsize::new(0);
        group.bench_function(format!("{label}_miss"), |b| {
            b.iter(|| {
                let db = fresh_db(&format!("m-{label}"), &counter);
                let n = run_once(&rt, &def, db, &orders);
                black_box(n);
            })
        });

        // ── cache HIT: pre-warmed DB ──
        let db = fresh_db(&format!("h-{label}"), &AtomicUsize::new(0));
        run_once(&rt, &def, db.clone(), &orders); // warm

        group.bench_function(format!("{label}_hit"), |b| {
            b.iter(|| {
                let n = run_once(&rt, &def, db.clone(), &orders);
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

    // --- built-in filter ---
    let yaml_builtin = build_single_filter_pipeline();
    let def_builtin = parse(&yaml_builtin).expect("parse");

    let counter = AtomicUsize::new(0);
    group.bench_function("filter_builtin_miss", |b| {
        b.iter(|| {
            let db = fresh_db("bm", &counter);
            let n = run_once(&rt, &def_builtin, db, &orders);
            black_box(n);
        })
    });

    let db = fresh_db("bh", &AtomicUsize::new(0));
    run_once(&rt, &def_builtin, db.clone(), &orders);
    group.bench_function("filter_builtin_hit", |b| {
        b.iter(|| {
            let n = run_once(&rt, &def_builtin, db.clone(), &orders);
            black_box(n);
        })
    });

    // --- inline JS filter ---
    let yaml_js = build_inline_js_pipeline();
    let def_js = parse(&yaml_js).expect("parse js");

    let counter2 = AtomicUsize::new(0);
    group.bench_function("filter_inline_js_miss", |b| {
        b.iter(|| {
            let db = fresh_db("jm", &counter2);
            let n = run_once(&rt, &def_js, db, &orders);
            black_box(n);
        })
    });

    let db_js = fresh_db("jh", &AtomicUsize::new(0));
    run_once(&rt, &def_js, db_js.clone(), &orders);
    group.bench_function("filter_inline_js_hit", |b| {
        b.iter(|| {
            let n = run_once(&rt, &def_js, db_js.clone(), &orders);
            black_box(n);
        })
    });

    group.finish();
}

criterion_group!(benches, bench_iterate_batch, bench_builtin_vs_js);
criterion_main!(benches);
