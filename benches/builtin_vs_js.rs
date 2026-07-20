use std::collections::HashMap;
use std::sync::atomic::AtomicUsize;
use std::time::Duration;

use criterion::{Criterion, black_box, criterion_group, criterion_main};
use serde_json::json;

#[path = "shared.rs"]
mod shared;
use shared::*;

fn bench_builtin_vs_js(c: &mut Criterion) {
    let mut group = c.benchmark_group("builtin_vs_js");
    group.sample_size(20);
    group.warm_up_time(Duration::from_secs(1));
    group.measurement_time(Duration::from_secs(5));
    let orders = generate_orders(DATA_SIZE);
    let rt = tokio::runtime::Runtime::new().unwrap();
    let mut slots = HashMap::new();
    slots.insert("orders".into(), json!(orders));
    let tmpdir = TempDir::new("bvj");

    let yaml_builtin = build_single_filter_pipeline();
    let def_builtin = weaveflow::dsl::parser::parse(&yaml_builtin).expect("parse");
    let counter = AtomicUsize::new(0);

    group.bench_function("filter_builtin_miss", |b| {
        b.iter(|| {
            let (db, dir) = fresh_db(&tmpdir, "bm", &counter);
            let n = run_once(&rt, &def_builtin, &db, slots.clone());
            drop(db);
            let _ = std::fs::remove_dir_all(&dir);
            black_box(n);
        })
    });

    let (db_hit, _dir_hit) = fresh_db(&tmpdir, "bh", &AtomicUsize::new(0));
    run_once(&rt, &def_builtin, &db_hit, slots.clone());
    group.bench_function("filter_builtin_hit", |b| {
        b.iter(|| {
            let n = run_once(&rt, &def_builtin, &db_hit, slots.clone());
            black_box(n);
        })
    });

    let yaml_js = build_inline_js_pipeline();
    let def_js = weaveflow::dsl::parser::parse(&yaml_js).expect("parse js");
    let counter2 = AtomicUsize::new(0);

    group.bench_function("filter_inline_js_miss", |b| {
        b.iter(|| {
            let (db, dir) = fresh_db(&tmpdir, "jm", &counter2);
            let n = run_once(&rt, &def_js, &db, slots.clone());
            drop(db);
            let _ = std::fs::remove_dir_all(&dir);
            black_box(n);
        })
    });

    let (db_js, _dir_js) = fresh_db(&tmpdir, "jh", &AtomicUsize::new(0));
    run_once(&rt, &def_js, &db_js, slots.clone());
    group.bench_function("filter_inline_js_hit", |b| {
        b.iter(|| {
            let n = run_once(&rt, &def_js, &db_js, slots.clone());
            black_box(n);
        })
    });

    group.finish();
}

criterion_group!(benches, bench_builtin_vs_js);
criterion_main!(benches);
