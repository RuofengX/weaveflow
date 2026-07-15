use std::collections::HashMap;
use std::sync::atomic::AtomicUsize;
use std::time::Duration;

use criterion::{Criterion, black_box, criterion_group, criterion_main};

#[path = "shared.rs"]
mod shared;
use shared::*;

fn bench_parallel_layer(c: &mut Criterion) {
    let mut group = c.benchmark_group("parallel_layer");
    group.sample_size(20);
    group.warm_up_time(Duration::from_secs(1));
    group.measurement_time(Duration::from_secs(5));
    let rt = tokio::runtime::Runtime::new().unwrap();
    let slots = HashMap::new();
    let tmpdir = TempDir::new("par");

    let yaml = build_parallel_pipeline();
    let def = weave::dsl::parser::parse(&yaml).expect("parse");

    let counter = AtomicUsize::new(0);
    group.bench_function("parallel_filter_sort_merge", |b| {
        b.iter(|| {
            let (db, dir) = fresh_db(&tmpdir, "par", &counter);
            let n = run_once(&rt, &def, &db, slots.clone());
            drop(db);
            let _ = std::fs::remove_dir_all(&dir);
            black_box(n);
        })
    });

    group.finish();
}

criterion_group!(benches, bench_parallel_layer);
criterion_main!(benches);
