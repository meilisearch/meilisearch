use std::time::Duration;

use heed::EnvOpenOptions;
use milli::Index;
use criterion::{criterion_group, criterion_main, BenchmarkId};

fn bench_search(c: &mut criterion::Criterion) {
    let database = "books-4cpu.mmdb";
    let queries = [
        "minogue kylie",
        "minogue kylie live",
    ];

    let mut options = EnvOpenOptions::new();
    options.map_size(100 * 1024 * 1024 * 1024); // 100 GB
    options.max_readers(10);
    let index = Index::new(options, database).unwrap();

    let mut group = c.benchmark_group("search");
    group.sample_size(10);
    group.measurement_time(Duration::from_secs(12));

    for query in &queries {
        group.bench_with_input(BenchmarkId::from_parameter(query), &query, |b, &query| {
            b.iter(|| {
                let rtxn = index.read_txn().unwrap();
                let _documents_ids = index.search(&rtxn).query(*query).execute().unwrap();
            });
        });
    }

    group.finish();
}

criterion_group!(benches, bench_search);
criterion_main!(benches);
