use std::time::Duration;

use heed::EnvOpenOptions;
use mega_mini_indexer::Index;
use criterion::{criterion_group, criterion_main, BenchmarkId};

fn bench_search(c: &mut criterion::Criterion) {
    let database = "books-4cpu.mmdb";
    let queries = [
        "minogue kylie",
        "minogue kylie live",
    ];

    std::fs::create_dir_all(database).unwrap();
    let env = EnvOpenOptions::new()
        .map_size(100 * 1024 * 1024 * 1024) // 100 GB
        .max_readers(10)
        .max_dbs(5)
        .open(database).unwrap();

    let index = Index::new(&env).unwrap();

    let mut group = c.benchmark_group("search");
    group.sample_size(10);
    group.measurement_time(Duration::from_secs(12));

    for query in &queries {
        group.bench_with_input(BenchmarkId::from_parameter(query), &query, |b, &query| {
            b.iter(|| {
                let rtxn = env.read_txn().unwrap();
                let _documents_ids = index.search(&rtxn, query).unwrap();
            });
        });
    }


    group.finish();
}

criterion_group!(benches, bench_search);
criterion_main!(benches);
