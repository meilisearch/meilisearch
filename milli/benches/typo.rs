mod utils;

use std::time::Duration;
use criterion::{criterion_group, criterion_main, BenchmarkId};

fn bench_typo(c: &mut criterion::Criterion) {
    let index = utils::base_setup(Some(vec!["typo".to_string()]));

    let queries = [
        "mongus ",
        "thelonius monk ",
        "Disnaylande ",
        "the white striper ",
        "indochie ",
        "indochien ",
        "klub des loopers ",
        "fear of the duck ",
        "michel depech ",
        "stromal ",
        "dire straights ",
        "Arethla Franklin ",
    ];

    let mut group = c.benchmark_group("typo");
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

criterion_group!(benches, bench_typo);
criterion_main!(benches);
