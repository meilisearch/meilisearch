mod utils;

use std::time::Duration;
use criterion::{criterion_group, criterion_main, BenchmarkId};

fn bench_proximity(c: &mut criterion::Criterion) {
    let index = utils::base_setup(Some(vec!["words".to_string()]));

    let queries = [
        "black saint sinner lady ",
        "les dangeureuses 1960 ",
        "The Disneyland Sing-Alone song ",
        "Under Great Northern Lights ",
        "7000 Danses Un Jour Dans Notre Vie",
    ];

    let mut group = c.benchmark_group("proximity");
    group.measurement_time(Duration::from_secs(10));

    for query in &queries {
        group.bench_with_input(BenchmarkId::from_parameter(query), &query, |b, &query| {
            b.iter(|| {
                let rtxn = index.read_txn().unwrap();
                let _documents_ids = index.search(&rtxn).query(*query).optional_words(false).execute().unwrap();
            });
        });
    }

    group.finish();
}

criterion_group!(benches, bench_proximity);
criterion_main!(benches);
