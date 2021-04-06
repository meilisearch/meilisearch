mod utils;

use std::time::Duration;
use criterion::{criterion_group, criterion_main, BenchmarkId};

fn bench_words(c: &mut criterion::Criterion) {
    let index = utils::base_setup(Some(vec!["words".to_string()]));

    let queries = [
        "the black saint and the sinner lady and the good doggo ", // four words to pop
        "les liaisons dangeureuses 1793 ", // one word to pop
        "The Disneyland Children's Sing-Alone song ", // two words to pop
        "seven nation mummy ", // one word to pop
        "7000 Danses / Le Baiser / je me trompe de mots ", // four words to pop
        "Bring Your Daughter To The Slaughter but now this is not part of the title ", // nine words to pop
        "whathavenotnsuchforth and then a good amount of words tot pop in order to match the first one ", // 16
    ];

    let mut group = c.benchmark_group("words");
    group.measurement_time(Duration::from_secs(10));

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

criterion_group!(benches, bench_words);
criterion_main!(benches);
