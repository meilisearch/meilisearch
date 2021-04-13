mod utils;

use criterion::{criterion_group, criterion_main};

fn bench_normal(c: &mut criterion::Criterion) {
    let confs = &[
        utils::Conf {
            group_name: "basic placeholder",
            queries: &[
                "",
            ],
            ..utils::Conf::BASE_SONGS
        },
        utils::Conf {
            group_name: "basic without quote",
            queries: &[
                "david bowie", // 1200
                "michael jackson", // 600
                "marcus miller", // 60
                "Notstandskomitee", // 4
            ],
            ..utils::Conf::BASE_SONGS
        },
        utils::Conf {
            group_name: "basic with quote",
            queries: &[
                "\"david\" \"bowie\"", // 1200
                "\"michael\" \"jackson\"", // 600
                "\"marcus\" \"miller\"", // 60
                "\"Notstandskomitee\"", // 4
            ],
            ..utils::Conf::BASE_SONGS
        },
        utils::Conf {
            group_name: "prefix search",
            queries: &[
                "s", // 500k+ results
                "a",
                "b",
                "i",
                "x", // only 7k results
            ],
            ..utils::Conf::BASE_SONGS
        },
    ];

    utils::run_benches(c, confs);
}

criterion_group!(benches, bench_normal);
criterion_main!(benches);
