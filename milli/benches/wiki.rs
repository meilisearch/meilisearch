mod utils;

use criterion::{criterion_group, criterion_main};
use milli::update::Settings;
use utils::Conf;

fn base_conf(builder: &mut Settings) {
    let displayed_fields = ["title", "body", "url"]
        .iter()
        .map(|s| s.to_string())
        .collect();
    builder.set_displayed_fields(displayed_fields);

    let searchable_fields = ["title", "body"].iter().map(|s| s.to_string()).collect();
    builder.set_searchable_fields(searchable_fields);
}

const BASE_CONF: Conf = Conf {
    dataset: "smol-wiki-articles.csv",
    queries: &[
        "mingus ",        // 46 candidates
        "miles davis ",   // 159
        "rock and roll ", // 1007
        "machine ",       // 3448
        "spain ",         // 7002
        "japan ",         // 10.593
        "france ",        // 17.616
        "film ",          //  24.959
    ],
    configure: base_conf,
    ..Conf::BASE
};

fn bench_songs(c: &mut criterion::Criterion) {
    let basic_with_quote: Vec<String> = BASE_CONF
        .queries
        .iter()
        .map(|s| {
            s.trim()
                .split(' ')
                .map(|s| format!(r#""{}""#, s))
                .collect::<Vec<String>>()
                .join(" ")
        })
        .collect();
    let basic_with_quote: &[&str] = &basic_with_quote
        .iter()
        .map(|s| s.as_str())
        .collect::<Vec<&str>>();

    let confs = &[
        /* first we bench each criterion alone */
        utils::Conf {
            group_name: "proximity",
            queries: &[
                "herald sings ",
                "april paris ",
                "tea two ",
                "diesel engine ",
            ],
            criterion: Some(&["proximity"]),
            optional_words: false,
            ..BASE_CONF
        },
        utils::Conf {
            group_name: "typo",
            queries: &[
                "migrosoft ",
                "linax ",
                "Disnaylande ",
                "phytogropher ",
                "nympalidea ",
                "aritmetric ",
                "the fronce ",
                "sisan ",
            ],
            criterion: Some(&["typo"]),
            optional_words: false,
            ..BASE_CONF
        },
        utils::Conf {
            group_name: "words",
            queries: &[
                "the black saint and the sinner lady and the good doggo ", // four words to pop, 27 results
                "Kameya Tokujirō mingus monk ",                           // two words to pop, 55
                "Ulrich Hensel meilisearch milli ",                        // two words to pop, 306
                "Idaho Bellevue pizza ",                                   // one word to pop, 800
                "Abraham machin ",                                         // one word to pop, 1141
            ],
            criterion: Some(&["words"]),
            ..BASE_CONF
        },
        /* the we bench some global / normal search with all the default criterion in the default
         * order */
        utils::Conf {
            group_name: "basic placeholder",
            queries: &[""],
            ..BASE_CONF
        },
        utils::Conf {
            group_name: "basic without quote",
            queries: &BASE_CONF
                .queries
                .iter()
                .map(|s| s.trim()) // we remove the space at the end of each request
                .collect::<Vec<&str>>(),
            ..BASE_CONF
        },
        utils::Conf {
            group_name: "basic with quote",
            queries: basic_with_quote,
            ..BASE_CONF
        },
        utils::Conf {
            group_name: "prefix search",
            queries: &[
                "t", // 453k results
                "c", // 405k
                "g", // 318k
                "j", // 227k
                "q", // 71k
                "x", // 17k
            ],
            ..BASE_CONF
        },
    ];

    utils::run_benches(c, confs);
}

criterion_group!(benches, bench_songs);
criterion_main!(benches);
