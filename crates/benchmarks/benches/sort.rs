//! This benchmark module is used to compare the performance of sorting documents in /search VS /documents
//!
//! The tests/benchmarks were designed in the context of a query returning only 20 documents.

mod datasets_paths;
mod utils;

use criterion::{criterion_group, criterion_main};
use milli::update::Settings;
use utils::Conf;

#[cfg(not(windows))]
#[global_allocator]
static ALLOC: mimalloc::MiMalloc = mimalloc::MiMalloc;

fn base_conf(builder: &mut Settings) {
    let displayed_fields =
        ["geonameid", "name", "asciiname", "alternatenames", "_geo", "population"]
            .iter()
            .map(|s| s.to_string())
            .collect();
    builder.set_displayed_fields(displayed_fields);

    let sortable_fields =
        ["_geo", "name", "population", "elevation", "timezone", "modification-date"]
            .iter()
            .map(|s| s.to_string())
            .collect();
    builder.set_sortable_fields(sortable_fields);
}

#[rustfmt::skip]
const BASE_CONF: Conf = Conf {
    dataset: datasets_paths::SMOL_ALL_COUNTRIES,
    dataset_format: "jsonl",
    configure: base_conf,
    primary_key: Some("geonameid"),
    queries: &[""],
    offsets: &[
        Some((0, 20)), // The most common query in the real world
        Some((0, 500)), // A query that ranges over many documents
        Some((980, 20)), // The worst query that could happen in the real world
        Some((800_000, 20)) // The worst query
    ],
    get_documents: true,
    ..Conf::BASE
};

fn bench_sort(c: &mut criterion::Criterion) {
    #[rustfmt::skip]
    let confs = &[
        utils::Conf {
            group_name: "without sort",
            sort: None,
            ..BASE_CONF
        },

        utils::Conf {
            group_name: "sort on many different values",
            sort: Some(vec!["name:asc"]),
            ..BASE_CONF
        },

        utils::Conf {
            group_name: "sort on many similar values",
            sort: Some(vec!["timezone:desc"]),
            ..BASE_CONF
        },

        utils::Conf {
            group_name: "sort on many similar then different values",
            sort: Some(vec!["timezone:desc", "name:asc"]),
            ..BASE_CONF
        },

        utils::Conf {
            group_name: "sort on many different then similar values",
            sort: Some(vec!["timezone:desc", "name:asc"]),
            ..BASE_CONF
        },

        utils::Conf {
            group_name: "geo sort",
            sample_size: Some(10),
            sort: Some(vec!["_geoPoint(45.4777599, 9.1967508):asc"]),
            ..BASE_CONF
        },

        utils::Conf {
            group_name: "sort on many similar values then geo sort",
            sample_size: Some(50),
            sort: Some(vec!["timezone:desc", "_geoPoint(45.4777599, 9.1967508):asc"]),
            ..BASE_CONF
        },

        utils::Conf {
            group_name: "sort on many different values then geo sort",
            sample_size: Some(50),
            sort: Some(vec!["name:desc", "_geoPoint(45.4777599, 9.1967508):asc"]),
            ..BASE_CONF
        },

        utils::Conf {
            group_name: "sort on many fields",
            sort: Some(vec!["population:asc", "name:asc", "elevation:asc", "timezone:asc"]),
            ..BASE_CONF
        },
    ];

    utils::run_benches(c, confs);
}

criterion_group!(benches, bench_sort);
criterion_main!(benches);
