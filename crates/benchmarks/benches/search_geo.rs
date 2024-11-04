mod datasets_paths;
mod utils;

use criterion::{criterion_group, criterion_main};
use milli::update::Settings;
use utils::Conf;

#[global_allocator]
static ALLOC: mimalloc::MiMalloc = mimalloc::MiMalloc;

fn base_conf(builder: &mut Settings) {
    let displayed_fields =
        ["geonameid", "name", "asciiname", "alternatenames", "_geo", "population"]
            .iter()
            .map(|s| s.to_string())
            .collect();
    builder.set_displayed_fields(displayed_fields);

    let searchable_fields =
        ["name", "alternatenames", "elevation"].iter().map(|s| s.to_string()).collect();
    builder.set_searchable_fields(searchable_fields);

    let filterable_fields =
        ["_geo", "population", "elevation"].iter().map(|s| s.to_string()).collect();
    builder.set_filterable_fields(filterable_fields);

    let sortable_fields =
        ["_geo", "population", "elevation"].iter().map(|s| s.to_string()).collect();
    builder.set_sortable_fields(sortable_fields);
}

#[rustfmt::skip]
const BASE_CONF: Conf = Conf {
    dataset: datasets_paths::SMOL_ALL_COUNTRIES,
    dataset_format: "jsonl",
    queries: &[
        "",
    ],
    configure: base_conf,
    primary_key: Some("geonameid"),
    ..Conf::BASE
};

fn bench_geo(c: &mut criterion::Criterion) {
    #[rustfmt::skip]
    let confs = &[
        // A basic placeholder with no geo
        utils::Conf {
            group_name: "placeholder with no geo",
            ..BASE_CONF
        },
        // Medium aglomeration: probably the most common usecase
        utils::Conf {
            group_name: "asc sort from Lille",
            sort: Some(vec!["_geoPoint(50.62999333378238, 3.086269263384099):asc"]),
            ..BASE_CONF
        },
        utils::Conf {
            group_name: "desc sort from Lille",
            sort: Some(vec!["_geoPoint(50.62999333378238, 3.086269263384099):desc"]),
            ..BASE_CONF
        },
        // Big agglomeration: a lot of documents close to our point
        utils::Conf {
            group_name: "asc sort from Tokyo",
            sort: Some(vec!["_geoPoint(35.749512532692144, 139.61664952543356):asc"]),
            ..BASE_CONF
        },
        utils::Conf {
            group_name: "desc sort from Tokyo",
            sort: Some(vec!["_geoPoint(35.749512532692144, 139.61664952543356):desc"]),
            ..BASE_CONF
        },
        // The furthest point from any civilization
        utils::Conf {
            group_name: "asc sort from Point Nemo",
            sort: Some(vec!["_geoPoint(-48.87561645055408, -123.39275749319793):asc"]),
            ..BASE_CONF
        },
        utils::Conf {
            group_name: "desc sort from Point Nemo",
            sort: Some(vec!["_geoPoint(-48.87561645055408, -123.39275749319793):desc"]),
            ..BASE_CONF
        },
        // Filters
        utils::Conf {
            group_name: "filter of 100km from Lille",
            filter: Some("_geoRadius(50.62999333378238, 3.086269263384099, 100000)"),
            ..BASE_CONF
        },
        utils::Conf {
            group_name: "filter of 1km from Lille",
            filter: Some("_geoRadius(50.62999333378238, 3.086269263384099, 1000)"),
            ..BASE_CONF
        },
        utils::Conf {
            group_name: "filter of 100km from Tokyo",
            filter: Some("_geoRadius(35.749512532692144, 139.61664952543356, 100000)"),
            ..BASE_CONF
        },
        utils::Conf {
            group_name: "filter of 1km from Tokyo",
            filter: Some("_geoRadius(35.749512532692144, 139.61664952543356, 1000)"),
            ..BASE_CONF
        },
        utils::Conf {
            group_name: "filter of 100km from Point Nemo",
            filter: Some("_geoRadius(-48.87561645055408, -123.39275749319793, 100000)"),
            ..BASE_CONF
        },
        utils::Conf {
            group_name: "filter of 1km from Point Nemo",
            filter: Some("_geoRadius(-48.87561645055408, -123.39275749319793, 1000)"),
            ..BASE_CONF
        },
    ];

    utils::run_benches(c, confs);
}

criterion_group!(benches, bench_geo);
criterion_main!(benches);
