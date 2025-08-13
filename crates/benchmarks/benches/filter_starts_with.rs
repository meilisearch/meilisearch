mod datasets_paths;
mod utils;

use criterion::{criterion_group, criterion_main};
use milli::update::Settings;
use milli::FilterableAttributesRule;
use utils::Conf;

#[cfg(not(windows))]
#[global_allocator]
static ALLOC: mimalloc::MiMalloc = mimalloc::MiMalloc;

fn base_conf(builder: &mut Settings) {
    let displayed_fields = ["geonameid", "name"].iter().map(|s| s.to_string()).collect();
    builder.set_displayed_fields(displayed_fields);

    let filterable_fields =
        ["name"].iter().map(|s| FilterableAttributesRule::Field(s.to_string())).collect();
    builder.set_filterable_fields(filterable_fields);
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

fn filter_starts_with(c: &mut criterion::Criterion) {
    #[rustfmt::skip]
    let confs = &[
        utils::Conf {
            group_name: "1 letter",
            filter: Some("name STARTS WITH e"),
            ..BASE_CONF
        },

        utils::Conf {
            group_name: "2 letters",
            filter: Some("name STARTS WITH es"),
            ..BASE_CONF
        },

        utils::Conf {
            group_name: "3 letters",
            filter: Some("name STARTS WITH est"),
            ..BASE_CONF
        },

        utils::Conf {
            group_name: "6 letters",
            filter: Some("name STARTS WITH estoni"),
            ..BASE_CONF
        }
    ];

    utils::run_benches(c, confs);
}

criterion_group!(benches, filter_starts_with);
criterion_main!(benches);
