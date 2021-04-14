use std::{
    fs::{create_dir_all, remove_dir_all, File},
    time::Duration,
};

use criterion::BenchmarkId;
use heed::EnvOpenOptions;
use milli::{
    update::{IndexDocumentsMethod, Settings, UpdateBuilder, UpdateFormat},
    FacetCondition, Index,
};

pub struct Conf<'a> {
    /// where we are going to create our database.mmdb directory
    /// each benchmark will first try to delete it and then recreate it
    pub database_name: &'a str,
    /// the dataset to be used, it must be an uncompressed csv
    pub dataset: &'a str,
    pub group_name: &'a str,
    pub queries: &'a [&'a str],
    /// here you can change which criterion are used and in which order.
    /// - if you specify something all the base configuration will be thrown out
    /// - if you don't specify anything (None) the default configuration will be kept
    pub criterion: Option<&'a [&'a str]>,
    /// the last chance to configure your database as you want
    pub configure: fn(&mut Settings),
    pub facet_condition: Option<FacetCondition>,
    /// enable or disable the optional words on the query
    pub optional_words: bool,
}

impl Conf<'_> {
    fn nop(_builder: &mut Settings) {}

    pub const BASE: Self = Conf {
        database_name: "benches.mmdb",
        dataset: "",
        group_name: "",
        queries: &[],
        criterion: None,
        configure: Self::nop,
        facet_condition: None,
        optional_words: true,
    };
}

pub fn base_setup(conf: &Conf) -> Index {
    match remove_dir_all(&conf.database_name) {
        Ok(_) => (),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => (),
        Err(e) => panic!("{}", e),
    }
    create_dir_all(&conf.database_name).unwrap();

    let mut options = EnvOpenOptions::new();
    options.map_size(100 * 1024 * 1024 * 1024); // 100 GB
    options.max_readers(10);
    let index = Index::new(options, conf.database_name).unwrap();

    let update_builder = UpdateBuilder::new(0);
    let mut wtxn = index.write_txn().unwrap();
    let mut builder = update_builder.settings(&mut wtxn, &index);

    if let Some(criterion) = conf.criterion {
        builder.reset_faceted_fields();
        builder.reset_criteria();
        builder.reset_stop_words();

        let criterion = criterion.iter().map(|s| s.to_string()).collect();
        builder.set_criteria(criterion);
    }

    (conf.configure)(&mut builder);

    builder.execute(|_, _| ()).unwrap();
    wtxn.commit().unwrap();

    let update_builder = UpdateBuilder::new(0);
    let mut wtxn = index.write_txn().unwrap();
    let mut builder = update_builder.index_documents(&mut wtxn, &index);
    builder.update_format(UpdateFormat::Csv);
    builder.index_documents_method(IndexDocumentsMethod::ReplaceDocuments);
    // we called from cargo the current directory is supposed to be milli/milli
    let dataset_path = format!("benches/{}", conf.dataset);
    let reader = File::open(&dataset_path)
        .expect(&format!("could not find the dataset in: {}", &dataset_path));
    builder.execute(reader, |_, _| ()).unwrap();
    wtxn.commit().unwrap();

    index
}

pub fn run_benches(c: &mut criterion::Criterion, confs: &[Conf]) {
    for conf in confs {
        let index = base_setup(conf);

        let mut group = c.benchmark_group(&format!("{}: {}", conf.dataset, conf.group_name));
        group.measurement_time(Duration::from_secs(10));

        for &query in conf.queries {
            group.bench_with_input(BenchmarkId::from_parameter(query), &query, |b, &query| {
                b.iter(|| {
                    let rtxn = index.read_txn().unwrap();
                    let mut search = index.search(&rtxn);
                    search.query(query).optional_words(conf.optional_words);
                    if let Some(facet_condition) = conf.facet_condition.clone() {
                        search.facet_condition(facet_condition);
                    }
                    let _ids = search.execute().unwrap();
                });
            });
        }
        group.finish();
    }
}
