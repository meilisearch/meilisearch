use std::{fs::{File, create_dir_all, remove_dir_all}, time::Duration};

use heed::EnvOpenOptions;
use criterion::BenchmarkId;
use milli::{FacetCondition, Index, update::{IndexDocumentsMethod, UpdateBuilder, UpdateFormat}};

pub struct Conf<'a> {
    /// where we are going to create our database.mmdb directory
    /// each benchmark will first try to delete it and then recreate it
    pub database_name: &'a str,
    /// the dataset to be used, it must be an uncompressed csv
    pub dataset: &'a str,
    pub group_name: &'a str,
    pub queries: &'a[&'a str],
    pub criterion: Option<&'a [&'a str]>,
    pub facet_condition: Option<FacetCondition>,
    pub optional_words: bool,
}

impl Conf<'_> {
    pub const BASE: Self = Conf {
        database_name: "benches.mmdb",
        dataset: "",
        group_name: "",
        queries: &[],
        criterion: None,
        facet_condition: None,
        optional_words: true,
    };
}

pub fn base_setup(database: &str, dataset: &str, criterion: Option<Vec<String>>) -> Index {
    match remove_dir_all(&database) {
        Ok(_) => (),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => (),
        Err(e) => panic!("{}", e),
    }
    create_dir_all(&database).unwrap();

    let mut options = EnvOpenOptions::new();
    options.map_size(100 * 1024 * 1024 * 1024); // 100 GB
    options.max_readers(10);
    let index = Index::new(options, database).unwrap();

    let update_builder = UpdateBuilder::new(0);
    let mut wtxn = index.write_txn().unwrap();
    let mut builder = update_builder.settings(&mut wtxn, &index);

    if let Some(criterion) = criterion {
        builder.reset_faceted_fields();
        builder.reset_criteria();
        builder.reset_stop_words();

        builder.set_criteria(criterion);
    }

    builder.execute(|_, _| ()).unwrap();
    wtxn.commit().unwrap();

    let update_builder = UpdateBuilder::new(0);
    let mut wtxn = index.write_txn().unwrap();
    let mut builder = update_builder.index_documents(&mut wtxn, &index);
    builder.update_format(UpdateFormat::Csv);
    builder.index_documents_method(IndexDocumentsMethod::ReplaceDocuments);
    // we called from cargo the current directory is supposed to be milli/milli
    let reader = File::open(dataset).unwrap();
    builder.execute(reader, |_, _| ()).unwrap();
    wtxn.commit().unwrap();

    index
}

pub fn run_benches(c: &mut criterion::Criterion, confs: &[Conf]) {
    for conf in confs {
        let criterion = conf.criterion.map(|s| s.iter().map(|s| s.to_string()).collect());
        let index = base_setup(conf.database_name, conf.dataset, criterion);

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
