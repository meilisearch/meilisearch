use std::{fs::{File, create_dir_all}, time::Duration};

use heed::EnvOpenOptions;
use criterion::BenchmarkId;
use milli::{Index, update::{IndexDocumentsMethod, UpdateBuilder, UpdateFormat}};

pub struct Conf<'a> {
    pub group_name: &'a str,
    pub queries: &'a[&'a str],
    pub criterion: Option<&'a [&'a str]>,
    pub optional_words: bool,
}

pub fn base_setup(criterion: Option<Vec<String>>) -> Index {
    let database = "songs.mmdb";
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
    let reader = File::open("benches/smol_songs.csv").unwrap();
    builder.execute(reader, |_, _| ()).unwrap();
    wtxn.commit().unwrap();

    index
}

pub fn run_benches(c: &mut criterion::Criterion, confs: &[Conf]) {
    for conf in confs {
        let criterion = conf.criterion.map(|s| s.iter().map(|s| s.to_string()).collect());
        let index = base_setup(criterion);

        let mut group = c.benchmark_group(conf.group_name);
        group.measurement_time(Duration::from_secs(10));

        for &query in conf.queries {
            group.bench_with_input(BenchmarkId::from_parameter(query), &query, |b, &query| {
                b.iter(|| {
                    let rtxn = index.read_txn().unwrap();
                    let _documents_ids = index.search(&rtxn).query(query).optional_words(conf.optional_words).execute().unwrap();
                });
            });
        }
        group.finish();
    }
}
