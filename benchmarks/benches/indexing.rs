mod datasets_paths;
mod utils;

use std::fs::{create_dir_all, remove_dir_all};
use std::path::Path;

use criterion::{criterion_group, criterion_main, Criterion};
use heed::EnvOpenOptions;
use milli::update::{IndexDocuments, IndexDocumentsConfig, IndexerConfig, Settings};
use milli::Index;

#[cfg(target_os = "linux")]
#[global_allocator]
static ALLOC: jemallocator::Jemalloc = jemallocator::Jemalloc;

fn setup_dir(path: impl AsRef<Path>) {
    match remove_dir_all(path.as_ref()) {
        Ok(_) => (),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => (),
        Err(e) => panic!("{}", e),
    }
    create_dir_all(path).unwrap();
}

fn setup_index() -> Index {
    let path = "benches.mmdb";
    setup_dir(&path);
    let mut options = EnvOpenOptions::new();
    options.map_size(100 * 1024 * 1024 * 1024); // 100 GB
    options.max_readers(10);
    Index::new(options, path).unwrap()
}

fn indexing_songs_default(c: &mut Criterion) {
    let mut group = c.benchmark_group("indexing");
    group.sample_size(10);
    group.bench_function("Indexing songs with default settings", |b| {
        b.iter_with_setup(
            move || {
                let index = setup_index();

                let config = IndexerConfig::default();
                let mut wtxn = index.write_txn().unwrap();
                let mut builder = Settings::new(&mut wtxn, &index, &config);

                builder.set_primary_key("id".to_owned());
                let displayed_fields =
                    ["title", "album", "artist", "genre", "country", "released", "duration"]
                        .iter()
                        .map(|s| s.to_string())
                        .collect();
                builder.set_displayed_fields(displayed_fields);

                let searchable_fields =
                    ["title", "album", "artist"].iter().map(|s| s.to_string()).collect();
                builder.set_searchable_fields(searchable_fields);

                let faceted_fields =
                    ["released-timestamp", "duration-float", "genre", "country", "artist"]
                        .iter()
                        .map(|s| s.to_string())
                        .collect();
                builder.set_filterable_fields(faceted_fields);
                builder.execute(|_| ()).unwrap();
                wtxn.commit().unwrap();
                index
            },
            move |index| {
                let config = IndexerConfig::default();
                let indexing_config = IndexDocumentsConfig::default();
                let mut wtxn = index.write_txn().unwrap();
                let mut builder =
                    IndexDocuments::new(&mut wtxn, &index, &config, indexing_config, |_| ());

                let documents = utils::documents_from(datasets_paths::SMOL_SONGS, "csv");
                builder.add_documents(documents).unwrap();
                builder.execute().unwrap();
                wtxn.commit().unwrap();

                index.prepare_for_closing().wait();
            },
        )
    });
}

fn indexing_songs_in_three_batches_default(c: &mut Criterion) {
    let mut group = c.benchmark_group("indexing");
    group.sample_size(10);
    group.bench_function("Indexing songs in three batches with default settings", |b| {
        b.iter_with_setup(
            move || {
                let index = setup_index();

                let config = IndexerConfig::default();
                let mut wtxn = index.write_txn().unwrap();
                let mut builder = Settings::new(&mut wtxn, &index, &config);

                builder.set_primary_key("id".to_owned());
                let displayed_fields =
                    ["title", "album", "artist", "genre", "country", "released", "duration"]
                        .iter()
                        .map(|s| s.to_string())
                        .collect();
                builder.set_displayed_fields(displayed_fields);

                let searchable_fields =
                    ["title", "album", "artist"].iter().map(|s| s.to_string()).collect();
                builder.set_searchable_fields(searchable_fields);

                let faceted_fields =
                    ["released-timestamp", "duration-float", "genre", "country", "artist"]
                        .iter()
                        .map(|s| s.to_string())
                        .collect();
                builder.set_filterable_fields(faceted_fields);
                builder.execute(|_| ()).unwrap();

                // We index only one half of the dataset in the setup part
                // as we don't care about the time it take.
                let config = IndexerConfig::default();
                let indexing_config = IndexDocumentsConfig::default();
                let mut builder =
                    IndexDocuments::new(&mut wtxn, &index, &config, indexing_config, |_| ());
                let documents = utils::documents_from(datasets_paths::SMOL_SONGS_1_2, "csv");
                builder.add_documents(documents).unwrap();
                builder.execute().unwrap();

                wtxn.commit().unwrap();

                index
            },
            move |index| {
                let config = IndexerConfig::default();
                let indexing_config = IndexDocumentsConfig::default();
                let mut wtxn = index.write_txn().unwrap();
                let mut builder =
                    IndexDocuments::new(&mut wtxn, &index, &config, indexing_config, |_| ());
                let documents = utils::documents_from(datasets_paths::SMOL_SONGS_3_4, "csv");
                builder.add_documents(documents).unwrap();
                builder.execute().unwrap();

                let indexing_config = IndexDocumentsConfig::default();
                let mut builder =
                    IndexDocuments::new(&mut wtxn, &index, &config, indexing_config, |_| ());
                let documents = utils::documents_from(datasets_paths::SMOL_SONGS_4_4, "csv");
                builder.add_documents(documents).unwrap();
                builder.execute().unwrap();

                wtxn.commit().unwrap();

                index.prepare_for_closing().wait();
            },
        )
    });
}

fn indexing_songs_without_faceted_numbers(c: &mut Criterion) {
    let mut group = c.benchmark_group("indexing");
    group.sample_size(10);
    group.bench_function("Indexing songs without faceted numbers", |b| {
        b.iter_with_setup(
            move || {
                let index = setup_index();

                let config = IndexerConfig::default();
                let mut wtxn = index.write_txn().unwrap();
                let mut builder = Settings::new(&mut wtxn, &index, &config);

                builder.set_primary_key("id".to_owned());
                let displayed_fields =
                    ["title", "album", "artist", "genre", "country", "released", "duration"]
                        .iter()
                        .map(|s| s.to_string())
                        .collect();
                builder.set_displayed_fields(displayed_fields);

                let searchable_fields =
                    ["title", "album", "artist"].iter().map(|s| s.to_string()).collect();
                builder.set_searchable_fields(searchable_fields);

                let faceted_fields =
                    ["genre", "country", "artist"].iter().map(|s| s.to_string()).collect();
                builder.set_filterable_fields(faceted_fields);
                builder.execute(|_| ()).unwrap();
                wtxn.commit().unwrap();
                index
            },
            move |index| {
                let config = IndexerConfig::default();
                let indexing_config = IndexDocumentsConfig::default();
                let mut wtxn = index.write_txn().unwrap();
                let mut builder =
                    IndexDocuments::new(&mut wtxn, &index, &config, indexing_config, |_| ());

                let documents = utils::documents_from(datasets_paths::SMOL_SONGS, "csv");

                builder.add_documents(documents).unwrap();
                builder.execute().unwrap();
                wtxn.commit().unwrap();

                index.prepare_for_closing().wait();
            },
        )
    });
}

fn indexing_songs_without_faceted_fields(c: &mut Criterion) {
    let mut group = c.benchmark_group("indexing");
    group.sample_size(10);
    group.bench_function("Indexing songs without any facets", |b| {
        b.iter_with_setup(
            move || {
                let index = setup_index();

                let config = IndexerConfig::default();
                let mut wtxn = index.write_txn().unwrap();
                let mut builder = Settings::new(&mut wtxn, &index, &config);

                builder.set_primary_key("id".to_owned());
                let displayed_fields =
                    ["title", "album", "artist", "genre", "country", "released", "duration"]
                        .iter()
                        .map(|s| s.to_string())
                        .collect();
                builder.set_displayed_fields(displayed_fields);

                let searchable_fields =
                    ["title", "album", "artist"].iter().map(|s| s.to_string()).collect();
                builder.set_searchable_fields(searchable_fields);
                builder.execute(|_| ()).unwrap();
                wtxn.commit().unwrap();
                index
            },
            move |index| {
                let config = IndexerConfig::default();
                let indexing_config = IndexDocumentsConfig::default();
                let mut wtxn = index.write_txn().unwrap();
                let mut builder =
                    IndexDocuments::new(&mut wtxn, &index, &config, indexing_config, |_| ());

                let documents = utils::documents_from(datasets_paths::SMOL_SONGS, "csv");
                builder.add_documents(documents).unwrap();
                builder.execute().unwrap();
                wtxn.commit().unwrap();

                index.prepare_for_closing().wait();
            },
        )
    });
}

fn indexing_wiki(c: &mut Criterion) {
    let mut group = c.benchmark_group("indexing");
    group.sample_size(10);
    group.bench_function("Indexing wiki", |b| {
        b.iter_with_setup(
            move || {
                let index = setup_index();

                let config = IndexerConfig::default();
                let mut wtxn = index.write_txn().unwrap();
                let mut builder = Settings::new(&mut wtxn, &index, &config);

                builder.set_primary_key("id".to_owned());
                let displayed_fields =
                    ["title", "body", "url"].iter().map(|s| s.to_string()).collect();
                builder.set_displayed_fields(displayed_fields);

                let searchable_fields = ["title", "body"].iter().map(|s| s.to_string()).collect();
                builder.set_searchable_fields(searchable_fields);

                // there is NO faceted fields at all

                builder.execute(|_| ()).unwrap();
                wtxn.commit().unwrap();
                index
            },
            move |index| {
                let config = IndexerConfig::default();
                let indexing_config =
                    IndexDocumentsConfig { autogenerate_docids: true, ..Default::default() };
                let mut wtxn = index.write_txn().unwrap();
                let mut builder =
                    IndexDocuments::new(&mut wtxn, &index, &config, indexing_config, |_| ());

                let documents = utils::documents_from(datasets_paths::SMOL_WIKI_ARTICLES, "csv");
                builder.add_documents(documents).unwrap();
                builder.execute().unwrap();
                wtxn.commit().unwrap();

                index.prepare_for_closing().wait();
            },
        )
    });
}

fn indexing_movies_default(c: &mut Criterion) {
    let mut group = c.benchmark_group("indexing");
    group.sample_size(10);
    group.bench_function("Indexing movies with default settings", |b| {
        b.iter_with_setup(
            move || {
                let index = setup_index();

                let config = IndexerConfig::default();
                let mut wtxn = index.write_txn().unwrap();
                let mut builder = Settings::new(&mut wtxn, &index, &config);

                builder.set_primary_key("id".to_owned());
                let displayed_fields = ["title", "poster", "overview", "release_date", "genres"]
                    .iter()
                    .map(|s| s.to_string())
                    .collect();
                builder.set_displayed_fields(displayed_fields);

                let searchable_fields =
                    ["title", "overview"].iter().map(|s| s.to_string()).collect();
                builder.set_searchable_fields(searchable_fields);

                let faceted_fields =
                    ["released_date", "genres"].iter().map(|s| s.to_string()).collect();
                builder.set_filterable_fields(faceted_fields);

                builder.execute(|_| ()).unwrap();
                wtxn.commit().unwrap();
                index
            },
            move |index| {
                let config = IndexerConfig::default();
                let indexing_config = IndexDocumentsConfig::default();
                let mut wtxn = index.write_txn().unwrap();
                let mut builder =
                    IndexDocuments::new(&mut wtxn, &index, &config, indexing_config, |_| ());

                let documents = utils::documents_from(datasets_paths::MOVIES, "json");
                builder.add_documents(documents).unwrap();
                builder.execute().unwrap();
                wtxn.commit().unwrap();

                index.prepare_for_closing().wait();
            },
        )
    });
}

fn indexing_geo(c: &mut Criterion) {
    let mut group = c.benchmark_group("indexing");
    group.sample_size(10);
    group.bench_function("Indexing geo_point", |b| {
        b.iter_with_setup(
            move || {
                let index = setup_index();

                let config = IndexerConfig::default();
                let mut wtxn = index.write_txn().unwrap();
                let mut builder = Settings::new(&mut wtxn, &index, &config);

                builder.set_primary_key("geonameid".to_owned());
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

                builder.execute(|_| ()).unwrap();
                wtxn.commit().unwrap();
                index
            },
            move |index| {
                let config = IndexerConfig::default();
                let indexing_config = IndexDocumentsConfig::default();
                let mut wtxn = index.write_txn().unwrap();
                let mut builder =
                    IndexDocuments::new(&mut wtxn, &index, &config, indexing_config, |_| ());

                let documents = utils::documents_from(datasets_paths::SMOL_ALL_COUNTRIES, "jsonl");
                builder.add_documents(documents).unwrap();
                builder.execute().unwrap();

                wtxn.commit().unwrap();

                index.prepare_for_closing().wait();
            },
        )
    });
}

criterion_group!(
    benches,
    indexing_songs_default,
    indexing_songs_without_faceted_numbers,
    indexing_songs_without_faceted_fields,
    indexing_songs_in_three_batches_default,
    indexing_wiki,
    indexing_movies_default,
    indexing_geo
);
criterion_main!(benches);
