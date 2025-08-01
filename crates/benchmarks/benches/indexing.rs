mod datasets_paths;
mod utils;

use std::fs::{create_dir_all, remove_dir_all};
use std::path::Path;

use bumpalo::Bump;
use criterion::{criterion_group, criterion_main, Criterion};
use milli::documents::PrimaryKey;
use milli::heed::{EnvOpenOptions, RwTxn};
use milli::progress::Progress;
use milli::update::new::indexer;
use milli::update::{IndexerConfig, Settings};
use milli::vector::RuntimeEmbedders;
use milli::{FilterableAttributesRule, Index};
use rand::seq::SliceRandom;
use rand_chacha::rand_core::SeedableRng;
use roaring::RoaringBitmap;

#[cfg(not(windows))]
#[global_allocator]
static ALLOC: mimalloc::MiMalloc = mimalloc::MiMalloc;

const BENCHMARK_ITERATION: usize = 10;

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
    setup_dir(path);
    let options = EnvOpenOptions::new();
    let mut options = options.read_txn_without_tls();
    options.map_size(100 * 1024 * 1024 * 1024); // 100 GB
    options.max_readers(100);
    Index::new(options, path, true).unwrap()
}

fn setup_settings<'t>(
    wtxn: &mut RwTxn<'t>,
    index: &'t Index,
    primary_key: &str,
    searchable_fields: &[&str],
    filterable_fields: &[&str],
    sortable_fields: &[&str],
) {
    let config = IndexerConfig::default();
    let mut builder = Settings::new(wtxn, index, &config);

    builder.set_primary_key(primary_key.to_owned());

    let searchable_fields = searchable_fields.iter().map(|s| s.to_string()).collect();
    builder.set_searchable_fields(searchable_fields);

    let filterable_fields =
        filterable_fields.iter().map(|s| FilterableAttributesRule::Field(s.to_string())).collect();
    builder.set_filterable_fields(filterable_fields);

    let sortable_fields = sortable_fields.iter().map(|s| s.to_string()).collect();
    builder.set_sortable_fields(sortable_fields);

    builder.execute(&|| false, &Progress::default(), Default::default()).unwrap();
}

fn setup_index_with_settings(
    primary_key: &str,
    searchable_fields: &[&str],
    filterable_fields: &[&str],
    sortable_fields: &[&str],
) -> milli::Index {
    let index = setup_index();
    let mut wtxn = index.write_txn().unwrap();
    setup_settings(
        &mut wtxn,
        &index,
        primary_key,
        searchable_fields,
        filterable_fields,
        sortable_fields,
    );
    wtxn.commit().unwrap();

    index
}

fn choose_document_ids_from_index_batched(
    index: &Index,
    count: usize,
    batch_size: usize,
) -> Vec<RoaringBitmap> {
    let rtxn = index.read_txn().unwrap();
    // create batch of document ids to delete
    let mut rng = rand_chacha::ChaCha8Rng::seed_from_u64(7700);
    let document_ids: Vec<_> = index.documents_ids(&rtxn).unwrap().into_iter().collect();
    let document_ids_to_delete: Vec<_> =
        document_ids.choose_multiple(&mut rng, count).map(Clone::clone).collect();

    document_ids_to_delete
        .chunks(batch_size)
        .map(|c| {
            let mut batch = RoaringBitmap::new();
            for id in c {
                batch.insert(*id);
            }

            batch
        })
        .collect()
}

fn indexing_songs_default(c: &mut Criterion) {
    let mut group = c.benchmark_group("indexing");
    group.sample_size(BENCHMARK_ITERATION);
    group.bench_function("Indexing songs with default settings", |b| {
        b.iter_with_setup(
            move || {
                let primary_key = "id";
                let searchable_fields = ["title", "album", "artist"];
                let filterable_fields =
                    ["released-timestamp", "duration-float", "genre", "country", "artist"];
                let sortable_fields = [];

                setup_index_with_settings(
                    primary_key,
                    &searchable_fields,
                    &filterable_fields,
                    &sortable_fields,
                )
            },
            move |index| {
                let config = IndexerConfig::default();
                let mut wtxn = index.write_txn().unwrap();
                let rtxn = index.read_txn().unwrap();
                let db_fields_ids_map = index.fields_ids_map(&rtxn).unwrap();
                let mut new_fields_ids_map = db_fields_ids_map.clone();

                let mut indexer = indexer::DocumentOperation::new();
                let documents = utils::documents_from(datasets_paths::SMOL_SONGS, "csv");
                indexer.replace_documents(&documents).unwrap();

                let indexer_alloc = Bump::new();
                let (document_changes, _operation_stats, primary_key) = indexer
                    .into_changes(
                        &indexer_alloc,
                        &index,
                        &rtxn,
                        None,
                        &mut new_fields_ids_map,
                        &|| false,
                        Progress::default(),
                    )
                    .unwrap();

                indexer::index(
                    &mut wtxn,
                    &index,
                    &milli::ThreadPoolNoAbortBuilder::new().build().unwrap(),
                    config.grenad_parameters(),
                    &db_fields_ids_map,
                    new_fields_ids_map,
                    primary_key,
                    &document_changes,
                    RuntimeEmbedders::default(),
                    &|| false,
                    &Progress::default(),
                    &Default::default(),
                )
                .unwrap();

                wtxn.commit().unwrap();
                drop(rtxn);

                index.prepare_for_closing().wait();
            },
        )
    });
}

fn reindexing_songs_default(c: &mut Criterion) {
    let mut group = c.benchmark_group("indexing");
    group.sample_size(BENCHMARK_ITERATION);
    group.bench_function("Reindexing songs with default settings", |b| {
        b.iter_with_setup(
            move || {
                let primary_key = "id";
                let searchable_fields = ["title", "album", "artist"];
                let filterable_fields =
                    ["released-timestamp", "duration-float", "genre", "country", "artist"];
                let sortable_fields = [];

                let index = setup_index_with_settings(
                    primary_key,
                    &searchable_fields,
                    &filterable_fields,
                    &sortable_fields,
                );

                let config = IndexerConfig::default();
                let mut wtxn = index.write_txn().unwrap();
                let rtxn = index.read_txn().unwrap();
                let db_fields_ids_map = index.fields_ids_map(&rtxn).unwrap();
                let mut new_fields_ids_map = db_fields_ids_map.clone();

                let mut indexer = indexer::DocumentOperation::new();
                let documents = utils::documents_from(datasets_paths::SMOL_SONGS, "csv");
                indexer.replace_documents(&documents).unwrap();

                let indexer_alloc = Bump::new();
                let (document_changes, _operation_stats, primary_key) = indexer
                    .into_changes(
                        &indexer_alloc,
                        &index,
                        &rtxn,
                        None,
                        &mut new_fields_ids_map,
                        &|| false,
                        Progress::default(),
                    )
                    .unwrap();

                indexer::index(
                    &mut wtxn,
                    &index,
                    &milli::ThreadPoolNoAbortBuilder::new().build().unwrap(),
                    config.grenad_parameters(),
                    &db_fields_ids_map,
                    new_fields_ids_map,
                    primary_key,
                    &document_changes,
                    RuntimeEmbedders::default(),
                    &|| false,
                    &Progress::default(),
                    &Default::default(),
                )
                .unwrap();

                wtxn.commit().unwrap();
                drop(rtxn);

                index
            },
            move |index| {
                let config = IndexerConfig::default();
                let mut wtxn = index.write_txn().unwrap();
                let rtxn = index.read_txn().unwrap();
                let db_fields_ids_map = index.fields_ids_map(&rtxn).unwrap();
                let mut new_fields_ids_map = db_fields_ids_map.clone();

                let mut indexer = indexer::DocumentOperation::new();
                let documents = utils::documents_from(datasets_paths::SMOL_SONGS, "csv");
                indexer.replace_documents(&documents).unwrap();

                let indexer_alloc = Bump::new();
                let (document_changes, _operation_stats, primary_key) = indexer
                    .into_changes(
                        &indexer_alloc,
                        &index,
                        &rtxn,
                        None,
                        &mut new_fields_ids_map,
                        &|| false,
                        Progress::default(),
                    )
                    .unwrap();

                indexer::index(
                    &mut wtxn,
                    &index,
                    &milli::ThreadPoolNoAbortBuilder::new().build().unwrap(),
                    config.grenad_parameters(),
                    &db_fields_ids_map,
                    new_fields_ids_map,
                    primary_key,
                    &document_changes,
                    RuntimeEmbedders::default(),
                    &|| false,
                    &Progress::default(),
                    &Default::default(),
                )
                .unwrap();

                wtxn.commit().unwrap();
                drop(rtxn);

                index.prepare_for_closing().wait();
            },
        )
    });
}

fn deleting_songs_in_batches_default(c: &mut Criterion) {
    let mut group = c.benchmark_group("indexing");
    group.sample_size(BENCHMARK_ITERATION);
    group.bench_function("-songs-delete-facetedString-facetedNumber-searchable-", |b| {
        b.iter_with_setup(
            move || {
                let primary_key = "id";
                let searchable_fields = ["title", "album", "artist"];
                let filterable_fields =
                    ["released-timestamp", "duration-float", "genre", "country", "artist"];
                let sortable_fields = [];

                let index = setup_index_with_settings(
                    primary_key,
                    &searchable_fields,
                    &filterable_fields,
                    &sortable_fields,
                );

                // We index only one half of the dataset in the setup part
                // as we don't care about the time it takes.
                let config = IndexerConfig::default();
                let mut wtxn = index.write_txn().unwrap();
                let rtxn = index.read_txn().unwrap();
                let db_fields_ids_map = index.fields_ids_map(&rtxn).unwrap();
                let mut new_fields_ids_map = db_fields_ids_map.clone();

                let mut indexer = indexer::DocumentOperation::new();
                let documents = utils::documents_from(datasets_paths::SMOL_SONGS, "csv");
                indexer.replace_documents(&documents).unwrap();

                let indexer_alloc = Bump::new();
                let (document_changes, _operation_stats, primary_key) = indexer
                    .into_changes(
                        &indexer_alloc,
                        &index,
                        &rtxn,
                        None,
                        &mut new_fields_ids_map,
                        &|| false,
                        Progress::default(),
                    )
                    .unwrap();

                indexer::index(
                    &mut wtxn,
                    &index,
                    &milli::ThreadPoolNoAbortBuilder::new().build().unwrap(),
                    config.grenad_parameters(),
                    &db_fields_ids_map,
                    new_fields_ids_map,
                    primary_key,
                    &document_changes,
                    RuntimeEmbedders::default(),
                    &|| false,
                    &Progress::default(),
                    &Default::default(),
                )
                .unwrap();

                wtxn.commit().unwrap();
                drop(rtxn);

                let count = 1250;
                let batch_size = 250;
                let document_ids_to_delete =
                    choose_document_ids_from_index_batched(&index, count, batch_size);

                (index, document_ids_to_delete)
            },
            move |(index, document_ids_to_delete)| {
                delete_documents_from_ids(index, document_ids_to_delete)
            },
        )
    });
}

fn indexing_songs_in_three_batches_default(c: &mut Criterion) {
    let mut group = c.benchmark_group("indexing");
    group.sample_size(BENCHMARK_ITERATION);
    group.bench_function("Indexing songs in three batches with default settings", |b| {
        b.iter_with_setup(
            move || {
                let primary_key = "id";
                let searchable_fields = ["title", "album", "artist"];
                let filterable_fields =
                    ["released-timestamp", "duration-float", "genre", "country", "artist"];
                let sortable_fields = [];

                let index = setup_index_with_settings(
                    primary_key,
                    &searchable_fields,
                    &filterable_fields,
                    &sortable_fields,
                );

                // We index only one half of the dataset in the setup part
                // as we don't care about the time it takes.
                let config = IndexerConfig::default();
                let mut wtxn = index.write_txn().unwrap();
                let rtxn = index.read_txn().unwrap();
                let db_fields_ids_map = index.fields_ids_map(&rtxn).unwrap();
                let mut new_fields_ids_map = db_fields_ids_map.clone();

                let mut indexer = indexer::DocumentOperation::new();
                let documents = utils::documents_from(datasets_paths::SMOL_SONGS_1_2, "csv");
                indexer.replace_documents(&documents).unwrap();

                let indexer_alloc = Bump::new();
                let (document_changes, _operation_stats, primary_key) = indexer
                    .into_changes(
                        &indexer_alloc,
                        &index,
                        &rtxn,
                        None,
                        &mut new_fields_ids_map,
                        &|| false,
                        Progress::default(),
                    )
                    .unwrap();

                indexer::index(
                    &mut wtxn,
                    &index,
                    &milli::ThreadPoolNoAbortBuilder::new().build().unwrap(),
                    config.grenad_parameters(),
                    &db_fields_ids_map,
                    new_fields_ids_map,
                    primary_key,
                    &document_changes,
                    RuntimeEmbedders::default(),
                    &|| false,
                    &Progress::default(),
                    &Default::default(),
                )
                .unwrap();

                wtxn.commit().unwrap();
                drop(rtxn);

                index
            },
            move |index| {
                let config = IndexerConfig::default();
                let mut wtxn = index.write_txn().unwrap();
                let rtxn = index.read_txn().unwrap();
                let db_fields_ids_map = index.fields_ids_map(&rtxn).unwrap();
                let mut new_fields_ids_map = db_fields_ids_map.clone();

                let mut indexer = indexer::DocumentOperation::new();
                let documents = utils::documents_from(datasets_paths::SMOL_SONGS_3_4, "csv");
                indexer.replace_documents(&documents).unwrap();

                let indexer_alloc = Bump::new();
                let (document_changes, _operation_stats, primary_key) = indexer
                    .into_changes(
                        &indexer_alloc,
                        &index,
                        &rtxn,
                        None,
                        &mut new_fields_ids_map,
                        &|| false,
                        Progress::default(),
                    )
                    .unwrap();

                indexer::index(
                    &mut wtxn,
                    &index,
                    &milli::ThreadPoolNoAbortBuilder::new().build().unwrap(),
                    config.grenad_parameters(),
                    &db_fields_ids_map,
                    new_fields_ids_map,
                    primary_key,
                    &document_changes,
                    RuntimeEmbedders::default(),
                    &|| false,
                    &Progress::default(),
                    &Default::default(),
                )
                .unwrap();

                wtxn.commit().unwrap();
                drop(rtxn);

                let mut wtxn = index.write_txn().unwrap();
                let rtxn = index.read_txn().unwrap();
                let db_fields_ids_map = index.fields_ids_map(&rtxn).unwrap();
                let mut new_fields_ids_map = db_fields_ids_map.clone();

                let mut indexer = indexer::DocumentOperation::new();
                let documents = utils::documents_from(datasets_paths::SMOL_SONGS_4_4, "csv");
                indexer.replace_documents(&documents).unwrap();

                let indexer_alloc = Bump::new();
                let (document_changes, _operation_stats, primary_key) = indexer
                    .into_changes(
                        &indexer_alloc,
                        &index,
                        &rtxn,
                        None,
                        &mut new_fields_ids_map,
                        &|| false,
                        Progress::default(),
                    )
                    .unwrap();

                indexer::index(
                    &mut wtxn,
                    &index,
                    &milli::ThreadPoolNoAbortBuilder::new().build().unwrap(),
                    config.grenad_parameters(),
                    &db_fields_ids_map,
                    new_fields_ids_map,
                    primary_key,
                    &document_changes,
                    RuntimeEmbedders::default(),
                    &|| false,
                    &Progress::default(),
                    &Default::default(),
                )
                .unwrap();

                wtxn.commit().unwrap();
                drop(rtxn);

                index.prepare_for_closing().wait();
            },
        )
    });
}

fn indexing_songs_without_faceted_numbers(c: &mut Criterion) {
    let mut group = c.benchmark_group("indexing");
    group.sample_size(BENCHMARK_ITERATION);
    group.bench_function("Indexing songs without faceted numbers", |b| {
        b.iter_with_setup(
            move || {
                let primary_key = "id";
                let searchable_fields = ["title", "album", "artist"];
                let filterable_fields = ["genre", "country", "artist"];
                let sortable_fields = [];

                setup_index_with_settings(
                    primary_key,
                    &searchable_fields,
                    &filterable_fields,
                    &sortable_fields,
                )
            },
            move |index| {
                let config = IndexerConfig::default();
                let mut wtxn = index.write_txn().unwrap();
                let rtxn = index.read_txn().unwrap();
                let db_fields_ids_map = index.fields_ids_map(&rtxn).unwrap();
                let mut new_fields_ids_map = db_fields_ids_map.clone();

                let mut indexer = indexer::DocumentOperation::new();
                let documents = utils::documents_from(datasets_paths::SMOL_SONGS, "csv");

                indexer.replace_documents(&documents).unwrap();

                let indexer_alloc = Bump::new();
                let (document_changes, _operation_stats, primary_key) = indexer
                    .into_changes(
                        &indexer_alloc,
                        &index,
                        &rtxn,
                        None,
                        &mut new_fields_ids_map,
                        &|| false,
                        Progress::default(),
                    )
                    .unwrap();

                indexer::index(
                    &mut wtxn,
                    &index,
                    &milli::ThreadPoolNoAbortBuilder::new().build().unwrap(),
                    config.grenad_parameters(),
                    &db_fields_ids_map,
                    new_fields_ids_map,
                    primary_key,
                    &document_changes,
                    RuntimeEmbedders::default(),
                    &|| false,
                    &Progress::default(),
                    &Default::default(),
                )
                .unwrap();

                wtxn.commit().unwrap();
                drop(rtxn);

                index.prepare_for_closing().wait();
            },
        )
    });
}

fn indexing_songs_without_faceted_fields(c: &mut Criterion) {
    let mut group = c.benchmark_group("indexing");
    group.sample_size(BENCHMARK_ITERATION);
    group.bench_function("Indexing songs without any facets", |b| {
        b.iter_with_setup(
            move || {
                let primary_key = "id";
                let searchable_fields = ["title", "album", "artist"];
                let filterable_fields = [];
                let sortable_fields = [];

                setup_index_with_settings(
                    primary_key,
                    &searchable_fields,
                    &filterable_fields,
                    &sortable_fields,
                )
            },
            move |index| {
                let config = IndexerConfig::default();
                let mut wtxn = index.write_txn().unwrap();
                let rtxn = index.read_txn().unwrap();
                let db_fields_ids_map = index.fields_ids_map(&rtxn).unwrap();
                let mut new_fields_ids_map = db_fields_ids_map.clone();

                let mut indexer = indexer::DocumentOperation::new();
                let documents = utils::documents_from(datasets_paths::SMOL_SONGS, "csv");
                indexer.replace_documents(&documents).unwrap();

                let indexer_alloc = Bump::new();
                let (document_changes, _operation_stats, primary_key) = indexer
                    .into_changes(
                        &indexer_alloc,
                        &index,
                        &rtxn,
                        None,
                        &mut new_fields_ids_map,
                        &|| false,
                        Progress::default(),
                    )
                    .unwrap();

                indexer::index(
                    &mut wtxn,
                    &index,
                    &milli::ThreadPoolNoAbortBuilder::new().build().unwrap(),
                    config.grenad_parameters(),
                    &db_fields_ids_map,
                    new_fields_ids_map,
                    primary_key,
                    &document_changes,
                    RuntimeEmbedders::default(),
                    &|| false,
                    &Progress::default(),
                    &Default::default(),
                )
                .unwrap();

                wtxn.commit().unwrap();
                drop(rtxn);

                index.prepare_for_closing().wait();
            },
        )
    });
}

fn indexing_wiki(c: &mut Criterion) {
    let mut group = c.benchmark_group("indexing");
    group.sample_size(BENCHMARK_ITERATION);
    group.bench_function("Indexing wiki", |b| {
        b.iter_with_setup(
            move || {
                let primary_key = "id";
                let searchable_fields = ["title", "body"];
                let filterable_fields = [];
                let sortable_fields = [];

                setup_index_with_settings(
                    primary_key,
                    &searchable_fields,
                    &filterable_fields,
                    &sortable_fields,
                )
            },
            move |index| {
                let config = IndexerConfig::default();
                let mut wtxn = index.write_txn().unwrap();
                let rtxn = index.read_txn().unwrap();
                let db_fields_ids_map = index.fields_ids_map(&rtxn).unwrap();
                let mut new_fields_ids_map = db_fields_ids_map.clone();

                let mut indexer = indexer::DocumentOperation::new();
                let documents = utils::documents_from(datasets_paths::SMOL_WIKI_ARTICLES, "csv");
                indexer.replace_documents(&documents).unwrap();

                let indexer_alloc = Bump::new();
                let (document_changes, _operation_stats, primary_key) = indexer
                    .into_changes(
                        &indexer_alloc,
                        &index,
                        &rtxn,
                        None,
                        &mut new_fields_ids_map,
                        &|| false,
                        Progress::default(),
                    )
                    .unwrap();

                indexer::index(
                    &mut wtxn,
                    &index,
                    &milli::ThreadPoolNoAbortBuilder::new().build().unwrap(),
                    config.grenad_parameters(),
                    &db_fields_ids_map,
                    new_fields_ids_map,
                    primary_key,
                    &document_changes,
                    RuntimeEmbedders::default(),
                    &|| false,
                    &Progress::default(),
                    &Default::default(),
                )
                .unwrap();

                wtxn.commit().unwrap();
                drop(rtxn);

                index.prepare_for_closing().wait();
            },
        )
    });
}

fn reindexing_wiki(c: &mut Criterion) {
    let mut group = c.benchmark_group("indexing");
    group.sample_size(BENCHMARK_ITERATION);
    group.bench_function("Reindexing wiki", |b| {
        b.iter_with_setup(
            move || {
                let primary_key = "id";
                let searchable_fields = ["title", "body"];
                let filterable_fields = [];
                let sortable_fields = [];

                let index = setup_index_with_settings(
                    primary_key,
                    &searchable_fields,
                    &filterable_fields,
                    &sortable_fields,
                );

                let config = IndexerConfig::default();
                let mut wtxn = index.write_txn().unwrap();
                let rtxn = index.read_txn().unwrap();
                let db_fields_ids_map = index.fields_ids_map(&rtxn).unwrap();
                let mut new_fields_ids_map = db_fields_ids_map.clone();

                let mut indexer = indexer::DocumentOperation::new();
                let documents = utils::documents_from(datasets_paths::SMOL_WIKI_ARTICLES, "csv");
                indexer.replace_documents(&documents).unwrap();

                let indexer_alloc = Bump::new();
                let (document_changes, _operation_stats, primary_key) = indexer
                    .into_changes(
                        &indexer_alloc,
                        &index,
                        &rtxn,
                        None,
                        &mut new_fields_ids_map,
                        &|| false,
                        Progress::default(),
                    )
                    .unwrap();

                indexer::index(
                    &mut wtxn,
                    &index,
                    &milli::ThreadPoolNoAbortBuilder::new().build().unwrap(),
                    config.grenad_parameters(),
                    &db_fields_ids_map,
                    new_fields_ids_map,
                    primary_key,
                    &document_changes,
                    RuntimeEmbedders::default(),
                    &|| false,
                    &Progress::default(),
                    &Default::default(),
                )
                .unwrap();

                wtxn.commit().unwrap();
                drop(rtxn);

                index
            },
            move |index| {
                let config = IndexerConfig::default();
                let mut wtxn = index.write_txn().unwrap();
                let rtxn = index.read_txn().unwrap();
                let db_fields_ids_map = index.fields_ids_map(&rtxn).unwrap();
                let mut new_fields_ids_map = db_fields_ids_map.clone();

                let mut indexer = indexer::DocumentOperation::new();
                let documents = utils::documents_from(datasets_paths::SMOL_WIKI_ARTICLES, "csv");
                indexer.replace_documents(&documents).unwrap();

                let indexer_alloc = Bump::new();
                let (document_changes, _operation_stats, primary_key) = indexer
                    .into_changes(
                        &indexer_alloc,
                        &index,
                        &rtxn,
                        None,
                        &mut new_fields_ids_map,
                        &|| false,
                        Progress::default(),
                    )
                    .unwrap();

                indexer::index(
                    &mut wtxn,
                    &index,
                    &milli::ThreadPoolNoAbortBuilder::new().build().unwrap(),
                    config.grenad_parameters(),
                    &db_fields_ids_map,
                    new_fields_ids_map,
                    primary_key,
                    &document_changes,
                    RuntimeEmbedders::default(),
                    &|| false,
                    &Progress::default(),
                    &Default::default(),
                )
                .unwrap();

                wtxn.commit().unwrap();
                drop(rtxn);

                index.prepare_for_closing().wait();
            },
        )
    });
}

fn deleting_wiki_in_batches_default(c: &mut Criterion) {
    let mut group = c.benchmark_group("indexing");
    group.sample_size(BENCHMARK_ITERATION);
    group.bench_function("-wiki-delete-searchable-", |b| {
        b.iter_with_setup(
            move || {
                let primary_key = "id";
                let searchable_fields = ["title", "body"];
                let filterable_fields = [];
                let sortable_fields = [];

                let index = setup_index_with_settings(
                    primary_key,
                    &searchable_fields,
                    &filterable_fields,
                    &sortable_fields,
                );

                // We index only one half of the dataset in the setup part
                // as we don't care about the time it takes.
                let config = IndexerConfig::default();
                let mut wtxn = index.write_txn().unwrap();
                let rtxn = index.read_txn().unwrap();
                let db_fields_ids_map = index.fields_ids_map(&rtxn).unwrap();
                let mut new_fields_ids_map = db_fields_ids_map.clone();

                let mut indexer = indexer::DocumentOperation::new();
                let documents = utils::documents_from(datasets_paths::SMOL_WIKI_ARTICLES, "csv");
                indexer.replace_documents(&documents).unwrap();

                let indexer_alloc = Bump::new();
                let (document_changes, _operation_stats, primary_key) = indexer
                    .into_changes(
                        &indexer_alloc,
                        &index,
                        &rtxn,
                        None,
                        &mut new_fields_ids_map,
                        &|| false,
                        Progress::default(),
                    )
                    .unwrap();

                indexer::index(
                    &mut wtxn,
                    &index,
                    &milli::ThreadPoolNoAbortBuilder::new().build().unwrap(),
                    config.grenad_parameters(),
                    &db_fields_ids_map,
                    new_fields_ids_map,
                    primary_key,
                    &document_changes,
                    RuntimeEmbedders::default(),
                    &|| false,
                    &Progress::default(),
                    &Default::default(),
                )
                .unwrap();

                wtxn.commit().unwrap();
                drop(rtxn);

                let count = 1250;
                let batch_size = 250;
                let document_ids_to_delete =
                    choose_document_ids_from_index_batched(&index, count, batch_size);

                (index, document_ids_to_delete)
            },
            move |(index, document_ids_to_delete)| {
                delete_documents_from_ids(index, document_ids_to_delete)
            },
        )
    });
}

fn indexing_wiki_in_three_batches(c: &mut Criterion) {
    let mut group = c.benchmark_group("indexing");
    group.sample_size(BENCHMARK_ITERATION);
    group.bench_function("Indexing wiki in three batches", |b| {
        b.iter_with_setup(
            move || {
                let primary_key = "id";
                let searchable_fields = ["title", "body"];
                let filterable_fields = [];
                let sortable_fields = [];

                let index = setup_index_with_settings(
                    primary_key,
                    &searchable_fields,
                    &filterable_fields,
                    &sortable_fields,
                );

                // We index only one half of the dataset in the setup part
                // as we don't care about the time it takes.
                let config = IndexerConfig::default();
                let mut wtxn = index.write_txn().unwrap();
                let rtxn = index.read_txn().unwrap();
                let db_fields_ids_map = index.fields_ids_map(&rtxn).unwrap();
                let mut new_fields_ids_map = db_fields_ids_map.clone();

                let mut indexer = indexer::DocumentOperation::new();
                let documents =
                    utils::documents_from(datasets_paths::SMOL_WIKI_ARTICLES_1_2, "csv");
                indexer.replace_documents(&documents).unwrap();

                let indexer_alloc = Bump::new();
                let (document_changes, _operation_stats, primary_key) = indexer
                    .into_changes(
                        &indexer_alloc,
                        &index,
                        &rtxn,
                        None,
                        &mut new_fields_ids_map,
                        &|| false,
                        Progress::default(),
                    )
                    .unwrap();

                indexer::index(
                    &mut wtxn,
                    &index,
                    &milli::ThreadPoolNoAbortBuilder::new().build().unwrap(),
                    config.grenad_parameters(),
                    &db_fields_ids_map,
                    new_fields_ids_map,
                    primary_key,
                    &document_changes,
                    RuntimeEmbedders::default(),
                    &|| false,
                    &Progress::default(),
                    &Default::default(),
                )
                .unwrap();

                wtxn.commit().unwrap();
                drop(rtxn);

                index
            },
            move |index| {
                let config = IndexerConfig::default();
                let mut wtxn = index.write_txn().unwrap();
                let rtxn = index.read_txn().unwrap();
                let db_fields_ids_map = index.fields_ids_map(&rtxn).unwrap();
                let mut new_fields_ids_map = db_fields_ids_map.clone();

                let mut indexer = indexer::DocumentOperation::new();
                let documents =
                    utils::documents_from(datasets_paths::SMOL_WIKI_ARTICLES_3_4, "csv");
                indexer.replace_documents(&documents).unwrap();

                let indexer_alloc = Bump::new();
                let (document_changes, _operation_stats, primary_key) = indexer
                    .into_changes(
                        &indexer_alloc,
                        &index,
                        &rtxn,
                        None,
                        &mut new_fields_ids_map,
                        &|| false,
                        Progress::default(),
                    )
                    .unwrap();

                indexer::index(
                    &mut wtxn,
                    &index,
                    &milli::ThreadPoolNoAbortBuilder::new().build().unwrap(),
                    config.grenad_parameters(),
                    &db_fields_ids_map,
                    new_fields_ids_map,
                    primary_key,
                    &document_changes,
                    RuntimeEmbedders::default(),
                    &|| false,
                    &Progress::default(),
                    &Default::default(),
                )
                .unwrap();

                wtxn.commit().unwrap();
                drop(rtxn);

                let mut wtxn = index.write_txn().unwrap();
                let rtxn = index.read_txn().unwrap();
                let db_fields_ids_map = index.fields_ids_map(&rtxn).unwrap();
                let mut new_fields_ids_map = db_fields_ids_map.clone();

                let mut indexer = indexer::DocumentOperation::new();
                let documents =
                    utils::documents_from(datasets_paths::SMOL_WIKI_ARTICLES_4_4, "csv");
                indexer.replace_documents(&documents).unwrap();

                let indexer_alloc = Bump::new();
                let (document_changes, _operation_stats, primary_key) = indexer
                    .into_changes(
                        &indexer_alloc,
                        &index,
                        &rtxn,
                        None,
                        &mut new_fields_ids_map,
                        &|| false,
                        Progress::default(),
                    )
                    .unwrap();

                indexer::index(
                    &mut wtxn,
                    &index,
                    &milli::ThreadPoolNoAbortBuilder::new().build().unwrap(),
                    config.grenad_parameters(),
                    &db_fields_ids_map,
                    new_fields_ids_map,
                    primary_key,
                    &document_changes,
                    RuntimeEmbedders::default(),
                    &|| false,
                    &Progress::default(),
                    &Default::default(),
                )
                .unwrap();

                wtxn.commit().unwrap();
                drop(rtxn);

                index.prepare_for_closing().wait();
            },
        )
    });
}

fn indexing_movies_default(c: &mut Criterion) {
    let mut group = c.benchmark_group("indexing");
    group.sample_size(BENCHMARK_ITERATION);
    group.bench_function("Indexing movies with default settings", |b| {
        b.iter_with_setup(
            move || {
                let primary_key = "id";
                let searchable_fields = ["title", "overview"];
                let filterable_fields = ["release_date", "genres"];
                let sortable_fields = [];

                setup_index_with_settings(
                    primary_key,
                    &searchable_fields,
                    &filterable_fields,
                    &sortable_fields,
                )
            },
            move |index| {
                let config = IndexerConfig::default();
                let mut wtxn = index.write_txn().unwrap();
                let rtxn = index.read_txn().unwrap();
                let db_fields_ids_map = index.fields_ids_map(&rtxn).unwrap();
                let mut new_fields_ids_map = db_fields_ids_map.clone();

                let mut indexer = indexer::DocumentOperation::new();
                let documents = utils::documents_from(datasets_paths::MOVIES, "json");
                indexer.replace_documents(&documents).unwrap();

                let indexer_alloc = Bump::new();
                let (document_changes, _operation_stats, primary_key) = indexer
                    .into_changes(
                        &indexer_alloc,
                        &index,
                        &rtxn,
                        None,
                        &mut new_fields_ids_map,
                        &|| false,
                        Progress::default(),
                    )
                    .unwrap();

                indexer::index(
                    &mut wtxn,
                    &index,
                    &milli::ThreadPoolNoAbortBuilder::new().build().unwrap(),
                    config.grenad_parameters(),
                    &db_fields_ids_map,
                    new_fields_ids_map,
                    primary_key,
                    &document_changes,
                    RuntimeEmbedders::default(),
                    &|| false,
                    &Progress::default(),
                    &Default::default(),
                )
                .unwrap();

                wtxn.commit().unwrap();
                drop(rtxn);

                index.prepare_for_closing().wait();
            },
        )
    });
}

fn reindexing_movies_default(c: &mut Criterion) {
    let mut group = c.benchmark_group("indexing");
    group.sample_size(BENCHMARK_ITERATION);
    group.bench_function("Reindexing movies with default settings", |b| {
        b.iter_with_setup(
            move || {
                let primary_key = "id";
                let searchable_fields = ["title", "overview"];
                let filterable_fields = ["release_date", "genres"];
                let sortable_fields = [];

                let index = setup_index_with_settings(
                    primary_key,
                    &searchable_fields,
                    &filterable_fields,
                    &sortable_fields,
                );

                let config = IndexerConfig::default();
                let mut wtxn = index.write_txn().unwrap();
                let rtxn = index.read_txn().unwrap();
                let db_fields_ids_map = index.fields_ids_map(&rtxn).unwrap();
                let mut new_fields_ids_map = db_fields_ids_map.clone();

                let mut indexer = indexer::DocumentOperation::new();
                let documents = utils::documents_from(datasets_paths::MOVIES, "json");
                indexer.replace_documents(&documents).unwrap();

                let indexer_alloc = Bump::new();
                let (document_changes, _operation_stats, primary_key) = indexer
                    .into_changes(
                        &indexer_alloc,
                        &index,
                        &rtxn,
                        None,
                        &mut new_fields_ids_map,
                        &|| false,
                        Progress::default(),
                    )
                    .unwrap();

                indexer::index(
                    &mut wtxn,
                    &index,
                    &milli::ThreadPoolNoAbortBuilder::new().build().unwrap(),
                    config.grenad_parameters(),
                    &db_fields_ids_map,
                    new_fields_ids_map,
                    primary_key,
                    &document_changes,
                    RuntimeEmbedders::default(),
                    &|| false,
                    &Progress::default(),
                    &Default::default(),
                )
                .unwrap();

                wtxn.commit().unwrap();
                drop(rtxn);

                index
            },
            move |index| {
                let config = IndexerConfig::default();
                let mut wtxn = index.write_txn().unwrap();
                let rtxn = index.read_txn().unwrap();
                let db_fields_ids_map = index.fields_ids_map(&rtxn).unwrap();
                let mut new_fields_ids_map = db_fields_ids_map.clone();

                let mut indexer = indexer::DocumentOperation::new();
                let documents = utils::documents_from(datasets_paths::MOVIES, "json");
                indexer.replace_documents(&documents).unwrap();

                let indexer_alloc = Bump::new();
                let (document_changes, _operation_stats, primary_key) = indexer
                    .into_changes(
                        &indexer_alloc,
                        &index,
                        &rtxn,
                        None,
                        &mut new_fields_ids_map,
                        &|| false,
                        Progress::default(),
                    )
                    .unwrap();

                indexer::index(
                    &mut wtxn,
                    &index,
                    &milli::ThreadPoolNoAbortBuilder::new().build().unwrap(),
                    config.grenad_parameters(),
                    &db_fields_ids_map,
                    new_fields_ids_map,
                    primary_key,
                    &document_changes,
                    RuntimeEmbedders::default(),
                    &|| false,
                    &Progress::default(),
                    &Default::default(),
                )
                .unwrap();

                wtxn.commit().unwrap();
                drop(rtxn);

                index.prepare_for_closing().wait();
            },
        )
    });
}

fn deleting_movies_in_batches_default(c: &mut Criterion) {
    let mut group = c.benchmark_group("indexing");
    group.sample_size(BENCHMARK_ITERATION);
    group.bench_function("-movies-delete-facetedString-facetedNumber-searchable-", |b| {
        b.iter_with_setup(
            move || {
                let primary_key = "id";
                let searchable_fields = ["title", "overview"];
                let filterable_fields = ["release_date", "genres"];
                let sortable_fields = [];

                let index = setup_index_with_settings(
                    primary_key,
                    &searchable_fields,
                    &filterable_fields,
                    &sortable_fields,
                );

                // We index only one half of the dataset in the setup part
                // as we don't care about the time it takes.
                let config = IndexerConfig::default();
                let mut wtxn = index.write_txn().unwrap();
                let rtxn = index.read_txn().unwrap();
                let db_fields_ids_map = index.fields_ids_map(&rtxn).unwrap();
                let mut new_fields_ids_map = db_fields_ids_map.clone();

                let mut indexer = indexer::DocumentOperation::new();
                let documents = utils::documents_from(datasets_paths::MOVIES, "json");
                indexer.replace_documents(&documents).unwrap();

                let indexer_alloc = Bump::new();
                let (document_changes, _operation_stats, primary_key) = indexer
                    .into_changes(
                        &indexer_alloc,
                        &index,
                        &rtxn,
                        None,
                        &mut new_fields_ids_map,
                        &|| false,
                        Progress::default(),
                    )
                    .unwrap();

                indexer::index(
                    &mut wtxn,
                    &index,
                    &milli::ThreadPoolNoAbortBuilder::new().build().unwrap(),
                    config.grenad_parameters(),
                    &db_fields_ids_map,
                    new_fields_ids_map,
                    primary_key,
                    &document_changes,
                    RuntimeEmbedders::default(),
                    &|| false,
                    &Progress::default(),
                    &Default::default(),
                )
                .unwrap();

                wtxn.commit().unwrap();
                drop(rtxn);

                let count = 1250;
                let batch_size = 250;
                let document_ids_to_delete =
                    choose_document_ids_from_index_batched(&index, count, batch_size);

                (index, document_ids_to_delete)
            },
            move |(index, document_ids_to_delete)| {
                delete_documents_from_ids(index, document_ids_to_delete)
            },
        )
    });
}

fn delete_documents_from_ids(index: Index, document_ids_to_delete: Vec<RoaringBitmap>) {
    let config = IndexerConfig::default();
    for ids in document_ids_to_delete {
        let mut wtxn = index.write_txn().unwrap();
        let rtxn = index.read_txn().unwrap();
        let db_fields_ids_map = index.fields_ids_map(&rtxn).unwrap();
        let new_fields_ids_map = db_fields_ids_map.clone();
        let primary_key = index.primary_key(&rtxn).unwrap().unwrap();
        let primary_key = PrimaryKey::new(primary_key, &db_fields_ids_map).unwrap();

        let mut indexer = indexer::DocumentDeletion::new();
        indexer.delete_documents_by_docids(ids);

        let indexer_alloc = Bump::new();
        let document_changes = indexer.into_changes(&indexer_alloc, primary_key);

        indexer::index(
            &mut wtxn,
            &index,
            &milli::ThreadPoolNoAbortBuilder::new().build().unwrap(),
            config.grenad_parameters(),
            &db_fields_ids_map,
            new_fields_ids_map,
            Some(primary_key),
            &document_changes,
            RuntimeEmbedders::default(),
            &|| false,
            &Progress::default(),
            &Default::default(),
        )
        .unwrap();

        wtxn.commit().unwrap();
    }

    index.prepare_for_closing().wait();
}

fn indexing_movies_in_three_batches(c: &mut Criterion) {
    let mut group = c.benchmark_group("indexing");
    group.sample_size(BENCHMARK_ITERATION);
    group.bench_function("Indexing movies in three batches", |b| {
        b.iter_with_setup(
            move || {
                let primary_key = "id";
                let searchable_fields = ["title", "overview"];
                let filterable_fields = ["release_date", "genres"];
                let sortable_fields = [];

                let index = setup_index_with_settings(
                    primary_key,
                    &searchable_fields,
                    &filterable_fields,
                    &sortable_fields,
                );

                // We index only one half of the dataset in the setup part
                // as we don't care about the time it takes.
                let config = IndexerConfig::default();
                let mut wtxn = index.write_txn().unwrap();
                let rtxn = index.read_txn().unwrap();
                let db_fields_ids_map = index.fields_ids_map(&rtxn).unwrap();
                let mut new_fields_ids_map = db_fields_ids_map.clone();

                let mut indexer = indexer::DocumentOperation::new();
                let documents = utils::documents_from(datasets_paths::MOVIES_1_2, "json");
                indexer.replace_documents(&documents).unwrap();

                let indexer_alloc = Bump::new();
                let (document_changes, _operation_stats, primary_key) = indexer
                    .into_changes(
                        &indexer_alloc,
                        &index,
                        &rtxn,
                        None,
                        &mut new_fields_ids_map,
                        &|| false,
                        Progress::default(),
                    )
                    .unwrap();

                indexer::index(
                    &mut wtxn,
                    &index,
                    &milli::ThreadPoolNoAbortBuilder::new().build().unwrap(),
                    config.grenad_parameters(),
                    &db_fields_ids_map,
                    new_fields_ids_map,
                    primary_key,
                    &document_changes,
                    RuntimeEmbedders::default(),
                    &|| false,
                    &Progress::default(),
                    &Default::default(),
                )
                .unwrap();

                wtxn.commit().unwrap();
                drop(rtxn);

                index
            },
            move |index| {
                let config = IndexerConfig::default();
                let mut wtxn = index.write_txn().unwrap();
                let rtxn = index.read_txn().unwrap();
                let db_fields_ids_map = index.fields_ids_map(&rtxn).unwrap();
                let mut new_fields_ids_map = db_fields_ids_map.clone();

                let mut indexer = indexer::DocumentOperation::new();
                let documents = utils::documents_from(datasets_paths::MOVIES_3_4, "json");
                indexer.replace_documents(&documents).unwrap();

                let indexer_alloc = Bump::new();
                let (document_changes, _operation_stats, primary_key) = indexer
                    .into_changes(
                        &indexer_alloc,
                        &index,
                        &rtxn,
                        None,
                        &mut new_fields_ids_map,
                        &|| false,
                        Progress::default(),
                    )
                    .unwrap();

                indexer::index(
                    &mut wtxn,
                    &index,
                    &milli::ThreadPoolNoAbortBuilder::new().build().unwrap(),
                    config.grenad_parameters(),
                    &db_fields_ids_map,
                    new_fields_ids_map,
                    primary_key,
                    &document_changes,
                    RuntimeEmbedders::default(),
                    &|| false,
                    &Progress::default(),
                    &Default::default(),
                )
                .unwrap();

                wtxn.commit().unwrap();
                drop(rtxn);

                let mut wtxn = index.write_txn().unwrap();
                let rtxn = index.read_txn().unwrap();
                let db_fields_ids_map = index.fields_ids_map(&rtxn).unwrap();
                let mut new_fields_ids_map = db_fields_ids_map.clone();

                let mut indexer = indexer::DocumentOperation::new();
                let documents = utils::documents_from(datasets_paths::MOVIES_4_4, "json");
                indexer.replace_documents(&documents).unwrap();

                let indexer_alloc = Bump::new();
                let (document_changes, _operation_stats, primary_key) = indexer
                    .into_changes(
                        &indexer_alloc,
                        &index,
                        &rtxn,
                        None,
                        &mut new_fields_ids_map,
                        &|| false,
                        Progress::default(),
                    )
                    .unwrap();

                indexer::index(
                    &mut wtxn,
                    &index,
                    &milli::ThreadPoolNoAbortBuilder::new().build().unwrap(),
                    config.grenad_parameters(),
                    &db_fields_ids_map,
                    new_fields_ids_map,
                    primary_key,
                    &document_changes,
                    RuntimeEmbedders::default(),
                    &|| false,
                    &Progress::default(),
                    &Default::default(),
                )
                .unwrap();

                wtxn.commit().unwrap();
                drop(rtxn);

                index.prepare_for_closing().wait();
            },
        )
    });
}

fn indexing_nested_movies_default(c: &mut Criterion) {
    let mut group = c.benchmark_group("indexing");
    group.sample_size(BENCHMARK_ITERATION);
    group.bench_function("Indexing nested movies with default settings", |b| {
        b.iter_with_setup(
            move || {
                let primary_key = "id";
                let searchable_fields = [
                    "title",
                    "overview",
                    "provider_names",
                    "genres",
                    "crew.name",
                    "cast.character",
                    "cast.name",
                ];
                let filterable_fields = [
                    "popularity",
                    "release_date",
                    "runtime",
                    "vote_average",
                    "external_ids",
                    "keywords",
                    "providers.buy.name",
                    "providers.rent.name",
                    "providers.flatrate.name",
                    "provider_names",
                    "genres",
                    "crew.name",
                    "cast.character",
                    "cast.name",
                ];
                let sortable_fields = ["popularity", "runtime", "vote_average", "release_date"];

                setup_index_with_settings(
                    primary_key,
                    &searchable_fields,
                    &filterable_fields,
                    &sortable_fields,
                )
            },
            move |index| {
                let config = IndexerConfig::default();
                let mut wtxn = index.write_txn().unwrap();
                let rtxn = index.read_txn().unwrap();
                let db_fields_ids_map = index.fields_ids_map(&rtxn).unwrap();
                let mut new_fields_ids_map = db_fields_ids_map.clone();

                let mut indexer = indexer::DocumentOperation::new();
                let documents = utils::documents_from(datasets_paths::NESTED_MOVIES, "json");
                indexer.replace_documents(&documents).unwrap();

                let indexer_alloc = Bump::new();
                let (document_changes, _operation_stats, primary_key) = indexer
                    .into_changes(
                        &indexer_alloc,
                        &index,
                        &rtxn,
                        None,
                        &mut new_fields_ids_map,
                        &|| false,
                        Progress::default(),
                    )
                    .unwrap();

                indexer::index(
                    &mut wtxn,
                    &index,
                    &milli::ThreadPoolNoAbortBuilder::new().build().unwrap(),
                    config.grenad_parameters(),
                    &db_fields_ids_map,
                    new_fields_ids_map,
                    primary_key,
                    &document_changes,
                    RuntimeEmbedders::default(),
                    &|| false,
                    &Progress::default(),
                    &Default::default(),
                )
                .unwrap();

                wtxn.commit().unwrap();
                drop(rtxn);

                index.prepare_for_closing().wait();
            },
        )
    });
}

fn deleting_nested_movies_in_batches_default(c: &mut Criterion) {
    let mut group = c.benchmark_group("indexing");
    group.sample_size(BENCHMARK_ITERATION);
    group.bench_function("-movies-delete-facetedString-facetedNumber-searchable-nested-", |b| {
        b.iter_with_setup(
            move || {
                let primary_key = "id";
                let searchable_fields = [
                    "title",
                    "overview",
                    "provider_names",
                    "genres",
                    "crew.name",
                    "cast.character",
                    "cast.name",
                ];
                let filterable_fields = [
                    "popularity",
                    "release_date",
                    "runtime",
                    "vote_average",
                    "external_ids",
                    "keywords",
                    "providers.buy.name",
                    "providers.rent.name",
                    "providers.flatrate.name",
                    "provider_names",
                    "genres",
                    "crew.name",
                    "cast.character",
                    "cast.name",
                ];
                let sortable_fields = ["popularity", "runtime", "vote_average", "release_date"];

                let index = setup_index_with_settings(
                    primary_key,
                    &searchable_fields,
                    &filterable_fields,
                    &sortable_fields,
                );

                // We index only one half of the dataset in the setup part
                // as we don't care about the time it takes.
                let config = IndexerConfig::default();
                let mut wtxn = index.write_txn().unwrap();
                let rtxn = index.read_txn().unwrap();
                let db_fields_ids_map = index.fields_ids_map(&rtxn).unwrap();
                let mut new_fields_ids_map = db_fields_ids_map.clone();

                let mut indexer = indexer::DocumentOperation::new();
                let documents = utils::documents_from(datasets_paths::NESTED_MOVIES, "json");
                indexer.replace_documents(&documents).unwrap();

                let indexer_alloc = Bump::new();
                let (document_changes, _operation_stats, primary_key) = indexer
                    .into_changes(
                        &indexer_alloc,
                        &index,
                        &rtxn,
                        None,
                        &mut new_fields_ids_map,
                        &|| false,
                        Progress::default(),
                    )
                    .unwrap();

                indexer::index(
                    &mut wtxn,
                    &index,
                    &milli::ThreadPoolNoAbortBuilder::new().build().unwrap(),
                    config.grenad_parameters(),
                    &db_fields_ids_map,
                    new_fields_ids_map,
                    primary_key,
                    &document_changes,
                    RuntimeEmbedders::default(),
                    &|| false,
                    &Progress::default(),
                    &Default::default(),
                )
                .unwrap();

                wtxn.commit().unwrap();
                drop(rtxn);

                let count = 1250;
                let batch_size = 250;
                let document_ids_to_delete =
                    choose_document_ids_from_index_batched(&index, count, batch_size);

                (index, document_ids_to_delete)
            },
            move |(index, document_ids_to_delete)| {
                delete_documents_from_ids(index, document_ids_to_delete)
            },
        )
    });
}

fn indexing_nested_movies_without_faceted_fields(c: &mut Criterion) {
    let mut group = c.benchmark_group("indexing");
    group.sample_size(BENCHMARK_ITERATION);
    group.bench_function("Indexing nested movies without any facets", |b| {
        b.iter_with_setup(
            move || {
                let primary_key = "id";
                let searchable_fields = [
                    "title",
                    "overview",
                    "provider_names",
                    "genres",
                    "crew.name",
                    "cast.character",
                    "cast.name",
                ];
                let filterable_fields = [];
                let sortable_fields = [];

                setup_index_with_settings(
                    primary_key,
                    &searchable_fields,
                    &filterable_fields,
                    &sortable_fields,
                )
            },
            move |index| {
                let config = IndexerConfig::default();
                let mut wtxn = index.write_txn().unwrap();
                let rtxn = index.read_txn().unwrap();
                let db_fields_ids_map = index.fields_ids_map(&rtxn).unwrap();
                let mut new_fields_ids_map = db_fields_ids_map.clone();

                let mut indexer = indexer::DocumentOperation::new();
                let documents = utils::documents_from(datasets_paths::NESTED_MOVIES, "json");
                indexer.replace_documents(&documents).unwrap();

                let indexer_alloc = Bump::new();
                let (document_changes, _operation_stats, primary_key) = indexer
                    .into_changes(
                        &indexer_alloc,
                        &index,
                        &rtxn,
                        None,
                        &mut new_fields_ids_map,
                        &|| false,
                        Progress::default(),
                    )
                    .unwrap();

                indexer::index(
                    &mut wtxn,
                    &index,
                    &milli::ThreadPoolNoAbortBuilder::new().build().unwrap(),
                    config.grenad_parameters(),
                    &db_fields_ids_map,
                    new_fields_ids_map,
                    primary_key,
                    &document_changes,
                    RuntimeEmbedders::default(),
                    &|| false,
                    &Progress::default(),
                    &Default::default(),
                )
                .unwrap();

                wtxn.commit().unwrap();
                drop(rtxn);

                index.prepare_for_closing().wait();
            },
        )
    });
}

fn indexing_geo(c: &mut Criterion) {
    let mut group = c.benchmark_group("indexing");
    group.sample_size(BENCHMARK_ITERATION);
    group.bench_function("Indexing geo_point", |b| {
        b.iter_with_setup(
            move || {
                let primary_key = "geonameid";
                let searchable_fields = ["name", "alternatenames", "elevation"];
                let filterable_fields = ["_geo", "population", "elevation"];
                let sortable_fields = ["_geo", "population", "elevation"];

                setup_index_with_settings(
                    primary_key,
                    &searchable_fields,
                    &filterable_fields,
                    &sortable_fields,
                )
            },
            move |index| {
                let config = IndexerConfig::default();
                let mut wtxn = index.write_txn().unwrap();
                let rtxn = index.read_txn().unwrap();
                let db_fields_ids_map = index.fields_ids_map(&rtxn).unwrap();
                let mut new_fields_ids_map = db_fields_ids_map.clone();

                let mut indexer = indexer::DocumentOperation::new();
                let documents = utils::documents_from(datasets_paths::SMOL_ALL_COUNTRIES, "jsonl");
                indexer.replace_documents(&documents).unwrap();

                let indexer_alloc = Bump::new();
                let (document_changes, _operation_stats, primary_key) = indexer
                    .into_changes(
                        &indexer_alloc,
                        &index,
                        &rtxn,
                        None,
                        &mut new_fields_ids_map,
                        &|| false,
                        Progress::default(),
                    )
                    .unwrap();

                indexer::index(
                    &mut wtxn,
                    &index,
                    &milli::ThreadPoolNoAbortBuilder::new().build().unwrap(),
                    config.grenad_parameters(),
                    &db_fields_ids_map,
                    new_fields_ids_map,
                    primary_key,
                    &document_changes,
                    RuntimeEmbedders::default(),
                    &|| false,
                    &Progress::default(),
                    &Default::default(),
                )
                .unwrap();

                wtxn.commit().unwrap();
                drop(rtxn);

                index.prepare_for_closing().wait();
            },
        )
    });
}

fn reindexing_geo(c: &mut Criterion) {
    let mut group = c.benchmark_group("indexing");
    group.sample_size(BENCHMARK_ITERATION);
    group.bench_function("Reindexing geo_point", |b| {
        b.iter_with_setup(
            move || {
                let primary_key = "geonameid";
                let searchable_fields = ["name", "alternatenames", "elevation"];
                let filterable_fields = ["_geo", "population", "elevation"];
                let sortable_fields = ["_geo", "population", "elevation"];

                let index = setup_index_with_settings(
                    primary_key,
                    &searchable_fields,
                    &filterable_fields,
                    &sortable_fields,
                );

                let config = IndexerConfig::default();
                let mut wtxn = index.write_txn().unwrap();
                let rtxn = index.read_txn().unwrap();
                let db_fields_ids_map = index.fields_ids_map(&rtxn).unwrap();
                let mut new_fields_ids_map = db_fields_ids_map.clone();

                let mut indexer = indexer::DocumentOperation::new();
                let documents = utils::documents_from(datasets_paths::SMOL_ALL_COUNTRIES, "jsonl");
                indexer.replace_documents(&documents).unwrap();

                let indexer_alloc = Bump::new();
                let (document_changes, _operation_stats, primary_key) = indexer
                    .into_changes(
                        &indexer_alloc,
                        &index,
                        &rtxn,
                        None,
                        &mut new_fields_ids_map,
                        &|| false,
                        Progress::default(),
                    )
                    .unwrap();

                indexer::index(
                    &mut wtxn,
                    &index,
                    &milli::ThreadPoolNoAbortBuilder::new().build().unwrap(),
                    config.grenad_parameters(),
                    &db_fields_ids_map,
                    new_fields_ids_map,
                    primary_key,
                    &document_changes,
                    RuntimeEmbedders::default(),
                    &|| false,
                    &Progress::default(),
                    &Default::default(),
                )
                .unwrap();

                wtxn.commit().unwrap();
                drop(rtxn);

                index
            },
            move |index| {
                let config = IndexerConfig::default();
                let mut wtxn = index.write_txn().unwrap();
                let rtxn = index.read_txn().unwrap();
                let db_fields_ids_map = index.fields_ids_map(&rtxn).unwrap();
                let mut new_fields_ids_map = db_fields_ids_map.clone();

                let mut indexer = indexer::DocumentOperation::new();
                let documents = utils::documents_from(datasets_paths::SMOL_ALL_COUNTRIES, "jsonl");
                indexer.replace_documents(&documents).unwrap();

                let indexer_alloc = Bump::new();
                let (document_changes, _operation_stats, primary_key) = indexer
                    .into_changes(
                        &indexer_alloc,
                        &index,
                        &rtxn,
                        None,
                        &mut new_fields_ids_map,
                        &|| false,
                        Progress::default(),
                    )
                    .unwrap();

                indexer::index(
                    &mut wtxn,
                    &index,
                    &milli::ThreadPoolNoAbortBuilder::new().build().unwrap(),
                    config.grenad_parameters(),
                    &db_fields_ids_map,
                    new_fields_ids_map,
                    primary_key,
                    &document_changes,
                    RuntimeEmbedders::default(),
                    &|| false,
                    &Progress::default(),
                    &Default::default(),
                )
                .unwrap();

                wtxn.commit().unwrap();
                drop(rtxn);

                index.prepare_for_closing().wait();
            },
        )
    });
}

fn deleting_geo_in_batches_default(c: &mut Criterion) {
    let mut group = c.benchmark_group("indexing");
    group.sample_size(BENCHMARK_ITERATION);
    group.bench_function("-geo-delete-facetedNumber-facetedGeo-searchable-", |b| {
        b.iter_with_setup(
            move || {
                let primary_key = "geonameid";
                let searchable_fields = ["name", "alternatenames", "elevation"];
                let filterable_fields = ["_geo", "population", "elevation"];
                let sortable_fields = ["_geo", "population", "elevation"];

                let index = setup_index_with_settings(
                    primary_key,
                    &searchable_fields,
                    &filterable_fields,
                    &sortable_fields,
                );

                // We index only one half of the dataset in the setup part
                // as we don't care about the time it takes.
                let config = IndexerConfig::default();
                let mut wtxn = index.write_txn().unwrap();
                let rtxn = index.read_txn().unwrap();
                let db_fields_ids_map = index.fields_ids_map(&rtxn).unwrap();
                let mut new_fields_ids_map = db_fields_ids_map.clone();

                let mut indexer = indexer::DocumentOperation::new();
                let documents = utils::documents_from(datasets_paths::SMOL_ALL_COUNTRIES, "jsonl");
                indexer.replace_documents(&documents).unwrap();

                let indexer_alloc = Bump::new();
                let (document_changes, _operation_stats, primary_key) = indexer
                    .into_changes(
                        &indexer_alloc,
                        &index,
                        &rtxn,
                        None,
                        &mut new_fields_ids_map,
                        &|| false,
                        Progress::default(),
                    )
                    .unwrap();

                indexer::index(
                    &mut wtxn,
                    &index,
                    &milli::ThreadPoolNoAbortBuilder::new().build().unwrap(),
                    config.grenad_parameters(),
                    &db_fields_ids_map,
                    new_fields_ids_map,
                    primary_key,
                    &document_changes,
                    RuntimeEmbedders::default(),
                    &|| false,
                    &Progress::default(),
                    &Default::default(),
                )
                .unwrap();

                wtxn.commit().unwrap();
                drop(rtxn);

                let count = 1250;
                let batch_size = 250;
                let document_ids_to_delete =
                    choose_document_ids_from_index_batched(&index, count, batch_size);

                (index, document_ids_to_delete)
            },
            move |(index, document_ids_to_delete)| {
                delete_documents_from_ids(index, document_ids_to_delete)
            },
        )
    });
}

criterion_group!(
    benches,
    indexing_songs_default,
    reindexing_songs_default,
    deleting_songs_in_batches_default,
    indexing_songs_without_faceted_numbers,
    indexing_songs_without_faceted_fields,
    indexing_songs_in_three_batches_default,
    indexing_wiki,
    reindexing_wiki,
    deleting_wiki_in_batches_default,
    indexing_wiki_in_three_batches,
    indexing_movies_default,
    reindexing_movies_default,
    deleting_movies_in_batches_default,
    indexing_movies_in_three_batches,
    indexing_nested_movies_default,
    deleting_nested_movies_in_batches_default,
    indexing_nested_movies_without_faceted_fields,
    indexing_geo,
    reindexing_geo,
    deleting_geo_in_batches_default
);
criterion_main!(benches);
