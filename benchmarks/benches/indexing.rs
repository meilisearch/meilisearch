mod datasets_paths;
mod utils;

use std::fs::{create_dir_all, remove_dir_all};
use std::path::Path;

use criterion::{criterion_group, criterion_main, Criterion};
use milli::heed::{EnvOpenOptions, RwTxn};
use milli::update::{
    DeleteDocuments, IndexDocuments, IndexDocumentsConfig, IndexerConfig, Settings,
};
use milli::Index;
use rand::seq::SliceRandom;
use rand_chacha::rand_core::SeedableRng;
use roaring::RoaringBitmap;

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
    let mut options = EnvOpenOptions::new();
    options.map_size(100 * 1024 * 1024 * 1024); // 100 GB
    options.max_readers(10);
    Index::new(options, path).unwrap()
}

fn setup_settings<'t>(
    wtxn: &mut RwTxn<'t, '_>,
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

    let filterable_fields = filterable_fields.iter().map(|s| s.to_string()).collect();
    builder.set_filterable_fields(filterable_fields);

    let sortable_fields = sortable_fields.iter().map(|s| s.to_string()).collect();
    builder.set_sortable_fields(sortable_fields);

    builder.execute(|_| (), || false).unwrap();
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
                let indexing_config = IndexDocumentsConfig::default();
                let mut wtxn = index.write_txn().unwrap();
                let builder = IndexDocuments::new(
                    &mut wtxn,
                    &index,
                    &config,
                    indexing_config,
                    |_| (),
                    || false,
                )
                .unwrap();

                let documents = utils::documents_from(datasets_paths::SMOL_SONGS, "csv");
                let (builder, user_error) = builder.add_documents(documents).unwrap();
                user_error.unwrap();
                builder.execute().unwrap();
                wtxn.commit().unwrap();

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
                let indexing_config = IndexDocumentsConfig::default();
                let mut wtxn = index.write_txn().unwrap();
                let builder = IndexDocuments::new(
                    &mut wtxn,
                    &index,
                    &config,
                    indexing_config,
                    |_| (),
                    || false,
                )
                .unwrap();

                let documents = utils::documents_from(datasets_paths::SMOL_SONGS, "csv");
                let (builder, user_error) = builder.add_documents(documents).unwrap();
                user_error.unwrap();
                builder.execute().unwrap();
                wtxn.commit().unwrap();

                index
            },
            move |index| {
                let config = IndexerConfig::default();
                let indexing_config = IndexDocumentsConfig::default();
                let mut wtxn = index.write_txn().unwrap();
                let builder = IndexDocuments::new(
                    &mut wtxn,
                    &index,
                    &config,
                    indexing_config,
                    |_| (),
                    || false,
                )
                .unwrap();

                let documents = utils::documents_from(datasets_paths::SMOL_SONGS, "csv");
                let (builder, user_error) = builder.add_documents(documents).unwrap();
                user_error.unwrap();
                builder.execute().unwrap();
                wtxn.commit().unwrap();

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
                let indexing_config = IndexDocumentsConfig::default();
                let builder = IndexDocuments::new(
                    &mut wtxn,
                    &index,
                    &config,
                    indexing_config,
                    |_| (),
                    || false,
                )
                .unwrap();
                let documents = utils::documents_from(datasets_paths::SMOL_SONGS, "csv");
                let (builder, user_error) = builder.add_documents(documents).unwrap();
                user_error.unwrap();
                builder.execute().unwrap();
                wtxn.commit().unwrap();

                let count = 1250;
                let batch_size = 250;
                let document_ids_to_delete =
                    choose_document_ids_from_index_batched(&index, count, batch_size);

                (index, document_ids_to_delete)
            },
            move |(index, document_ids_to_delete)| {
                let mut wtxn = index.write_txn().unwrap();

                for ids in document_ids_to_delete {
                    let mut builder = DeleteDocuments::new(&mut wtxn, &index).unwrap();
                    builder.delete_documents(&ids);
                    builder.execute().unwrap();
                }

                wtxn.commit().unwrap();

                index.prepare_for_closing().wait();
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
                let indexing_config = IndexDocumentsConfig::default();
                let builder = IndexDocuments::new(
                    &mut wtxn,
                    &index,
                    &config,
                    indexing_config,
                    |_| (),
                    || false,
                )
                .unwrap();
                let documents = utils::documents_from(datasets_paths::SMOL_SONGS_1_2, "csv");
                let (builder, user_error) = builder.add_documents(documents).unwrap();
                user_error.unwrap();
                builder.execute().unwrap();

                wtxn.commit().unwrap();

                index
            },
            move |index| {
                let config = IndexerConfig::default();
                let indexing_config = IndexDocumentsConfig::default();
                let mut wtxn = index.write_txn().unwrap();
                let builder = IndexDocuments::new(
                    &mut wtxn,
                    &index,
                    &config,
                    indexing_config,
                    |_| (),
                    || false,
                )
                .unwrap();
                let documents = utils::documents_from(datasets_paths::SMOL_SONGS_3_4, "csv");
                let (builder, user_error) = builder.add_documents(documents).unwrap();
                user_error.unwrap();
                builder.execute().unwrap();

                let indexing_config = IndexDocumentsConfig::default();
                let builder = IndexDocuments::new(
                    &mut wtxn,
                    &index,
                    &config,
                    indexing_config,
                    |_| (),
                    || false,
                )
                .unwrap();
                let documents = utils::documents_from(datasets_paths::SMOL_SONGS_4_4, "csv");
                let (builder, user_error) = builder.add_documents(documents).unwrap();
                user_error.unwrap();
                builder.execute().unwrap();

                wtxn.commit().unwrap();

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
                let indexing_config = IndexDocumentsConfig::default();
                let mut wtxn = index.write_txn().unwrap();
                let builder = IndexDocuments::new(
                    &mut wtxn,
                    &index,
                    &config,
                    indexing_config,
                    |_| (),
                    || false,
                )
                .unwrap();

                let documents = utils::documents_from(datasets_paths::SMOL_SONGS, "csv");

                let (builder, user_error) = builder.add_documents(documents).unwrap();
                user_error.unwrap();
                builder.execute().unwrap();
                wtxn.commit().unwrap();

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
                let indexing_config = IndexDocumentsConfig::default();
                let mut wtxn = index.write_txn().unwrap();
                let builder = IndexDocuments::new(
                    &mut wtxn,
                    &index,
                    &config,
                    indexing_config,
                    |_| (),
                    || false,
                )
                .unwrap();

                let documents = utils::documents_from(datasets_paths::SMOL_SONGS, "csv");
                let (builder, user_error) = builder.add_documents(documents).unwrap();
                user_error.unwrap();
                builder.execute().unwrap();
                wtxn.commit().unwrap();

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
                let indexing_config =
                    IndexDocumentsConfig { autogenerate_docids: true, ..Default::default() };
                let mut wtxn = index.write_txn().unwrap();
                let builder = IndexDocuments::new(
                    &mut wtxn,
                    &index,
                    &config,
                    indexing_config,
                    |_| (),
                    || false,
                )
                .unwrap();

                let documents = utils::documents_from(datasets_paths::SMOL_WIKI_ARTICLES, "csv");
                let (builder, user_error) = builder.add_documents(documents).unwrap();
                user_error.unwrap();
                builder.execute().unwrap();
                wtxn.commit().unwrap();

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
                let indexing_config =
                    IndexDocumentsConfig { autogenerate_docids: true, ..Default::default() };
                let mut wtxn = index.write_txn().unwrap();
                let builder = IndexDocuments::new(
                    &mut wtxn,
                    &index,
                    &config,
                    indexing_config,
                    |_| (),
                    || false,
                )
                .unwrap();

                let documents = utils::documents_from(datasets_paths::SMOL_WIKI_ARTICLES, "csv");
                let (builder, user_error) = builder.add_documents(documents).unwrap();
                user_error.unwrap();
                builder.execute().unwrap();
                wtxn.commit().unwrap();

                index
            },
            move |index| {
                let config = IndexerConfig::default();
                let indexing_config =
                    IndexDocumentsConfig { autogenerate_docids: true, ..Default::default() };
                let mut wtxn = index.write_txn().unwrap();
                let builder = IndexDocuments::new(
                    &mut wtxn,
                    &index,
                    &config,
                    indexing_config,
                    |_| (),
                    || false,
                )
                .unwrap();

                let documents = utils::documents_from(datasets_paths::SMOL_WIKI_ARTICLES, "csv");
                let (builder, user_error) = builder.add_documents(documents).unwrap();
                user_error.unwrap();
                builder.execute().unwrap();
                wtxn.commit().unwrap();

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
                let indexing_config =
                    IndexDocumentsConfig { autogenerate_docids: true, ..Default::default() };
                let builder = IndexDocuments::new(
                    &mut wtxn,
                    &index,
                    &config,
                    indexing_config,
                    |_| (),
                    || false,
                )
                .unwrap();
                let documents = utils::documents_from(datasets_paths::SMOL_WIKI_ARTICLES, "csv");
                let (builder, user_error) = builder.add_documents(documents).unwrap();
                user_error.unwrap();
                builder.execute().unwrap();
                wtxn.commit().unwrap();

                let count = 1250;
                let batch_size = 250;
                let document_ids_to_delete =
                    choose_document_ids_from_index_batched(&index, count, batch_size);

                (index, document_ids_to_delete)
            },
            move |(index, document_ids_to_delete)| {
                let mut wtxn = index.write_txn().unwrap();

                for ids in document_ids_to_delete {
                    let mut builder = DeleteDocuments::new(&mut wtxn, &index).unwrap();
                    builder.delete_documents(&ids);
                    builder.execute().unwrap();
                }

                wtxn.commit().unwrap();

                index.prepare_for_closing().wait();
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

                let mut wtxn = index.write_txn().unwrap();

                // We index only one half of the dataset in the setup part
                // as we don't care about the time it takes.
                let config = IndexerConfig::default();
                let indexing_config =
                    IndexDocumentsConfig { autogenerate_docids: true, ..Default::default() };
                let builder = IndexDocuments::new(
                    &mut wtxn,
                    &index,
                    &config,
                    indexing_config,
                    |_| (),
                    || false,
                )
                .unwrap();
                let documents =
                    utils::documents_from(datasets_paths::SMOL_WIKI_ARTICLES_1_2, "csv");
                let (builder, user_error) = builder.add_documents(documents).unwrap();
                user_error.unwrap();
                builder.execute().unwrap();

                wtxn.commit().unwrap();

                index
            },
            move |index| {
                let config = IndexerConfig::default();
                let indexing_config =
                    IndexDocumentsConfig { autogenerate_docids: true, ..Default::default() };
                let mut wtxn = index.write_txn().unwrap();
                let builder = IndexDocuments::new(
                    &mut wtxn,
                    &index,
                    &config,
                    indexing_config,
                    |_| (),
                    || false,
                )
                .unwrap();

                let documents =
                    utils::documents_from(datasets_paths::SMOL_WIKI_ARTICLES_3_4, "csv");
                let (builder, user_error) = builder.add_documents(documents).unwrap();
                user_error.unwrap();
                builder.execute().unwrap();

                let indexing_config =
                    IndexDocumentsConfig { autogenerate_docids: true, ..Default::default() };
                let builder = IndexDocuments::new(
                    &mut wtxn,
                    &index,
                    &config,
                    indexing_config,
                    |_| (),
                    || false,
                )
                .unwrap();

                let documents =
                    utils::documents_from(datasets_paths::SMOL_WIKI_ARTICLES_4_4, "csv");
                let (builder, user_error) = builder.add_documents(documents).unwrap();
                user_error.unwrap();
                builder.execute().unwrap();

                wtxn.commit().unwrap();

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
                let indexing_config = IndexDocumentsConfig::default();
                let mut wtxn = index.write_txn().unwrap();
                let builder = IndexDocuments::new(
                    &mut wtxn,
                    &index,
                    &config,
                    indexing_config,
                    |_| (),
                    || false,
                )
                .unwrap();

                let documents = utils::documents_from(datasets_paths::MOVIES, "json");
                let (builder, user_error) = builder.add_documents(documents).unwrap();
                user_error.unwrap();
                builder.execute().unwrap();
                wtxn.commit().unwrap();

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
                let indexing_config = IndexDocumentsConfig::default();
                let mut wtxn = index.write_txn().unwrap();
                let builder = IndexDocuments::new(
                    &mut wtxn,
                    &index,
                    &config,
                    indexing_config,
                    |_| (),
                    || false,
                )
                .unwrap();

                let documents = utils::documents_from(datasets_paths::MOVIES, "json");
                let (builder, user_error) = builder.add_documents(documents).unwrap();
                user_error.unwrap();
                builder.execute().unwrap();
                wtxn.commit().unwrap();

                index
            },
            move |index| {
                let config = IndexerConfig::default();
                let indexing_config = IndexDocumentsConfig::default();
                let mut wtxn = index.write_txn().unwrap();
                let builder = IndexDocuments::new(
                    &mut wtxn,
                    &index,
                    &config,
                    indexing_config,
                    |_| (),
                    || false,
                )
                .unwrap();

                let documents = utils::documents_from(datasets_paths::MOVIES, "json");
                let (builder, user_error) = builder.add_documents(documents).unwrap();
                user_error.unwrap();
                builder.execute().unwrap();
                wtxn.commit().unwrap();

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
                let indexing_config = IndexDocumentsConfig::default();
                let builder = IndexDocuments::new(
                    &mut wtxn,
                    &index,
                    &config,
                    indexing_config,
                    |_| (),
                    || false,
                )
                .unwrap();
                let documents = utils::documents_from(datasets_paths::MOVIES, "json");
                let (builder, user_error) = builder.add_documents(documents).unwrap();
                user_error.unwrap();
                builder.execute().unwrap();
                wtxn.commit().unwrap();

                let count = 1250;
                let batch_size = 250;
                let document_ids_to_delete =
                    choose_document_ids_from_index_batched(&index, count, batch_size);

                (index, document_ids_to_delete)
            },
            move |(index, document_ids_to_delete)| {
                let mut wtxn = index.write_txn().unwrap();

                for ids in document_ids_to_delete {
                    let mut builder = DeleteDocuments::new(&mut wtxn, &index).unwrap();
                    builder.delete_documents(&ids);
                    builder.execute().unwrap();
                }

                wtxn.commit().unwrap();

                index.prepare_for_closing().wait();
            },
        )
    });
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

                let mut wtxn = index.write_txn().unwrap();
                // We index only one half of the dataset in the setup part
                // as we don't care about the time it takes.
                let config = IndexerConfig::default();
                let indexing_config = IndexDocumentsConfig::default();
                let builder = IndexDocuments::new(
                    &mut wtxn,
                    &index,
                    &config,
                    indexing_config,
                    |_| (),
                    || false,
                )
                .unwrap();

                let documents = utils::documents_from(datasets_paths::MOVIES_1_2, "json");
                let (builder, user_error) = builder.add_documents(documents).unwrap();
                user_error.unwrap();
                builder.execute().unwrap();

                wtxn.commit().unwrap();

                index
            },
            move |index| {
                let config = IndexerConfig::default();
                let indexing_config = IndexDocumentsConfig::default();
                let mut wtxn = index.write_txn().unwrap();
                let builder = IndexDocuments::new(
                    &mut wtxn,
                    &index,
                    &config,
                    indexing_config,
                    |_| (),
                    || false,
                )
                .unwrap();

                let documents = utils::documents_from(datasets_paths::MOVIES_3_4, "json");
                let (builder, user_error) = builder.add_documents(documents).unwrap();
                user_error.unwrap();
                builder.execute().unwrap();

                let indexing_config = IndexDocumentsConfig::default();
                let builder = IndexDocuments::new(
                    &mut wtxn,
                    &index,
                    &config,
                    indexing_config,
                    |_| (),
                    || false,
                )
                .unwrap();

                let documents = utils::documents_from(datasets_paths::MOVIES_4_4, "json");
                let (builder, user_error) = builder.add_documents(documents).unwrap();
                user_error.unwrap();
                builder.execute().unwrap();

                wtxn.commit().unwrap();

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
                let indexing_config = IndexDocumentsConfig::default();
                let mut wtxn = index.write_txn().unwrap();
                let builder = IndexDocuments::new(
                    &mut wtxn,
                    &index,
                    &config,
                    indexing_config,
                    |_| (),
                    || false,
                )
                .unwrap();

                let documents = utils::documents_from(datasets_paths::NESTED_MOVIES, "json");
                let (builder, user_error) = builder.add_documents(documents).unwrap();
                user_error.unwrap();
                builder.execute().unwrap();
                wtxn.commit().unwrap();

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
                let indexing_config = IndexDocumentsConfig::default();
                let builder = IndexDocuments::new(
                    &mut wtxn,
                    &index,
                    &config,
                    indexing_config,
                    |_| (),
                    || false,
                )
                .unwrap();
                let documents = utils::documents_from(datasets_paths::NESTED_MOVIES, "json");
                let (builder, user_error) = builder.add_documents(documents).unwrap();
                user_error.unwrap();
                builder.execute().unwrap();
                wtxn.commit().unwrap();

                let count = 1250;
                let batch_size = 250;
                let document_ids_to_delete =
                    choose_document_ids_from_index_batched(&index, count, batch_size);

                (index, document_ids_to_delete)
            },
            move |(index, document_ids_to_delete)| {
                let mut wtxn = index.write_txn().unwrap();

                for ids in document_ids_to_delete {
                    let mut builder = DeleteDocuments::new(&mut wtxn, &index).unwrap();
                    builder.delete_documents(&ids);
                    builder.execute().unwrap();
                }

                wtxn.commit().unwrap();

                index.prepare_for_closing().wait();
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
                let indexing_config = IndexDocumentsConfig::default();
                let mut wtxn = index.write_txn().unwrap();
                let builder = IndexDocuments::new(
                    &mut wtxn,
                    &index,
                    &config,
                    indexing_config,
                    |_| (),
                    || false,
                )
                .unwrap();

                let documents = utils::documents_from(datasets_paths::NESTED_MOVIES, "json");
                let (builder, user_error) = builder.add_documents(documents).unwrap();
                user_error.unwrap();
                builder.execute().unwrap();
                wtxn.commit().unwrap();

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
                let indexing_config = IndexDocumentsConfig::default();
                let mut wtxn = index.write_txn().unwrap();
                let builder = IndexDocuments::new(
                    &mut wtxn,
                    &index,
                    &config,
                    indexing_config,
                    |_| (),
                    || false,
                )
                .unwrap();

                let documents = utils::documents_from(datasets_paths::SMOL_ALL_COUNTRIES, "jsonl");
                let (builder, user_error) = builder.add_documents(documents).unwrap();
                user_error.unwrap();
                builder.execute().unwrap();

                wtxn.commit().unwrap();

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
                let indexing_config = IndexDocumentsConfig::default();
                let mut wtxn = index.write_txn().unwrap();
                let builder = IndexDocuments::new(
                    &mut wtxn,
                    &index,
                    &config,
                    indexing_config,
                    |_| (),
                    || false,
                )
                .unwrap();

                let documents = utils::documents_from(datasets_paths::SMOL_ALL_COUNTRIES, "jsonl");
                let (builder, user_error) = builder.add_documents(documents).unwrap();
                user_error.unwrap();
                builder.execute().unwrap();

                wtxn.commit().unwrap();

                index
            },
            move |index| {
                let config = IndexerConfig::default();
                let indexing_config = IndexDocumentsConfig::default();
                let mut wtxn = index.write_txn().unwrap();
                let builder = IndexDocuments::new(
                    &mut wtxn,
                    &index,
                    &config,
                    indexing_config,
                    |_| (),
                    || false,
                )
                .unwrap();

                let documents = utils::documents_from(datasets_paths::SMOL_ALL_COUNTRIES, "jsonl");
                let (builder, user_error) = builder.add_documents(documents).unwrap();
                user_error.unwrap();
                builder.execute().unwrap();

                wtxn.commit().unwrap();

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
                let indexing_config = IndexDocumentsConfig::default();
                let builder = IndexDocuments::new(
                    &mut wtxn,
                    &index,
                    &config,
                    indexing_config,
                    |_| (),
                    || false,
                )
                .unwrap();
                let documents = utils::documents_from(datasets_paths::SMOL_ALL_COUNTRIES, "jsonl");
                let (builder, user_error) = builder.add_documents(documents).unwrap();
                user_error.unwrap();
                builder.execute().unwrap();
                wtxn.commit().unwrap();

                let count = 1250;
                let batch_size = 250;
                let document_ids_to_delete =
                    choose_document_ids_from_index_batched(&index, count, batch_size);

                (index, document_ids_to_delete)
            },
            move |(index, document_ids_to_delete)| {
                let mut wtxn = index.write_txn().unwrap();

                for ids in document_ids_to_delete {
                    let mut builder = DeleteDocuments::new(&mut wtxn, &index).unwrap();
                    builder.delete_documents(&ids);
                    builder.execute().unwrap();
                }

                wtxn.commit().unwrap();

                index.prepare_for_closing().wait();
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
