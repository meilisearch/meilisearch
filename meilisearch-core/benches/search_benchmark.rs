#[cfg(test)]
#[macro_use]
extern crate assert_matches;

use std::sync::mpsc;
use std::path::Path;
use std::{fs, fs::File, io::BufReader};
use std::iter;

use meilisearch_core::Database;
use meilisearch_core::{ProcessedUpdateResult, UpdateStatus};
use meilisearch_core::settings::{Settings, SettingsUpdate, UpdateState};
use meilisearch_schema::Schema;
use serde_json::Value;

use criterion::{criterion_group, criterion_main, Criterion, BenchmarkId};

fn prepare_database(path: &Path) -> Database {
    let database = Database::open_or_create(path).unwrap();
    let db = &database;

    let (sender, receiver) = mpsc::sync_channel(100);
    let update_fn = move |_name: &str, update: ProcessedUpdateResult| {
        sender.send(update.update_id).unwrap()
    };
    let index = database.create_index("bench").unwrap();

    database.set_update_callback(Box::new(update_fn));
    
    let mut writer = db.main_write_txn().unwrap();
    index.main.put_schema(&mut writer, &Schema::with_primary_key("id")).unwrap();
    writer.commit().unwrap();

    let settings_update: SettingsUpdate = {
        let path = concat!(env!("CARGO_MANIFEST_DIR"), "/../datasets/movies/settings.json");
        let file = File::open(path).unwrap();
        let reader = BufReader::new(file);
        let settings: Settings = serde_json::from_reader(reader).unwrap();
        settings.into_update().unwrap()
    };

    let mut update_writer = db.update_write_txn().unwrap();
    let _update_id = index.settings_update(&mut update_writer, settings_update).unwrap();

    update_writer.commit().unwrap();

    let mut additions = index.documents_addition();

    let json: Value = {
        let path = concat!(env!("CARGO_MANIFEST_DIR"), "/../datasets/movies/movies.json");
        let movies_file = fs::File::open(path).expect("find movies");
        serde_json::from_reader(movies_file).unwrap()
    };

    let documents = json.as_array().unwrap();

    for document in documents {
        additions.update_document(document);
    }

    let mut update_writer = db.update_write_txn().unwrap();
    let update_id = additions.finalize(&mut update_writer).unwrap();
    update_writer.commit().unwrap();

    // block until the transaction is processed
    let _ = receiver.into_iter().find(|id| *id == update_id);

    let update_reader = db.update_read_txn().unwrap();
    let result = index.update_status(&update_reader, update_id).unwrap();
    assert_matches!(result, Some(UpdateStatus::Processed { content }) if content.error.is_none());

    database
}

pub fn criterion_benchmark(c: &mut Criterion) {
    let dir = tempfile::tempdir().unwrap();
    let database = prepare_database(dir.path());

    let reader = database.main_read_txn().unwrap();
    let index = database.open_index("bench").unwrap();

    let mut count = 0;
    let query = "I love paris ";

    let iter = iter::from_fn(|| {
        count += 1;
        query.get(0..count)
    });

    let mut group = c.benchmark_group("searching in movies (19654 docs)");
    group.sample_size(10);

    for query in iter {
        let bench_name = BenchmarkId::from_parameter(format!("{:?}", query));
        group.bench_with_input(bench_name, &query, |b, query| b.iter(|| {
            let builder = index.query_builder();
            builder.query(&reader, query, 0..20).unwrap();
        }));
    }
    group.finish();
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
