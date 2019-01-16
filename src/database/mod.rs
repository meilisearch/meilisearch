use std::sync::{Arc, Mutex};
use std::error::Error;
use std::ops::Deref;
use std::path::Path;

use rocksdb::rocksdb_options::{DBOptions, IngestExternalFileOptions, ColumnFamilyOptions};
use rocksdb::rocksdb::{Writable, Snapshot};
use rocksdb::{DB, DBVector, MergeOperands};
use crossbeam::atomic::ArcCell;
use log::info;

pub use self::document_key::{DocumentKey, DocumentKeyAttr};
pub use self::view::{DatabaseView, DocumentIter};
pub use self::update::{Update, UpdateBuilder};
pub use self::serde::SerializerError;
pub use self::schema::Schema;
pub use self::index::Index;

const DATA_INDEX:  &[u8] = b"data-index";
const DATA_SCHEMA: &[u8] = b"data-schema";

pub mod schema;
pub(crate) mod index;
mod deserializer;
mod document_key;
mod serde;
mod update;
mod view;

fn retrieve_data_schema<D>(snapshot: &Snapshot<D>) -> Result<Schema, Box<Error>>
where D: Deref<Target=DB>
{
    match snapshot.get(DATA_SCHEMA)? {
        Some(vector) => Ok(Schema::read_from_bin(&*vector)?),
        None => Err(String::from("BUG: no schema found in the database").into()),
    }
}

fn retrieve_data_index<D>(snapshot: &Snapshot<D>) -> Result<Index, Box<Error>>
where D: Deref<Target=DB>
{
    let index = match snapshot.get(DATA_INDEX)? {
        Some(vector) => {
            let bytes = vector.as_ref().to_vec();
            Index::from_bytes(bytes)?
        },
        None => Index::default(),
    };

    Ok(index)
}

fn merge_indexes(key: &[u8], existing: Option<&[u8]>, operands: &mut MergeOperands) -> Vec<u8> {
    assert_eq!(key, DATA_INDEX, "The merge operator only supports \"data-index\" merging");

    let mut index: Option<Index> = None;
    for bytes in existing.into_iter().chain(operands) {
        let operand = Index::from_bytes(bytes.to_vec()).unwrap();
        let merged = match index {
            Some(ref index) => index.merge(&operand).unwrap(),
            None            => operand,
        };

        index.replace(merged);
    }

    let index = index.unwrap_or_default();
    let mut bytes = Vec::new();
    index.write_to_bytes(&mut bytes);
    bytes
}

pub struct Database {
    // DB is under a Mutex to sync update ingestions and separate DB update locking
    // and DatabaseView acquiring locking in other words:
    // "Block readers the minimum possible amount of time"
    db: Mutex<Arc<DB>>,

    // This view is updated each time the DB ingests an update
    view: ArcCell<DatabaseView<Arc<DB>>>,
}

impl Database {
    pub fn create<P: AsRef<Path>>(path: P, schema: &Schema) -> Result<Database, Box<Error>> {
        let path = path.as_ref();
        if path.exists() {
            return Err(format!("File already exists at path: {}, cannot create database.",
                                path.display()).into())
        }

        let path = path.to_string_lossy();
        let mut opts = DBOptions::new();
        opts.create_if_missing(true);
        // opts.error_if_exists(true); // FIXME pull request that

        let mut cf_opts = ColumnFamilyOptions::new();
        cf_opts.add_merge_operator("data-index merge operator", merge_indexes);

        let db = DB::open_cf(opts, &path, vec![("default", cf_opts)])?;

        let mut schema_bytes = Vec::new();
        schema.write_to_bin(&mut schema_bytes)?;
        db.put(DATA_SCHEMA, &schema_bytes)?;

        let db = Arc::new(db);
        let snapshot = Snapshot::new(db.clone());
        let view = ArcCell::new(Arc::new(DatabaseView::new(snapshot)?));

        Ok(Database { db: Mutex::new(db), view })
    }

    pub fn open<P: AsRef<Path>>(path: P) -> Result<Database, Box<Error>> {
        let path = path.as_ref().to_string_lossy();

        let mut opts = DBOptions::new();
        opts.create_if_missing(false);

        let mut cf_opts = ColumnFamilyOptions::new();
        cf_opts.add_merge_operator("data-index merge operator", merge_indexes);

        let db = DB::open_cf(opts, &path, vec![("default", cf_opts)])?;

        // FIXME create a generic function to do that !
        let _schema = match db.get(DATA_SCHEMA)? {
            Some(value) => Schema::read_from_bin(&*value)?,
            None => return Err(String::from("Database does not contain a schema").into()),
        };

        let db = Arc::new(db);
        let snapshot = Snapshot::new(db.clone());
        let view = ArcCell::new(Arc::new(DatabaseView::new(snapshot)?));

        Ok(Database { db: Mutex::new(db), view })
    }

    pub fn ingest_update_file(&self, update: Update) -> Result<Arc<DatabaseView<Arc<DB>>>, Box<Error>> {
        let snapshot = {
            // We must have a mutex here to ensure that update ingestions and compactions
            // are done atomatically and in the right order.
            // This way update ingestions will block other update ingestions without blocking view
            // creations while doing the "data-index" compaction
            let db = match self.db.lock() {
                Ok(db) => db,
                Err(e) => return Err(e.to_string().into()),
            };

            let path = update.path().to_string_lossy();
            let options = IngestExternalFileOptions::new();
            // options.move_files(move_update);

            let (elapsed, result) = elapsed::measure_time(|| {
                let cf_handle = db.cf_handle("default").expect("\"default\" column family not found");
                db.ingest_external_file_optimized(&cf_handle, &options, &[&path])
            });
            let _ = result?;
            info!("ingesting update file took {}", elapsed);

            let (elapsed, _) = elapsed::measure_time(|| {
                // Compacting to trigger the merge operator only one time
                // while ingesting the update and not each time searching
                db.compact_range(Some(DATA_INDEX), Some(DATA_INDEX));
            });
            info!("compacting index range took {}", elapsed);

            Snapshot::new(db.clone())
        };

        let view = Arc::new(DatabaseView::new(snapshot)?);
        self.view.set(view.clone());

        Ok(view)
    }

    pub fn get(&self, key: &[u8]) -> Result<Option<DBVector>, Box<Error>> {
        self.view().get(key)
    }

    pub fn flush(&self) -> Result<(), Box<Error>> {
        match self.db.lock() {
            Ok(db) => Ok(db.flush(true)?),
            Err(e) => Err(e.to_string().into()),
        }
    }

    pub fn view(&self) -> Arc<DatabaseView<Arc<DB>>> {
        self.view.get()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::error::Error;

    use serde_derive::{Serialize, Deserialize};
    use hashbrown::HashSet;
    use tempfile::tempdir;

    use crate::database::schema::{SchemaBuilder, STORED, INDEXED};
    use crate::database::update::UpdateBuilder;
    use crate::tokenizer::DefaultBuilder;

    #[test]
    fn ingest_one_update_file() -> Result<(), Box<Error>> {
        let dir = tempdir()?;
        let stop_words = HashSet::new();

        let rocksdb_path = dir.path().join("rocksdb.rdb");

        #[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
        struct SimpleDoc {
            id: u64,
            title: String,
            description: String,
            timestamp: u64,
        }

        let schema = {
            let mut builder = SchemaBuilder::with_identifier("id");
            builder.new_attribute("id", STORED);
            builder.new_attribute("title", STORED | INDEXED);
            builder.new_attribute("description", STORED | INDEXED);
            builder.new_attribute("timestamp", STORED);
            builder.build()
        };

        let database = Database::create(&rocksdb_path, &schema)?;

        let update_path = dir.path().join("update.sst");

        let doc0 = SimpleDoc {
            id: 0,
            title: String::from("I am a title"),
            description: String::from("I am a description"),
            timestamp: 1234567,
        };
        let doc1 = SimpleDoc {
            id: 1,
            title: String::from("I am the second title"),
            description: String::from("I am the second description"),
            timestamp: 7654321,
        };

        let docid0;
        let docid1;
        let update = {
            let tokenizer_builder = DefaultBuilder::new();
            let mut builder = UpdateBuilder::new(update_path, schema);

            docid0 = builder.update_document(&doc0, &tokenizer_builder, &stop_words)?;
            docid1 = builder.update_document(&doc1, &tokenizer_builder, &stop_words)?;

            builder.build()?
        };

        database.ingest_update_file(update)?;
        let view = database.view();

        let de_doc0: SimpleDoc = view.document_by_id(docid0)?;
        let de_doc1: SimpleDoc = view.document_by_id(docid1)?;

        assert_eq!(doc0, de_doc0);
        assert_eq!(doc1, de_doc1);

        Ok(dir.close()?)
    }

    #[test]
    fn ingest_two_update_files() -> Result<(), Box<Error>> {
        let dir = tempdir()?;
        let stop_words = HashSet::new();

        let rocksdb_path = dir.path().join("rocksdb.rdb");

        #[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
        struct SimpleDoc {
            id: u64,
            title: String,
            description: String,
            timestamp: u64,
        }

        let schema = {
            let mut builder = SchemaBuilder::with_identifier("id");
            builder.new_attribute("id", STORED);
            builder.new_attribute("title", STORED | INDEXED);
            builder.new_attribute("description", STORED | INDEXED);
            builder.new_attribute("timestamp", STORED);
            builder.build()
        };

        let database = Database::create(&rocksdb_path, &schema)?;

        let doc0 = SimpleDoc {
            id: 0,
            title: String::from("I am a title"),
            description: String::from("I am a description"),
            timestamp: 1234567,
        };
        let doc1 = SimpleDoc {
            id: 1,
            title: String::from("I am the second title"),
            description: String::from("I am the second description"),
            timestamp: 7654321,
        };
        let doc2 = SimpleDoc {
            id: 2,
            title: String::from("I am the third title"),
            description: String::from("I am the third description"),
            timestamp: 7654321,
        };
        let doc3 = SimpleDoc {
            id: 3,
            title: String::from("I am the fourth title"),
            description: String::from("I am the fourth description"),
            timestamp: 7654321,
        };

        let docid0;
        let docid1;
        let update1 = {
            let tokenizer_builder = DefaultBuilder::new();
            let update_path = dir.path().join("update-000.sst");
            let mut builder = UpdateBuilder::new(update_path, schema.clone());

            docid0 = builder.update_document(&doc0, &tokenizer_builder, &stop_words)?;
            docid1 = builder.update_document(&doc1, &tokenizer_builder, &stop_words)?;

            builder.build()?
        };

        let docid2;
        let docid3;
        let update2 = {
            let tokenizer_builder = DefaultBuilder::new();
            let update_path = dir.path().join("update-001.sst");
            let mut builder = UpdateBuilder::new(update_path, schema);

            docid2 = builder.update_document(&doc2, &tokenizer_builder, &stop_words)?;
            docid3 = builder.update_document(&doc3, &tokenizer_builder, &stop_words)?;

            builder.build()?
        };

        database.ingest_update_file(update1)?;
        database.ingest_update_file(update2)?;

        let view = database.view();

        let de_doc0: SimpleDoc = view.document_by_id(docid0)?;
        let de_doc1: SimpleDoc = view.document_by_id(docid1)?;

        assert_eq!(doc0, de_doc0);
        assert_eq!(doc1, de_doc1);

        let de_doc2: SimpleDoc = view.document_by_id(docid2)?;
        let de_doc3: SimpleDoc = view.document_by_id(docid3)?;

        assert_eq!(doc2, de_doc2);
        assert_eq!(doc3, de_doc3);

        Ok(dir.close()?)
    }
}

#[cfg(all(feature = "nightly", test))]
mod bench {
    extern crate test;

    use super::*;
    use std::error::Error;
    use std::iter::repeat_with;
    use self::test::Bencher;

    use rand::distributions::Alphanumeric;
    use rand_xorshift::XorShiftRng;
    use rand::{Rng, SeedableRng};
    use serde_derive::Serialize;
    use rand::seq::SliceRandom;
    use hashbrown::HashSet;

    use crate::tokenizer::DefaultBuilder;
    use crate::database::update::UpdateBuilder;
    use crate::database::schema::*;

    fn random_sentences<R: Rng>(number: usize, rng: &mut R) -> String {
        let mut words = String::new();

        for i in 0..number {
            let word_len = rng.gen_range(1, 12);
            let iter = repeat_with(|| rng.sample(Alphanumeric)).take(word_len);
            words.extend(iter);

            if i == number - 1 { // last word
                let final_ = [".", "?", "!", "..."].choose(rng).cloned();
                words.extend(final_);
            } else {
                let middle = [",", ", "].choose(rng).cloned();
                words.extend(middle);
            }
        }

        words
    }

    #[bench]
    fn open_little_database(bench: &mut Bencher) -> Result<(), Box<Error>> {
        let dir = tempfile::tempdir()?;
        let stop_words = HashSet::new();

        let mut builder = SchemaBuilder::with_identifier("id");
        builder.new_attribute("title", STORED | INDEXED);
        builder.new_attribute("description", STORED | INDEXED);
        let schema = builder.build();

        let db_path = dir.path().join("bench.mdb");
        let database = Database::create(db_path.clone(), &schema)?;

        #[derive(Serialize)]
        struct Document {
            id: u64,
            title: String,
            description: String,
        }

        let path = dir.path().join("update-000.sst");
        let tokenizer_builder = DefaultBuilder;
        let mut builder = UpdateBuilder::new(path, schema);
        let mut rng = XorShiftRng::seed_from_u64(42);

        for i in 0..300 {
            let document = Document {
                id: i,
                title: random_sentences(rng.gen_range(1, 8), &mut rng),
                description: random_sentences(rng.gen_range(20, 200), &mut rng),
            };
            builder.update_document(&document, &tokenizer_builder, &stop_words)?;
        }

        let update = builder.build()?;
        database.ingest_update_file(update)?;

        drop(database);

        bench.iter(|| {
            let database = Database::open(db_path.clone()).unwrap();
            test::black_box(|| database);
        });

        Ok(())
    }

    #[bench]
    fn open_medium_database(bench: &mut Bencher) -> Result<(), Box<Error>> {
        let dir = tempfile::tempdir()?;
        let stop_words = HashSet::new();

        let mut builder = SchemaBuilder::with_identifier("id");
        builder.new_attribute("title", STORED | INDEXED);
        builder.new_attribute("description", STORED | INDEXED);
        let schema = builder.build();

        let db_path = dir.path().join("bench.mdb");
        let database = Database::create(db_path.clone(), &schema)?;

        #[derive(Serialize)]
        struct Document {
            id: u64,
            title: String,
            description: String,
        }

        let path = dir.path().join("update-000.sst");
        let tokenizer_builder = DefaultBuilder;
        let mut builder = UpdateBuilder::new(path, schema);
        let mut rng = XorShiftRng::seed_from_u64(42);

        for i in 0..3000 {
            let document = Document {
                id: i,
                title: random_sentences(rng.gen_range(1, 8), &mut rng),
                description: random_sentences(rng.gen_range(20, 200), &mut rng),
            };
            builder.update_document(&document, &tokenizer_builder, &stop_words)?;
        }

        let update = builder.build()?;
        database.ingest_update_file(update)?;

        drop(database);

        bench.iter(|| {
            let database = Database::open(db_path.clone()).unwrap();
            test::black_box(|| database);
        });

        Ok(())
    }

    #[bench]
    #[ignore]
    fn open_big_database(bench: &mut Bencher) -> Result<(), Box<Error>> {
        let dir = tempfile::tempdir()?;
        let stop_words = HashSet::new();

        let mut builder = SchemaBuilder::with_identifier("id");
        builder.new_attribute("title", STORED | INDEXED);
        builder.new_attribute("description", STORED | INDEXED);
        let schema = builder.build();

        let db_path = dir.path().join("bench.mdb");
        let database = Database::create(db_path.clone(), &schema)?;

        #[derive(Serialize)]
        struct Document {
            id: u64,
            title: String,
            description: String,
        }

        let path = dir.path().join("update-000.sst");
        let tokenizer_builder = DefaultBuilder;
        let mut builder = UpdateBuilder::new(path, schema);
        let mut rng = XorShiftRng::seed_from_u64(42);

        for i in 0..30_000 {
            let document = Document {
                id: i,
                title: random_sentences(rng.gen_range(1, 8), &mut rng),
                description: random_sentences(rng.gen_range(20, 200), &mut rng),
            };
            builder.update_document(&document, &tokenizer_builder, &stop_words)?;
        }

        let update = builder.build()?;
        database.ingest_update_file(update)?;

        drop(database);

        bench.iter(|| {
            let database = Database::open(db_path.clone()).unwrap();
            test::black_box(|| database);
        });

        Ok(())
    }

    #[bench]
    fn search_oneletter_little_database(bench: &mut Bencher) -> Result<(), Box<Error>> {
        let dir = tempfile::tempdir()?;
        let stop_words = HashSet::new();

        let mut builder = SchemaBuilder::with_identifier("id");
        builder.new_attribute("title", STORED | INDEXED);
        builder.new_attribute("description", STORED | INDEXED);
        let schema = builder.build();

        let db_path = dir.path().join("bench.mdb");
        let database = Database::create(db_path.clone(), &schema)?;

        #[derive(Serialize)]
        struct Document {
            id: u64,
            title: String,
            description: String,
        }

        let path = dir.path().join("update-000.sst");
        let tokenizer_builder = DefaultBuilder;
        let mut builder = UpdateBuilder::new(path, schema);
        let mut rng = XorShiftRng::seed_from_u64(42);

        for i in 0..300 {
            let document = Document {
                id: i,
                title: random_sentences(rng.gen_range(1, 8), &mut rng),
                description: random_sentences(rng.gen_range(20, 200), &mut rng),
            };
            builder.update_document(&document, &tokenizer_builder, &stop_words)?;
        }

        let update = builder.build()?;
        let view = database.ingest_update_file(update)?;

        bench.iter(|| {
            for q in &["a", "b", "c", "d", "e"] {
                let documents = view.query_builder().unwrap().query(q, 0..20);
                test::black_box(|| documents);
            }
        });

        Ok(())
    }

    #[bench]
    fn search_oneletter_medium_database(bench: &mut Bencher) -> Result<(), Box<Error>> {
        let dir = tempfile::tempdir()?;
        let stop_words = HashSet::new();

        let mut builder = SchemaBuilder::with_identifier("id");
        builder.new_attribute("title", STORED | INDEXED);
        builder.new_attribute("description", STORED | INDEXED);
        let schema = builder.build();

        let db_path = dir.path().join("bench.mdb");
        let database = Database::create(db_path.clone(), &schema)?;

        #[derive(Serialize)]
        struct Document {
            id: u64,
            title: String,
            description: String,
        }

        let path = dir.path().join("update-000.sst");
        let tokenizer_builder = DefaultBuilder;
        let mut builder = UpdateBuilder::new(path, schema);
        let mut rng = XorShiftRng::seed_from_u64(42);

        for i in 0..3000 {
            let document = Document {
                id: i,
                title: random_sentences(rng.gen_range(1, 8), &mut rng),
                description: random_sentences(rng.gen_range(20, 200), &mut rng),
            };
            builder.update_document(&document, &tokenizer_builder, &stop_words)?;
        }

        let update = builder.build()?;
        let view = database.ingest_update_file(update)?;

        bench.iter(|| {
            for q in &["a", "b", "c", "d", "e"] {
                let documents = view.query_builder().unwrap().query(q, 0..20);
                test::black_box(|| documents);
            }
        });

        Ok(())
    }

    #[bench]
    #[ignore]
    fn search_oneletter_big_database(bench: &mut Bencher) -> Result<(), Box<Error>> {
        let dir = tempfile::tempdir()?;
        let stop_words = HashSet::new();

        let mut builder = SchemaBuilder::with_identifier("id");
        builder.new_attribute("title", STORED | INDEXED);
        builder.new_attribute("description", STORED | INDEXED);
        let schema = builder.build();

        let db_path = dir.path().join("bench.mdb");
        let database = Database::create(db_path.clone(), &schema)?;

        #[derive(Serialize)]
        struct Document {
            id: u64,
            title: String,
            description: String,
        }

        let path = dir.path().join("update-000.sst");
        let tokenizer_builder = DefaultBuilder;
        let mut builder = UpdateBuilder::new(path, schema);
        let mut rng = XorShiftRng::seed_from_u64(42);

        for i in 0..30_000 {
            let document = Document {
                id: i,
                title: random_sentences(rng.gen_range(1, 8), &mut rng),
                description: random_sentences(rng.gen_range(20, 200), &mut rng),
            };
            builder.update_document(&document, &tokenizer_builder, &stop_words)?;
        }

        let update = builder.build()?;
        let view = database.ingest_update_file(update)?;

        bench.iter(|| {
            for q in &["a", "b", "c", "d", "e"] {
                let documents = view.query_builder().unwrap().query(q, 0..20);
                test::black_box(|| documents);
            }
        });

        Ok(())
    }
}
