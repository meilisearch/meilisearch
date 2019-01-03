use std::error::Error;
use std::path::Path;
use std::ops::Deref;
use std::sync::Arc;

use rocksdb::rocksdb_options::{DBOptions, IngestExternalFileOptions, ColumnFamilyOptions};
use rocksdb::rocksdb_options::{ColumnFamilyDescriptor, WriteOptions};
use rocksdb::rocksdb::{Snapshot, CFHandle};
use rocksdb::{DB, MergeOperands};

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

fn retrieve_data_schema<D>(snapshot: &Snapshot<D>, handle: &CFHandle) -> Result<Schema, Box<Error>>
where D: Deref<Target=DB>,
{
    match snapshot.get_cf(handle, DATA_SCHEMA)? {
        Some(vector) => Ok(Schema::read_from_bin(&*vector)?),
        None => Err(String::from("BUG: no schema found in the database").into()),
    }
}

fn retrieve_data_index<D>(snapshot: &Snapshot<D>, handle: &CFHandle) -> Result<Index, Box<Error>>
where D: Deref<Target=DB>,
{
    let index = match snapshot.get_cf(handle, DATA_INDEX)? {
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

pub struct Database(Arc<DB>);

impl Database {
    pub fn create<P: AsRef<Path>>(path: P) -> Result<Database, Box<Error>> {
        let path = path.as_ref();
        if path.exists() {
            let msg = format!("File {:?} already exists, cannot create database.", path.display());
            return Err(msg.into())
        }

        let path = path.to_string_lossy();
        let mut opts = DBOptions::new();
        opts.create_if_missing(true);
        // opts.error_if_exists(true); // FIXME pull request that

        // create the database (with mandatory "default" column family)
        let db = DB::open(opts, &path)?;
        let db = Arc::new(db);

        Ok(Database(db))
    }

    pub fn open<P: AsRef<Path>>(path: P) -> Result<Database, Box<Error>> {
        let path = path.as_ref().to_string_lossy();

        let mut opts = DBOptions::new();
        opts.create_if_missing(false);

        let cfs = DB::list_column_families(&opts, &path)?;
        let cfds = cfs.iter().map(|name| {
            let mut cf_opts = ColumnFamilyOptions::new();
            cf_opts.add_merge_operator("data-index merge operator", merge_indexes);
            ColumnFamilyDescriptor::new(name, cf_opts)
        }).collect();

        // open the database with all every column families
        let db = DB::open_cf(opts, &path, cfds)?;
        let db = Arc::new(db);

        Ok(Database(db))
    }

    pub fn create_index(&self, name: &str, schema: &Schema) -> Result<DatabaseView<Arc<DB>>, Box<Error>> {
        let mut cf_opts = ColumnFamilyOptions::new();
        cf_opts.add_merge_operator("data-index merge operator", merge_indexes);

        let cf_descriptor = ColumnFamilyDescriptor::new(name, cf_opts);
        self.0.create_cf(cf_descriptor)?;

        let mut schema_bytes = Vec::new();
        schema.write_to_bin(&mut schema_bytes)?;

        let writeopts = WriteOptions::new();
        let handle = self.0.cf_handle(name).unwrap();
        self.0.put_cf_opt(&handle, DATA_SCHEMA, &schema_bytes, &writeopts)?;

        let snapshot = Snapshot::new(self.0.clone());
        let view = DatabaseView::new(snapshot, handle)?;

        Ok(view)
    }

    pub fn open_index(&self, name: &str) -> Result<Option<DatabaseView<Arc<DB>>>, Box<Error>> {
        match self.0.cf_handle(name) {
            Some(handle) => {
                let snapshot = Snapshot::new(self.0.clone());
                let view = DatabaseView::new(snapshot, handle)?;
                Ok(Some(view))
            },
            None => Ok(None),
        }
    }

    pub fn update_index(&self, name: &str, update: Update) -> Result<DatabaseView<Arc<DB>>, Box<Error>> {
        // FIXME We must have a mutex here to ensure that update ingestions and compactions
        //       are done atomatically and in the right order.
        //       This way update ingestions will block other update ingestions without blocking view
        //       creations while doing the "data-index" compaction

        match self.0.cf_handle(name) {
            Some(handle) => {
                let path = update.path().to_string_lossy();
                let options = IngestExternalFileOptions::new();
                // options.move_files(move_update);

                self.0.ingest_external_file_optimized(&handle, &options, &[&path])?;

                // Compacting to trigger the merge operator only one time
                // while ingesting the update and not each time searching
                self.0.compact_range(Some(DATA_INDEX), Some(DATA_INDEX));

                let snapshot = Snapshot::new(self.0.clone());
                let view = DatabaseView::new(snapshot, handle)?;
                Ok(view)
            },
            None => Err("Invalid column family".into()),
        }
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

        let database = Database::create(&rocksdb_path)?;
        database.create_index("ingest-one", &schema)?;
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

        database.update_index("ingest-one", update)?;
        let view = database.open_index("ingest-one")?;
        let view = view.unwrap();

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

        let database = Database::create(&rocksdb_path)?;
        database.create_index("ingest-two", &schema)?;

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

        database.update_index("ingest-two", update1)?;
        database.update_index("ingest-two", update2)?;

        let view = database.open_index("ingest-two")?;
        let view = view.unwrap();

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
        let database = Database::create(db_path.clone())?;
        database.create_index("little", &schema)?;

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
        database.update_index("little", update)?;

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
        let database = Database::create(db_path.clone())?;
        database.create_index("medium", &schema)?;

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
        database.update_index("medium", update)?;

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
        let database = Database::create(db_path.clone())?;
        database.create_index("big", &schema)?;

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
        database.update_index("big", update)?;

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
        let database = Database::create(db_path.clone())?;
        database.create_index("one-letter-little", &schema)?;

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
        let view = database.update_index("one-letter-little", update)?;

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
        let database = Database::create(db_path.clone())?;
        database.create_index("one-letter-medium", &schema)?;

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
        let view = database.update_index("one-letter-medium", update)?;

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
        let database = Database::create(db_path.clone())?;
        database.create_index("one-letter-big", &schema)?;

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
        let view = database.update_index("one-letter-big", update)?;

        bench.iter(|| {
            for q in &["a", "b", "c", "d", "e"] {
                let documents = view.query_builder().unwrap().query(q, 0..20);
                test::black_box(|| documents);
            }
        });

        Ok(())
    }
}
