use std::error::Error;
use std::path::Path;
use std::ops::Deref;
use std::sync::Arc;

use rocksdb::rocksdb_options::{DBOptions, ColumnFamilyOptions};
use rocksdb::rocksdb::{Writable, Snapshot};
use rocksdb::{DB, MergeOperands};
use crossbeam::atomic::ArcCell;
use log::info;

pub use self::document_key::{DocumentKey, DocumentKeyAttr};
pub use self::view::{DatabaseView, DocumentIter};
pub use self::update::Update;
pub use self::serde::SerializerError;
pub use self::schema::Schema;
pub use self::index::Index;

const DATA_INDEX:  &[u8] = b"data-index";
const DATA_SCHEMA: &[u8] = b"data-schema";

pub mod schema;
pub(crate) mod index;
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
    let (elapsed, vector) = elapsed::measure_time(|| snapshot.get(DATA_INDEX));
    info!("loading index from kv-store took {}", elapsed);

    let index = match vector? {
        Some(vector) => {
            let bytes = vector.as_ref().to_vec();
            info!("index size if {} MiB", bytes.len() / 1024 / 1024);

            let (elapsed, index) = elapsed::measure_time(|| Index::from_bytes(bytes));
            info!("loading index from bytes took {}", elapsed);
            index?

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
    db: Arc<DB>,
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

        Ok(Database { db, view })
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

        Ok(Database { db, view })
    }

    pub fn update(&self) -> Result<Update, Box<Error>> {
        let schema = match self.db.get(DATA_SCHEMA)? {
            Some(value) => Schema::read_from_bin(&*value)?,
            None => panic!("Database does not contain a schema"),
        };

        Ok(Update::new(self, schema))
    }

    pub fn view(&self) -> Arc<DatabaseView<Arc<DB>>> {
        self.view.get()
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;
    use std::error::Error;

    use serde_derive::{Serialize, Deserialize};

    use crate::database::schema::{SchemaBuilder, STORED, INDEXED};
    use crate::tokenizer::DefaultBuilder;

    use super::*;

    #[test]
    fn ingest_one_easy_update() -> Result<(), Box<Error>> {
        let dir = tempfile::tempdir()?;
        let stop_words = HashSet::new();

        let meilidb_path = dir.path().join("meilidb.mdb");

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

        let database = Database::create(&meilidb_path, &schema)?;

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

        let tokenizer_builder = DefaultBuilder::new();
        let mut builder = database.update()?;

        let docid0 = builder.update_document(&doc0, &tokenizer_builder, &stop_words)?;
        let docid1 = builder.update_document(&doc1, &tokenizer_builder, &stop_words)?;

        let view = builder.commit()?;

        let de_doc0: SimpleDoc = view.document_by_id(docid0)?;
        let de_doc1: SimpleDoc = view.document_by_id(docid1)?;

        assert_eq!(doc0, de_doc0);
        assert_eq!(doc1, de_doc1);

        Ok(dir.close()?)
    }

    #[test]
    fn ingest_two_easy_updates() -> Result<(), Box<Error>> {
        let dir = tempfile::tempdir()?;
        let stop_words = HashSet::new();

        let meilidb_path = dir.path().join("meilidb.mdb");

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

        let database = Database::create(&meilidb_path, &schema)?;

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

        let tokenizer_builder = DefaultBuilder::new();

        let mut builder = database.update()?;
        let docid0 = builder.update_document(&doc0, &tokenizer_builder, &stop_words)?;
        let docid1 = builder.update_document(&doc1, &tokenizer_builder, &stop_words)?;
        builder.commit()?;

        let mut builder = database.update()?;
        let docid2 = builder.update_document(&doc2, &tokenizer_builder, &stop_words)?;
        let docid3 = builder.update_document(&doc3, &tokenizer_builder, &stop_words)?;
        let view = builder.commit()?;

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

    use std::collections::HashSet;
    use std::error::Error;
    use std::iter::repeat_with;
    use self::test::Bencher;

    use rand::distributions::Alphanumeric;
    use rand_xorshift::XorShiftRng;
    use rand::{Rng, SeedableRng};
    use serde_derive::Serialize;
    use rand::seq::SliceRandom;

    use crate::tokenizer::DefaultBuilder;
    use crate::database::schema::*;

    use super::*;

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

        let tokenizer_builder = DefaultBuilder;
        let mut builder = database.update()?;
        let mut rng = XorShiftRng::seed_from_u64(42);

        for i in 0..300 {
            let document = Document {
                id: i,
                title: random_sentences(rng.gen_range(1, 8), &mut rng),
                description: random_sentences(rng.gen_range(20, 200), &mut rng),
            };
            builder.update_document(&document, &tokenizer_builder, &stop_words)?;
        }

        builder.commit()?;

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

        let tokenizer_builder = DefaultBuilder;
        let mut builder = database.update()?;
        let mut rng = XorShiftRng::seed_from_u64(42);

        for i in 0..3000 {
            let document = Document {
                id: i,
                title: random_sentences(rng.gen_range(1, 8), &mut rng),
                description: random_sentences(rng.gen_range(20, 200), &mut rng),
            };
            builder.update_document(&document, &tokenizer_builder, &stop_words)?;
        }

        builder.commit()?;

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

        let tokenizer_builder = DefaultBuilder;
        let mut builder = database.update()?;
        let mut rng = XorShiftRng::seed_from_u64(42);

        for i in 0..30_000 {
            let document = Document {
                id: i,
                title: random_sentences(rng.gen_range(1, 8), &mut rng),
                description: random_sentences(rng.gen_range(20, 200), &mut rng),
            };
            builder.update_document(&document, &tokenizer_builder, &stop_words)?;
        }

        builder.commit()?;

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

        let tokenizer_builder = DefaultBuilder;
        let mut builder = database.update()?;
        let mut rng = XorShiftRng::seed_from_u64(42);

        for i in 0..300 {
            let document = Document {
                id: i,
                title: random_sentences(rng.gen_range(1, 8), &mut rng),
                description: random_sentences(rng.gen_range(20, 200), &mut rng),
            };
            builder.update_document(&document, &tokenizer_builder, &stop_words)?;
        }

        let view = builder.commit()?;

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

        let tokenizer_builder = DefaultBuilder;
        let mut builder = database.update()?;
        let mut rng = XorShiftRng::seed_from_u64(42);

        for i in 0..3000 {
            let document = Document {
                id: i,
                title: random_sentences(rng.gen_range(1, 8), &mut rng),
                description: random_sentences(rng.gen_range(20, 200), &mut rng),
            };
            builder.update_document(&document, &tokenizer_builder, &stop_words)?;
        }

        let view = builder.commit()?;

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

        let tokenizer_builder = DefaultBuilder;
        let mut builder = database.update()?;
        let mut rng = XorShiftRng::seed_from_u64(42);

        for i in 0..30_000 {
            let document = Document {
                id: i,
                title: random_sentences(rng.gen_range(1, 8), &mut rng),
                description: random_sentences(rng.gen_range(20, 200), &mut rng),
            };
            builder.update_document(&document, &tokenizer_builder, &stop_words)?;
        }

        let view = builder.commit()?;

        bench.iter(|| {
            for q in &["a", "b", "c", "d", "e"] {
                let documents = view.query_builder().unwrap().query(q, 0..20);
                test::black_box(|| documents);
            }
        });

        Ok(())
    }
}
