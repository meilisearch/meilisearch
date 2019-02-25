use std::time::Instant;
use std::error::Error;
use std::ffi::OsStr;
use std::sync::Arc;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::ops::{Deref, DerefMut};

use rocksdb::rocksdb_options::{DBOptions, ColumnFamilyOptions};
use rocksdb::rocksdb::{Writable, Snapshot};
use rocksdb::{DB, MergeOperands};
use size_format::SizeFormatterBinary;
use arc_swap::ArcSwap;
use lockfree::map::Map;
use hashbrown::HashMap;
use log::{info, error, warn};

use crate::database::schema::SchemaAttr;
use meilidb_core::shared_data_cursor::FromSharedDataCursor;
use meilidb_core::write_to_bytes::WriteToBytes;
use meilidb_core::{Index, DocumentId};

use self::update::{ReadIndexEvent, ReadRankedMapEvent};

pub use self::config::Config;
pub use self::document_key::{DocumentKey, DocumentKeyAttr};
pub use self::view::{DatabaseView, DocumentIter};
pub use self::update::Update;
pub use self::serde::SerializerError;
pub use self::schema::Schema;
pub use self::number::{Number, ParseNumberError};

pub type RankedMap = HashMap<(DocumentId, SchemaAttr), Number>;

const DATA_INDEX:      &[u8] = b"data-index";
const DATA_RANKED_MAP: &[u8] = b"data-ranked-map";
const DATA_SCHEMA:     &[u8] = b"data-schema";
const CONFIG:          &[u8] = b"config";

pub mod config;
pub mod schema;
mod number;
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
    let start = Instant::now();
    let vector = snapshot.get(DATA_INDEX)?;
    info!("loading index from kv-store took {:.2?}", start.elapsed());

    match vector {
        Some(vector) => {
            let start = Instant::now();

            let bytes = vector.as_ref().to_vec();
            info!("index size is {}B", SizeFormatterBinary::new(bytes.len() as u64));

            let event = ReadIndexEvent::from_bytes(bytes)?;
            let index = event.updated_documents().expect("BUG: invalid event deserialized");

            info!("loading index from bytes took {:.2?}", start.elapsed());

            Ok(index)
        },
        None => Ok(Index::default()),
    }
}

fn retrieve_data_ranked_map<D>(snapshot: &Snapshot<D>) -> Result<RankedMap, Box<Error>>
where D: Deref<Target=DB>,
{
    let start = Instant::now();
    let vector = snapshot.get(DATA_RANKED_MAP)?;
    info!("loading ranked map from kv-store took {:.2?}", start.elapsed());

    match vector {
        Some(vector) => {
            let start = Instant::now();

            let bytes = vector.as_ref().to_vec();
            info!("ranked map size is {}B", SizeFormatterBinary::new(bytes.len() as u64));

            let event = ReadRankedMapEvent::from_bytes(bytes)?;
            let ranked_map = event.updated_documents().expect("BUG: invalid event deserialized");

            info!("loading ranked map from bytes took {:.2?}", start.elapsed());

            Ok(ranked_map)
        },
        None => Ok(RankedMap::new()),
    }
}

fn retrieve_config<D>(snapshot: &Snapshot<D>) -> Result<Config, Box<Error>>
where D: Deref<Target=DB>,
{
    match snapshot.get(CONFIG)? {
        Some(vector) => Ok(bincode::deserialize(&*vector)?),
        None => Ok(Config::default()),
    }
}

fn merge_indexes(existing: Option<&[u8]>, operands: &mut MergeOperands) -> Vec<u8> {
    use self::update::ReadIndexEvent::{self, *};
    use self::update::WriteIndexEvent;

    let mut index = Index::default();
    for bytes in existing.into_iter().chain(operands) {
        match ReadIndexEvent::from_bytes(bytes.to_vec()).unwrap() {
            RemovedDocuments(d) => index = index.remove_documents(d.as_ref()),
            UpdatedDocuments(i) => index = index.union(&i),
        }
    }

    WriteIndexEvent::UpdatedDocuments(&index).into_bytes()
}

fn merge_ranked_maps(existing: Option<&[u8]>, operands: &mut MergeOperands) -> Vec<u8> {
    use self::update::ReadRankedMapEvent::{self, *};
    use self::update::WriteRankedMapEvent;

    let mut ranked_map = RankedMap::default();
    for bytes in existing.into_iter().chain(operands) {
        match ReadRankedMapEvent::from_bytes(bytes.to_vec()).unwrap() {
            RemovedDocuments(d) => ranked_map.retain(|(k, _), _| !d.as_ref().binary_search(k).is_ok()),
            UpdatedDocuments(i) => ranked_map.extend(i),
        }
    }

    WriteRankedMapEvent::UpdatedDocuments(&ranked_map).into_bytes()
}

fn merge_operator(key: &[u8], existing: Option<&[u8]>, operands: &mut MergeOperands) -> Vec<u8> {
    match key {
        DATA_INDEX      => merge_indexes(existing, operands),
        DATA_RANKED_MAP => merge_ranked_maps(existing, operands),
        key             => panic!("The merge operator does not support merging {:?}", key),
    }
}

pub struct IndexUpdate {
    index: String,
    update: Update,
}

impl Deref for IndexUpdate {
    type Target = Update;

    fn deref(&self) -> &Update {
        &self.update
    }
}

impl DerefMut for IndexUpdate {
    fn deref_mut(&mut self) -> &mut Update {
        &mut self.update
    }
}

struct DatabaseIndex {
    db: Arc<DB>,

    // This view is updated each time the DB ingests an update.
    view: ArcSwap<DatabaseView<Arc<DB>>>,

    // The path of the mdb folder stored on disk.
    path: PathBuf,

    // must_die false by default, must be set as true when the Index is dropped.
    // It is used to erase the folder saved on disk when the user request to delete an index.
    must_die: AtomicBool,
}

impl DatabaseIndex {
    fn create<P: AsRef<Path>>(path: P, schema: &Schema) -> Result<DatabaseIndex, Box<Error>> {
        let path = path.as_ref();
        if path.exists() {
            return Err(format!("File already exists at path: {}, cannot create database.",
                                path.display()).into())
        }

        let path_lossy = path.to_string_lossy();
        let mut opts = DBOptions::new();
        opts.create_if_missing(true);
        // opts.error_if_exists(true); // FIXME pull request that

        let mut cf_opts = ColumnFamilyOptions::new();
        cf_opts.add_merge_operator("data merge operator", merge_operator);

        let db = DB::open_cf(opts, &path_lossy, vec![("default", cf_opts)])?;

        let mut schema_bytes = Vec::new();
        schema.write_to_bin(&mut schema_bytes)?;
        db.put(DATA_SCHEMA, &schema_bytes)?;

        let db = Arc::new(db);
        let snapshot = Snapshot::new(db.clone());
        let view = ArcSwap::new(Arc::new(DatabaseView::new(snapshot)?));

        Ok(DatabaseIndex {
            db: db,
            view: view,
            path: path.to_path_buf(),
            must_die: AtomicBool::new(false)
        })
    }

    fn open<P: AsRef<Path>>(path: P) -> Result<DatabaseIndex, Box<Error>> {
        let path_lossy = path.as_ref().to_string_lossy();

        let mut opts = DBOptions::new();
        opts.create_if_missing(false);

        let mut cf_opts = ColumnFamilyOptions::new();
        cf_opts.add_merge_operator("data merge operator", merge_operator);

        let db = DB::open_cf(opts, &path_lossy, vec![("default", cf_opts)])?;

        // FIXME create a generic function to do that !
        let _schema = match db.get(DATA_SCHEMA)? {
            Some(value) => Schema::read_from_bin(&*value)?,
            None => return Err(String::from("Database does not contain a schema").into()),
        };

        let db = Arc::new(db);
        let snapshot = Snapshot::new(db.clone());
        let view = ArcSwap::new(Arc::new(DatabaseView::new(snapshot)?));

        Ok(DatabaseIndex {
            db: db,
            view: view,
            path: path.as_ref().to_path_buf(),
            must_die: AtomicBool::new(false)
        })
    }

    fn must_die(&self) {
        self.must_die.store(true, Ordering::Relaxed)
    }

    fn start_update(&self) -> Result<Update, Box<Error>> {
        let schema = match self.db.get(DATA_SCHEMA)? {
            Some(value) => Schema::read_from_bin(&*value)?,
            None => panic!("Database does not contain a schema"),
        };

        Ok(Update::new(schema))
    }

    fn commit_update(&self, update: Update) -> Result<Arc<DatabaseView<Arc<DB>>>, Box<Error>> {
        let batch = update.build()?;
        self.db.write(batch)?;
        self.db.compact_range(None, None);
        self.db.flush(true)?;

        let snapshot = Snapshot::new(self.db.clone());
        let view = Arc::new(DatabaseView::new(snapshot)?);
        self.view.store(view.clone());

        Ok(view)
    }

    fn view(&self) -> Arc<DatabaseView<Arc<DB>>> {
        self.view.load()
    }

    fn get_config(&self) -> Config {
        self.view().config().clone()
    }

    fn update_config(&self, config: Config) -> Result<Arc<DatabaseView<Arc<DB>>>, Box<Error>>{
        let data = bincode::serialize(&config)?;
        self.db.put(CONFIG, &data)?;

        let snapshot = Snapshot::new(self.db.clone());
        let view = Arc::new(DatabaseView::new(snapshot)?);
        self.view.store(view.clone());

        Ok(view)
    }

    fn path(&self) -> &Path {
        self.path.as_path()
    }
}

impl Drop for DatabaseIndex {
    fn drop(&mut self) {
        if self.must_die.load(Ordering::Relaxed) {
            if let Err(err) = fs::remove_dir_all(&self.path) {
                error!("Impossible to remove mdb when Database is dropped; {}", err);
            }
        }
    }
}

pub struct Database {
    indexes: Map<String, Arc<DatabaseIndex>>,
    path: PathBuf,
}

impl Database {
    pub fn create<P: AsRef<Path>>(path: P) -> Result<Database, Box<Error>> {
        Ok(Database {
            indexes: Map::new(),
            path: path.as_ref().to_path_buf(),
        })
    }

    pub fn open<P: AsRef<Path>>(path: P) -> Result<Database, Box<Error>> {
        let entries = fs::read_dir(&path)?;

        let indexes = Map::new();
        for entry in entries {
            let path = match entry {
                Ok(p) => p.path(),
                Err(err) => {
                    warn!("Impossible to retrieve the path from an entry; {}", err);
                    continue
                }
            };

            let name = match path.file_stem().and_then(OsStr::to_str) {
                Some(name) => name.to_owned(),
                None => continue
            };

            let db = match DatabaseIndex::open(path.clone()) {
                Ok(db) => db,
                Err(err) => {
                    warn!("Impossible to open the database; {}", err);
                    continue
                }
            };

            info!("Load database {}", name);
            indexes.insert(name, Arc::new(db));
        }

        Ok(Database {
            indexes: indexes,
            path: path.as_ref().to_path_buf(),
        })
    }

    pub fn create_index(&self, name: &str, schema: &Schema) -> Result<(), Box<Error>> {
        let index_path = self.path.join(name);

        if index_path.exists() {
            return Err("Index already exists".into());
        }

        let index = DatabaseIndex::create(index_path, schema)?;
        self.indexes.insert(name.to_owned(), Arc::new(index));

        Ok(())
    }

    pub fn delete_index(&self, name: &str) -> Result<(), Box<Error>> {
        let index_guard = self.indexes.remove(name).ok_or("Index not found")?;
        index_guard.val().must_die();

        Ok(())
    }

    pub fn list_indexes(&self) -> Vec<String> {
        self.indexes.iter().map(|g| g.key().clone()).collect()
    }

    pub fn start_update(&self, index: &str) -> Result<IndexUpdate, Box<Error>> {
        let index_guard = self.indexes.get(index).ok_or("Index not found")?;
        let update = index_guard.val().start_update()?;

        Ok(IndexUpdate { index: index.to_owned(), update })
    }

    pub fn commit_update(&self, update: IndexUpdate)-> Result<Arc<DatabaseView<Arc<DB>>>, Box<Error>> {
        let index_guard = self.indexes.get(&update.index).ok_or("Index not found")?;

        index_guard.val().commit_update(update.update)
    }

    pub fn view(&self, index: &str) -> Result<Arc<DatabaseView<Arc<DB>>>, Box<Error>> {
        let index_guard = self.indexes.get(index).ok_or("Index not found")?;

        Ok(index_guard.val().view())
    }

    pub fn get_config(&self, index: &str) -> Result<Config, Box<Error>> {
        let index_guard = self.indexes.get(index).ok_or("Index not found")?;

        Ok(index_guard.val().get_config())
    }

    pub fn update_config(&self, index: &str, config: Config) -> Result<Arc<DatabaseView<Arc<DB>>>, Box<Error>>{
        let index_guard = self.indexes.get(index).ok_or("Index not found")?;

        Ok(index_guard.val().update_config(config)?)
    }

    pub fn path(&self) -> &Path {
        self.path.as_path()
    }

    pub fn index_path(&self, index: &str) -> Result<PathBuf, Box<Error>> {
        let index_guard = self.indexes.get(index).ok_or("Index not found")?;
        let path = index_guard.val().path();
        Ok(path.to_path_buf())
    }

}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;
    use std::error::Error;

    use serde_derive::{Serialize, Deserialize};
    use meilidb_tokenizer::DefaultBuilder;

    use crate::database::schema::{SchemaBuilder, STORED, INDEXED};

    use super::*;

    #[test]
    fn ingest_one_easy_update() -> Result<(), Box<Error>> {
        let dir = tempfile::tempdir()?;
        let stop_words = HashSet::new();

        let meilidb_path = dir.path().join("meilidb.mdb");
        let meilidb_index_name = "default";

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

        let database = Database::create(&meilidb_path)?;

        database.create_index(meilidb_index_name, &schema)?;

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
        let mut builder = database.start_update(meilidb_index_name)?;

        let docid0 = builder.update_document(&doc0, &tokenizer_builder, &stop_words)?;
        let docid1 = builder.update_document(&doc1, &tokenizer_builder, &stop_words)?;

        let view = database.commit_update(builder)?;

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
        let meilidb_index_name = "default";

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

        let database = Database::create(&meilidb_path)?;

        database.create_index(meilidb_index_name, &schema)?;

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

        let mut builder = database.start_update(meilidb_index_name)?;
        let docid0 = builder.update_document(&doc0, &tokenizer_builder, &stop_words)?;
        let docid1 = builder.update_document(&doc1, &tokenizer_builder, &stop_words)?;
        database.commit_update(builder)?;

        let mut builder = database.start_update(meilidb_index_name)?;
        let docid2 = builder.update_document(&doc2, &tokenizer_builder, &stop_words)?;
        let docid3 = builder.update_document(&doc3, &tokenizer_builder, &stop_words)?;
        let view = database.commit_update(builder)?;

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
        let index_name = "default";

        let database = Database::create(&db_path)?;
        database.create_index(index_name, &schema)?;

        #[derive(Serialize)]
        struct Document {
            id: u64,
            title: String,
            description: String,
        }

        let tokenizer_builder = DefaultBuilder;
        let mut builder = database.start_update(index_name)?;
        let mut rng = XorShiftRng::seed_from_u64(42);

        for i in 0..300 {
            let document = Document {
                id: i,
                title: random_sentences(rng.gen_range(1, 8), &mut rng),
                description: random_sentences(rng.gen_range(20, 200), &mut rng),
            };
            builder.update_document(&document, &tokenizer_builder, &stop_words)?;
        }

        database.commit_update(builder)?;

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
        let index_name = "default";

        let database = Database::create(&db_path)?;
        database.create_index(index_name, &schema)?;

        #[derive(Serialize)]
        struct Document {
            id: u64,
            title: String,
            description: String,
        }

        let tokenizer_builder = DefaultBuilder;
        let mut builder = database.start_update(index_name)?;
        let mut rng = XorShiftRng::seed_from_u64(42);

        for i in 0..3000 {
            let document = Document {
                id: i,
                title: random_sentences(rng.gen_range(1, 8), &mut rng),
                description: random_sentences(rng.gen_range(20, 200), &mut rng),
            };
            builder.update_document(&document, &tokenizer_builder, &stop_words)?;
        }

        database.commit_update(builder)?;

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
        let index_name = "default";

        let database = Database::create(&db_path)?;
        database.create_index(index_name, &schema)?;

        #[derive(Serialize)]
        struct Document {
            id: u64,
            title: String,
            description: String,
        }

        let tokenizer_builder = DefaultBuilder;
        let mut builder = database.start_update(index_name)?;
        let mut rng = XorShiftRng::seed_from_u64(42);

        for i in 0..30_000 {
            let document = Document {
                id: i,
                title: random_sentences(rng.gen_range(1, 8), &mut rng),
                description: random_sentences(rng.gen_range(20, 200), &mut rng),
            };
            builder.update_document(&document, &tokenizer_builder, &stop_words)?;
        }

        database.commit_update(builder)?;

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
        let index_name = "default";

        let database = Database::create(&db_path)?;
        database.create_index(index_name, &schema)?;

        #[derive(Serialize)]
        struct Document {
            id: u64,
            title: String,
            description: String,
        }

        let tokenizer_builder = DefaultBuilder;
        let mut builder = database.start_update(index_name)?;
        let mut rng = XorShiftRng::seed_from_u64(42);

        for i in 0..300 {
            let document = Document {
                id: i,
                title: random_sentences(rng.gen_range(1, 8), &mut rng),
                description: random_sentences(rng.gen_range(20, 200), &mut rng),
            };
            builder.update_document(&document, &tokenizer_builder, &stop_words)?;
        }

        let view = database.commit_update(builder)?;

        bench.iter(|| {
            for q in &["a", "b", "c", "d", "e"] {
                let documents = view.query_builder().query(q, 0..20);
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
        let index_name = "default";

        let database = Database::create(&db_path)?;
        database.create_index(index_name, &schema)?;

        #[derive(Serialize)]
        struct Document {
            id: u64,
            title: String,
            description: String,
        }

        let tokenizer_builder = DefaultBuilder;
        let mut builder = database.start_update(index_name)?;
        let mut rng = XorShiftRng::seed_from_u64(42);

        for i in 0..3000 {
            let document = Document {
                id: i,
                title: random_sentences(rng.gen_range(1, 8), &mut rng),
                description: random_sentences(rng.gen_range(20, 200), &mut rng),
            };
            builder.update_document(&document, &tokenizer_builder, &stop_words)?;
        }

        let view = database.commit_update(builder)?;

        bench.iter(|| {
            for q in &["a", "b", "c", "d", "e"] {
                let documents = view.query_builder().query(q, 0..20);
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
        let index_name = "default";

        let database = Database::create(&db_path)?;
        database.create_index(index_name, &schema)?;

        #[derive(Serialize)]
        struct Document {
            id: u64,
            title: String,
            description: String,
        }

        let tokenizer_builder = DefaultBuilder;
        let mut builder = database.start_update(index_name)?;
        let mut rng = XorShiftRng::seed_from_u64(42);

        for i in 0..30_000 {
            let document = Document {
                id: i,
                title: random_sentences(rng.gen_range(1, 8), &mut rng),
                description: random_sentences(rng.gen_range(20, 200), &mut rng),
            };
            builder.update_document(&document, &tokenizer_builder, &stop_words)?;
        }

        let view = database.commit_update(builder)?;

        bench.iter(|| {
            for q in &["a", "b", "c", "d", "e"] {
                let documents = view.query_builder().query(q, 0..20);
                test::black_box(|| documents);
            }
        });

        Ok(())
    }
}
