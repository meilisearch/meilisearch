use std::error::Error;
use std::path::Path;
use std::fmt;

use rocksdb::rocksdb_options::{DBOptions, IngestExternalFileOptions, ColumnFamilyOptions};
use rocksdb::{DB, DBVector, MergeOperands, SeekKey};
use rocksdb::rocksdb::Writable;

pub use crate::database::database_view::DatabaseView;
pub use crate::database::document_key::{DocumentKey, DocumentKeyAttr};
use crate::index::update::Update;
use crate::index::schema::Schema;
use crate::blob::{self, Blob};

mod document_key;
mod database_view;
mod deserializer;

const DATA_INDEX:  &[u8] = b"data-index";
const DATA_SCHEMA: &[u8] = b"data-schema";

pub struct Database(DB);

impl Database {
    pub fn create<P: AsRef<Path>>(path: P, schema: Schema) -> Result<Database, Box<Error>> {
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
        schema.write_to(&mut schema_bytes)?;
        db.put(DATA_SCHEMA, &schema_bytes)?;

        Ok(Database(db))
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
            Some(value) => Schema::read_from(&*value)?,
            None => return Err(String::from("Database does not contain a schema").into()),
        };

        Ok(Database(db))
    }

    pub fn ingest_update_file(&self, update: Update) -> Result<(), Box<Error>> {
        let move_update = update.can_be_moved();
        let path = update.into_path_buf();
        let path = path.to_string_lossy();

        let mut options = IngestExternalFileOptions::new();
        options.move_files(move_update);

        let cf_handle = self.0.cf_handle("default").unwrap();
        self.0.ingest_external_file_optimized(&cf_handle, &options, &[&path])?;

        // compacting to avoid calling the merge operator
        self.0.compact_range(Some(DATA_INDEX), Some(DATA_INDEX));

        Ok(())
    }

    pub fn get(&self, key: &[u8]) -> Result<Option<DBVector>, Box<Error>> {
        Ok(self.0.get(key)?)
    }

    pub fn flush(&self) -> Result<(), Box<Error>> {
        Ok(self.0.flush(true)?)
    }

    pub fn view(&self) -> Result<DatabaseView, Box<Error>> {
        let snapshot = self.0.snapshot();
        DatabaseView::new(snapshot)
    }
}

impl fmt::Debug for Database {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Database([")?;
        let mut iter = self.0.iter();
        iter.seek(SeekKey::Start);
        let mut first = true;
        for (key, value) in &mut iter {
            if !first { write!(f, ", ")?; }
            first = false;
            let key = String::from_utf8_lossy(&key);
            write!(f, "{:?}", key)?;
        }
        write!(f, "])")
    }
}

fn merge_indexes(key: &[u8], existing_value: Option<&[u8]>, operands: &mut MergeOperands) -> Vec<u8> {
    if key != DATA_INDEX {
        panic!("The merge operator only supports \"data-index\" merging")
    }

    let capacity = {
        let remaining = operands.size_hint().0;
        let already_exist = usize::from(existing_value.is_some());
        remaining + already_exist
    };

    let mut op = blob::OpBuilder::with_capacity(capacity);
    if let Some(existing_value) = existing_value {
        let blob = bincode::deserialize(existing_value).expect("BUG: could not deserialize data-index");
        op.push(Blob::Positive(blob));
    }

    for bytes in operands {
        let blob = bincode::deserialize(bytes).expect("BUG: could not deserialize blob");
        op.push(blob);
    }

    let blob = op.merge().expect("BUG: could not merge blobs");
    bincode::serialize(&blob).expect("BUG: could not serialize merged blob")
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::error::Error;
    use std::path::PathBuf;

    use serde_derive::{Serialize, Deserialize};
    use tempfile::tempdir;

    use crate::tokenizer::DefaultBuilder;
    use crate::index::update::PositiveUpdateBuilder;
    use crate::index::schema::{Schema, SchemaBuilder, STORED, INDEXED};

    #[test]
    fn ingest_update_file() -> Result<(), Box<Error>> {
        let dir = tempdir()?;

        let rocksdb_path = dir.path().join("rocksdb.rdb");

        #[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
        struct SimpleDoc {
            title: String,
            description: String,
        }

        let title;
        let description;
        let schema = {
            let mut builder = SchemaBuilder::new();
            title = builder.new_attribute("title", STORED | INDEXED);
            description = builder.new_attribute("description", STORED | INDEXED);
            builder.build()
        };

        let database = Database::create(&rocksdb_path, schema.clone())?;
        let tokenizer_builder = DefaultBuilder::new();

        let update_path = dir.path().join("update.sst");

        let doc0 = SimpleDoc {
            title: String::from("I am a title"),
            description: String::from("I am a description"),
        };
        let doc1 = SimpleDoc {
            title: String::from("I am the second title"),
            description: String::from("I am the second description"),
        };

        let mut update = {
            let mut builder = PositiveUpdateBuilder::new(update_path, schema, tokenizer_builder);

            // builder.update_field(0, title, doc0.title.clone());
            // builder.update_field(0, description, doc0.description.clone());

            // builder.update_field(1, title, doc1.title.clone());
            // builder.update_field(1, description, doc1.description.clone());

            builder.update(0, &doc0).unwrap();
            builder.update(1, &doc1).unwrap();

            builder.build()?
        };

        update.set_move(true);
        database.ingest_update_file(update)?;
        let view = database.view()?;

        println!("{:?}", view);

        #[derive(Deserialize, Debug, Clone, PartialEq, Eq)]
        struct DeSimpleDoc {
            title: char,
        }

        let de_doc0: DeSimpleDoc = view.retrieve_document(0)?;
        let de_doc1: DeSimpleDoc = view.retrieve_document(1)?;

        println!("{:?}", de_doc0);
        println!("{:?}", de_doc1);

        // assert_eq!(doc0, de_doc0);
        // assert_eq!(doc1, de_doc1);

        Ok(dir.close()?)
    }
}
