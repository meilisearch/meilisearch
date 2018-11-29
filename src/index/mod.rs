pub mod schema;
pub mod update;

use std::error::Error;
use std::path::Path;

use ::rocksdb::rocksdb::Writable;
use ::rocksdb::{rocksdb, rocksdb_options};
use ::rocksdb::merge_operator::MergeOperands;

use crate::rank::Document;
use crate::index::schema::Schema;
use crate::index::update::Update;
use crate::rank::QueryBuilder;
use crate::blob::{self, Blob};

const DATA_INDEX: &[u8] =  b"data-index";
const DATA_SCHEMA: &[u8] = b"data-schema";

fn merge_indexes(key: &[u8], existing_value: Option<&[u8]>, operands: &mut MergeOperands) -> Vec<u8> {
    if key != DATA_INDEX { panic!("The merge operator only supports \"data-index\" merging") }

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

    let blob = op.merge().expect("BUG: could no merge blobs");
    bincode::serialize(&blob).expect("BUG: could not serialize merged blob")
}

pub struct Index {
    database: rocksdb::DB,
}

impl Index {
    pub fn create<P: AsRef<Path>>(path: P, schema: Schema) -> Result<Index, Box<Error>> {
        // Self::open must not take a parameter for create_if_missing
        // or we must create an OpenOptions with many parameters
        // https://doc.rust-lang.org/std/fs/struct.OpenOptions.html

        let path = path.as_ref();
        if path.exists() {
            return Err(format!("File already exists at path: {}, cannot create database.",
                                path.display()).into())
        }

        let path = path.to_string_lossy();
        let mut opts = rocksdb_options::DBOptions::new();
        opts.create_if_missing(true);

        let mut cf_opts = rocksdb_options::ColumnFamilyOptions::new();
        cf_opts.add_merge_operator("data-index merge operator", merge_indexes);

        let database = rocksdb::DB::open_cf(opts, &path, vec![("default", cf_opts)])?;

        let mut schema_bytes = Vec::new();
        schema.write_to(&mut schema_bytes)?;
        database.put(DATA_SCHEMA, &schema_bytes)?;

        Ok(Self { database })
    }

    pub fn open<P: AsRef<Path>>(path: P) -> Result<Index, Box<Error>> {
        let path = path.as_ref().to_string_lossy();

        let mut opts = rocksdb_options::DBOptions::new();
        opts.create_if_missing(false);

        let mut cf_opts = rocksdb_options::ColumnFamilyOptions::new();
        cf_opts.add_merge_operator("data-index merge operator", merge_indexes);

        let database = rocksdb::DB::open_cf(opts, &path, vec![("default", cf_opts)])?;

        let _schema = match database.get(DATA_SCHEMA)? {
            Some(value) => Schema::read_from(&*value)?,
            None => return Err(String::from("Database does not contain a schema").into()),
        };

        Ok(Self { database })
    }

    pub fn ingest_update(&self, update: Update) -> Result<(), Box<Error>> {
        let path = update.into_path_buf();
        let path = path.to_string_lossy();

        let mut options = rocksdb_options::IngestExternalFileOptions::new();
        // options.move_files(true);

        let cf_handle = self.database.cf_handle("default").unwrap();
        self.database.ingest_external_file_optimized(&cf_handle, &options, &[&path])?;

        Ok(())
    }

    pub fn schema(&self) -> Result<Schema, Box<Error>> {
        let bytes = self.database.get(DATA_SCHEMA)?.expect("data-schema entry not found");
        Ok(Schema::read_from(&*bytes).expect("Invalid schema"))
    }

    pub fn search(&self, query: &str) -> Result<Vec<Document>, Box<Error>> {
        // this snapshot will allow consistent reads for the whole search operation
        let snapshot = self.database.snapshot();

        let builder = QueryBuilder::new(snapshot)?;
        let documents = builder.query(query, 0..20);

        Ok(documents)
    }
}

#[cfg(test)]
mod tests {
    use tempfile::NamedTempFile;

    use super::*;
    use crate::index::schema::{Schema, SchemaBuilder, STORED, INDEXED};
    use crate::index::update::{PositiveUpdateBuilder, NegativeUpdateBuilder};

    #[test]
    fn generate_negative_update() -> Result<(), Box<Error>> {
        let path = NamedTempFile::new()?.into_temp_path();
        let mut builder = NegativeUpdateBuilder::new(&path);

        // you can insert documents in any order,
        // it is sorted internally
        builder.remove(1);
        builder.remove(5);
        builder.remove(2);

        let update = builder.build()?;

        assert_eq!(update.info().sign, Sign::Negative);

        Ok(())
    }

    #[test]
    fn generate_positive_update() -> Result<(), Box<Error>> {
        let title;
        let description;
        let schema = {
            let mut builder = SchemaBuilder::new();
            title =       builder.new_attribute("title",       STORED | INDEXED);
            description = builder.new_attribute("description", STORED | INDEXED);
            builder.build()
        };

        let sst_path = NamedTempFile::new()?.into_temp_path();
        let tokenizer_builder = DefaultBuilder::new();
        let mut builder = PositiveUpdateBuilder::new(&sst_path, schema.clone(), tokenizer_builder);

        // you can insert documents in any order,
        // it is sorted internally
        builder.update_field(1, title, "hallo!".to_owned());
        builder.update_field(5, title, "hello!".to_owned());
        builder.update_field(2, title, "hi!".to_owned());

        builder.remove_field(4, description);

        let update = builder.build()?;

        assert_eq!(update.info().sign, Sign::Positive);

        Ok(())
    }

    #[test]
    fn execution() -> Result<(), Box<Error>> {

        let index = Index::open("/meili/data")?;
        let update = Update::open("update-0001.sst")?;
        index.ingest_update(update)?;
        // directly apply changes to the database and see new results
        let results = index.search("helo");

        //////////////

        // let index = Index::open("/meili/data")?;
        // let update = Update::open("update-0001.sst")?;

        // // if you create a snapshot before an update
        // let snapshot = index.snapshot();
        // index.ingest_update(update)?;

        // // the snapshot does not see the updates
        // let results = snapshot.search("helo");

        // // the raw index itself see new results
        // let results = index.search("helo");

        Ok(())
    }
}
