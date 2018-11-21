pub mod blob_name;
pub mod schema;
pub mod update;

use std::io;
use std::rc::Rc;
use std::error::Error;
use std::fs::{self, File};
use std::fmt::{self, Write};
use std::ops::{Deref, BitOr};
use std::path::{Path, PathBuf};
use std::collections::{BTreeSet, BTreeMap};

use fs2::FileExt;
use ::rocksdb::rocksdb::Writable;
use ::rocksdb::{rocksdb, rocksdb_options};
use ::rocksdb::merge_operator::MergeOperands;

use crate::rank::Document;
use crate::data::DocIdsBuilder;
use crate::{DocIndex, DocumentId};
use crate::index::schema::Schema;
use crate::index::update::Update;
use crate::blob::{PositiveBlobBuilder, Blob, Sign};
use crate::blob::ordered_blobs_from_slice;
use crate::tokenizer::{TokenizerBuilder, DefaultBuilder, Tokenizer};
use crate::rank::{criterion, Config, RankedStream};
use crate::automaton;

const DATA_PREFIX: &str = "data";
const BLOB_PREFIX: &str = "blob";
const DOCU_PREFIX: &str = "docu";

const DATA_BLOBS_ORDER: &str = "data-blobs-order";
const DATA_SCHEMA:      &str = "data-schema";

fn simple_vec_append(key: &[u8], value: Option<&[u8]>, operands: &mut MergeOperands) -> Vec<u8> {
    let mut output = Vec::new();
    for bytes in operands.chain(value) {
        output.extend_from_slice(bytes);
    }
    output
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
        cf_opts.add_merge_operator("blobs order operator", simple_vec_append);

        let database = rocksdb::DB::open_cf(opts, &path, vec![("default", cf_opts)])?;

        let mut schema_bytes = Vec::new();
        schema.write_to(&mut schema_bytes)?;
        database.put(DATA_SCHEMA.as_bytes(), &schema_bytes)?;

        Ok(Self { database })
    }

    pub fn open<P: AsRef<Path>>(path: P) -> Result<Index, Box<Error>> {
        let path = path.as_ref().to_string_lossy();

        let mut opts = rocksdb_options::DBOptions::new();
        opts.create_if_missing(false);

        let mut cf_opts = rocksdb_options::ColumnFamilyOptions::new();
        cf_opts.add_merge_operator("blobs order operator", simple_vec_append);

        let database = rocksdb::DB::open_cf(opts, &path, vec![("default", cf_opts)])?;

        let _schema = match database.get(DATA_SCHEMA.as_bytes())? {
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
        let bytes = self.database.get(DATA_SCHEMA.as_bytes())?.expect("data-schema entry not found");
        Ok(Schema::read_from(&*bytes).expect("Invalid schema"))
    }

    pub fn search(&self, query: &str) -> Result<Vec<Document>, Box<Error>> {
        // this snapshot will allow consistent operations on documents
        let snapshot = self.database.snapshot();

        // FIXME create a SNAPSHOT for the search !
        let blobs = match snapshot.get(DATA_BLOBS_ORDER.as_bytes())? {
            Some(value) => ordered_blobs_from_slice(&value)?,
            None => Vec::new(),
        };

        let mut automatons = Vec::new();
        for query in query.split_whitespace().map(str::to_lowercase) {
            let lev = automaton::build_prefix_dfa(&query);
            automatons.push(lev);
        }

        let config = Config {
            blobs: &blobs,
            automatons: automatons,
            criteria: criterion::default(),
            distinct: ((), 1),
        };

        Ok(RankedStream::new(config).retrieve_documents(0..20))
    }
}

#[cfg(test)]
mod tests {
    use tempfile::NamedTempFile;

    use super::*;
    use crate::index::schema::Schema;
    use crate::index::update::{PositiveUpdateBuilder, NegativeUpdateBuilder};

    #[test]
    fn generate_negative_update() -> Result<(), Box<Error>> {
        let path = NamedTempFile::new()?.into_temp_path();
        let mut builder = NegativeUpdateBuilder::new(&path);

        // you can insert documents in any order, it is sorted internally
        builder.remove(1);
        builder.remove(5);
        builder.remove(2);

        let update = builder.build()?;

        assert_eq!(update.info().sign, Sign::Negative);

        Ok(())
    }

    #[test]
    fn generate_positive_update() -> Result<(), Box<Error>> {

        let schema = Schema::open("/meili/default.sch")?;
        let tokenizer_builder = DefaultBuilder::new();
        let mut builder = PositiveUpdateBuilder::new("update-positive-0001.sst", schema.clone(), tokenizer_builder);

        // you can insert documents in any order, it is sorted internally
        let title_field = schema.attribute("title").unwrap();
        builder.update_field(1, title_field, "hallo!".to_owned());
        builder.update_field(5, title_field, "hello!".to_owned());
        builder.update_field(2, title_field, "hi!".to_owned());

        let name_field = schema.attribute("name").unwrap();
        builder.remove_field(4, name_field);

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
