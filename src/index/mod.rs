pub mod blob_name;
pub mod schema;
pub mod search;
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
use ::rocksdb::{rocksdb, rocksdb_options};
use ::rocksdb::merge_operator::MergeOperands;

use crate::rank::Document;
use crate::data::DocIdsBuilder;
use crate::{DocIndex, DocumentId};
use crate::index::{update::Update, search::Search};
use crate::blob::{PositiveBlobBuilder, Blob, Sign};
use crate::tokenizer::{TokenizerBuilder, DefaultBuilder, Tokenizer};

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
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Index, Box<Error>> {
        let path = path.as_ref().to_string_lossy();

        let mut opts = rocksdb_options::DBOptions::new();
        opts.create_if_missing(true);

        let mut cf_opts = rocksdb_options::ColumnFamilyOptions::new();
        cf_opts.add_merge_operator("blobs order operator", simple_vec_append);

        let database = rocksdb::DB::open_cf(opts, &path, vec![("default", cf_opts)])?;

        // check if index is a valid RocksDB and
        // contains the right key-values (i.e. "blobs-order")

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

    pub fn snapshot(&self) -> Snapshot<&rocksdb::DB> {
        Snapshot::new(&self.database)
    }
}

impl Search for Index {
    fn search(&self, text: &str) -> Vec<Document> {
        unimplemented!()
    }
}

pub struct Snapshot<D>
where D: Deref<Target=rocksdb::DB>,
{
    inner: rocksdb::Snapshot<D>,
}

impl<D> Snapshot<D>
where D: Deref<Target=rocksdb::DB>,
{
    pub fn new(inner: D) -> Snapshot<D> {
        Self { inner: rocksdb::Snapshot::new(inner) }
    }
}

impl<D> Search for Snapshot<D>
where D: Deref<Target=rocksdb::DB>,
{
    fn search(&self, text: &str) -> Vec<Document> {
        unimplemented!()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::index::schema::Schema;
    use crate::index::update::{PositiveUpdateBuilder, NegativeUpdateBuilder};

    #[test]
    fn generate_negative_update() -> Result<(), Box<Error>> {

        let schema = Schema::open("/meili/default.sch")?;
        let mut builder = NegativeUpdateBuilder::new("update-delete-0001.sst");

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
        let title_field = schema.field("title").unwrap();
        builder.update_field(1, title_field, "hallo!".to_owned());
        builder.update_field(5, title_field, "hello!".to_owned());
        builder.update_field(2, title_field, "hi!".to_owned());

        let name_field = schema.field("name").unwrap();
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

        let index = Index::open("/meili/data")?;
        let update = Update::open("update-0001.sst")?;

        // if you create a snapshot before an update
        let snapshot = index.snapshot();
        index.ingest_update(update)?;

        // the snapshot does not see the updates
        let results = snapshot.search("helo");

        // the raw index itself see new results
        let results = index.search("helo");

        Ok(())
    }
}
