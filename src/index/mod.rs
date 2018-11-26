pub mod identifier;
pub mod schema;
pub mod update;

use std::error::Error;
use std::path::Path;

use fst::map::{Map, MapBuilder, OpBuilder};
use fst::{IntoStreamer, Streamer};
use sdset::duo::Union as SdUnion;
use sdset::duo::DifferenceByKey;
use sdset::{Set, SetOperation};
use ::rocksdb::rocksdb::Writable;
use ::rocksdb::{rocksdb, rocksdb_options};
use ::rocksdb::merge_operator::MergeOperands;

use crate::DocIndex;
use crate::automaton;
use crate::rank::Document;
use crate::index::schema::Schema;
use crate::index::update::Update;
use crate::tokenizer::TokenizerBuilder;
use crate::index::identifier::Identifier;
use crate::rank::{criterion, Config, RankedStream};
use crate::data::{DocIds, DocIndexes, RawDocIndexesBuilder};
use crate::blob::{PositiveBlob, NegativeBlob, Blob};

fn union_positives(a: &PositiveBlob, b: &PositiveBlob) -> Result<PositiveBlob, Box<Error>> {
    let (a_map, a_indexes) = (a.as_map(), a.as_indexes());
    let (b_map, b_indexes) = (b.as_map(), b.as_indexes());

    let mut map_builder = MapBuilder::memory();
    let mut indexes_builder = RawDocIndexesBuilder::memory();

    let op_builder = OpBuilder::new().add(a_map).add(b_map);
    let mut stream = op_builder.union();
    let mut i = 0;

    while let Some((key, indexed)) = stream.next() {
        let doc_idx: Vec<DocIndex> = match indexed {
            [a, b] => {
                let a_doc_idx = a_indexes.get(a.value).expect("BUG: could not find document indexes");
                let b_doc_idx = b_indexes.get(b.value).expect("BUG: could not find document indexes");

                let a_doc_idx = Set::new_unchecked(a_doc_idx);
                let b_doc_idx = Set::new_unchecked(b_doc_idx);

                let sd_union = SdUnion::new(a_doc_idx, b_doc_idx);
                sd_union.into_set_buf().into_vec()
            },
            [a] => {
                let indexes = if a.index == 0 { a_indexes } else { b_indexes };
                let doc_idx = indexes.get(a.value).expect("BUG: could not find document indexes");
                doc_idx.to_vec()
            },
            _ => unreachable!(),
        };

        if !doc_idx.is_empty() {
            map_builder.insert(key, i)?;
            indexes_builder.insert(&doc_idx)?;
            i += 1;
        }
    }

    let inner = map_builder.into_inner()?;
    let map = Map::from_bytes(inner)?;

    let inner = indexes_builder.into_inner()?;
    let indexes = DocIndexes::from_bytes(inner)?;

    Ok(PositiveBlob::from_raw(map, indexes))
}

fn union_negatives(a: &NegativeBlob, b: &NegativeBlob) -> NegativeBlob {
    let a_doc_ids = a.as_ids().doc_ids();
    let b_doc_ids = b.as_ids().doc_ids();

    let a_doc_ids = Set::new_unchecked(a_doc_ids);
    let b_doc_ids = Set::new_unchecked(b_doc_ids);

    let sd_union = SdUnion::new(a_doc_ids, b_doc_ids);
    let doc_ids = sd_union.into_set_buf().into_vec();
    let doc_ids = DocIds::from_document_ids(doc_ids);

    NegativeBlob::from_raw(doc_ids)
}

fn merge_positive_negative(pos: &PositiveBlob, neg: &NegativeBlob) -> Result<PositiveBlob, Box<Error>> {
    let (map, indexes) = (pos.as_map(), pos.as_indexes());
    let doc_ids = neg.as_ids().doc_ids();

    let doc_ids = Set::new_unchecked(doc_ids);

    let mut map_builder = MapBuilder::memory();
    let mut indexes_builder = RawDocIndexesBuilder::memory();

    let mut stream = map.into_stream();
    let mut i = 0;

    while let Some((key, index)) = stream.next() {
        let doc_idx = indexes.get(index).expect("BUG: could not find document indexes");
        let doc_idx = Set::new_unchecked(doc_idx);

        let diff = DifferenceByKey::new(doc_idx, doc_ids, |&d| d.document_id, |id| *id);
        let doc_idx: Vec<DocIndex> = diff.into_set_buf().into_vec();

        map_builder.insert(key, i)?;
        indexes_builder.insert(&doc_idx)?;
        i += 1;
    }

    let inner = map_builder.into_inner()?;
    let map = Map::from_bytes(inner)?;

    let inner = indexes_builder.into_inner()?;
    let indexes = DocIndexes::from_bytes(inner)?;

    Ok(PositiveBlob::from_raw(map, indexes))
}

#[derive(Default)]
struct Merge {
    blob: PositiveBlob,
}

impl Merge {
    fn new(blob: PositiveBlob) -> Merge {
        Merge { blob }
    }

    fn merge(&mut self, blob: Blob) {
        self.blob = match blob {
            Blob::Positive(blob) => union_positives(&self.blob, &blob).unwrap(),
            Blob::Negative(blob) => merge_positive_negative(&self.blob, &blob).unwrap(),
        };
    }

    fn build(self) -> PositiveBlob {
        self.blob
    }
}

fn merge_indexes(key: &[u8], existing_value: Option<&[u8]>, operands: &mut MergeOperands) -> Vec<u8> {
    if key != b"data-index" { panic!("The merge operator only supports \"data-index\" merging") }

    let mut merge = match existing_value {
        Some(existing_value) => {
            let blob = bincode::deserialize(existing_value).expect("BUG: could not deserialize data-index");
            Merge::new(blob)
        },
        None => Merge::default(),
    };

    for bytes in operands {
        let blob = bincode::deserialize(bytes).expect("BUG: could not deserialize blobs");
        merge.merge(blob);
    }

    let blob = merge.build();
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
        let data_key = Identifier::data().schema().build();
        database.put(&data_key, &schema_bytes)?;

        Ok(Self { database })
    }

    pub fn open<P: AsRef<Path>>(path: P) -> Result<Index, Box<Error>> {
        let path = path.as_ref().to_string_lossy();

        let mut opts = rocksdb_options::DBOptions::new();
        opts.create_if_missing(false);

        let mut cf_opts = rocksdb_options::ColumnFamilyOptions::new();
        cf_opts.add_merge_operator("data-index merge operator", merge_indexes);

        let database = rocksdb::DB::open_cf(opts, &path, vec![("default", cf_opts)])?;

        let data_key = Identifier::data().schema().build();
        let _schema = match database.get(&data_key)? {
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
        let data_key = Identifier::data().schema().build();
        let bytes = self.database.get(&data_key)?.expect("data-schema entry not found");
        Ok(Schema::read_from(&*bytes).expect("Invalid schema"))
    }

    pub fn search(&self, query: &str) -> Result<Vec<Document>, Box<Error>> {
        // this snapshot will allow consistent reads for the whole search operation
        let snapshot = self.database.snapshot();

        let index_key = Identifier::data().index().build();
        let map = match snapshot.get(&index_key)? {
            Some(value) => bincode::deserialize(&value)?,
            None => Vec::new(),
        };

        let mut automatons = Vec::new();
        for query in query.split_whitespace().map(str::to_lowercase) {
            let lev = automaton::build_prefix_dfa(&query);
            automatons.push(lev);
        }

        let config = Config {
            map: map,
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
