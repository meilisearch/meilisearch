use std::borrow::Cow;
use std::path::Path;

use anyhow::Context;
use heed::types::*;
use heed::{PolyDatabase, Database, RwTxn, RoTxn};
use roaring::RoaringBitmap;

use crate::Search;
use crate::{BEU32, DocumentId};
use crate::fields_ids_map::FieldsIdsMap;
use crate::{
    RoaringBitmapCodec, BEU32StrCodec, StrStrU8Codec, ObkvCodec,
    BoRoaringBitmapCodec, CboRoaringBitmapCodec,
};

pub const DISPLAYED_FIELDS_KEY: &str = "displayed-fields";
pub const DOCUMENTS_IDS_KEY: &str = "documents-ids";
pub const FIELDS_IDS_MAP_KEY: &str = "fields-ids-map";
pub const PRIMARY_KEY_KEY: &str = "primary-key";
pub const USERS_IDS_DOCUMENTS_IDS_KEY: &str = "users-ids-documents-ids";
pub const WORDS_FST_KEY: &str = "words-fst";

#[derive(Clone)]
pub struct Index {
    /// The LMDB environment which this index is associated with.
    pub env: heed::Env,
    /// Contains many different types (e.g. the fields ids map).
    pub main: PolyDatabase,
    /// A word and all the documents ids containing the word.
    pub word_docids: Database<Str, RoaringBitmapCodec>,
    /// Maps a word and a document id (u32) to all the positions where the given word appears.
    pub docid_word_positions: Database<BEU32StrCodec, BoRoaringBitmapCodec>,
    /// Maps the proximity between a pair of words with all the docids where this relation appears.
    pub word_pair_proximity_docids: Database<StrStrU8Codec, CboRoaringBitmapCodec>,
    /// Maps the document id to the document as an obkv store.
    pub documents: Database<OwnedType<BEU32>, ObkvCodec>,
}

impl Index {
    pub fn new<P: AsRef<Path>>(mut options: heed::EnvOpenOptions, path: P) -> anyhow::Result<Index> {
        options.max_dbs(5);

        let env = options.open(path)?;
        let main = env.create_poly_database(Some("main"))?;
        let word_docids = env.create_database(Some("word-docids"))?;
        let docid_word_positions = env.create_database(Some("docid-word-positions"))?;
        let word_pair_proximity_docids = env.create_database(Some("word-pair-proximity-docids"))?;
        let documents = env.create_database(Some("documents"))?;

        Ok(Index { env, main, word_docids, docid_word_positions, word_pair_proximity_docids, documents })
    }

    /// Create a write transaction to be able to write into the index.
    pub fn write_txn(&self) -> heed::Result<RwTxn> {
        self.env.write_txn()
    }

    /// Create a read transaction to be able to read the index.
    pub fn read_txn(&self) -> heed::Result<RoTxn> {
        self.env.read_txn()
    }

    /// Returns the canonicalized path where the heed `Env` of this `Index` lives.
    pub fn path(&self) -> &Path {
        self.env.path()
    }

    /// Returns an `EnvClosingEvent` that can be used to wait for the closing event,
    /// multiple threads can wait on this event.
    ///
    /// Make sure that you drop all the copies of `Index`es you have, env closing are triggered
    /// when all references are dropped, the last one will eventually close the environment.
    pub fn prepare_for_closing(self) -> heed::EnvClosingEvent {
        self.env.prepare_for_closing()
    }

    /* documents ids */

    /// Writes the documents ids that corresponds to the user-ids-documents-ids FST.
    pub fn put_documents_ids(&self, wtxn: &mut RwTxn, docids: &RoaringBitmap) -> heed::Result<()> {
        self.main.put::<_, Str, RoaringBitmapCodec>(wtxn, DOCUMENTS_IDS_KEY, docids)
    }

    /// Returns the internal documents ids.
    pub fn documents_ids(&self, rtxn: &RoTxn) -> heed::Result<RoaringBitmap> {
        Ok(self.main.get::<_, Str, RoaringBitmapCodec>(rtxn, DOCUMENTS_IDS_KEY)?.unwrap_or_default())
    }

    /* primary key */

    /// Writes the documents primary key, this is the field name that is used to store the id.
    pub fn put_primary_key(&self, wtxn: &mut RwTxn, primary_key: u8) -> heed::Result<()> {
        self.main.put::<_, Str, OwnedType<u8>>(wtxn, PRIMARY_KEY_KEY, &primary_key)
    }

    /// Delete the primary key of the documents, this can be done to reset indexes settings.
    pub fn delete_primary_key(&self, wtxn: &mut RwTxn) -> heed::Result<bool> {
        self.main.delete::<_, Str>(wtxn, PRIMARY_KEY_KEY)
    }

    /// Returns the documents primary key, `None` if it hasn't been defined.
    pub fn primary_key(&self, rtxn: &RoTxn) -> heed::Result<Option<u8>> {
        self.main.get::<_, Str, OwnedType<u8>>(rtxn, PRIMARY_KEY_KEY)
    }

    /* users ids documents ids */

    /// Writes the users ids documents ids, a user id is a byte slice (i.e. `[u8]`)
    /// and refers to an internal id (i.e. `u32`).
    pub fn put_users_ids_documents_ids<A: AsRef<[u8]>>(&self, wtxn: &mut RwTxn, fst: &fst::Map<A>) -> heed::Result<()> {
        self.main.put::<_, Str, ByteSlice>(wtxn, USERS_IDS_DOCUMENTS_IDS_KEY, fst.as_fst().as_bytes())
    }

    /// Returns the user ids documents ids map which associate the user ids (i.e. `[u8]`)
    /// with the internal ids (i.e. `u32`).
    pub fn users_ids_documents_ids<'t>(&self, rtxn: &'t RoTxn) -> anyhow::Result<fst::Map<Cow<'t, [u8]>>> {
        match self.main.get::<_, Str, ByteSlice>(rtxn, USERS_IDS_DOCUMENTS_IDS_KEY)? {
            Some(bytes) => Ok(fst::Map::new(bytes)?.map_data(Cow::Borrowed)?),
            None => Ok(fst::Map::default().map_data(Cow::Owned)?),
        }
    }

    /* fields ids map */

    /// Writes the fields ids map which associate the documents keys with an internal field id
    /// (i.e. `u8`), this field id is used to identify fields in the obkv documents.
    pub fn put_fields_ids_map(&self, wtxn: &mut RwTxn, map: &FieldsIdsMap) -> heed::Result<()> {
        self.main.put::<_, Str, SerdeJson<FieldsIdsMap>>(wtxn, FIELDS_IDS_MAP_KEY, map)
    }

    /// Returns the fields ids map which associate the documents keys with an internal field id
    /// (i.e. `u8`), this field id is used to identify fields in the obkv documents.
    pub fn fields_ids_map(&self, rtxn: &RoTxn) -> heed::Result<FieldsIdsMap> {
        Ok(self.main.get::<_, Str, SerdeJson<FieldsIdsMap>>(rtxn, FIELDS_IDS_MAP_KEY)?.unwrap_or_default())
    }

    /* displayed fields */

    /// Writes the fields ids that must be displayed in the defined order.
    /// There must be not be any duplicate field id.
    pub fn put_displayed_fields(&self, wtxn: &mut RwTxn, fields: &[u8]) -> heed::Result<()> {
        self.main.put::<_, Str, ByteSlice>(wtxn, DISPLAYED_FIELDS_KEY, fields)
    }

    /// Deletes the displayed fields ids, this will make the engine to display
    /// all the documents attributes in the order of the `FieldsIdsMap`.
    pub fn delete_displayed_fields(&self, wtxn: &mut RwTxn) -> heed::Result<bool> {
        self.main.delete::<_, Str>(wtxn, DISPLAYED_FIELDS_KEY)
    }

    /// Returns the displayed fields ids in the order they must be returned. If it returns
    /// `None` it means that all the attributes are displayed in the order of the `FieldsIdsMap`.
    pub fn displayed_fields<'t>(&self, rtxn: &'t RoTxn) -> heed::Result<Option<&'t [u8]>> {
        self.main.get::<_, Str, ByteSlice>(rtxn, DISPLAYED_FIELDS_KEY)
    }

    /* words fst */

    /// Writes the FST which is the words dictionnary of the engine.
    pub fn put_words_fst<A: AsRef<[u8]>>(&self, wtxn: &mut RwTxn, fst: &fst::Set<A>) -> heed::Result<()> {
        self.main.put::<_, Str, ByteSlice>(wtxn, WORDS_FST_KEY, fst.as_fst().as_bytes())
    }

    /// Returns the FST which is the words dictionnary of the engine.
    pub fn words_fst<'t>(&self, rtxn: &'t RoTxn) -> anyhow::Result<fst::Set<Cow<'t, [u8]>>> {
        match self.main.get::<_, Str, ByteSlice>(rtxn, WORDS_FST_KEY)? {
            Some(bytes) => Ok(fst::Set::new(bytes)?.map_data(Cow::Borrowed)?),
            None => Ok(fst::Set::default().map_data(Cow::Owned)?),
        }
    }

    /// Returns a [`Vec`] of the requested documents. Returns an error if a document is missing.
    pub fn documents<'t>(
        &self,
        rtxn: &'t RoTxn,
        ids: impl IntoIterator<Item=DocumentId>,
    ) -> anyhow::Result<Vec<(DocumentId, obkv::KvReader<'t>)>>
    {
        let mut documents = Vec::new();

        for id in ids {
            let kv = self.documents.get(rtxn, &BEU32::new(id))?
                .with_context(|| format!("Could not find document {}", id))?;
            documents.push((id, kv));
        }

        Ok(documents)
    }

    /// Returns the number of documents indexed in the database.
    pub fn number_of_documents(&self, rtxn: &RoTxn) -> anyhow::Result<usize> {
        Ok(self.documents_ids(rtxn).map(|docids| docids.len() as usize)?)
    }

    pub fn search<'a>(&'a self, rtxn: &'a RoTxn) -> Search<'a> {
        Search::new(rtxn, self)
    }
}
