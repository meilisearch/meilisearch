use std::borrow::Cow;
use std::collections::HashMap;
use std::path::Path;

use anyhow::Context;
use chrono::{DateTime, Utc};
use heed::{Database, PolyDatabase, RoTxn, RwTxn};
use heed::types::*;
use roaring::RoaringBitmap;

use crate::{Criterion, default_criteria, FacetDistribution, FieldsDistribution, Search};
use crate::{BEU32, DocumentId, ExternalDocumentsIds, FieldId};
use crate::{
    BEU32StrCodec, BoRoaringBitmapCodec, CboRoaringBitmapCodec,
    ObkvCodec, RoaringBitmapCodec, RoaringBitmapLenCodec, StrLevelPositionCodec, StrStrU8Codec,
};
use crate::facet::FacetType;
use crate::fields_ids_map::FieldsIdsMap;

pub const CRITERIA_KEY: &str = "criteria";
pub const DISPLAYED_FIELDS_KEY: &str = "displayed-fields";
pub const DISTINCT_ATTRIBUTE_KEY: &str = "distinct-attribute-key";
pub const DOCUMENTS_IDS_KEY: &str = "documents-ids";
pub const FACETED_DOCUMENTS_IDS_PREFIX: &str = "faceted-documents-ids";
pub const FACETED_FIELDS_KEY: &str = "faceted-fields";
pub const FIELDS_IDS_MAP_KEY: &str = "fields-ids-map";
pub const FIELDS_DISTRIBUTION_KEY: &str = "fields-distribution";
pub const PRIMARY_KEY_KEY: &str = "primary-key";
pub const SEARCHABLE_FIELDS_KEY: &str = "searchable-fields";
pub const HARD_EXTERNAL_DOCUMENTS_IDS_KEY: &str = "hard-external-documents-ids";
pub const SOFT_EXTERNAL_DOCUMENTS_IDS_KEY: &str = "soft-external-documents-ids";
pub const WORDS_FST_KEY: &str = "words-fst";
pub const STOP_WORDS_KEY: &str = "stop-words";
pub const SYNONYMS_KEY: &str = "synonyms";
pub const WORDS_PREFIXES_FST_KEY: &str = "words-prefixes-fst";
const CREATED_AT_KEY: &str = "created-at";
const UPDATED_AT_KEY: &str = "updated-at";

#[derive(Clone)]
pub struct Index {
    /// The LMDB environment which this index is associated with.
    pub env: heed::Env,
    /// Contains many different types (e.g. the fields ids map).
    pub main: PolyDatabase,
    /// A word and all the documents ids containing the word.
    pub word_docids: Database<Str, RoaringBitmapCodec>,
    /// A prefix of word and all the documents ids containing this prefix.
    pub word_prefix_docids: Database<Str, RoaringBitmapCodec>,
    /// Maps a word and a document id (u32) to all the positions where the given word appears.
    pub docid_word_positions: Database<BEU32StrCodec, BoRoaringBitmapCodec>,
    /// Maps the proximity between a pair of words with all the docids where this relation appears.
    pub word_pair_proximity_docids: Database<StrStrU8Codec, CboRoaringBitmapCodec>,
    /// Maps the proximity between a pair of word and prefix with all the docids where this relation appears.
    pub word_prefix_pair_proximity_docids: Database<StrStrU8Codec, CboRoaringBitmapCodec>,
    /// Maps the word, level and position range with the docids that corresponds to it.
    pub word_level_position_docids: Database<StrLevelPositionCodec, CboRoaringBitmapCodec>,
    /// Maps the facet field id and the globally ordered value with the docids that corresponds to it.
    pub facet_field_id_value_docids: Database<ByteSlice, CboRoaringBitmapCodec>,
    /// Maps the document id, the facet field id and the globally ordered value.
    pub field_id_docid_facet_values: Database<ByteSlice, Unit>,
    /// Maps the document id to the document as an obkv store.
    pub documents: Database<OwnedType<BEU32>, ObkvCodec>,
}

impl Index {
    pub fn new<P: AsRef<Path>>(mut options: heed::EnvOpenOptions, path: P) -> anyhow::Result<Index> {
        options.max_dbs(10);

        let env = options.open(path)?;
        let main = env.create_poly_database(Some("main"))?;
        let word_docids = env.create_database(Some("word-docids"))?;
        let word_prefix_docids = env.create_database(Some("word-prefix-docids"))?;
        let docid_word_positions = env.create_database(Some("docid-word-positions"))?;
        let word_pair_proximity_docids = env.create_database(Some("word-pair-proximity-docids"))?;
        let word_prefix_pair_proximity_docids = env.create_database(Some("word-prefix-pair-proximity-docids"))?;
        let word_level_position_docids = env.create_database(Some("word-level-position-docids"))?;
        let facet_field_id_value_docids = env.create_database(Some("facet-field-id-value-docids"))?;
        let field_id_docid_facet_values = env.create_database(Some("field-id-docid-facet-values"))?;
        let documents = env.create_database(Some("documents"))?;

        {
            let mut txn = env.write_txn()?;
            // The db was just created, we update its metadata with the relevant information.
            if main.get::<_, Str, SerdeJson<DateTime<Utc>>>(&txn, CREATED_AT_KEY)?.is_none() {
                let now = Utc::now();
                main.put::<_, Str, SerdeJson<DateTime<Utc>>>(&mut txn, UPDATED_AT_KEY, &now)?;
                main.put::<_, Str, SerdeJson<DateTime<Utc>>>(&mut txn, CREATED_AT_KEY, &now)?;
                txn.commit()?;
            }
        }

        Ok(Index {
            env,
            main,
            word_docids,
            word_prefix_docids,
            docid_word_positions,
            word_pair_proximity_docids,
            word_prefix_pair_proximity_docids,
            word_level_position_docids,
            facet_field_id_value_docids,
            field_id_docid_facet_values,
            documents,
        })
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

    /// Returns the number of documents indexed in the database.
    pub fn number_of_documents(&self, rtxn: &RoTxn) -> anyhow::Result<u64> {
        let count = self.main.get::<_, Str, RoaringBitmapLenCodec>(rtxn, DOCUMENTS_IDS_KEY)?;
        Ok(count.unwrap_or_default())
    }

    /* primary key */

    /// Writes the documents primary key, this is the field name that is used to store the id.
    pub fn put_primary_key(&self, wtxn: &mut RwTxn, primary_key: &str) -> heed::Result<()> {
        self.set_updated_at(wtxn, &Utc::now())?;
        self.main.put::<_, Str, Str>(wtxn, PRIMARY_KEY_KEY, &primary_key)
    }

    /// Deletes the primary key of the documents, this can be done to reset indexes settings.
    pub fn delete_primary_key(&self, wtxn: &mut RwTxn) -> heed::Result<bool> {
        self.main.delete::<_, Str>(wtxn, PRIMARY_KEY_KEY)
    }

    /// Returns the documents primary key, `None` if it hasn't been defined.
    pub fn primary_key<'t>(&self, rtxn: &'t RoTxn) -> heed::Result<Option<&'t str>> {
        self.main.get::<_, Str, Str>(rtxn, PRIMARY_KEY_KEY)
    }

    /* external documents ids */

    /// Writes the external documents ids and internal ids (i.e. `u32`).
    pub fn put_external_documents_ids<'a>(
        &self,
        wtxn: &mut RwTxn,
        external_documents_ids: &ExternalDocumentsIds<'a>,
    ) -> heed::Result<()>
    {
        let ExternalDocumentsIds { hard, soft } = external_documents_ids;
        let hard = hard.as_fst().as_bytes();
        let soft = soft.as_fst().as_bytes();
        self.main.put::<_, Str, ByteSlice>(wtxn, HARD_EXTERNAL_DOCUMENTS_IDS_KEY, hard)?;
        self.main.put::<_, Str, ByteSlice>(wtxn, SOFT_EXTERNAL_DOCUMENTS_IDS_KEY, soft)?;
        Ok(())
    }

    /// Returns the external documents ids map which associate the external ids
    /// with the internal ids (i.e. `u32`).
    pub fn external_documents_ids<'t>(&self, rtxn: &'t RoTxn) -> anyhow::Result<ExternalDocumentsIds<'t>> {
        let hard = self.main.get::<_, Str, ByteSlice>(rtxn, HARD_EXTERNAL_DOCUMENTS_IDS_KEY)?;
        let soft = self.main.get::<_, Str, ByteSlice>(rtxn, SOFT_EXTERNAL_DOCUMENTS_IDS_KEY)?;
        let hard = match hard {
            Some(hard) => fst::Map::new(hard)?.map_data(Cow::Borrowed)?,
            None => fst::Map::default().map_data(Cow::Owned)?,
        };
        let soft = match soft {
            Some(soft) => fst::Map::new(soft)?.map_data(Cow::Borrowed)?,
            None => fst::Map::default().map_data(Cow::Owned)?,
        };
        Ok(ExternalDocumentsIds::new(hard, soft))
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

    /* fields distribution */

    /// Writes the fields distribution which associates every field name with
    /// the number of times it occurs in the documents.
    pub fn put_fields_distribution(&self, wtxn: &mut RwTxn, distribution: &FieldsDistribution) -> heed::Result<()> {
        self.main.put::<_, Str, SerdeJson<FieldsDistribution>>(wtxn, FIELDS_DISTRIBUTION_KEY, distribution)
    }

    /// Returns the fields distribution which associates every field name with
    /// the number of times it occurs in the documents.
    pub fn fields_distribution(&self, rtxn: &RoTxn) -> heed::Result<FieldsDistribution> {
        Ok(self.main.get::<_, Str, SerdeJson<FieldsDistribution>>(rtxn, FIELDS_DISTRIBUTION_KEY)?.unwrap_or_default())
    }

    /* displayed fields */

    /// Writes the fields that must be displayed in the defined order.
    /// There must be not be any duplicate field id.
    pub fn put_displayed_fields(&self, wtxn: &mut RwTxn, fields: &[&str]) -> heed::Result<()> {
        self.main.put::<_, Str, SerdeBincode<&[&str]>>(wtxn, DISPLAYED_FIELDS_KEY, &fields)
    }

    /// Deletes the displayed fields ids, this will make the engine to display
    /// all the documents attributes in the order of the `FieldsIdsMap`.
    pub fn delete_displayed_fields(&self, wtxn: &mut RwTxn) -> heed::Result<bool> {
        self.main.delete::<_, Str>(wtxn, DISPLAYED_FIELDS_KEY)
    }

    /// Returns the displayed fields in the order they were set by the user. If it returns
    /// `None` it means that all the attributes are set as displayed in the order of the `FieldsIdsMap`.
    pub fn displayed_fields<'t>(&self, rtxn: &'t RoTxn) -> heed::Result<Option<Vec<&'t str>>> {
        self.main.get::<_, Str, SerdeBincode<Vec<&'t str>>>(rtxn, DISPLAYED_FIELDS_KEY)
    }

    pub fn displayed_fields_ids(&self, rtxn: &RoTxn) -> heed::Result<Option<Vec<FieldId>>> {
        let fields_ids_map = self.fields_ids_map(rtxn)?;
        let ids = self.displayed_fields(rtxn)?
            .map(|fields| fields
                .into_iter()
                .map(|name| fields_ids_map.id(name).expect("Field not found"))
                .collect::<Vec<_>>());
        Ok(ids)
    }

    /* searchable fields */

    /// Writes the searchable fields, when this list is specified, only these are indexed.
    pub fn put_searchable_fields(&self, wtxn: &mut RwTxn, fields: &[&str]) -> heed::Result<()> {
        self.main.put::<_, Str, SerdeBincode<&[&str]>>(wtxn, SEARCHABLE_FIELDS_KEY, &fields)
    }

    /// Deletes the searchable fields, when no fields are specified, all fields are indexed.
    pub fn delete_searchable_fields(&self, wtxn: &mut RwTxn) -> heed::Result<bool> {
        self.main.delete::<_, Str>(wtxn, SEARCHABLE_FIELDS_KEY)
    }

    /// Returns the searchable fields, those are the fields that are indexed,
    /// if the searchable fields aren't there it means that **all** the fields are indexed.
    pub fn searchable_fields<'t>(&self, rtxn: &'t RoTxn) -> heed::Result<Option<Vec<&'t str>>> {
        self.main.get::<_, Str, SerdeBincode<Vec<&'t str>>>(rtxn, SEARCHABLE_FIELDS_KEY)
    }

    /// Identical to `searchable_fields`, but returns the ids instead.
    pub fn searchable_fields_ids(&self, rtxn: &RoTxn) -> heed::Result<Option<Vec<FieldId>>> {
        match self.searchable_fields(rtxn)? {
            Some(names) => {
                let fields_map = self.fields_ids_map(rtxn)?;
                let mut ids = Vec::new();
                for name in names {
                    let id = fields_map
                        .id(name)
                        .ok_or_else(|| format!("field id map must contain {:?}", name))
                        .expect("corrupted data: ");
                    ids.push(id);
                }
                Ok(Some(ids))
            }
            None => Ok(None),
        }
    }

    /* faceted fields */

    /// Writes the facet fields associated with their facet type or `None` if
    /// the facet type is currently unknown.
    pub fn put_faceted_fields(&self, wtxn: &mut RwTxn, fields_types: &HashMap<String, FacetType>) -> heed::Result<()> {
        self.main.put::<_, Str, SerdeJson<_>>(wtxn, FACETED_FIELDS_KEY, fields_types)
    }

    /// Deletes the facet fields ids associated with their facet type.
    pub fn delete_faceted_fields(&self, wtxn: &mut RwTxn) -> heed::Result<bool> {
        self.main.delete::<_, Str>(wtxn, FACETED_FIELDS_KEY)
    }

    /// Returns the facet fields names associated with their facet type.
    pub fn faceted_fields(&self, rtxn: &RoTxn) -> heed::Result<HashMap<String, FacetType>> {
        Ok(self.main.get::<_, Str, SerdeJson<_>>(rtxn, FACETED_FIELDS_KEY)?.unwrap_or_default())
    }

    /// Same as `faceted_fields`, but returns ids instead.
    pub fn faceted_fields_ids(&self, rtxn: &RoTxn) -> heed::Result<HashMap<FieldId, FacetType>> {
        let faceted_fields = self.faceted_fields(rtxn)?;
        let fields_ids_map = self.fields_ids_map(rtxn)?;
        let faceted_fields = faceted_fields
            .iter()
            .map(|(k, v)| {
                let kid = fields_ids_map
                    .id(k)
                    .ok_or_else(|| format!("{:?} should be present in the field id map", k))
                    .expect("corrupted data: ");
                (kid, *v)
            })
            .collect();
        Ok(faceted_fields)
    }

    /* faceted documents ids */

    /// Writes the documents ids that are faceted under this field id.
    pub fn put_faceted_documents_ids(&self, wtxn: &mut RwTxn, field_id: FieldId, docids: &RoaringBitmap) -> heed::Result<()> {
        let mut buffer = [0u8; FACETED_DOCUMENTS_IDS_PREFIX.len() + 1];
        buffer[..FACETED_DOCUMENTS_IDS_PREFIX.len()].clone_from_slice(FACETED_DOCUMENTS_IDS_PREFIX.as_bytes());
        *buffer.last_mut().unwrap() = field_id;
        self.main.put::<_, ByteSlice, RoaringBitmapCodec>(wtxn, &buffer, docids)
    }

    /// Retrieve all the documents ids that faceted under this field id.
    pub fn faceted_documents_ids(&self, rtxn: &RoTxn, field_id: FieldId) -> heed::Result<RoaringBitmap> {
        let mut buffer = [0u8; FACETED_DOCUMENTS_IDS_PREFIX.len() + 1];
        buffer[..FACETED_DOCUMENTS_IDS_PREFIX.len()].clone_from_slice(FACETED_DOCUMENTS_IDS_PREFIX.as_bytes());
        *buffer.last_mut().unwrap() = field_id;
        match self.main.get::<_, ByteSlice, RoaringBitmapCodec>(rtxn, &buffer)? {
            Some(docids) => Ok(docids),
            None => Ok(RoaringBitmap::new()),
        }
    }

    /* Distinct attribute */

    pub(crate) fn put_distinct_attribute(&self, wtxn: &mut RwTxn, distinct_attribute: &str) -> heed::Result<()> {
        self.main.put::<_, Str, Str>(wtxn, DISTINCT_ATTRIBUTE_KEY, distinct_attribute)
    }

    pub fn distinct_attribute<'a>(&self, rtxn: &'a RoTxn) -> heed::Result<Option<&'a str>> {
        self.main.get::<_, Str, Str>(rtxn, DISTINCT_ATTRIBUTE_KEY)
    }

    pub(crate) fn delete_distinct_attribute(&self, wtxn: &mut RwTxn) -> heed::Result<bool> {
        self.main.delete::<_, Str>(wtxn, DISTINCT_ATTRIBUTE_KEY)
    }

    /* criteria */

    pub fn put_criteria(&self, wtxn: &mut RwTxn, criteria: &[Criterion]) -> heed::Result<()> {
        self.main.put::<_, Str, SerdeJson<&[Criterion]>>(wtxn, CRITERIA_KEY, &criteria)
    }

    pub fn delete_criteria(&self, wtxn: &mut RwTxn) -> heed::Result<bool> {
        self.main.delete::<_, Str>(wtxn, CRITERIA_KEY)
    }

    pub fn criteria(&self, rtxn: &RoTxn) -> heed::Result<Vec<Criterion>> {
        match self.main.get::<_, Str, SerdeJson<Vec<Criterion>>>(rtxn, CRITERIA_KEY)? {
            Some(criteria) => Ok(criteria),
            None => Ok(default_criteria()),
        }
    }

    /* words fst */

    /// Writes the FST which is the words dictionary of the engine.
    pub fn put_words_fst<A: AsRef<[u8]>>(&self, wtxn: &mut RwTxn, fst: &fst::Set<A>) -> heed::Result<()> {
        self.main.put::<_, Str, ByteSlice>(wtxn, WORDS_FST_KEY, fst.as_fst().as_bytes())
    }

    /// Returns the FST which is the words dictionary of the engine.
    pub fn words_fst<'t>(&self, rtxn: &'t RoTxn) -> anyhow::Result<fst::Set<Cow<'t, [u8]>>> {
        match self.main.get::<_, Str, ByteSlice>(rtxn, WORDS_FST_KEY)? {
            Some(bytes) => Ok(fst::Set::new(bytes)?.map_data(Cow::Borrowed)?),
            None => Ok(fst::Set::default().map_data(Cow::Owned)?),
        }
    }

    /* stop words */

    pub fn put_stop_words<A: AsRef<[u8]>>(&self, wtxn: &mut RwTxn, fst: &fst::Set<A>) -> heed::Result<()> {
        self.main.put::<_, Str, ByteSlice>(wtxn, STOP_WORDS_KEY, fst.as_fst().as_bytes())
    }

    pub fn delete_stop_words(&self, wtxn: &mut RwTxn) -> heed::Result<bool> {
        self.main.delete::<_, Str>(wtxn, STOP_WORDS_KEY)
    }

    pub fn stop_words<'t>(&self, rtxn: &'t RoTxn) -> anyhow::Result<Option<fst::Set<&'t [u8]>>> {
        match self.main.get::<_, Str, ByteSlice>(rtxn, STOP_WORDS_KEY)? {
            Some(bytes) => Ok(Some(fst::Set::new(bytes)?)),
            None => Ok(None),
        }
    }

    /* synonyms */

    pub fn put_synonyms(&self, wtxn: &mut RwTxn, synonyms: &HashMap<Vec<String>, Vec<Vec<String>>>) -> heed::Result<()> {
        self.main.put::<_, Str, SerdeBincode<_>>(wtxn, SYNONYMS_KEY, synonyms)
    }

    pub fn delete_synonyms(&self, wtxn: &mut RwTxn) -> heed::Result<bool> {
        self.main.delete::<_, Str>(wtxn, SYNONYMS_KEY)
    }

    pub fn synonyms(&self, rtxn: &RoTxn) -> heed::Result<HashMap<Vec<String>, Vec<Vec<String>>>> {
        Ok(self.main.get::<_, Str, SerdeBincode<_>>(rtxn, SYNONYMS_KEY)?.unwrap_or_default())
    }

    pub fn words_synonyms<S: AsRef<str>>(&self, rtxn: &RoTxn, words: &[S]) -> heed::Result<Option<Vec<Vec<String>>>> {
        let words: Vec<_> = words.iter().map(|s| s.as_ref().to_owned()).collect();
        Ok(self.synonyms(rtxn)?.remove(&words))
    }

    /* words prefixes fst */

    /// Writes the FST which is the words prefixes dictionnary of the engine.
    pub fn put_words_prefixes_fst<A: AsRef<[u8]>>(&self, wtxn: &mut RwTxn, fst: &fst::Set<A>) -> heed::Result<()> {
        self.main.put::<_, Str, ByteSlice>(wtxn, WORDS_PREFIXES_FST_KEY, fst.as_fst().as_bytes())
    }

    /// Returns the FST which is the words prefixes dictionnary of the engine.
    pub fn words_prefixes_fst<'t>(&self, rtxn: &'t RoTxn) -> anyhow::Result<fst::Set<Cow<'t, [u8]>>> {
        match self.main.get::<_, Str, ByteSlice>(rtxn, WORDS_PREFIXES_FST_KEY)? {
            Some(bytes) => Ok(fst::Set::new(bytes)?.map_data(Cow::Borrowed)?),
            None => Ok(fst::Set::default().map_data(Cow::Owned)?),
        }
    }

    /* word documents count */

    /// Returns the number of documents ids associated with the given word,
    /// it is much faster than deserializing the bitmap and getting the length of it.
    pub fn word_documents_count(&self, rtxn: &RoTxn, word: &str) -> heed::Result<Option<u64>> {
        self.word_docids.remap_data_type::<RoaringBitmapLenCodec>().get(rtxn, word)
    }

    /* documents */

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

    pub fn facets_distribution<'a>(&'a self, rtxn: &'a RoTxn) -> FacetDistribution<'a> {
        FacetDistribution::new(rtxn, self)
    }

    pub fn search<'a>(&'a self, rtxn: &'a RoTxn) -> Search<'a> {
        Search::new(rtxn, self)
    }

    /// Returns the index creation time.
    pub fn created_at(&self, rtxn: &RoTxn) -> heed::Result<DateTime<Utc>> {
        let time = self.main
            .get::<_, Str, SerdeJson<DateTime<Utc>>>(rtxn, CREATED_AT_KEY)?
            .expect("Index without creation time");
        Ok(time)
    }

    /// Returns the index last updated time.
    pub fn updated_at(&self, rtxn: &RoTxn) -> heed::Result<DateTime<Utc>> {
        let time = self.main
            .get::<_, Str, SerdeJson<DateTime<Utc>>>(rtxn, UPDATED_AT_KEY)?
            .expect("Index without update time");
        Ok(time)
    }

    pub(crate) fn set_updated_at(&self, wtxn: &mut RwTxn, time: &DateTime<Utc>) -> heed::Result<()> {
        self.main.put::<_, Str, SerdeJson<DateTime<Utc>>>(wtxn, UPDATED_AT_KEY, &time)
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use std::ops::Deref;

    use heed::EnvOpenOptions;
    use maplit::hashmap;
    use tempfile::TempDir;

    use crate::Index;
    use crate::update::{IndexDocuments, UpdateFormat};

    pub(crate) struct TempIndex {
        inner: Index,
        _tempdir: TempDir,
    }

    impl Deref for TempIndex {
        type Target = Index;

        fn deref(&self) -> &Self::Target {
            &self.inner
        }
    }

    impl TempIndex {
        /// Creates a temporary index, with a default `4096 * 100` size. This should be enough for
        /// most tests.
        pub fn new() -> Self {
            let mut options = EnvOpenOptions::new();
            options.map_size(100 * 4096);
            let _tempdir = TempDir::new_in(".").unwrap();
            let inner = Index::new(options, _tempdir.path()).unwrap();
            Self {
                inner,
                _tempdir
            }
        }
    }

    #[test]
    fn initial_fields_distribution() {
        let path = tempfile::tempdir().unwrap();
        let mut options = EnvOpenOptions::new();
        options.map_size(10 * 1024 * 1024); // 10 MB
        let index = Index::new(options, &path).unwrap();

        let mut wtxn = index.write_txn().unwrap();
        let content = &br#"[
            { "name": "kevin" },
            { "name": "bob", "age": 20 }
        ]"#[..];
        let mut builder = IndexDocuments::new(&mut wtxn, &index, 0);
        builder.update_format(UpdateFormat::Json);
        builder.execute(content, |_, _| ()).unwrap();
        wtxn.commit().unwrap();

        let rtxn = index.read_txn().unwrap();

        let fields_distribution = index.fields_distribution(&rtxn).unwrap();
        assert_eq!(fields_distribution, hashmap! {
            "name".to_string() => 2,
            "age".to_string() => 1,
        });
    }
}
