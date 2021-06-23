use std::borrow::Cow;
use std::collections::{HashMap, HashSet};
use std::path::Path;

use chrono::{DateTime, Utc};
use heed::flags::Flags;
use heed::types::*;
use heed::{Database, PolyDatabase, RoTxn, RwTxn};
use roaring::RoaringBitmap;

use crate::error::{FieldIdMapMissingEntry, InternalError, UserError};
use crate::fields_ids_map::FieldsIdsMap;
use crate::heed_codec::facet::{
    FacetLevelValueF64Codec, FacetStringLevelZeroCodec, FieldDocIdFacetF64Codec,
    FieldDocIdFacetStringCodec,
};
use crate::{
    default_criteria, BEU32StrCodec, BoRoaringBitmapCodec, CboRoaringBitmapCodec, Criterion,
    DocumentId, ExternalDocumentsIds, FacetDistribution, FieldDistribution, FieldId,
    FieldIdWordCountCodec, ObkvCodec, Result, RoaringBitmapCodec, RoaringBitmapLenCodec, Search,
    StrLevelPositionCodec, StrStrU8Codec, BEU32,
};

pub mod main_key {
    pub const CRITERIA_KEY: &str = "criteria";
    pub const DISPLAYED_FIELDS_KEY: &str = "displayed-fields";
    pub const DISTINCT_FIELD_KEY: &str = "distinct-field-key";
    pub const DOCUMENTS_IDS_KEY: &str = "documents-ids";
    pub const FILTERABLE_FIELDS_KEY: &str = "filterable-fields";
    pub const FIELD_DISTRIBUTION_KEY: &str = "fields-distribution";
    pub const FIELDS_IDS_MAP_KEY: &str = "fields-ids-map";
    pub const HARD_EXTERNAL_DOCUMENTS_IDS_KEY: &str = "hard-external-documents-ids";
    pub const NUMBER_FACETED_DOCUMENTS_IDS_PREFIX: &str = "number-faceted-documents-ids";
    pub const PRIMARY_KEY_KEY: &str = "primary-key";
    pub const SEARCHABLE_FIELDS_KEY: &str = "searchable-fields";
    pub const SOFT_EXTERNAL_DOCUMENTS_IDS_KEY: &str = "soft-external-documents-ids";
    pub const STOP_WORDS_KEY: &str = "stop-words";
    pub const STRING_FACETED_DOCUMENTS_IDS_PREFIX: &str = "string-faceted-documents-ids";
    pub const SYNONYMS_KEY: &str = "synonyms";
    pub const WORDS_FST_KEY: &str = "words-fst";
    pub const WORDS_PREFIXES_FST_KEY: &str = "words-prefixes-fst";
    pub const CREATED_AT_KEY: &str = "created-at";
    pub const UPDATED_AT_KEY: &str = "updated-at";
}

pub mod db_name {
    pub const MAIN: &str = "main";
    pub const WORD_DOCIDS: &str = "word-docids";
    pub const WORD_PREFIX_DOCIDS: &str = "word-prefix-docids";
    pub const DOCID_WORD_POSITIONS: &str = "docid-word-positions";
    pub const WORD_PAIR_PROXIMITY_DOCIDS: &str = "word-pair-proximity-docids";
    pub const WORD_PREFIX_PAIR_PROXIMITY_DOCIDS: &str = "word-prefix-pair-proximity-docids";
    pub const WORD_LEVEL_POSITION_DOCIDS: &str = "word-level-position-docids";
    pub const WORD_PREFIX_LEVEL_POSITION_DOCIDS: &str = "word-prefix-level-position-docids";
    pub const FIELD_ID_WORD_COUNT_DOCIDS: &str = "field-id-word-count-docids";
    pub const FACET_ID_F64_DOCIDS: &str = "facet-id-f64-docids";
    pub const FACET_ID_STRING_DOCIDS: &str = "facet-id-string-docids";
    pub const FIELD_ID_DOCID_FACET_F64S: &str = "field-id-docid-facet-f64s";
    pub const FIELD_ID_DOCID_FACET_STRINGS: &str = "field-id-docid-facet-strings";
    pub const DOCUMENTS: &str = "documents";
}

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
    /// Maps the field id and the word count with the docids that corresponds to it.
    pub field_id_word_count_docids: Database<FieldIdWordCountCodec, CboRoaringBitmapCodec>,
    /// Maps the level positions of a word prefix with all the docids where this prefix appears.
    pub word_prefix_level_position_docids: Database<StrLevelPositionCodec, CboRoaringBitmapCodec>,

    /// Maps the facet field id, level and the number with the docids that corresponds to it.
    pub facet_id_f64_docids: Database<FacetLevelValueF64Codec, CboRoaringBitmapCodec>,
    /// Maps the facet field id and the string with the docids that corresponds to it.
    pub facet_id_string_docids: Database<FacetStringLevelZeroCodec, CboRoaringBitmapCodec>,

    /// Maps the document id, the facet field id and the numbers.
    pub field_id_docid_facet_f64s: Database<FieldDocIdFacetF64Codec, Unit>,
    /// Maps the document id, the facet field id and the strings.
    pub field_id_docid_facet_strings: Database<FieldDocIdFacetStringCodec, Unit>,

    /// Maps the document id to the document as an obkv store.
    pub documents: Database<OwnedType<BEU32>, ObkvCodec>,
}

impl Index {
    pub fn new<P: AsRef<Path>>(mut options: heed::EnvOpenOptions, path: P) -> Result<Index> {
        use db_name::*;

        options.max_dbs(14);
        unsafe { options.flag(Flags::MdbAlwaysFreePages) };

        let env = options.open(path)?;
        let main = env.create_poly_database(Some(MAIN))?;
        let word_docids = env.create_database(Some(WORD_DOCIDS))?;
        let word_prefix_docids = env.create_database(Some(WORD_PREFIX_DOCIDS))?;
        let docid_word_positions = env.create_database(Some(DOCID_WORD_POSITIONS))?;
        let word_pair_proximity_docids = env.create_database(Some(WORD_PAIR_PROXIMITY_DOCIDS))?;
        let word_prefix_pair_proximity_docids =
            env.create_database(Some(WORD_PREFIX_PAIR_PROXIMITY_DOCIDS))?;
        let word_level_position_docids = env.create_database(Some(WORD_LEVEL_POSITION_DOCIDS))?;
        let field_id_word_count_docids = env.create_database(Some(FIELD_ID_WORD_COUNT_DOCIDS))?;
        let word_prefix_level_position_docids =
            env.create_database(Some(WORD_PREFIX_LEVEL_POSITION_DOCIDS))?;
        let facet_id_f64_docids = env.create_database(Some(FACET_ID_F64_DOCIDS))?;
        let facet_id_string_docids = env.create_database(Some(FACET_ID_STRING_DOCIDS))?;
        let field_id_docid_facet_f64s = env.create_database(Some(FIELD_ID_DOCID_FACET_F64S))?;
        let field_id_docid_facet_strings =
            env.create_database(Some(FIELD_ID_DOCID_FACET_STRINGS))?;
        let documents = env.create_database(Some(DOCUMENTS))?;

        Index::initialize_creation_dates(&env, main)?;

        Ok(Index {
            env,
            main,
            word_docids,
            word_prefix_docids,
            docid_word_positions,
            word_pair_proximity_docids,
            word_prefix_pair_proximity_docids,
            word_level_position_docids,
            word_prefix_level_position_docids,
            field_id_word_count_docids,
            facet_id_f64_docids,
            facet_id_string_docids,
            field_id_docid_facet_f64s,
            field_id_docid_facet_strings,
            documents,
        })
    }

    fn initialize_creation_dates(env: &heed::Env, main: PolyDatabase) -> heed::Result<()> {
        let mut txn = env.write_txn()?;
        // The db was just created, we update its metadata with the relevant information.
        if main.get::<_, Str, SerdeJson<DateTime<Utc>>>(&txn, main_key::CREATED_AT_KEY)?.is_none() {
            let now = Utc::now();
            main.put::<_, Str, SerdeJson<DateTime<Utc>>>(&mut txn, main_key::UPDATED_AT_KEY, &now)?;
            main.put::<_, Str, SerdeJson<DateTime<Utc>>>(&mut txn, main_key::CREATED_AT_KEY, &now)?;
            txn.commit()?;
        }
        Ok(())
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
    pub(crate) fn put_documents_ids(
        &self,
        wtxn: &mut RwTxn,
        docids: &RoaringBitmap,
    ) -> heed::Result<()> {
        self.main.put::<_, Str, RoaringBitmapCodec>(wtxn, main_key::DOCUMENTS_IDS_KEY, docids)
    }

    /// Returns the internal documents ids.
    pub fn documents_ids(&self, rtxn: &RoTxn) -> heed::Result<RoaringBitmap> {
        Ok(self
            .main
            .get::<_, Str, RoaringBitmapCodec>(rtxn, main_key::DOCUMENTS_IDS_KEY)?
            .unwrap_or_default())
    }

    /// Returns the number of documents indexed in the database.
    pub fn number_of_documents(&self, rtxn: &RoTxn) -> Result<u64> {
        let count =
            self.main.get::<_, Str, RoaringBitmapLenCodec>(rtxn, main_key::DOCUMENTS_IDS_KEY)?;
        Ok(count.unwrap_or_default())
    }

    /* primary key */

    /// Writes the documents primary key, this is the field name that is used to store the id.
    pub(crate) fn put_primary_key(&self, wtxn: &mut RwTxn, primary_key: &str) -> heed::Result<()> {
        self.set_updated_at(wtxn, &Utc::now())?;
        self.main.put::<_, Str, Str>(wtxn, main_key::PRIMARY_KEY_KEY, &primary_key)
    }

    /// Deletes the primary key of the documents, this can be done to reset indexes settings.
    pub(crate) fn delete_primary_key(&self, wtxn: &mut RwTxn) -> heed::Result<bool> {
        self.main.delete::<_, Str>(wtxn, main_key::PRIMARY_KEY_KEY)
    }

    /// Returns the documents primary key, `None` if it hasn't been defined.
    pub fn primary_key<'t>(&self, rtxn: &'t RoTxn) -> heed::Result<Option<&'t str>> {
        self.main.get::<_, Str, Str>(rtxn, main_key::PRIMARY_KEY_KEY)
    }

    /* external documents ids */

    /// Writes the external documents ids and internal ids (i.e. `u32`).
    pub(crate) fn put_external_documents_ids<'a>(
        &self,
        wtxn: &mut RwTxn,
        external_documents_ids: &ExternalDocumentsIds<'a>,
    ) -> heed::Result<()> {
        let ExternalDocumentsIds { hard, soft } = external_documents_ids;
        let hard = hard.as_fst().as_bytes();
        let soft = soft.as_fst().as_bytes();
        self.main.put::<_, Str, ByteSlice>(
            wtxn,
            main_key::HARD_EXTERNAL_DOCUMENTS_IDS_KEY,
            hard,
        )?;
        self.main.put::<_, Str, ByteSlice>(
            wtxn,
            main_key::SOFT_EXTERNAL_DOCUMENTS_IDS_KEY,
            soft,
        )?;
        Ok(())
    }

    /// Returns the external documents ids map which associate the external ids
    /// with the internal ids (i.e. `u32`).
    pub fn external_documents_ids<'t>(&self, rtxn: &'t RoTxn) -> Result<ExternalDocumentsIds<'t>> {
        let hard =
            self.main.get::<_, Str, ByteSlice>(rtxn, main_key::HARD_EXTERNAL_DOCUMENTS_IDS_KEY)?;
        let soft =
            self.main.get::<_, Str, ByteSlice>(rtxn, main_key::SOFT_EXTERNAL_DOCUMENTS_IDS_KEY)?;
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
    pub(crate) fn put_fields_ids_map(
        &self,
        wtxn: &mut RwTxn,
        map: &FieldsIdsMap,
    ) -> heed::Result<()> {
        self.main.put::<_, Str, SerdeJson<FieldsIdsMap>>(wtxn, main_key::FIELDS_IDS_MAP_KEY, map)
    }

    /// Returns the fields ids map which associate the documents keys with an internal field id
    /// (i.e. `u8`), this field id is used to identify fields in the obkv documents.
    pub fn fields_ids_map(&self, rtxn: &RoTxn) -> heed::Result<FieldsIdsMap> {
        Ok(self
            .main
            .get::<_, Str, SerdeJson<FieldsIdsMap>>(rtxn, main_key::FIELDS_IDS_MAP_KEY)?
            .unwrap_or_default())
    }

    /* field distribution */

    /// Writes the field distribution which associates every field name with
    /// the number of times it occurs in the documents.
    pub(crate) fn put_field_distribution(
        &self,
        wtxn: &mut RwTxn,
        distribution: &FieldDistribution,
    ) -> heed::Result<()> {
        self.main.put::<_, Str, SerdeJson<FieldDistribution>>(
            wtxn,
            main_key::FIELD_DISTRIBUTION_KEY,
            distribution,
        )
    }

    /// Returns the field distribution which associates every field name with
    /// the number of times it occurs in the documents.
    pub fn field_distribution(&self, rtxn: &RoTxn) -> heed::Result<FieldDistribution> {
        Ok(self
            .main
            .get::<_, Str, SerdeJson<FieldDistribution>>(rtxn, main_key::FIELD_DISTRIBUTION_KEY)?
            .unwrap_or_default())
    }

    /* displayed fields */

    /// Writes the fields that must be displayed in the defined order.
    /// There must be not be any duplicate field id.
    pub(crate) fn put_displayed_fields(
        &self,
        wtxn: &mut RwTxn,
        fields: &[&str],
    ) -> heed::Result<()> {
        self.main.put::<_, Str, SerdeBincode<&[&str]>>(
            wtxn,
            main_key::DISPLAYED_FIELDS_KEY,
            &fields,
        )
    }

    /// Deletes the displayed fields ids, this will make the engine to display
    /// all the documents attributes in the order of the `FieldsIdsMap`.
    pub(crate) fn delete_displayed_fields(&self, wtxn: &mut RwTxn) -> heed::Result<bool> {
        self.main.delete::<_, Str>(wtxn, main_key::DISPLAYED_FIELDS_KEY)
    }

    /// Returns the displayed fields in the order they were set by the user. If it returns
    /// `None` it means that all the attributes are set as displayed in the order of the `FieldsIdsMap`.
    pub fn displayed_fields<'t>(&self, rtxn: &'t RoTxn) -> heed::Result<Option<Vec<&'t str>>> {
        self.main.get::<_, Str, SerdeBincode<Vec<&'t str>>>(rtxn, main_key::DISPLAYED_FIELDS_KEY)
    }

    /// Identical to `displayed_fields`, but returns the ids instead.
    pub fn displayed_fields_ids(&self, rtxn: &RoTxn) -> Result<Option<Vec<FieldId>>> {
        match self.displayed_fields(rtxn)? {
            Some(fields) => {
                let fields_ids_map = self.fields_ids_map(rtxn)?;
                let mut fields_ids = Vec::new();
                for name in fields.into_iter() {
                    match fields_ids_map.id(name) {
                        Some(field_id) => fields_ids.push(field_id),
                        None => {
                            return Err(FieldIdMapMissingEntry::FieldName {
                                field_name: name.to_string(),
                                process: "Index::displayed_fields_ids",
                            }
                            .into())
                        }
                    }
                }
                Ok(Some(fields_ids))
            }
            None => Ok(None),
        }
    }

    /* searchable fields */

    /// Writes the searchable fields, when this list is specified, only these are indexed.
    pub(crate) fn put_searchable_fields(
        &self,
        wtxn: &mut RwTxn,
        fields: &[&str],
    ) -> heed::Result<()> {
        self.main.put::<_, Str, SerdeBincode<&[&str]>>(
            wtxn,
            main_key::SEARCHABLE_FIELDS_KEY,
            &fields,
        )
    }

    /// Deletes the searchable fields, when no fields are specified, all fields are indexed.
    pub(crate) fn delete_searchable_fields(&self, wtxn: &mut RwTxn) -> heed::Result<bool> {
        self.main.delete::<_, Str>(wtxn, main_key::SEARCHABLE_FIELDS_KEY)
    }

    /// Returns the searchable fields, those are the fields that are indexed,
    /// if the searchable fields aren't there it means that **all** the fields are indexed.
    pub fn searchable_fields<'t>(&self, rtxn: &'t RoTxn) -> heed::Result<Option<Vec<&'t str>>> {
        self.main.get::<_, Str, SerdeBincode<Vec<&'t str>>>(rtxn, main_key::SEARCHABLE_FIELDS_KEY)
    }

    /// Identical to `searchable_fields`, but returns the ids instead.
    pub fn searchable_fields_ids(&self, rtxn: &RoTxn) -> Result<Option<Vec<FieldId>>> {
        match self.searchable_fields(rtxn)? {
            Some(fields) => {
                let fields_ids_map = self.fields_ids_map(rtxn)?;
                let mut fields_ids = Vec::new();
                for name in fields {
                    match fields_ids_map.id(name) {
                        Some(field_id) => fields_ids.push(field_id),
                        None => {
                            return Err(FieldIdMapMissingEntry::FieldName {
                                field_name: name.to_string(),
                                process: "Index::searchable_fields_ids",
                            }
                            .into())
                        }
                    }
                }
                Ok(Some(fields_ids))
            }
            None => Ok(None),
        }
    }

    /* filterable fields */

    /// Writes the filterable fields names in the database.
    pub(crate) fn put_filterable_fields(
        &self,
        wtxn: &mut RwTxn,
        fields: &HashSet<String>,
    ) -> heed::Result<()> {
        self.main.put::<_, Str, SerdeJson<_>>(wtxn, main_key::FILTERABLE_FIELDS_KEY, fields)
    }

    /// Deletes the filterable fields ids in the database.
    pub(crate) fn delete_filterable_fields(&self, wtxn: &mut RwTxn) -> heed::Result<bool> {
        self.main.delete::<_, Str>(wtxn, main_key::FILTERABLE_FIELDS_KEY)
    }

    /// Returns the filterable fields names.
    pub fn filterable_fields(&self, rtxn: &RoTxn) -> heed::Result<HashSet<String>> {
        Ok(self
            .main
            .get::<_, Str, SerdeJson<_>>(rtxn, main_key::FILTERABLE_FIELDS_KEY)?
            .unwrap_or_default())
    }

    /// Identical to `filterable_fields`, but returns ids instead.
    pub fn filterable_fields_ids(&self, rtxn: &RoTxn) -> Result<HashSet<FieldId>> {
        let fields = self.filterable_fields(rtxn)?;
        let fields_ids_map = self.fields_ids_map(rtxn)?;

        let mut fields_ids = HashSet::new();
        for name in fields {
            match fields_ids_map.id(&name) {
                Some(field_id) => {
                    fields_ids.insert(field_id);
                }
                None => {
                    return Err(FieldIdMapMissingEntry::FieldName {
                        field_name: name,
                        process: "Index::filterable_fields_ids",
                    }
                    .into())
                }
            }
        }

        Ok(fields_ids)
    }

    /* faceted documents ids */

    /// Returns the faceted fields names.
    ///
    /// Faceted fields are the union of all the filterable, distinct, and Asc/Desc fields.
    pub fn faceted_fields(&self, rtxn: &RoTxn) -> Result<HashSet<String>> {
        let filterable_fields = self.filterable_fields(rtxn)?;
        let distinct_field = self.distinct_field(rtxn)?;
        let asc_desc_fields =
            self.criteria(rtxn)?.into_iter().filter_map(|criterion| match criterion {
                Criterion::Asc(field) | Criterion::Desc(field) => Some(field),
                _otherwise => None,
            });

        let mut faceted_fields = filterable_fields;
        faceted_fields.extend(asc_desc_fields);
        if let Some(field) = distinct_field {
            faceted_fields.insert(field.to_owned());
        }

        Ok(faceted_fields)
    }

    /// Identical to `faceted_fields`, but returns ids instead.
    pub fn faceted_fields_ids(&self, rtxn: &RoTxn) -> Result<HashSet<FieldId>> {
        let fields = self.faceted_fields(rtxn)?;
        let fields_ids_map = self.fields_ids_map(rtxn)?;

        let mut fields_ids = HashSet::new();
        for name in fields.into_iter() {
            match fields_ids_map.id(&name) {
                Some(field_id) => {
                    fields_ids.insert(field_id);
                }
                None => {
                    return Err(FieldIdMapMissingEntry::FieldName {
                        field_name: name,
                        process: "Index::faceted_fields_ids",
                    }
                    .into())
                }
            }
        }

        Ok(fields_ids)
    }

    /* faceted documents ids */

    /// Writes the documents ids that are faceted with numbers under this field id.
    pub(crate) fn put_number_faceted_documents_ids(
        &self,
        wtxn: &mut RwTxn,
        field_id: FieldId,
        docids: &RoaringBitmap,
    ) -> heed::Result<()> {
        let mut buffer = [0u8; main_key::STRING_FACETED_DOCUMENTS_IDS_PREFIX.len() + 2];
        buffer[..main_key::STRING_FACETED_DOCUMENTS_IDS_PREFIX.len()]
            .copy_from_slice(main_key::STRING_FACETED_DOCUMENTS_IDS_PREFIX.as_bytes());
        buffer[main_key::STRING_FACETED_DOCUMENTS_IDS_PREFIX.len()..]
            .copy_from_slice(&field_id.to_be_bytes());
        self.main.put::<_, ByteSlice, RoaringBitmapCodec>(wtxn, &buffer, docids)
    }

    /// Retrieve all the documents ids that faceted with numbers under this field id.
    pub fn number_faceted_documents_ids(
        &self,
        rtxn: &RoTxn,
        field_id: FieldId,
    ) -> heed::Result<RoaringBitmap> {
        let mut buffer = [0u8; main_key::STRING_FACETED_DOCUMENTS_IDS_PREFIX.len() + 2];
        buffer[..main_key::STRING_FACETED_DOCUMENTS_IDS_PREFIX.len()]
            .copy_from_slice(main_key::STRING_FACETED_DOCUMENTS_IDS_PREFIX.as_bytes());
        buffer[main_key::STRING_FACETED_DOCUMENTS_IDS_PREFIX.len()..]
            .copy_from_slice(&field_id.to_be_bytes());
        match self.main.get::<_, ByteSlice, RoaringBitmapCodec>(rtxn, &buffer)? {
            Some(docids) => Ok(docids),
            None => Ok(RoaringBitmap::new()),
        }
    }

    /// Writes the documents ids that are faceted with strings under this field id.
    pub(crate) fn put_string_faceted_documents_ids(
        &self,
        wtxn: &mut RwTxn,
        field_id: FieldId,
        docids: &RoaringBitmap,
    ) -> heed::Result<()> {
        let mut buffer = [0u8; main_key::NUMBER_FACETED_DOCUMENTS_IDS_PREFIX.len() + 2];
        buffer[..main_key::NUMBER_FACETED_DOCUMENTS_IDS_PREFIX.len()]
            .copy_from_slice(main_key::NUMBER_FACETED_DOCUMENTS_IDS_PREFIX.as_bytes());
        buffer[main_key::NUMBER_FACETED_DOCUMENTS_IDS_PREFIX.len()..]
            .copy_from_slice(&field_id.to_be_bytes());
        self.main.put::<_, ByteSlice, RoaringBitmapCodec>(wtxn, &buffer, docids)
    }

    /// Retrieve all the documents ids that faceted with strings under this field id.
    pub fn string_faceted_documents_ids(
        &self,
        rtxn: &RoTxn,
        field_id: FieldId,
    ) -> heed::Result<RoaringBitmap> {
        let mut buffer = [0u8; main_key::NUMBER_FACETED_DOCUMENTS_IDS_PREFIX.len() + 1];
        buffer[..main_key::NUMBER_FACETED_DOCUMENTS_IDS_PREFIX.len()]
            .copy_from_slice(main_key::NUMBER_FACETED_DOCUMENTS_IDS_PREFIX.as_bytes());
        buffer[main_key::NUMBER_FACETED_DOCUMENTS_IDS_PREFIX.len()..]
            .copy_from_slice(&field_id.to_be_bytes());
        match self.main.get::<_, ByteSlice, RoaringBitmapCodec>(rtxn, &buffer)? {
            Some(docids) => Ok(docids),
            None => Ok(RoaringBitmap::new()),
        }
    }

    /* distinct field */

    pub(crate) fn put_distinct_field(
        &self,
        wtxn: &mut RwTxn,
        distinct_field: &str,
    ) -> heed::Result<()> {
        self.main.put::<_, Str, Str>(wtxn, main_key::DISTINCT_FIELD_KEY, distinct_field)
    }

    pub fn distinct_field<'a>(&self, rtxn: &'a RoTxn) -> heed::Result<Option<&'a str>> {
        self.main.get::<_, Str, Str>(rtxn, main_key::DISTINCT_FIELD_KEY)
    }

    pub(crate) fn delete_distinct_field(&self, wtxn: &mut RwTxn) -> heed::Result<bool> {
        self.main.delete::<_, Str>(wtxn, main_key::DISTINCT_FIELD_KEY)
    }

    /* criteria */

    pub(crate) fn put_criteria(
        &self,
        wtxn: &mut RwTxn,
        criteria: &[Criterion],
    ) -> heed::Result<()> {
        self.main.put::<_, Str, SerdeJson<&[Criterion]>>(wtxn, main_key::CRITERIA_KEY, &criteria)
    }

    pub(crate) fn delete_criteria(&self, wtxn: &mut RwTxn) -> heed::Result<bool> {
        self.main.delete::<_, Str>(wtxn, main_key::CRITERIA_KEY)
    }

    pub fn criteria(&self, rtxn: &RoTxn) -> heed::Result<Vec<Criterion>> {
        match self.main.get::<_, Str, SerdeJson<Vec<Criterion>>>(rtxn, main_key::CRITERIA_KEY)? {
            Some(criteria) => Ok(criteria),
            None => Ok(default_criteria()),
        }
    }

    /* words fst */

    /// Writes the FST which is the words dictionary of the engine.
    pub(crate) fn put_words_fst<A: AsRef<[u8]>>(
        &self,
        wtxn: &mut RwTxn,
        fst: &fst::Set<A>,
    ) -> heed::Result<()> {
        self.main.put::<_, Str, ByteSlice>(wtxn, main_key::WORDS_FST_KEY, fst.as_fst().as_bytes())
    }

    /// Returns the FST which is the words dictionary of the engine.
    pub fn words_fst<'t>(&self, rtxn: &'t RoTxn) -> Result<fst::Set<Cow<'t, [u8]>>> {
        match self.main.get::<_, Str, ByteSlice>(rtxn, main_key::WORDS_FST_KEY)? {
            Some(bytes) => Ok(fst::Set::new(bytes)?.map_data(Cow::Borrowed)?),
            None => Ok(fst::Set::default().map_data(Cow::Owned)?),
        }
    }

    /* stop words */

    pub(crate) fn put_stop_words<A: AsRef<[u8]>>(
        &self,
        wtxn: &mut RwTxn,
        fst: &fst::Set<A>,
    ) -> heed::Result<()> {
        self.main.put::<_, Str, ByteSlice>(wtxn, main_key::STOP_WORDS_KEY, fst.as_fst().as_bytes())
    }

    pub(crate) fn delete_stop_words(&self, wtxn: &mut RwTxn) -> heed::Result<bool> {
        self.main.delete::<_, Str>(wtxn, main_key::STOP_WORDS_KEY)
    }

    pub fn stop_words<'t>(&self, rtxn: &'t RoTxn) -> Result<Option<fst::Set<&'t [u8]>>> {
        match self.main.get::<_, Str, ByteSlice>(rtxn, main_key::STOP_WORDS_KEY)? {
            Some(bytes) => Ok(Some(fst::Set::new(bytes)?)),
            None => Ok(None),
        }
    }

    /* synonyms */

    pub(crate) fn put_synonyms(
        &self,
        wtxn: &mut RwTxn,
        synonyms: &HashMap<Vec<String>, Vec<Vec<String>>>,
    ) -> heed::Result<()> {
        self.main.put::<_, Str, SerdeBincode<_>>(wtxn, main_key::SYNONYMS_KEY, synonyms)
    }

    pub(crate) fn delete_synonyms(&self, wtxn: &mut RwTxn) -> heed::Result<bool> {
        self.main.delete::<_, Str>(wtxn, main_key::SYNONYMS_KEY)
    }

    pub fn synonyms(&self, rtxn: &RoTxn) -> heed::Result<HashMap<Vec<String>, Vec<Vec<String>>>> {
        Ok(self
            .main
            .get::<_, Str, SerdeBincode<_>>(rtxn, main_key::SYNONYMS_KEY)?
            .unwrap_or_default())
    }

    pub fn words_synonyms<S: AsRef<str>>(
        &self,
        rtxn: &RoTxn,
        words: &[S],
    ) -> heed::Result<Option<Vec<Vec<String>>>> {
        let words: Vec<_> = words.iter().map(|s| s.as_ref().to_owned()).collect();
        Ok(self.synonyms(rtxn)?.remove(&words))
    }

    /* words prefixes fst */

    /// Writes the FST which is the words prefixes dictionnary of the engine.
    pub(crate) fn put_words_prefixes_fst<A: AsRef<[u8]>>(
        &self,
        wtxn: &mut RwTxn,
        fst: &fst::Set<A>,
    ) -> heed::Result<()> {
        self.main.put::<_, Str, ByteSlice>(
            wtxn,
            main_key::WORDS_PREFIXES_FST_KEY,
            fst.as_fst().as_bytes(),
        )
    }

    /// Returns the FST which is the words prefixes dictionnary of the engine.
    pub fn words_prefixes_fst<'t>(&self, rtxn: &'t RoTxn) -> Result<fst::Set<Cow<'t, [u8]>>> {
        match self.main.get::<_, Str, ByteSlice>(rtxn, main_key::WORDS_PREFIXES_FST_KEY)? {
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
        ids: impl IntoIterator<Item = DocumentId>,
    ) -> Result<Vec<(DocumentId, obkv::KvReaderU16<'t>)>> {
        let mut documents = Vec::new();

        for id in ids {
            let kv = self
                .documents
                .get(rtxn, &BEU32::new(id))?
                .ok_or_else(|| UserError::UnknownInternalDocumentId { document_id: id })?;
            documents.push((id, kv));
        }

        Ok(documents)
    }

    /// Returns an iterator over all the documents in the index.
    pub fn all_documents<'t>(
        &self,
        rtxn: &'t RoTxn,
    ) -> Result<impl Iterator<Item = heed::Result<(DocumentId, obkv::KvReaderU16<'t>)>>> {
        Ok(self
            .documents
            .iter(rtxn)?
            // we cast the BEU32 to a DocumentId
            .map(|document| document.map(|(id, obkv)| (id.get(), obkv))))
    }

    pub fn facets_distribution<'a>(&'a self, rtxn: &'a RoTxn) -> FacetDistribution<'a> {
        FacetDistribution::new(rtxn, self)
    }

    pub fn search<'a>(&'a self, rtxn: &'a RoTxn) -> Search<'a> {
        Search::new(rtxn, self)
    }

    /// Returns the index creation time.
    pub fn created_at(&self, rtxn: &RoTxn) -> Result<DateTime<Utc>> {
        Ok(self
            .main
            .get::<_, Str, SerdeJson<DateTime<Utc>>>(rtxn, main_key::CREATED_AT_KEY)?
            .ok_or(InternalError::DatabaseMissingEntry {
                db_name: db_name::MAIN,
                key: Some(main_key::CREATED_AT_KEY),
            })?)
    }

    /// Returns the index last updated time.
    pub fn updated_at(&self, rtxn: &RoTxn) -> Result<DateTime<Utc>> {
        Ok(self
            .main
            .get::<_, Str, SerdeJson<DateTime<Utc>>>(rtxn, main_key::UPDATED_AT_KEY)?
            .ok_or(InternalError::DatabaseMissingEntry {
                db_name: db_name::MAIN,
                key: Some(main_key::UPDATED_AT_KEY),
            })?)
    }

    pub(crate) fn set_updated_at(
        &self,
        wtxn: &mut RwTxn,
        time: &DateTime<Utc>,
    ) -> heed::Result<()> {
        self.main.put::<_, Str, SerdeJson<DateTime<Utc>>>(wtxn, main_key::UPDATED_AT_KEY, &time)
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use std::ops::Deref;

    use heed::EnvOpenOptions;
    use maplit::btreemap;
    use tempfile::TempDir;

    use crate::update::{IndexDocuments, UpdateFormat};
    use crate::Index;

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
            Self { inner, _tempdir }
        }
    }

    #[test]
    fn initial_field_distribution() {
        let path = tempfile::tempdir().unwrap();
        let mut options = EnvOpenOptions::new();
        options.map_size(10 * 1024 * 1024); // 10 MB
        let index = Index::new(options, &path).unwrap();

        let mut wtxn = index.write_txn().unwrap();
        let content = &br#"[
            { "id": 1, "name": "kevin" },
            { "id": 2, "name": "bob", "age": 20 },
            { "id": 2, "name": "bob", "age": 20 }
        ]"#[..];
        let mut builder = IndexDocuments::new(&mut wtxn, &index, 0);
        builder.update_format(UpdateFormat::Json);
        builder.execute(content, |_, _| ()).unwrap();
        wtxn.commit().unwrap();

        let rtxn = index.read_txn().unwrap();

        let field_distribution = index.field_distribution(&rtxn).unwrap();
        assert_eq!(
            field_distribution,
            btreemap! {
                "id".to_string() => 2,
                "name".to_string() => 2,
                "age".to_string() => 1,
            }
        );

        // we add all the documents a second time. we are supposed to get the same
        // field_distribution in the end
        let mut wtxn = index.write_txn().unwrap();
        let mut builder = IndexDocuments::new(&mut wtxn, &index, 0);
        builder.update_format(UpdateFormat::Json);
        builder.execute(content, |_, _| ()).unwrap();
        wtxn.commit().unwrap();

        let rtxn = index.read_txn().unwrap();

        let field_distribution = index.field_distribution(&rtxn).unwrap();
        assert_eq!(
            field_distribution,
            btreemap! {
                "id".to_string() => 2,
                "name".to_string() => 2,
                "age".to_string() => 1,
            }
        );

        // then we update a document by removing one field and another by adding one field
        let content = &br#"[
            { "id": 1, "name": "kevin", "has_dog": true },
            { "id": 2, "name": "bob" }
        ]"#[..];
        let mut wtxn = index.write_txn().unwrap();
        let mut builder = IndexDocuments::new(&mut wtxn, &index, 0);
        builder.update_format(UpdateFormat::Json);
        builder.execute(content, |_, _| ()).unwrap();
        wtxn.commit().unwrap();

        let rtxn = index.read_txn().unwrap();

        let field_distribution = index.field_distribution(&rtxn).unwrap();
        assert_eq!(
            field_distribution,
            btreemap! {
                "id".to_string() => 2,
                "name".to_string() => 2,
                "has_dog".to_string() => 1,
            }
        );
    }
}
