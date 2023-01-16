use std::borrow::Cow;
use std::collections::{HashMap, HashSet};
use std::fs::File;
use std::mem::size_of;
use std::path::Path;

use heed::flags::Flags;
use heed::types::*;
use heed::{CompactionOption, Database, PolyDatabase, RoTxn, RwTxn};
use roaring::RoaringBitmap;
use rstar::RTree;
use time::OffsetDateTime;

use crate::error::{InternalError, UserError};
use crate::facet::FacetType;
use crate::fields_ids_map::FieldsIdsMap;
use crate::heed_codec::facet::{
    FacetGroupKeyCodec, FacetGroupValueCodec, FieldDocIdFacetF64Codec, FieldDocIdFacetStringCodec,
    FieldIdCodec, OrderedF64Codec,
};
use crate::heed_codec::StrRefCodec;
use crate::{
    default_criteria, BEU32StrCodec, BoRoaringBitmapCodec, CboRoaringBitmapCodec, Criterion,
    DocumentId, ExternalDocumentsIds, FacetDistribution, FieldDistribution, FieldId,
    FieldIdWordCountCodec, GeoPoint, ObkvCodec, Result, RoaringBitmapCodec, RoaringBitmapLenCodec,
    Search, StrBEU32Codec, U8StrStrCodec, BEU16, BEU32,
};

pub const DEFAULT_MIN_WORD_LEN_ONE_TYPO: u8 = 5;
pub const DEFAULT_MIN_WORD_LEN_TWO_TYPOS: u8 = 9;

pub mod main_key {
    pub const CRITERIA_KEY: &str = "criteria";
    pub const DISPLAYED_FIELDS_KEY: &str = "displayed-fields";
    pub const DISTINCT_FIELD_KEY: &str = "distinct-field-key";
    pub const DOCUMENTS_IDS_KEY: &str = "documents-ids";
    pub const SOFT_DELETED_DOCUMENTS_IDS_KEY: &str = "soft-deleted-documents-ids";
    pub const HIDDEN_FACETED_FIELDS_KEY: &str = "hidden-faceted-fields";
    pub const FILTERABLE_FIELDS_KEY: &str = "filterable-fields";
    pub const SORTABLE_FIELDS_KEY: &str = "sortable-fields";
    pub const FIELD_DISTRIBUTION_KEY: &str = "fields-distribution";
    pub const FIELDS_IDS_MAP_KEY: &str = "fields-ids-map";
    pub const GEO_FACETED_DOCUMENTS_IDS_KEY: &str = "geo-faceted-documents-ids";
    pub const GEO_RTREE_KEY: &str = "geo-rtree";
    pub const HARD_EXTERNAL_DOCUMENTS_IDS_KEY: &str = "hard-external-documents-ids";
    pub const NUMBER_FACETED_DOCUMENTS_IDS_PREFIX: &str = "number-faceted-documents-ids";
    pub const PRIMARY_KEY_KEY: &str = "primary-key";
    pub const SEARCHABLE_FIELDS_KEY: &str = "searchable-fields";
    pub const USER_DEFINED_SEARCHABLE_FIELDS_KEY: &str = "user-defined-searchable-fields";
    pub const SOFT_EXTERNAL_DOCUMENTS_IDS_KEY: &str = "soft-external-documents-ids";
    pub const STOP_WORDS_KEY: &str = "stop-words";
    pub const STRING_FACETED_DOCUMENTS_IDS_PREFIX: &str = "string-faceted-documents-ids";
    pub const SYNONYMS_KEY: &str = "synonyms";
    pub const WORDS_FST_KEY: &str = "words-fst";
    pub const WORDS_PREFIXES_FST_KEY: &str = "words-prefixes-fst";
    pub const CREATED_AT_KEY: &str = "created-at";
    pub const UPDATED_AT_KEY: &str = "updated-at";
    pub const AUTHORIZE_TYPOS: &str = "authorize-typos";
    pub const ONE_TYPO_WORD_LEN: &str = "one-typo-word-len";
    pub const TWO_TYPOS_WORD_LEN: &str = "two-typos-word-len";
    pub const EXACT_WORDS: &str = "exact-words";
    pub const EXACT_ATTRIBUTES: &str = "exact-attributes";
    pub const MAX_VALUES_PER_FACET: &str = "max-values-per-facet";
    pub const PAGINATION_MAX_TOTAL_HITS: &str = "pagination-max-total-hits";
}

pub mod db_name {
    pub const MAIN: &str = "main";
    pub const WORD_DOCIDS: &str = "word-docids";
    pub const EXACT_WORD_DOCIDS: &str = "exact-word-docids";
    pub const WORD_PREFIX_DOCIDS: &str = "word-prefix-docids";
    pub const EXACT_WORD_PREFIX_DOCIDS: &str = "exact-word-prefix-docids";
    pub const DOCID_WORD_POSITIONS: &str = "docid-word-positions";
    pub const WORD_PAIR_PROXIMITY_DOCIDS: &str = "word-pair-proximity-docids";
    pub const WORD_PREFIX_PAIR_PROXIMITY_DOCIDS: &str = "word-prefix-pair-proximity-docids";
    pub const PREFIX_WORD_PAIR_PROXIMITY_DOCIDS: &str = "prefix-word-pair-proximity-docids";
    pub const WORD_POSITION_DOCIDS: &str = "word-position-docids";
    pub const WORD_PREFIX_POSITION_DOCIDS: &str = "word-prefix-position-docids";
    pub const FIELD_ID_WORD_COUNT_DOCIDS: &str = "field-id-word-count-docids";
    pub const FACET_ID_F64_DOCIDS: &str = "facet-id-f64-docids";
    pub const FACET_ID_EXISTS_DOCIDS: &str = "facet-id-exists-docids";
    pub const FACET_ID_STRING_DOCIDS: &str = "facet-id-string-docids";
    pub const FIELD_ID_DOCID_FACET_F64S: &str = "field-id-docid-facet-f64s";
    pub const FIELD_ID_DOCID_FACET_STRINGS: &str = "field-id-docid-facet-strings";
    pub const DOCUMENTS: &str = "documents";
}

#[derive(Clone)]
pub struct Index {
    /// The LMDB environment which this index is associated with.
    pub(crate) env: heed::Env,

    /// Contains many different types (e.g. the fields ids map).
    pub(crate) main: PolyDatabase,

    /// A word and all the documents ids containing the word.
    pub word_docids: Database<Str, RoaringBitmapCodec>,

    /// A word and all the documents ids containing the word, from attributes for which typos are not allowed.
    pub exact_word_docids: Database<Str, RoaringBitmapCodec>,

    /// A prefix of word and all the documents ids containing this prefix.
    pub word_prefix_docids: Database<Str, RoaringBitmapCodec>,

    /// A prefix of word and all the documents ids containing this prefix, from attributes for which typos are not allowed.
    pub exact_word_prefix_docids: Database<Str, RoaringBitmapCodec>,

    /// Maps a word and a document id (u32) to all the positions where the given word appears.
    pub docid_word_positions: Database<BEU32StrCodec, BoRoaringBitmapCodec>,

    /// Maps the proximity between a pair of words with all the docids where this relation appears.
    pub word_pair_proximity_docids: Database<U8StrStrCodec, CboRoaringBitmapCodec>,
    /// Maps the proximity between a pair of word and prefix with all the docids where this relation appears.
    pub word_prefix_pair_proximity_docids: Database<U8StrStrCodec, CboRoaringBitmapCodec>,
    /// Maps the proximity between a pair of prefix and word with all the docids where this relation appears.
    pub prefix_word_pair_proximity_docids: Database<U8StrStrCodec, CboRoaringBitmapCodec>,

    /// Maps the word and the position with the docids that corresponds to it.
    pub word_position_docids: Database<StrBEU32Codec, CboRoaringBitmapCodec>,
    /// Maps the field id and the word count with the docids that corresponds to it.
    pub field_id_word_count_docids: Database<FieldIdWordCountCodec, CboRoaringBitmapCodec>,
    /// Maps the position of a word prefix with all the docids where this prefix appears.
    pub word_prefix_position_docids: Database<StrBEU32Codec, CboRoaringBitmapCodec>,

    /// Maps the facet field id and the docids for which this field exists
    pub facet_id_exists_docids: Database<FieldIdCodec, CboRoaringBitmapCodec>,

    /// Maps the facet field id and ranges of numbers with the docids that corresponds to them.
    pub facet_id_f64_docids: Database<FacetGroupKeyCodec<OrderedF64Codec>, FacetGroupValueCodec>,
    /// Maps the facet field id and ranges of strings with the docids that corresponds to them.
    pub facet_id_string_docids: Database<FacetGroupKeyCodec<StrRefCodec>, FacetGroupValueCodec>,

    /// Maps the document id, the facet field id and the numbers.
    pub field_id_docid_facet_f64s: Database<FieldDocIdFacetF64Codec, Unit>,
    /// Maps the document id, the facet field id and the strings.
    pub field_id_docid_facet_strings: Database<FieldDocIdFacetStringCodec, Str>,

    /// Maps the document id to the document as an obkv store.
    pub(crate) documents: Database<OwnedType<BEU32>, ObkvCodec>,
}

impl Index {
    pub fn new_with_creation_dates<P: AsRef<Path>>(
        mut options: heed::EnvOpenOptions,
        path: P,
        created_at: OffsetDateTime,
        updated_at: OffsetDateTime,
    ) -> Result<Index> {
        use db_name::*;

        options.max_dbs(18);
        unsafe { options.flag(Flags::MdbAlwaysFreePages) };

        let env = options.open(path)?;
        let main = env.create_poly_database(Some(MAIN))?;
        let word_docids = env.create_database(Some(WORD_DOCIDS))?;
        let exact_word_docids = env.create_database(Some(EXACT_WORD_DOCIDS))?;
        let word_prefix_docids = env.create_database(Some(WORD_PREFIX_DOCIDS))?;
        let exact_word_prefix_docids = env.create_database(Some(EXACT_WORD_PREFIX_DOCIDS))?;
        let docid_word_positions = env.create_database(Some(DOCID_WORD_POSITIONS))?;
        let word_pair_proximity_docids = env.create_database(Some(WORD_PAIR_PROXIMITY_DOCIDS))?;
        let word_prefix_pair_proximity_docids =
            env.create_database(Some(WORD_PREFIX_PAIR_PROXIMITY_DOCIDS))?;
        let prefix_word_pair_proximity_docids =
            env.create_database(Some(PREFIX_WORD_PAIR_PROXIMITY_DOCIDS))?;
        let word_position_docids = env.create_database(Some(WORD_POSITION_DOCIDS))?;
        let field_id_word_count_docids = env.create_database(Some(FIELD_ID_WORD_COUNT_DOCIDS))?;
        let word_prefix_position_docids = env.create_database(Some(WORD_PREFIX_POSITION_DOCIDS))?;
        let facet_id_f64_docids = env.create_database(Some(FACET_ID_F64_DOCIDS))?;
        let facet_id_string_docids = env.create_database(Some(FACET_ID_STRING_DOCIDS))?;
        let facet_id_exists_docids = env.create_database(Some(FACET_ID_EXISTS_DOCIDS))?;

        let field_id_docid_facet_f64s = env.create_database(Some(FIELD_ID_DOCID_FACET_F64S))?;
        let field_id_docid_facet_strings =
            env.create_database(Some(FIELD_ID_DOCID_FACET_STRINGS))?;
        let documents = env.create_database(Some(DOCUMENTS))?;

        Index::set_creation_dates(&env, main, created_at, updated_at)?;

        Ok(Index {
            env,
            main,
            word_docids,
            exact_word_docids,
            word_prefix_docids,
            exact_word_prefix_docids,
            docid_word_positions,
            word_pair_proximity_docids,
            word_prefix_pair_proximity_docids,
            prefix_word_pair_proximity_docids,
            word_position_docids,
            word_prefix_position_docids,
            field_id_word_count_docids,
            facet_id_f64_docids,
            facet_id_string_docids,
            facet_id_exists_docids,
            field_id_docid_facet_f64s,
            field_id_docid_facet_strings,
            documents,
        })
    }

    pub fn new<P: AsRef<Path>>(options: heed::EnvOpenOptions, path: P) -> Result<Index> {
        let now = OffsetDateTime::now_utc();
        Self::new_with_creation_dates(options, path, now, now)
    }

    fn set_creation_dates(
        env: &heed::Env,
        main: PolyDatabase,
        created_at: OffsetDateTime,
        updated_at: OffsetDateTime,
    ) -> heed::Result<()> {
        let mut txn = env.write_txn()?;
        // The db was just created, we update its metadata with the relevant information.
        if main.get::<_, Str, SerdeJson<OffsetDateTime>>(&txn, main_key::CREATED_AT_KEY)?.is_none()
        {
            main.put::<_, Str, SerdeJson<OffsetDateTime>>(
                &mut txn,
                main_key::UPDATED_AT_KEY,
                &updated_at,
            )?;
            main.put::<_, Str, SerdeJson<OffsetDateTime>>(
                &mut txn,
                main_key::CREATED_AT_KEY,
                &created_at,
            )?;
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

    /// Returns the size used by the index without the cached pages.
    pub fn used_size(&self) -> Result<u64> {
        Ok(self.env.non_free_pages_size()?)
    }

    /// Returns the real size used by the index.
    pub fn on_disk_size(&self) -> Result<u64> {
        Ok(self.env.real_disk_size()?)
    }

    /// Returns the map size the underlying environment was opened with, in bytes.
    ///
    /// This value does not represent the current on-disk size of the index.
    ///
    /// This value is the maximum between the map size passed during the opening of the index
    /// and the on-disk size of the index at the time of opening.
    pub fn map_size(&self) -> Result<usize> {
        Ok(self.env.map_size()?)
    }

    pub fn copy_to_path<P: AsRef<Path>>(&self, path: P, option: CompactionOption) -> Result<File> {
        self.env.copy_to_path(path, option).map_err(Into::into)
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

    /* deleted documents ids */

    /// Writes the soft deleted documents ids.
    pub(crate) fn put_soft_deleted_documents_ids(
        &self,
        wtxn: &mut RwTxn,
        docids: &RoaringBitmap,
    ) -> heed::Result<()> {
        self.main.put::<_, Str, RoaringBitmapCodec>(
            wtxn,
            main_key::SOFT_DELETED_DOCUMENTS_IDS_KEY,
            docids,
        )
    }

    /// Returns the soft deleted documents ids.
    pub(crate) fn soft_deleted_documents_ids(&self, rtxn: &RoTxn) -> heed::Result<RoaringBitmap> {
        Ok(self
            .main
            .get::<_, Str, RoaringBitmapCodec>(rtxn, main_key::SOFT_DELETED_DOCUMENTS_IDS_KEY)?
            .unwrap_or_default())
    }

    /* primary key */

    /// Writes the documents primary key, this is the field name that is used to store the id.
    pub(crate) fn put_primary_key(&self, wtxn: &mut RwTxn, primary_key: &str) -> heed::Result<()> {
        self.set_updated_at(wtxn, &OffsetDateTime::now_utc())?;
        self.main.put::<_, Str, Str>(wtxn, main_key::PRIMARY_KEY_KEY, primary_key)
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
        let ExternalDocumentsIds { hard, soft, .. } = external_documents_ids;
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
        let soft_deleted_docids = self.soft_deleted_documents_ids(rtxn)?;
        Ok(ExternalDocumentsIds::new(hard, soft, soft_deleted_docids))
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

    /* geo rtree */

    /// Writes the provided `rtree` which associates coordinates to documents ids.
    pub(crate) fn put_geo_rtree(
        &self,
        wtxn: &mut RwTxn,
        rtree: &RTree<GeoPoint>,
    ) -> heed::Result<()> {
        self.main.put::<_, Str, SerdeBincode<RTree<GeoPoint>>>(wtxn, main_key::GEO_RTREE_KEY, rtree)
    }

    /// Delete the `rtree` which associates coordinates to documents ids.
    pub(crate) fn delete_geo_rtree(&self, wtxn: &mut RwTxn) -> heed::Result<bool> {
        self.main.delete::<_, Str>(wtxn, main_key::GEO_RTREE_KEY)
    }

    /// Returns the `rtree` which associates coordinates to documents ids.
    pub fn geo_rtree<'t>(&self, rtxn: &'t RoTxn) -> Result<Option<RTree<GeoPoint>>> {
        match self
            .main
            .get::<_, Str, SerdeBincode<RTree<GeoPoint>>>(rtxn, main_key::GEO_RTREE_KEY)?
        {
            Some(rtree) => Ok(Some(rtree)),
            None => Ok(None),
        }
    }

    /* geo faceted */

    /// Writes the documents ids that are faceted with a _geo field.
    pub(crate) fn put_geo_faceted_documents_ids(
        &self,
        wtxn: &mut RwTxn,
        docids: &RoaringBitmap,
    ) -> heed::Result<()> {
        self.main.put::<_, Str, RoaringBitmapCodec>(
            wtxn,
            main_key::GEO_FACETED_DOCUMENTS_IDS_KEY,
            docids,
        )
    }

    /// Delete the documents ids that are faceted with a _geo field.
    pub(crate) fn delete_geo_faceted_documents_ids(&self, wtxn: &mut RwTxn) -> heed::Result<bool> {
        self.main.delete::<_, Str>(wtxn, main_key::GEO_FACETED_DOCUMENTS_IDS_KEY)
    }

    /// Retrieve all the documents ids that are faceted with a _geo field.
    pub fn geo_faceted_documents_ids(&self, rtxn: &RoTxn) -> heed::Result<RoaringBitmap> {
        match self
            .main
            .get::<_, Str, RoaringBitmapCodec>(rtxn, main_key::GEO_FACETED_DOCUMENTS_IDS_KEY)?
        {
            Some(docids) => Ok(docids),
            None => Ok(RoaringBitmap::new()),
        }
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
                    if let Some(field_id) = fields_ids_map.id(name) {
                        fields_ids.push(field_id);
                    }
                }
                Ok(Some(fields_ids))
            }
            None => Ok(None),
        }
    }

    /* searchable fields */

    /// Write the user defined searchable fields and generate the real searchable fields from the specified fields ids map.
    pub(crate) fn put_all_searchable_fields_from_fields_ids_map(
        &self,
        wtxn: &mut RwTxn,
        user_fields: &[&str],
        fields_ids_map: &FieldsIdsMap,
    ) -> heed::Result<()> {
        // We can write the user defined searchable fields as-is.
        self.put_user_defined_searchable_fields(wtxn, user_fields)?;

        // Now we generate the real searchable fields:
        // 1. Take the user defined searchable fields as-is to keep the priority defined by the attributes criterion.
        // 2. Iterate over the user defined searchable fields.
        // 3. If a user defined field is a subset of a field defined in the fields_ids_map
        // (ie doggo.name is a subset of doggo) then we push it at the end of the fields.
        let mut real_fields = user_fields.to_vec();

        for field_from_map in fields_ids_map.names() {
            for user_field in user_fields {
                if crate::is_faceted_by(field_from_map, user_field)
                    && !user_fields.contains(&field_from_map)
                {
                    real_fields.push(field_from_map);
                }
            }
        }

        self.put_searchable_fields(wtxn, &real_fields)
    }

    pub(crate) fn delete_all_searchable_fields(&self, wtxn: &mut RwTxn) -> heed::Result<bool> {
        let did_delete_searchable = self.delete_searchable_fields(wtxn)?;
        let did_delete_user_defined = self.delete_user_defined_searchable_fields(wtxn)?;
        Ok(did_delete_searchable || did_delete_user_defined)
    }

    /// Writes the searchable fields, when this list is specified, only these are indexed.
    fn put_searchable_fields(&self, wtxn: &mut RwTxn, fields: &[&str]) -> heed::Result<()> {
        self.main.put::<_, Str, SerdeBincode<&[&str]>>(
            wtxn,
            main_key::SEARCHABLE_FIELDS_KEY,
            &fields,
        )
    }

    /// Deletes the searchable fields, when no fields are specified, all fields are indexed.
    fn delete_searchable_fields(&self, wtxn: &mut RwTxn) -> heed::Result<bool> {
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
                    if let Some(field_id) = fields_ids_map.id(name) {
                        fields_ids.push(field_id);
                    }
                }
                Ok(Some(fields_ids))
            }
            None => Ok(None),
        }
    }

    /// Writes the searchable fields, when this list is specified, only these are indexed.
    pub(crate) fn put_user_defined_searchable_fields(
        &self,
        wtxn: &mut RwTxn,
        fields: &[&str],
    ) -> heed::Result<()> {
        self.main.put::<_, Str, SerdeBincode<_>>(
            wtxn,
            main_key::USER_DEFINED_SEARCHABLE_FIELDS_KEY,
            &fields,
        )
    }

    /// Deletes the searchable fields, when no fields are specified, all fields are indexed.
    pub(crate) fn delete_user_defined_searchable_fields(
        &self,
        wtxn: &mut RwTxn,
    ) -> heed::Result<bool> {
        self.main.delete::<_, Str>(wtxn, main_key::USER_DEFINED_SEARCHABLE_FIELDS_KEY)
    }

    /// Returns the user defined searchable fields.
    pub fn user_defined_searchable_fields<'t>(
        &self,
        rtxn: &'t RoTxn,
    ) -> heed::Result<Option<Vec<&'t str>>> {
        self.main
            .get::<_, Str, SerdeBincode<Vec<_>>>(rtxn, main_key::USER_DEFINED_SEARCHABLE_FIELDS_KEY)
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
            if let Some(field_id) = fields_ids_map.id(&name) {
                fields_ids.insert(field_id);
            }
        }

        Ok(fields_ids)
    }

    /* sortable fields */

    /// Writes the sortable fields names in the database.
    pub(crate) fn put_sortable_fields(
        &self,
        wtxn: &mut RwTxn,
        fields: &HashSet<String>,
    ) -> heed::Result<()> {
        self.main.put::<_, Str, SerdeJson<_>>(wtxn, main_key::SORTABLE_FIELDS_KEY, fields)
    }

    /// Deletes the sortable fields ids in the database.
    pub(crate) fn delete_sortable_fields(&self, wtxn: &mut RwTxn) -> heed::Result<bool> {
        self.main.delete::<_, Str>(wtxn, main_key::SORTABLE_FIELDS_KEY)
    }

    /// Returns the sortable fields names.
    pub fn sortable_fields(&self, rtxn: &RoTxn) -> heed::Result<HashSet<String>> {
        Ok(self
            .main
            .get::<_, Str, SerdeJson<_>>(rtxn, main_key::SORTABLE_FIELDS_KEY)?
            .unwrap_or_default())
    }

    /// Identical to `sortable_fields`, but returns ids instead.
    pub fn sortable_fields_ids(&self, rtxn: &RoTxn) -> Result<HashSet<FieldId>> {
        let fields = self.sortable_fields(rtxn)?;
        let fields_ids_map = self.fields_ids_map(rtxn)?;
        Ok(fields.into_iter().filter_map(|name| fields_ids_map.id(&name)).collect())
    }

    /* faceted fields */

    /// Writes the faceted fields in the database.
    pub(crate) fn put_faceted_fields(
        &self,
        wtxn: &mut RwTxn,
        fields: &HashSet<String>,
    ) -> heed::Result<()> {
        self.main.put::<_, Str, SerdeJson<_>>(wtxn, main_key::HIDDEN_FACETED_FIELDS_KEY, fields)
    }

    /// Returns the faceted fields names.
    pub fn faceted_fields(&self, rtxn: &RoTxn) -> heed::Result<HashSet<String>> {
        Ok(self
            .main
            .get::<_, Str, SerdeJson<_>>(rtxn, main_key::HIDDEN_FACETED_FIELDS_KEY)?
            .unwrap_or_default())
    }

    /// Identical to `faceted_fields`, but returns ids instead.
    pub fn faceted_fields_ids(&self, rtxn: &RoTxn) -> Result<HashSet<FieldId>> {
        let fields = self.faceted_fields(rtxn)?;
        let fields_ids_map = self.fields_ids_map(rtxn)?;

        let mut fields_ids = HashSet::new();
        for name in fields {
            if let Some(field_id) = fields_ids_map.id(&name) {
                fields_ids.insert(field_id);
            }
        }

        Ok(fields_ids)
    }

    /* faceted documents ids */

    /// Returns the user defined faceted fields names.
    ///
    /// The user faceted fields are the union of all the filterable, sortable, distinct, and Asc/Desc fields.
    pub fn user_defined_faceted_fields(&self, rtxn: &RoTxn) -> Result<HashSet<String>> {
        let filterable_fields = self.filterable_fields(rtxn)?;
        let sortable_fields = self.sortable_fields(rtxn)?;
        let distinct_field = self.distinct_field(rtxn)?;
        let asc_desc_fields =
            self.criteria(rtxn)?.into_iter().filter_map(|criterion| match criterion {
                Criterion::Asc(field) | Criterion::Desc(field) => Some(field),
                _otherwise => None,
            });

        let mut faceted_fields = filterable_fields;
        faceted_fields.extend(sortable_fields);
        faceted_fields.extend(asc_desc_fields);
        if let Some(field) = distinct_field {
            faceted_fields.insert(field.to_owned());
        }

        Ok(faceted_fields)
    }

    /// Identical to `user_defined_faceted_fields`, but returns ids instead.
    pub fn user_defined_faceted_fields_ids(&self, rtxn: &RoTxn) -> Result<HashSet<FieldId>> {
        let fields = self.faceted_fields(rtxn)?;
        let fields_ids_map = self.fields_ids_map(rtxn)?;

        let mut fields_ids = HashSet::new();
        for name in fields.into_iter() {
            if let Some(field_id) = fields_ids_map.id(&name) {
                fields_ids.insert(field_id);
            }
        }

        Ok(fields_ids)
    }

    /* faceted documents ids */

    /// Writes the documents ids that are faceted under this field id for the given facet type.
    pub fn put_faceted_documents_ids(
        &self,
        wtxn: &mut RwTxn,
        field_id: FieldId,
        facet_type: FacetType,
        docids: &RoaringBitmap,
    ) -> heed::Result<()> {
        let key = match facet_type {
            FacetType::String => main_key::STRING_FACETED_DOCUMENTS_IDS_PREFIX,
            FacetType::Number => main_key::NUMBER_FACETED_DOCUMENTS_IDS_PREFIX,
        };
        let mut buffer = vec![0u8; key.len() + size_of::<FieldId>()];
        buffer[..key.len()].copy_from_slice(key.as_bytes());
        buffer[key.len()..].copy_from_slice(&field_id.to_be_bytes());
        self.main.put::<_, ByteSlice, RoaringBitmapCodec>(wtxn, &buffer, docids)
    }

    /// Retrieve all the documents ids that are faceted under this field id for the given facet type.
    pub fn faceted_documents_ids(
        &self,
        rtxn: &RoTxn,
        field_id: FieldId,
        facet_type: FacetType,
    ) -> heed::Result<RoaringBitmap> {
        let key = match facet_type {
            FacetType::String => main_key::STRING_FACETED_DOCUMENTS_IDS_PREFIX,
            FacetType::Number => main_key::NUMBER_FACETED_DOCUMENTS_IDS_PREFIX,
        };
        let mut buffer = vec![0u8; key.len() + size_of::<FieldId>()];
        buffer[..key.len()].copy_from_slice(key.as_bytes());
        buffer[key.len()..].copy_from_slice(&field_id.to_be_bytes());
        match self.main.get::<_, ByteSlice, RoaringBitmapCodec>(rtxn, &buffer)? {
            Some(docids) => Ok(docids),
            None => Ok(RoaringBitmap::new()),
        }
    }

    /// Retrieve all the documents which contain this field id
    pub fn exists_faceted_documents_ids(
        &self,
        rtxn: &RoTxn,
        field_id: FieldId,
    ) -> heed::Result<RoaringBitmap> {
        match self.facet_id_exists_docids.get(rtxn, &BEU16::new(field_id))? {
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
        let soft_deleted_documents = self.soft_deleted_documents_ids(rtxn)?;
        let mut documents = Vec::new();

        for id in ids {
            if soft_deleted_documents.contains(id) {
                return Err(UserError::AccessingSoftDeletedDocument { document_id: id })?;
            }
            let kv = self
                .documents
                .get(rtxn, &BEU32::new(id))?
                .ok_or(UserError::UnknownInternalDocumentId { document_id: id })?;
            documents.push((id, kv));
        }

        Ok(documents)
    }

    /// Returns an iterator over all the documents in the index.
    pub fn all_documents<'t>(
        &self,
        rtxn: &'t RoTxn,
    ) -> Result<impl Iterator<Item = heed::Result<(DocumentId, obkv::KvReaderU16<'t>)>>> {
        let soft_deleted_docids = self.soft_deleted_documents_ids(rtxn)?;

        Ok(self
            .documents
            .iter(rtxn)?
            // we cast the BEU32 to a DocumentId
            .map(|document| document.map(|(id, obkv)| (id.get(), obkv)))
            .filter(move |document| {
                document.as_ref().map_or(true, |(id, _)| !soft_deleted_docids.contains(*id))
            }))
    }

    pub fn facets_distribution<'a>(&'a self, rtxn: &'a RoTxn) -> FacetDistribution<'a> {
        FacetDistribution::new(rtxn, self)
    }

    pub fn search<'a>(&'a self, rtxn: &'a RoTxn) -> Search<'a> {
        Search::new(rtxn, self)
    }

    /// Returns the index creation time.
    pub fn created_at(&self, rtxn: &RoTxn) -> Result<OffsetDateTime> {
        Ok(self
            .main
            .get::<_, Str, SerdeJson<OffsetDateTime>>(rtxn, main_key::CREATED_AT_KEY)?
            .ok_or(InternalError::DatabaseMissingEntry {
                db_name: db_name::MAIN,
                key: Some(main_key::CREATED_AT_KEY),
            })?)
    }

    /// Returns the index last updated time.
    pub fn updated_at(&self, rtxn: &RoTxn) -> Result<OffsetDateTime> {
        Ok(self
            .main
            .get::<_, Str, SerdeJson<OffsetDateTime>>(rtxn, main_key::UPDATED_AT_KEY)?
            .ok_or(InternalError::DatabaseMissingEntry {
                db_name: db_name::MAIN,
                key: Some(main_key::UPDATED_AT_KEY),
            })?)
    }

    pub(crate) fn set_updated_at(
        &self,
        wtxn: &mut RwTxn,
        time: &OffsetDateTime,
    ) -> heed::Result<()> {
        self.main.put::<_, Str, SerdeJson<OffsetDateTime>>(wtxn, main_key::UPDATED_AT_KEY, time)
    }

    pub fn authorize_typos(&self, txn: &RoTxn) -> heed::Result<bool> {
        // It is not possible to put a bool in heed with OwnedType, so we put a u8 instead. We
        // identify 0 as being false, and anything else as true. The absence of a value is true,
        // because by default, we authorize typos.
        match self.main.get::<_, Str, OwnedType<u8>>(txn, main_key::AUTHORIZE_TYPOS)? {
            Some(0) => Ok(false),
            _ => Ok(true),
        }
    }

    pub(crate) fn put_authorize_typos(&self, txn: &mut RwTxn, flag: bool) -> heed::Result<()> {
        // It is not possible to put a bool in heed with OwnedType, so we put a u8 instead. We
        // identify 0 as being false, and anything else as true. The absence of a value is true,
        // because by default, we authorize typos.
        self.main.put::<_, Str, OwnedType<u8>>(txn, main_key::AUTHORIZE_TYPOS, &(flag as u8))?;

        Ok(())
    }

    pub fn min_word_len_one_typo(&self, txn: &RoTxn) -> heed::Result<u8> {
        // It is not possible to put a bool in heed with OwnedType, so we put a u8 instead. We
        // identify 0 as being false, and anything else as true. The absence of a value is true,
        // because by default, we authorize typos.
        Ok(self
            .main
            .get::<_, Str, OwnedType<u8>>(txn, main_key::ONE_TYPO_WORD_LEN)?
            .unwrap_or(DEFAULT_MIN_WORD_LEN_ONE_TYPO))
    }

    pub(crate) fn put_min_word_len_one_typo(&self, txn: &mut RwTxn, val: u8) -> heed::Result<()> {
        // It is not possible to put a bool in heed with OwnedType, so we put a u8 instead. We
        // identify 0 as being false, and anything else as true. The absence of a value is true,
        // because by default, we authorize typos.
        self.main.put::<_, Str, OwnedType<u8>>(txn, main_key::ONE_TYPO_WORD_LEN, &val)?;
        Ok(())
    }

    pub fn min_word_len_two_typos(&self, txn: &RoTxn) -> heed::Result<u8> {
        // It is not possible to put a bool in heed with OwnedType, so we put a u8 instead. We
        // identify 0 as being false, and anything else as true. The absence of a value is true,
        // because by default, we authorize typos.
        Ok(self
            .main
            .get::<_, Str, OwnedType<u8>>(txn, main_key::TWO_TYPOS_WORD_LEN)?
            .unwrap_or(DEFAULT_MIN_WORD_LEN_TWO_TYPOS))
    }

    pub(crate) fn put_min_word_len_two_typos(&self, txn: &mut RwTxn, val: u8) -> heed::Result<()> {
        // It is not possible to put a bool in heed with OwnedType, so we put a u8 instead. We
        // identify 0 as being false, and anything else as true. The absence of a value is true,
        // because by default, we authorize typos.
        self.main.put::<_, Str, OwnedType<u8>>(txn, main_key::TWO_TYPOS_WORD_LEN, &val)?;
        Ok(())
    }

    /// List the words on which typo are not allowed
    pub fn exact_words<'t>(&self, txn: &'t RoTxn) -> Result<Option<fst::Set<Cow<'t, [u8]>>>> {
        match self.main.get::<_, Str, ByteSlice>(txn, main_key::EXACT_WORDS)? {
            Some(bytes) => Ok(Some(fst::Set::new(bytes)?.map_data(Cow::Borrowed)?)),
            None => Ok(None),
        }
    }

    pub(crate) fn put_exact_words<A: AsRef<[u8]>>(
        &self,
        txn: &mut RwTxn,
        words: &fst::Set<A>,
    ) -> Result<()> {
        self.main.put::<_, Str, ByteSlice>(
            txn,
            main_key::EXACT_WORDS,
            words.as_fst().as_bytes(),
        )?;
        Ok(())
    }

    /// Returns the exact attributes: attributes for which typo is disallowed.
    pub fn exact_attributes<'t>(&self, txn: &'t RoTxn) -> Result<Vec<&'t str>> {
        Ok(self
            .main
            .get::<_, Str, SerdeBincode<Vec<&str>>>(txn, main_key::EXACT_ATTRIBUTES)?
            .unwrap_or_default())
    }

    /// Returns the list of exact attributes field ids.
    pub fn exact_attributes_ids(&self, txn: &RoTxn) -> Result<HashSet<FieldId>> {
        let attrs = self.exact_attributes(txn)?;
        let fid_map = self.fields_ids_map(txn)?;
        Ok(attrs.iter().filter_map(|attr| fid_map.id(attr)).collect())
    }

    /// Writes the exact attributes to the database.
    pub(crate) fn put_exact_attributes(&self, txn: &mut RwTxn, attrs: &[&str]) -> Result<()> {
        self.main.put::<_, Str, SerdeBincode<&[&str]>>(txn, main_key::EXACT_ATTRIBUTES, &attrs)?;
        Ok(())
    }

    /// Clears the exact attributes from the store.
    pub(crate) fn delete_exact_attributes(&self, txn: &mut RwTxn) -> heed::Result<bool> {
        self.main.delete::<_, Str>(txn, main_key::EXACT_ATTRIBUTES)
    }

    pub fn max_values_per_facet(&self, txn: &RoTxn) -> heed::Result<Option<usize>> {
        self.main.get::<_, Str, OwnedType<usize>>(txn, main_key::MAX_VALUES_PER_FACET)
    }

    pub(crate) fn put_max_values_per_facet(&self, txn: &mut RwTxn, val: usize) -> heed::Result<()> {
        self.main.put::<_, Str, OwnedType<usize>>(txn, main_key::MAX_VALUES_PER_FACET, &val)
    }

    pub(crate) fn delete_max_values_per_facet(&self, txn: &mut RwTxn) -> heed::Result<bool> {
        self.main.delete::<_, Str>(txn, main_key::MAX_VALUES_PER_FACET)
    }

    pub fn pagination_max_total_hits(&self, txn: &RoTxn) -> heed::Result<Option<usize>> {
        self.main.get::<_, Str, OwnedType<usize>>(txn, main_key::PAGINATION_MAX_TOTAL_HITS)
    }

    pub(crate) fn put_pagination_max_total_hits(
        &self,
        txn: &mut RwTxn,
        val: usize,
    ) -> heed::Result<()> {
        self.main.put::<_, Str, OwnedType<usize>>(txn, main_key::PAGINATION_MAX_TOTAL_HITS, &val)
    }

    pub(crate) fn delete_pagination_max_total_hits(&self, txn: &mut RwTxn) -> heed::Result<bool> {
        self.main.delete::<_, Str>(txn, main_key::PAGINATION_MAX_TOTAL_HITS)
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use std::collections::HashSet;
    use std::ops::Deref;

    use big_s::S;
    use heed::{EnvOpenOptions, RwTxn};
    use maplit::hashset;
    use tempfile::TempDir;

    use crate::documents::DocumentsBatchReader;
    use crate::error::{Error, InternalError};
    use crate::index::{DEFAULT_MIN_WORD_LEN_ONE_TYPO, DEFAULT_MIN_WORD_LEN_TWO_TYPOS};
    use crate::update::{
        self, DeleteDocuments, DeletionStrategy, IndexDocuments, IndexDocumentsConfig,
        IndexDocumentsMethod, IndexerConfig, Settings,
    };
    use crate::{db_snap, obkv_to_json, Index, Search, SearchResult};

    pub(crate) struct TempIndex {
        pub inner: Index,
        pub indexer_config: IndexerConfig,
        pub index_documents_config: IndexDocumentsConfig,
        _tempdir: TempDir,
    }

    impl Deref for TempIndex {
        type Target = Index;

        fn deref(&self) -> &Self::Target {
            &self.inner
        }
    }

    impl TempIndex {
        /// Creates a temporary index
        pub fn new_with_map_size(size: usize) -> Self {
            let mut options = EnvOpenOptions::new();
            options.map_size(size);
            let _tempdir = TempDir::new_in(".").unwrap();
            let inner = Index::new(options, _tempdir.path()).unwrap();
            let indexer_config = IndexerConfig::default();
            let index_documents_config = IndexDocumentsConfig::default();
            Self { inner, indexer_config, index_documents_config, _tempdir }
        }
        /// Creates a temporary index, with a default `4096 * 1000` size. This should be enough for
        /// most tests.
        pub fn new() -> Self {
            Self::new_with_map_size(4096 * 1000)
        }
        pub fn add_documents_using_wtxn<'t, R>(
            &'t self,
            wtxn: &mut RwTxn<'t, '_>,
            documents: DocumentsBatchReader<R>,
        ) -> Result<(), crate::error::Error>
        where
            R: std::io::Read + std::io::Seek,
        {
            let builder = IndexDocuments::new(
                wtxn,
                self,
                &self.indexer_config,
                self.index_documents_config.clone(),
                |_| (),
                || false,
            )
            .unwrap();
            let (builder, user_error) = builder.add_documents(documents).unwrap();
            user_error?;
            builder.execute()?;
            Ok(())
        }
        pub fn add_documents<R>(
            &self,
            documents: DocumentsBatchReader<R>,
        ) -> Result<(), crate::error::Error>
        where
            R: std::io::Read + std::io::Seek,
        {
            let mut wtxn = self.write_txn().unwrap();
            self.add_documents_using_wtxn(&mut wtxn, documents)?;
            wtxn.commit().unwrap();
            Ok(())
        }

        pub fn update_settings(
            &self,
            update: impl Fn(&mut Settings),
        ) -> Result<(), crate::error::Error> {
            let mut wtxn = self.write_txn().unwrap();
            self.update_settings_using_wtxn(&mut wtxn, update)?;
            wtxn.commit().unwrap();
            Ok(())
        }
        pub fn update_settings_using_wtxn<'t>(
            &'t self,
            wtxn: &mut RwTxn<'t, '_>,
            update: impl Fn(&mut Settings),
        ) -> Result<(), crate::error::Error> {
            let mut builder = update::Settings::new(wtxn, &self.inner, &self.indexer_config);
            update(&mut builder);
            builder.execute(drop, || false)?;
            Ok(())
        }

        pub fn delete_document(&self, external_document_id: &str) {
            let mut wtxn = self.write_txn().unwrap();

            let mut delete = DeleteDocuments::new(&mut wtxn, &self).unwrap();
            delete.strategy(self.index_documents_config.deletion_strategy);

            delete.delete_external_id(external_document_id);
            delete.execute().unwrap();
            wtxn.commit().unwrap();
        }
    }

    #[test]
    fn aborting_indexation() {
        use std::sync::atomic::AtomicBool;
        use std::sync::atomic::Ordering::Relaxed;

        let index = TempIndex::new();
        let mut wtxn = index.inner.write_txn().unwrap();

        let should_abort = AtomicBool::new(false);
        let builder = IndexDocuments::new(
            &mut wtxn,
            &index.inner,
            &index.indexer_config,
            index.index_documents_config.clone(),
            |_| (),
            || should_abort.load(Relaxed),
        )
        .unwrap();

        let (builder, user_error) = builder
            .add_documents(documents!([
                { "id": 1, "name": "kevin" },
                { "id": 2, "name": "bob", "age": 20 },
                { "id": 2, "name": "bob", "age": 20 },
            ]))
            .unwrap();
        user_error.unwrap();

        should_abort.store(true, Relaxed);
        let err = builder.execute().unwrap_err();

        assert!(matches!(err, Error::InternalError(InternalError::AbortedIndexation)));
    }

    #[test]
    fn initial_field_distribution() {
        let index = TempIndex::new();
        index
            .add_documents(documents!([
                { "id": 1, "name": "kevin" },
                { "id": 2, "name": "bob", "age": 20 },
                { "id": 2, "name": "bob", "age": 20 },
            ]))
            .unwrap();

        db_snap!(index, field_distribution, 1);

        db_snap!(index, word_docids,
            @r###"
        1                [0, ]
        2                [1, ]
        20               [1, ]
        bob              [1, ]
        kevin            [0, ]
        "###
        );

        db_snap!(index, field_distribution);

        db_snap!(index, field_distribution,
            @"
            age              1     
            id               2     
            name             2     
            "
        );

        // snapshot_index!(&index, "1", include: "^field_distribution$");

        // we add all the documents a second time. we are supposed to get the same
        // field_distribution in the end
        index
            .add_documents(documents!([
                { "id": 1, "name": "kevin" },
                { "id": 2, "name": "bob", "age": 20 },
                { "id": 2, "name": "bob", "age": 20 },
            ]))
            .unwrap();

        db_snap!(index, field_distribution,
            @r###"
            age              1     
            id               2     
            name             2     
            "###
        );

        // then we update a document by removing one field and another by adding one field
        index
            .add_documents(documents!([
                { "id": 1, "name": "kevin", "has_dog": true },
                { "id": 2, "name": "bob" }
            ]))
            .unwrap();

        db_snap!(index, field_distribution,
            @r###"
            has_dog          1     
            id               2     
            name             2     
            "###
        );
    }

    #[test]
    fn put_and_retrieve_disable_typo() {
        let index = TempIndex::new();
        let mut txn = index.write_txn().unwrap();
        // default value is true
        assert!(index.authorize_typos(&txn).unwrap());
        // set to false
        index.put_authorize_typos(&mut txn, false).unwrap();
        txn.commit().unwrap();

        let txn = index.read_txn().unwrap();
        assert!(!index.authorize_typos(&txn).unwrap());
    }

    #[test]
    fn set_min_word_len_for_typos() {
        let index = TempIndex::new();
        let mut txn = index.write_txn().unwrap();

        assert_eq!(index.min_word_len_one_typo(&txn).unwrap(), DEFAULT_MIN_WORD_LEN_ONE_TYPO);
        assert_eq!(index.min_word_len_two_typos(&txn).unwrap(), DEFAULT_MIN_WORD_LEN_TWO_TYPOS);

        index.put_min_word_len_one_typo(&mut txn, 3).unwrap();
        index.put_min_word_len_two_typos(&mut txn, 15).unwrap();

        txn.commit().unwrap();

        let txn = index.read_txn().unwrap();
        assert_eq!(index.min_word_len_one_typo(&txn).unwrap(), 3);
        assert_eq!(index.min_word_len_two_typos(&txn).unwrap(), 15);
    }

    #[test]
    fn add_documents_and_set_searchable_fields() {
        let index = TempIndex::new();
        index
            .add_documents(documents!([
                { "id": 1, "doggo": "kevin" },
                { "id": 2, "doggo": { "name": "bob", "age": 20 } },
                { "id": 3, "name": "jean", "age": 25 },
            ]))
            .unwrap();
        index
            .update_settings(|settings| {
                settings.set_searchable_fields(vec![S("doggo"), S("name")]);
            })
            .unwrap();

        // ensure we get the right real searchable fields + user defined searchable fields
        let rtxn = index.read_txn().unwrap();

        let real = index.searchable_fields(&rtxn).unwrap().unwrap();
        assert_eq!(real, &["doggo", "name", "doggo.name", "doggo.age"]);

        let user_defined = index.user_defined_searchable_fields(&rtxn).unwrap().unwrap();
        assert_eq!(user_defined, &["doggo", "name"]);
    }

    #[test]
    fn set_searchable_fields_and_add_documents() {
        let index = TempIndex::new();

        index
            .update_settings(|settings| {
                settings.set_searchable_fields(vec![S("doggo"), S("name")]);
            })
            .unwrap();

        // ensure we get the right real searchable fields + user defined searchable fields
        let rtxn = index.read_txn().unwrap();

        let real = index.searchable_fields(&rtxn).unwrap().unwrap();
        assert_eq!(real, &["doggo", "name"]);
        let user_defined = index.user_defined_searchable_fields(&rtxn).unwrap().unwrap();
        assert_eq!(user_defined, &["doggo", "name"]);

        index
            .add_documents(documents!([
                { "id": 1, "doggo": "kevin" },
                { "id": 2, "doggo": { "name": "bob", "age": 20 } },
                { "id": 3, "name": "jean", "age": 25 },
            ]))
            .unwrap();

        // ensure we get the right real searchable fields + user defined searchable fields
        let rtxn = index.read_txn().unwrap();

        let real = index.searchable_fields(&rtxn).unwrap().unwrap();
        assert_eq!(real, &["doggo", "name", "doggo.name", "doggo.age"]);

        let user_defined = index.user_defined_searchable_fields(&rtxn).unwrap().unwrap();
        assert_eq!(user_defined, &["doggo", "name"]);
    }

    #[test]
    fn replace_documents_external_ids_and_soft_deletion_check() {
        use big_s::S;
        use maplit::hashset;

        let mut index = TempIndex::new();
        index.index_documents_config.deletion_strategy = DeletionStrategy::AlwaysSoft;
        let index = index;

        index
            .update_settings(|settings| {
                settings.set_primary_key("id".to_owned());
                settings.set_filterable_fields(hashset! { S("doggo") });
            })
            .unwrap();

        let mut docs = vec![];
        for i in 0..4 {
            docs.push(serde_json::json!(
                { "id": i, "doggo": i }
            ));
        }
        index.add_documents(documents!(docs)).unwrap();

        db_snap!(index, documents_ids, @"[0, 1, 2, 3, ]");
        db_snap!(index, external_documents_ids, 1, @r###"
        soft:
        hard:
        0                        0
        1                        1
        2                        2
        3                        3
        "###);
        db_snap!(index, soft_deleted_documents_ids, 1, @"[]");
        db_snap!(index, facet_id_f64_docids, 1, @r###"
        1   0  0      1  [0, ]
        1   0  1      1  [1, ]
        1   0  2      1  [2, ]
        1   0  3      1  [3, ]
        "###);

        let mut docs = vec![];
        for i in 0..3 {
            docs.push(serde_json::json!(
                { "id": i, "doggo": i + 1 }
            ));
        }
        index.add_documents(documents!(docs)).unwrap();

        db_snap!(index, documents_ids, @"[3, 4, 5, 6, ]");
        db_snap!(index, external_documents_ids, 2, @r###"
        soft:
        hard:
        0                        4
        1                        5
        2                        6
        3                        3
        "###);
        db_snap!(index, soft_deleted_documents_ids, 2, @"[0, 1, 2, ]");
        db_snap!(index, facet_id_f64_docids, 2, @r###"
        1   0  0      1  [0, ]
        1   0  1      1  [1, 4, ]
        1   0  2      1  [2, 5, ]
        1   0  3      1  [3, 6, ]
        "###);

        index
            .add_documents(documents!([{ "id": 3, "doggo": 4 }, { "id": 3, "doggo": 5 },{ "id": 3, "doggo": 4 }]))
            .unwrap();

        db_snap!(index, documents_ids, @"[4, 5, 6, 7, ]");
        db_snap!(index, external_documents_ids, 3, @r###"
        soft:
        3                        7
        hard:
        0                        4
        1                        5
        2                        6
        3                        3
        "###);
        db_snap!(index, soft_deleted_documents_ids, 3, @"[0, 1, 2, 3, ]");
        db_snap!(index, facet_id_f64_docids, 3, @r###"
        1   0  0      1  [0, ]
        1   0  1      1  [1, 4, ]
        1   0  2      1  [2, 5, ]
        1   0  3      1  [3, 6, ]
        1   0  4      1  [7, ]
        "###);

        index
            .update_settings(|settings| {
                settings.set_distinct_field("id".to_owned());
            })
            .unwrap();

        db_snap!(index, documents_ids, @"[4, 5, 6, 7, ]");
        db_snap!(index, external_documents_ids, 3, @r###"
        soft:
        hard:
        0                        4
        1                        5
        2                        6
        3                        7
        "###);
        db_snap!(index, soft_deleted_documents_ids, 3, @"[]");
        db_snap!(index, facet_id_f64_docids, 3, @r###"
        0   0  0      1  [4, ]
        0   0  1      1  [5, ]
        0   0  2      1  [6, ]
        0   0  3      1  [7, ]
        1   0  1      1  [4, ]
        1   0  2      1  [5, ]
        1   0  3      1  [6, ]
        1   0  4      1  [7, ]
        "###);
    }

    #[test]
    fn replace_documents_in_batches_external_ids_and_soft_deletion_check() {
        use big_s::S;
        use maplit::hashset;

        let mut index = TempIndex::new();

        index
            .update_settings(|settings| {
                settings.set_primary_key("id".to_owned());
                settings.set_filterable_fields(hashset! { S("doggo") });
            })
            .unwrap();

        let add_documents = |index: &TempIndex, docs: Vec<Vec<serde_json::Value>>| {
            let mut wtxn = index.write_txn().unwrap();
            let mut builder = IndexDocuments::new(
                &mut wtxn,
                index,
                &index.indexer_config,
                index.index_documents_config.clone(),
                |_| (),
                || false,
            )
            .unwrap();
            for docs in docs {
                (builder, _) = builder.add_documents(documents!(docs)).unwrap();
            }
            builder.execute().unwrap();
            wtxn.commit().unwrap();
        };
        // First Batch
        {
            let mut docs1 = vec![];
            for i in 0..4 {
                docs1.push(serde_json::json!(
                    { "id": i, "doggo": i }
                ));
            }
            add_documents(&index, vec![docs1]);

            db_snap!(index, documents_ids, @"[0, 1, 2, 3, ]");
            db_snap!(index, external_documents_ids, 1, @r###"
            soft:
            hard:
            0                        0
            1                        1
            2                        2
            3                        3
            "###);
            db_snap!(index, soft_deleted_documents_ids, 1, @"[]");
            db_snap!(index, facet_id_f64_docids, 1, @r###"
            1   0  0      1  [0, ]
            1   0  1      1  [1, ]
            1   0  2      1  [2, ]
            1   0  3      1  [3, ]
            "###);
        }
        // Second Batch: replace the documents with soft-deletion
        {
            index.index_documents_config.deletion_strategy =
                crate::update::DeletionStrategy::AlwaysSoft;
            let mut docs1 = vec![];
            for i in 0..3 {
                docs1.push(serde_json::json!(
                    { "id": i, "doggo": i+1 }
                ));
            }
            let mut docs2 = vec![];
            for i in 0..3 {
                docs2.push(serde_json::json!(
                    { "id": i, "doggo": i }
                ));
            }
            add_documents(&index, vec![docs1, docs2]);

            db_snap!(index, documents_ids, @"[3, 4, 5, 6, ]");
            db_snap!(index, external_documents_ids, 1, @r###"
            soft:
            hard:
            0                        4
            1                        5
            2                        6
            3                        3
            "###);
            db_snap!(index, soft_deleted_documents_ids, 1, @"[0, 1, 2, ]");
            db_snap!(index, facet_id_f64_docids, 1, @r###"
            1   0  0      1  [0, 4, ]
            1   0  1      1  [1, 5, ]
            1   0  2      1  [2, 6, ]
            1   0  3      1  [3, ]
            "###);
        }
        let rtxn = index.read_txn().unwrap();
        let (_docid, obkv) = index.documents(&rtxn, [3]).unwrap()[0];
        let json = obkv_to_json(&[0, 1], &index.fields_ids_map(&rtxn).unwrap(), obkv).unwrap();
        insta::assert_debug_snapshot!(json, @r###"
        {
            "id": Number(3),
            "doggo": Number(3),
        }
        "###);
        let (_docid, obkv) = index.documents(&rtxn, [4]).unwrap()[0];

        let json = obkv_to_json(&[0, 1], &index.fields_ids_map(&rtxn).unwrap(), obkv).unwrap();
        insta::assert_debug_snapshot!(json, @r###"
        {
            "id": Number(0),
            "doggo": Number(0),
        }
        "###);
        let (_docid, obkv) = index.documents(&rtxn, [5]).unwrap()[0];
        let json = obkv_to_json(&[0, 1], &index.fields_ids_map(&rtxn).unwrap(), obkv).unwrap();
        insta::assert_debug_snapshot!(json, @r###"
        {
            "id": Number(1),
            "doggo": Number(1),
        }
        "###);
        let (_docid, obkv) = index.documents(&rtxn, [6]).unwrap()[0];
        let json = obkv_to_json(&[0, 1], &index.fields_ids_map(&rtxn).unwrap(), obkv).unwrap();
        insta::assert_debug_snapshot!(json, @r###"
        {
            "id": Number(2),
            "doggo": Number(2),
        }
        "###);
        drop(rtxn);
        // Third Batch: replace the documents with soft-deletion again
        {
            index.index_documents_config.deletion_strategy = DeletionStrategy::AlwaysSoft;
            let mut docs1 = vec![];
            for i in 0..3 {
                docs1.push(serde_json::json!(
                    { "id": i, "doggo": i+1 }
                ));
            }
            let mut docs2 = vec![];
            for i in 0..4 {
                docs2.push(serde_json::json!(
                    { "id": i, "doggo": i }
                ));
            }
            add_documents(&index, vec![docs1, docs2]);

            db_snap!(index, documents_ids, @"[3, 7, 8, 9, ]");
            db_snap!(index, external_documents_ids, 1, @r###"
            soft:
            hard:
            0                        7
            1                        8
            2                        9
            3                        3
            "###);
            db_snap!(index, soft_deleted_documents_ids, 1, @"[0, 1, 2, 4, 5, 6, ]");
            db_snap!(index, facet_id_f64_docids, 1, @r###"
            1   0  0      1  [0, 4, 7, ]
            1   0  1      1  [1, 5, 8, ]
            1   0  2      1  [2, 6, 9, ]
            1   0  3      1  [3, ]
            "###);
        }
        let rtxn = index.read_txn().unwrap();
        let (_docid, obkv) = index.documents(&rtxn, [3]).unwrap()[0];
        let json = obkv_to_json(&[0, 1], &index.fields_ids_map(&rtxn).unwrap(), obkv).unwrap();
        insta::assert_debug_snapshot!(json, @r###"
        {
            "id": Number(3),
            "doggo": Number(3),
        }
        "###);
        let (_docid, obkv) = index.documents(&rtxn, [7]).unwrap()[0];
        let json = obkv_to_json(&[0, 1], &index.fields_ids_map(&rtxn).unwrap(), obkv).unwrap();
        insta::assert_debug_snapshot!(json, @r###"
        {
            "id": Number(0),
            "doggo": Number(0),
        }
        "###);
        let (_docid, obkv) = index.documents(&rtxn, [8]).unwrap()[0];
        let json = obkv_to_json(&[0, 1], &index.fields_ids_map(&rtxn).unwrap(), obkv).unwrap();
        insta::assert_debug_snapshot!(json, @r###"
        {
            "id": Number(1),
            "doggo": Number(1),
        }
        "###);
        let (_docid, obkv) = index.documents(&rtxn, [9]).unwrap()[0];
        let json = obkv_to_json(&[0, 1], &index.fields_ids_map(&rtxn).unwrap(), obkv).unwrap();
        insta::assert_debug_snapshot!(json, @r###"
        {
            "id": Number(2),
            "doggo": Number(2),
        }
        "###);
        drop(rtxn);

        // Fourth Batch: replace the documents without soft-deletion
        {
            index.index_documents_config.deletion_strategy = DeletionStrategy::AlwaysHard;
            let mut docs1 = vec![];
            for i in 0..3 {
                docs1.push(serde_json::json!(
                    { "id": i, "doggo": i+2 }
                ));
            }
            let mut docs2 = vec![];
            for i in 0..1 {
                docs2.push(serde_json::json!(
                    { "id": i, "doggo": i }
                ));
            }
            add_documents(&index, vec![docs1, docs2]);

            db_snap!(index, documents_ids, @"[3, 10, 11, 12, ]");
            db_snap!(index, external_documents_ids, 1, @r###"
            soft:
            hard:
            0                        10
            1                        11
            2                        12
            3                        3
            "###);
            db_snap!(index, soft_deleted_documents_ids, 1, @"[]");
            db_snap!(index, facet_id_f64_docids, 1, @r###"
            1   0  0      1  [10, ]
            1   0  3      1  [3, 11, ]
            1   0  4      1  [12, ]
            "###);

            let rtxn = index.read_txn().unwrap();
            let (_docid, obkv) = index.documents(&rtxn, [3]).unwrap()[0];
            let json = obkv_to_json(&[0, 1], &index.fields_ids_map(&rtxn).unwrap(), obkv).unwrap();
            insta::assert_debug_snapshot!(json, @r###"
            {
                "id": Number(3),
                "doggo": Number(3),
            }
            "###);
            let (_docid, obkv) = index.documents(&rtxn, [10]).unwrap()[0];
            let json = obkv_to_json(&[0, 1], &index.fields_ids_map(&rtxn).unwrap(), obkv).unwrap();
            insta::assert_debug_snapshot!(json, @r###"
            {
                "id": Number(0),
                "doggo": Number(0),
            }
            "###);
            let (_docid, obkv) = index.documents(&rtxn, [11]).unwrap()[0];
            let json = obkv_to_json(&[0, 1], &index.fields_ids_map(&rtxn).unwrap(), obkv).unwrap();
            insta::assert_debug_snapshot!(json, @r###"
            {
                "id": Number(1),
                "doggo": Number(3),
            }
            "###);
            let (_docid, obkv) = index.documents(&rtxn, [12]).unwrap()[0];
            let json = obkv_to_json(&[0, 1], &index.fields_ids_map(&rtxn).unwrap(), obkv).unwrap();
            insta::assert_debug_snapshot!(json, @r###"
            {
                "id": Number(2),
                "doggo": Number(4),
            }
            "###);
            drop(rtxn);
        }
    }

    #[test]
    fn bug_3021_first() {
        // https://github.com/meilisearch/meilisearch/issues/3021
        let mut index = TempIndex::new();
        index.index_documents_config.deletion_strategy = DeletionStrategy::AlwaysSoft;
        index.index_documents_config.update_method = IndexDocumentsMethod::ReplaceDocuments;

        index
            .update_settings(|settings| {
                settings.set_primary_key("primary_key".to_owned());
            })
            .unwrap();

        index
            .add_documents(documents!([
                { "primary_key": 38 },
                { "primary_key": 34 }
            ]))
            .unwrap();

        db_snap!(index, documents_ids, @"[0, 1, ]");
        db_snap!(index, external_documents_ids, 1, @r###"
        soft:
        hard:
        34                       1
        38                       0
        "###);
        db_snap!(index, soft_deleted_documents_ids, 1, @"[]");

        index.delete_document("34");

        db_snap!(index, documents_ids, @"[0, ]");
        db_snap!(index, external_documents_ids, 2, @r###"
        soft:
        hard:
        34                       1
        38                       0
        "###);
        db_snap!(index, soft_deleted_documents_ids, 2, @"[1, ]");

        index
            .update_settings(|s| {
                s.set_searchable_fields(vec![]);
            })
            .unwrap();

        // The key point of the test is to verify that the external documents ids
        // do not contain any entry for previously soft-deleted document ids
        db_snap!(index, documents_ids, @"[0, ]");
        db_snap!(index, external_documents_ids, 3, @r###"
        soft:
        hard:
        38                       0
        "###);
        db_snap!(index, soft_deleted_documents_ids, 3, @"[]");

        // So that this document addition works correctly now.
        // It would be wrongly interpreted as a replacement before
        index.add_documents(documents!({ "primary_key": 34 })).unwrap();

        db_snap!(index, documents_ids, @"[0, 1, ]");
        db_snap!(index, external_documents_ids, 4, @r###"
        soft:
        hard:
        34                       1
        38                       0
        "###);
        db_snap!(index, soft_deleted_documents_ids, 4, @"[]");

        // We do the test again, but deleting the document with id 0 instead of id 1 now
        index.delete_document("38");

        db_snap!(index, documents_ids, @"[1, ]");
        db_snap!(index, external_documents_ids, 5, @r###"
        soft:
        hard:
        34                       1
        38                       0
        "###);
        db_snap!(index, soft_deleted_documents_ids, 5, @"[0, ]");

        index
            .update_settings(|s| {
                s.set_searchable_fields(vec!["primary_key".to_owned()]);
            })
            .unwrap();

        db_snap!(index, documents_ids, @"[1, ]");
        db_snap!(index, external_documents_ids, 6, @r###"
        soft:
        hard:
        34                       1
        "###);
        db_snap!(index, soft_deleted_documents_ids, 6, @"[]");

        // And adding lots of documents afterwards instead of just one.
        // These extra subtests don't add much, but it's better than nothing.
        index.add_documents(documents!([{ "primary_key": 38 }, { "primary_key": 39 }, { "primary_key": 41 }, { "primary_key": 40 }, { "primary_key": 41 }, { "primary_key": 42 }])).unwrap();

        db_snap!(index, documents_ids, @"[0, 1, 2, 3, 4, 5, ]");
        db_snap!(index, external_documents_ids, 7, @r###"
        soft:
        hard:
        34                       1
        38                       0
        39                       2
        40                       4
        41                       3
        42                       5
        "###);
        db_snap!(index, soft_deleted_documents_ids, 7, @"[]");
    }

    #[test]
    fn bug_3021_second() {
        // https://github.com/meilisearch/meilisearch/issues/3021
        let mut index = TempIndex::new();
        index.index_documents_config.deletion_strategy = DeletionStrategy::AlwaysSoft;
        index.index_documents_config.update_method = IndexDocumentsMethod::UpdateDocuments;

        index
            .update_settings(|settings| {
                settings.set_primary_key("primary_key".to_owned());
            })
            .unwrap();

        index
            .add_documents(documents!([
                { "primary_key": 30 },
                { "primary_key": 34 }
            ]))
            .unwrap();

        db_snap!(index, documents_ids, @"[0, 1, ]");
        db_snap!(index, external_documents_ids, 1, @r###"
        soft:
        hard:
        30                       0
        34                       1
        "###);
        db_snap!(index, soft_deleted_documents_ids, 1, @"[]");

        index.delete_document("34");

        db_snap!(index, documents_ids, @"[0, ]");
        db_snap!(index, external_documents_ids, 2, @r###"
        soft:
        hard:
        30                       0
        34                       1
        "###);
        db_snap!(index, soft_deleted_documents_ids, 2, @"[1, ]");

        index
            .update_settings(|s| {
                s.set_searchable_fields(vec![]);
            })
            .unwrap();

        // The key point of the test is to verify that the external documents ids
        // do not contain any entry for previously soft-deleted document ids
        db_snap!(index, documents_ids, @"[0, ]");
        db_snap!(index, external_documents_ids, 3, @r###"
        soft:
        hard:
        30                       0
        "###);
        db_snap!(index, soft_deleted_documents_ids, 3, @"[]");

        // So that when we add a new document
        index.add_documents(documents!({ "primary_key": 35, "b": 2 })).unwrap();

        db_snap!(index, documents_ids, @"[0, 1, ]");
        // The external documents ids don't have several external ids pointing to the same
        // internal document id
        db_snap!(index, external_documents_ids, 4, @r###"
        soft:
        hard:
        30                       0
        35                       1
        "###);
        db_snap!(index, soft_deleted_documents_ids, 4, @"[]");

        // And when we add 34 again, we don't replace document 35
        index.add_documents(documents!({ "primary_key": 34, "a": 1 })).unwrap();

        // And document 35 still exists, is not deleted
        db_snap!(index, documents_ids, @"[0, 1, 2, ]");
        db_snap!(index, external_documents_ids, 5, @r###"
        soft:
        hard:
        30                       0
        34                       2
        35                       1
        "###);
        db_snap!(index, soft_deleted_documents_ids, 5, @"[]");

        let rtxn = index.read_txn().unwrap();
        let (_docid, obkv) = index.documents(&rtxn, [0]).unwrap()[0];
        let json = obkv_to_json(&[0, 1, 2], &index.fields_ids_map(&rtxn).unwrap(), obkv).unwrap();
        insta::assert_debug_snapshot!(json, @r###"
        {
            "primary_key": Number(30),
        }
        "###);

        // Furthermore, when we retrieve document 34, it is not the result of merging 35 with 34
        let (_docid, obkv) = index.documents(&rtxn, [2]).unwrap()[0];
        let json = obkv_to_json(&[0, 1, 2], &index.fields_ids_map(&rtxn).unwrap(), obkv).unwrap();
        insta::assert_debug_snapshot!(json, @r###"
        {
            "primary_key": Number(34),
            "a": Number(1),
        }
        "###);

        drop(rtxn);

        // Add new documents again
        index
            .add_documents(
                documents!([{ "primary_key": 37 }, { "primary_key": 38 }, { "primary_key": 39 }]),
            )
            .unwrap();

        db_snap!(index, documents_ids, @"[0, 1, 2, 3, 4, 5, ]");
        db_snap!(index, external_documents_ids, 6, @r###"
        soft:
        hard:
        30                       0
        34                       2
        35                       1
        37                       3
        38                       4
        39                       5
        "###);
        db_snap!(index, soft_deleted_documents_ids, 6, @"[]");
    }

    #[test]
    fn bug_3021_third() {
        // https://github.com/meilisearch/meilisearch/issues/3021
        let mut index = TempIndex::new();
        index.index_documents_config.deletion_strategy = DeletionStrategy::AlwaysSoft;
        index.index_documents_config.update_method = IndexDocumentsMethod::UpdateDocuments;

        index
            .update_settings(|settings| {
                settings.set_primary_key("primary_key".to_owned());
            })
            .unwrap();

        index
            .add_documents(documents!([
                { "primary_key": 3 },
                { "primary_key": 4 },
                { "primary_key": 5 }
            ]))
            .unwrap();

        db_snap!(index, documents_ids, @"[0, 1, 2, ]");
        db_snap!(index, external_documents_ids, 1, @r###"
        soft:
        hard:
        3                        0
        4                        1
        5                        2
        "###);
        db_snap!(index, soft_deleted_documents_ids, 1, @"[]");

        index.delete_document("3");

        db_snap!(index, documents_ids, @"[1, 2, ]");
        db_snap!(index, external_documents_ids, 2, @r###"
        soft:
        hard:
        3                        0
        4                        1
        5                        2
        "###);
        db_snap!(index, soft_deleted_documents_ids, 2, @"[0, ]");

        index.index_documents_config.deletion_strategy = DeletionStrategy::AlwaysHard;

        index.add_documents(documents!([{ "primary_key": "4", "a": 2 }])).unwrap();

        db_snap!(index, documents_ids, @"[2, 3, ]");
        db_snap!(index, external_documents_ids, 2, @r###"
        soft:
        hard:
        4                        3
        5                        2
        "###);
        db_snap!(index, soft_deleted_documents_ids, 2, @"[]");

        index
            .add_documents(documents!([
                { "primary_key": "3" },
            ]))
            .unwrap();

        db_snap!(index, documents_ids, @"[0, 2, 3, ]");
        db_snap!(index, external_documents_ids, 2, @r###"
        soft:
        hard:
        3                        0
        4                        3
        5                        2
        "###);
        db_snap!(index, soft_deleted_documents_ids, 2, @"[]");
    }

    #[test]
    fn bug_3021_fourth() {
        // https://github.com/meilisearch/meilisearch/issues/3021
        let mut index = TempIndex::new();
        index.index_documents_config.update_method = IndexDocumentsMethod::UpdateDocuments;
        index.index_documents_config.deletion_strategy = DeletionStrategy::AlwaysSoft;

        index
            .update_settings(|settings| {
                settings.set_primary_key("primary_key".to_owned());
            })
            .unwrap();

        index
            .add_documents(documents!([
                { "primary_key": 11 },
                { "primary_key": 4 },
            ]))
            .unwrap();

        db_snap!(index, documents_ids, @"[0, 1, ]");
        db_snap!(index, external_documents_ids, @r###"
        soft:
        hard:
        11                       0
        4                        1
        "###);
        db_snap!(index, soft_deleted_documents_ids, @"[]");

        index
            .add_documents(documents!([
                { "primary_key": 4, "a": 0 },
                { "primary_key": 1 },
            ]))
            .unwrap();

        db_snap!(index, documents_ids, @"[0, 2, 3, ]");
        db_snap!(index, external_documents_ids, @r###"
        soft:
        hard:
        1                        3
        11                       0
        4                        2
        "###);
        db_snap!(index, soft_deleted_documents_ids, @"[1, ]");

        let mut wtxn = index.write_txn().unwrap();
        let mut delete = DeleteDocuments::new(&mut wtxn, &index).unwrap();
        delete.strategy(DeletionStrategy::AlwaysHard);
        delete.execute().unwrap();
        wtxn.commit().unwrap();

        db_snap!(index, documents_ids, @"[0, 2, 3, ]");
        db_snap!(index, external_documents_ids, @r###"
        soft:
        hard:
        1                        3
        11                       0
        4                        2
        "###);
        db_snap!(index, soft_deleted_documents_ids, @"[]");

        index
            .add_documents(documents!([
                { "primary_key": 4, "a": 1 },
                { "primary_key": 1, "a": 0 },
            ]))
            .unwrap();

        db_snap!(index, documents_ids, @"[0, 1, 4, ]");
        db_snap!(index, external_documents_ids, @r###"
        soft:
        hard:
        1                        4
        11                       0
        4                        1
        "###);
        db_snap!(index, soft_deleted_documents_ids, @"[2, 3, ]");

        let rtxn = index.read_txn().unwrap();
        let search = Search::new(&rtxn, &index);
        let SearchResult { matching_words: _, candidates: _, mut documents_ids } =
            search.execute().unwrap();
        let primary_key_id = index.fields_ids_map(&rtxn).unwrap().id("primary_key").unwrap();
        documents_ids.sort_unstable();
        let docs = index.documents(&rtxn, documents_ids).unwrap();
        let mut all_ids = HashSet::new();
        for (_docid, obkv) in docs {
            let id = obkv.get(primary_key_id).unwrap();
            assert!(all_ids.insert(id));
        }
    }
}
