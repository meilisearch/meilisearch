use std::borrow::Cow;
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::error::Error;
use std::fmt;
use std::fs::File;
use std::path::Path;

use cellulite::Cellulite;
use deserr::Deserr;
use heed::types::*;
use heed::{CompactionOption, Database, DatabaseStat, RoTxn, RwTxn, Unspecified, WithoutTls};
use indexmap::IndexMap;
use roaring::RoaringBitmap;
use rstar::RTree;
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

use crate::constants::{self, RESERVED_GEO_FIELD_NAME, RESERVED_VECTORS_FIELD_NAME};
use crate::database_stats::DatabaseStats;
use crate::documents::PrimaryKey;
use crate::error::{InternalError, UserError};
use crate::fields_ids_map::metadata::{FieldIdMapWithMetadata, MetadataBuilder};
use crate::fields_ids_map::FieldsIdsMap;
use crate::heed_codec::facet::{
    FacetGroupKeyCodec, FacetGroupValueCodec, FieldDocIdFacetF64Codec, FieldDocIdFacetStringCodec,
    FieldIdCodec, OrderedF64Codec,
};
use crate::heed_codec::version::VersionCodec;
use crate::heed_codec::{BEU16StrCodec, FstSetCodec, StrBEU16Codec, StrRefCodec};
use crate::order_by_map::OrderByMap;
use crate::prompt::PromptData;
use crate::proximity::ProximityPrecision;
use crate::update::new::StdResult;
use crate::vector::db::IndexEmbeddingConfigs;
use crate::vector::{Embedding, VectorStore, VectorStoreBackend, VectorStoreStats};
use crate::{
    default_criteria, CboRoaringBitmapCodec, Criterion, DocumentId, ExternalDocumentsIds,
    FacetDistribution, FieldDistribution, FieldId, FieldIdMapMissingEntry, FieldIdWordCountCodec,
    FieldidsWeightsMap, FilterableAttributesRule, GeoPoint, LocalizedAttributesRule, ObkvCodec,
    Result, RoaringBitmapCodec, RoaringBitmapLenCodec, Search, U8StrStrCodec, Weight, BEU16, BEU32,
    BEU64,
};

pub const DEFAULT_MIN_WORD_LEN_ONE_TYPO: u8 = 5;
pub const DEFAULT_MIN_WORD_LEN_TWO_TYPOS: u8 = 9;

pub mod main_key {
    pub const VERSION_KEY: &str = "version";
    pub const CRITERIA_KEY: &str = "criteria";
    pub const DISPLAYED_FIELDS_KEY: &str = "displayed-fields";
    pub const DISTINCT_FIELD_KEY: &str = "distinct-field-key";
    pub const DOCUMENTS_IDS_KEY: &str = "documents-ids";
    pub const HIDDEN_FACETED_FIELDS_KEY: &str = "hidden-faceted-fields";
    pub const FILTERABLE_FIELDS_KEY: &str = "filterable-fields";
    pub const SORTABLE_FIELDS_KEY: &str = "sortable-fields";
    pub const FOREIGN_KEYS_KEY: &str = "foreign-keys";
    pub const FIELD_DISTRIBUTION_KEY: &str = "fields-distribution";
    pub const FIELDS_IDS_MAP_KEY: &str = "fields-ids-map";
    pub const FIELDIDS_WEIGHTS_MAP_KEY: &str = "fieldids-weights-map";
    pub const GEO_FACETED_DOCUMENTS_IDS_KEY: &str = "geo-faceted-documents-ids";
    pub const GEO_RTREE_KEY: &str = "geo-rtree";
    pub const PRIMARY_KEY_KEY: &str = "primary-key";
    pub const SEARCHABLE_FIELDS_KEY: &str = "searchable-fields";
    pub const USER_DEFINED_SEARCHABLE_FIELDS_KEY: &str = "user-defined-searchable-fields";
    pub const STOP_WORDS_KEY: &str = "stop-words";
    pub const NON_SEPARATOR_TOKENS_KEY: &str = "non-separator-tokens";
    pub const SEPARATOR_TOKENS_KEY: &str = "separator-tokens";
    pub const DICTIONARY_KEY: &str = "dictionary";
    pub const SYNONYMS_KEY: &str = "synonyms";
    pub const USER_DEFINED_SYNONYMS_KEY: &str = "user-defined-synonyms";
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
    pub const SORT_FACET_VALUES_BY: &str = "sort-facet-values-by";
    pub const PAGINATION_MAX_TOTAL_HITS: &str = "pagination-max-total-hits";
    pub const PROXIMITY_PRECISION: &str = "proximity-precision";
    pub const EMBEDDING_CONFIGS: &str = "embedding_configs";
    pub const SEARCH_CUTOFF: &str = "search_cutoff";
    pub const LOCALIZED_ATTRIBUTES_RULES: &str = "localized_attributes_rules";
    pub const FACET_SEARCH: &str = "facet_search";
    pub const PREFIX_SEARCH: &str = "prefix_search";
    pub const DOCUMENTS_STATS: &str = "documents_stats";
    pub const DISABLED_TYPOS_TERMS: &str = "disabled_typos_terms";
    pub const CHAT: &str = "chat";
    pub const VECTOR_STORE_BACKEND: &str = "vector_store_backend";
}

pub mod db_name {
    pub const MAIN: &str = "main";
    pub const WORD_DOCIDS: &str = "word-docids";
    pub const EXACT_WORD_DOCIDS: &str = "exact-word-docids";
    pub const WORD_PREFIX_DOCIDS: &str = "word-prefix-docids";
    pub const EXACT_WORD_PREFIX_DOCIDS: &str = "exact-word-prefix-docids";
    pub const EXTERNAL_DOCUMENTS_IDS: &str = "external-documents-ids";
    pub const DOCID_WORD_POSITIONS: &str = "docid-word-positions";
    pub const WORD_PAIR_PROXIMITY_DOCIDS: &str = "word-pair-proximity-docids";
    pub const WORD_POSITION_DOCIDS: &str = "word-position-docids";
    pub const WORD_FIELD_ID_DOCIDS: &str = "word-field-id-docids";
    pub const WORD_PREFIX_POSITION_DOCIDS: &str = "word-prefix-position-docids";
    pub const WORD_PREFIX_FIELD_ID_DOCIDS: &str = "word-prefix-field-id-docids";
    pub const FIELD_ID_WORD_COUNT_DOCIDS: &str = "field-id-word-count-docids";
    pub const FACET_ID_F64_DOCIDS: &str = "facet-id-f64-docids";
    pub const FACET_ID_EXISTS_DOCIDS: &str = "facet-id-exists-docids";
    pub const FACET_ID_IS_NULL_DOCIDS: &str = "facet-id-is-null-docids";
    pub const FACET_ID_IS_EMPTY_DOCIDS: &str = "facet-id-is-empty-docids";
    pub const FACET_ID_STRING_DOCIDS: &str = "facet-id-string-docids";
    pub const FACET_ID_NORMALIZED_STRING_STRINGS: &str = "facet-id-normalized-string-strings";
    pub const FACET_ID_STRING_FST: &str = "facet-id-string-fst";
    pub const FIELD_ID_DOCID_FACET_F64S: &str = "field-id-docid-facet-f64s";
    pub const FIELD_ID_DOCID_FACET_STRINGS: &str = "field-id-docid-facet-strings";
    pub const VECTOR_EMBEDDER_CATEGORY_ID: &str = "vector-embedder-category-id";
    pub const VECTOR_STORE: &str = "vector-arroy";
    pub const CELLULITE: &str = "cellulite";
    pub const DOCUMENTS: &str = "documents";
}
const NUMBER_OF_DBS: u32 = 25 + Cellulite::nb_dbs();

#[derive(Clone)]
pub struct Index {
    /// The LMDB environment which this index is associated with.
    pub(crate) env: heed::Env<WithoutTls>,

    /// Contains many different types (e.g. the fields ids map).
    pub(crate) main: Database<Unspecified, Unspecified>,

    /// Maps the external documents ids with the internal document id.
    pub external_documents_ids: Database<Str, BEU32>,

    /// A word and all the documents ids containing the word.
    pub word_docids: Database<Str, CboRoaringBitmapCodec>,

    /// A word and all the documents ids containing the word, from attributes for which typos are not allowed.
    pub exact_word_docids: Database<Str, CboRoaringBitmapCodec>,

    /// A prefix of word and all the documents ids containing this prefix.
    pub word_prefix_docids: Database<Str, CboRoaringBitmapCodec>,

    /// A prefix of word and all the documents ids containing this prefix, from attributes for which typos are not allowed.
    pub exact_word_prefix_docids: Database<Str, CboRoaringBitmapCodec>,

    /// Maps the proximity between a pair of words with all the docids where this relation appears.
    pub word_pair_proximity_docids: Database<U8StrStrCodec, CboRoaringBitmapCodec>,

    /// Maps the word and the position with the docids that corresponds to it.
    pub word_position_docids: Database<StrBEU16Codec, CboRoaringBitmapCodec>,
    /// Maps the word and the field id with the docids that corresponds to it.
    pub word_fid_docids: Database<StrBEU16Codec, CboRoaringBitmapCodec>,

    /// Maps the field id and the word count with the docids that corresponds to it.
    pub field_id_word_count_docids: Database<FieldIdWordCountCodec, CboRoaringBitmapCodec>,
    /// Maps the word prefix and a position with all the docids where the prefix appears at the position.
    pub word_prefix_position_docids: Database<StrBEU16Codec, CboRoaringBitmapCodec>,
    /// Maps the word prefix and a field id with all the docids where the prefix appears inside the field
    pub word_prefix_fid_docids: Database<StrBEU16Codec, CboRoaringBitmapCodec>,

    /// Maps the facet field id and the docids for which this field exists
    pub facet_id_exists_docids: Database<FieldIdCodec, CboRoaringBitmapCodec>,
    /// Maps the facet field id and the docids for which this field is set as null
    pub facet_id_is_null_docids: Database<FieldIdCodec, CboRoaringBitmapCodec>,
    /// Maps the facet field id and the docids for which this field is considered empty
    pub facet_id_is_empty_docids: Database<FieldIdCodec, CboRoaringBitmapCodec>,

    /// Maps the facet field id and ranges of numbers with the docids that corresponds to them.
    pub facet_id_f64_docids: Database<FacetGroupKeyCodec<OrderedF64Codec>, FacetGroupValueCodec>,
    /// Maps the facet field id and ranges of strings with the docids that corresponds to them.
    pub facet_id_string_docids: Database<FacetGroupKeyCodec<StrRefCodec>, FacetGroupValueCodec>,
    /// Maps the facet field id of the normalized-for-search string facets with their original versions.
    pub facet_id_normalized_string_strings: Database<BEU16StrCodec, SerdeJson<BTreeSet<String>>>,
    /// Maps the facet field id of the string facets with an FST containing all the facets values.
    pub facet_id_string_fst: Database<BEU16, FstSetCodec>,

    /// Maps the document id, the facet field id and the numbers.
    pub field_id_docid_facet_f64s: Database<FieldDocIdFacetF64Codec, Unit>,
    /// Maps the document id, the facet field id and the strings.
    pub field_id_docid_facet_strings: Database<FieldDocIdFacetStringCodec, Str>,

    /// Maps an embedder name to its id in the vector store.
    pub(crate) embedder_category_id: Database<Unspecified, Unspecified>,
    /// Vector store based on hannoy™.
    pub vector_store: hannoy::Database<Unspecified>,

    /// Geo store based on cellulite™.
    pub cellulite: Cellulite,

    /// Maps the document id to the document as an obkv store.
    pub(crate) documents: Database<BEU32, ObkvCodec>,
}

impl Index {
    pub fn new_with_creation_dates<P: AsRef<Path>>(
        mut options: heed::EnvOpenOptions<WithoutTls>,
        path: P,
        created_at: time::OffsetDateTime,
        updated_at: time::OffsetDateTime,
        creation: bool,
    ) -> Result<Index> {
        use db_name::*;

        options.max_dbs(NUMBER_OF_DBS);

        let env = unsafe { options.open(path) }?;
        let mut wtxn = env.write_txn()?;
        let main = env.database_options().name(MAIN).create(&mut wtxn)?;
        let word_docids = env.create_database(&mut wtxn, Some(WORD_DOCIDS))?;
        let external_documents_ids =
            env.create_database(&mut wtxn, Some(EXTERNAL_DOCUMENTS_IDS))?;
        let exact_word_docids = env.create_database(&mut wtxn, Some(EXACT_WORD_DOCIDS))?;
        let word_prefix_docids = env.create_database(&mut wtxn, Some(WORD_PREFIX_DOCIDS))?;
        let exact_word_prefix_docids =
            env.create_database(&mut wtxn, Some(EXACT_WORD_PREFIX_DOCIDS))?;
        let word_pair_proximity_docids =
            env.create_database(&mut wtxn, Some(WORD_PAIR_PROXIMITY_DOCIDS))?;
        let word_position_docids = env.create_database(&mut wtxn, Some(WORD_POSITION_DOCIDS))?;
        let word_fid_docids = env.create_database(&mut wtxn, Some(WORD_FIELD_ID_DOCIDS))?;
        let field_id_word_count_docids =
            env.create_database(&mut wtxn, Some(FIELD_ID_WORD_COUNT_DOCIDS))?;
        let word_prefix_position_docids =
            env.create_database(&mut wtxn, Some(WORD_PREFIX_POSITION_DOCIDS))?;
        let word_prefix_fid_docids =
            env.create_database(&mut wtxn, Some(WORD_PREFIX_FIELD_ID_DOCIDS))?;
        let facet_id_f64_docids = env.create_database(&mut wtxn, Some(FACET_ID_F64_DOCIDS))?;
        let facet_id_string_docids =
            env.create_database(&mut wtxn, Some(FACET_ID_STRING_DOCIDS))?;
        let facet_id_normalized_string_strings =
            env.create_database(&mut wtxn, Some(FACET_ID_NORMALIZED_STRING_STRINGS))?;
        let facet_id_string_fst = env.create_database(&mut wtxn, Some(FACET_ID_STRING_FST))?;
        let facet_id_exists_docids =
            env.create_database(&mut wtxn, Some(FACET_ID_EXISTS_DOCIDS))?;
        let facet_id_is_null_docids =
            env.create_database(&mut wtxn, Some(FACET_ID_IS_NULL_DOCIDS))?;
        let facet_id_is_empty_docids =
            env.create_database(&mut wtxn, Some(FACET_ID_IS_EMPTY_DOCIDS))?;
        let field_id_docid_facet_f64s =
            env.create_database(&mut wtxn, Some(FIELD_ID_DOCID_FACET_F64S))?;
        let field_id_docid_facet_strings =
            env.create_database(&mut wtxn, Some(FIELD_ID_DOCID_FACET_STRINGS))?;
        // vector stuff
        let embedder_category_id =
            env.create_database(&mut wtxn, Some(VECTOR_EMBEDDER_CATEGORY_ID))?;
        let vector_store = env.create_database(&mut wtxn, Some(VECTOR_STORE))?;
        let cellulite = cellulite::Cellulite::create_from_env(&env, &mut wtxn, CELLULITE)?;

        let documents = env.create_database(&mut wtxn, Some(DOCUMENTS))?;

        let this = Index {
            env: env.clone(),
            main,
            external_documents_ids,
            word_docids,
            exact_word_docids,
            word_prefix_docids,
            exact_word_prefix_docids,
            word_pair_proximity_docids,
            word_position_docids,
            word_fid_docids,
            word_prefix_position_docids,
            word_prefix_fid_docids,
            field_id_word_count_docids,
            facet_id_f64_docids,
            facet_id_string_docids,
            facet_id_normalized_string_strings,
            facet_id_string_fst,
            facet_id_exists_docids,
            facet_id_is_null_docids,
            facet_id_is_empty_docids,
            field_id_docid_facet_f64s,
            field_id_docid_facet_strings,
            vector_store,
            embedder_category_id,
            cellulite,
            documents,
        };
        if this.get_version(&wtxn)?.is_none() && creation {
            this.put_version(
                &mut wtxn,
                (constants::VERSION_MAJOR, constants::VERSION_MINOR, constants::VERSION_PATCH),
            )?;
            // The database before v1.29 defaulted to using arroy, so we
            // need to set it explicitly because the new default is hannoy.
            this.put_vector_store(&mut wtxn, VectorStoreBackend::Hannoy)?;
        }
        wtxn.commit()?;

        Index::set_creation_dates(&this.env, this.main, created_at, updated_at)?;

        Ok(this)
    }

    pub fn new<P: AsRef<Path>>(
        options: heed::EnvOpenOptions<WithoutTls>,
        path: P,
        creation: bool,
    ) -> Result<Index> {
        let now = time::OffsetDateTime::now_utc();
        Self::new_with_creation_dates(options, path, now, now, creation)
    }

    /// Attempts to rollback the index at `path` to the version specified by `requested_version`.
    pub fn rollback<P: AsRef<Path>>(
        mut options: heed::EnvOpenOptions<WithoutTls>,
        path: P,
        requested_version: (u32, u32, u32),
    ) -> Result<RollbackOutcome> {
        options.max_dbs(NUMBER_OF_DBS);

        // optimistically check if the index is already at the requested version.
        let env = unsafe { options.open(path.as_ref()) }?;
        let rtxn = env.read_txn()?;
        let Some(main) = env.database_options().name(db_name::MAIN).open(&rtxn)? else {
            return Err(crate::Error::InternalError(crate::InternalError::DatabaseMissingEntry {
                db_name: db_name::MAIN,
                key: None,
            }));
        };
        let rollback_version =
            main.remap_types::<Str, VersionCodec>().get(&rtxn, main_key::VERSION_KEY)?;
        if rollback_version == Some(requested_version) {
            return Ok(RollbackOutcome::NoRollback);
        }

        // explicitly drop the environment before reopening it.
        drop(rtxn);
        drop(env);

        // really need to rollback then...
        unsafe { options.flags(heed::EnvFlags::PREV_SNAPSHOT) };
        let env = unsafe { options.open(path) }?;
        let mut wtxn = env.write_txn()?;
        let Some(main) = env.database_options().name(db_name::MAIN).open(&wtxn)? else {
            return Err(crate::Error::InternalError(crate::InternalError::DatabaseMissingEntry {
                db_name: db_name::MAIN,
                key: None,
            }));
        };

        let main = main.remap_key_type::<Str>();

        let Some(rollback_version) =
            main.remap_data_type::<VersionCodec>().get(&wtxn, main_key::VERSION_KEY)?
        else {
            return Ok(RollbackOutcome::VersionMismatch {
                requested_version,
                rollback_version: None,
            });
        };

        if requested_version != rollback_version {
            return Ok(RollbackOutcome::VersionMismatch {
                requested_version,
                rollback_version: Some(rollback_version),
            });
        }

        // this is a bit of a trick to force a change in the index
        // which is necessary to actually discard the next snapshot, replacing it with this transaction.
        let now = time::OffsetDateTime::now_utc();
        main.remap_data_type::<SerdeJson<OffsetDateTime>>().put(
            &mut wtxn,
            main_key::UPDATED_AT_KEY,
            &OffsetDateTime(now),
        )?;

        wtxn.commit()?;

        Ok(RollbackOutcome::Rollback)
    }

    fn set_creation_dates(
        env: &heed::Env<WithoutTls>,
        main: Database<Unspecified, Unspecified>,
        created_at: time::OffsetDateTime,
        updated_at: time::OffsetDateTime,
    ) -> heed::Result<()> {
        let mut txn = env.write_txn()?;
        // The db was just created, we update its metadata with the relevant information.
        let main = main.remap_types::<Str, SerdeJson<OffsetDateTime>>();
        if main.get(&txn, main_key::CREATED_AT_KEY)?.is_none() {
            main.put(&mut txn, main_key::UPDATED_AT_KEY, &OffsetDateTime(updated_at))?;
            main.put(&mut txn, main_key::CREATED_AT_KEY, &OffsetDateTime(created_at))?;
            txn.commit()?;
        }
        Ok(())
    }

    /// Create a write transaction to be able to write into the index.
    pub fn write_txn(&self) -> heed::Result<RwTxn<'_>> {
        self.env.write_txn()
    }

    /// Create a read transaction to be able to read the index.
    pub fn read_txn(&self) -> heed::Result<RoTxn<'_, WithoutTls>> {
        self.env.read_txn()
    }

    /// Create a static read transaction to be able to read the index without keeping a reference to it.
    pub fn static_read_txn(&self) -> heed::Result<RoTxn<'static, WithoutTls>> {
        self.env.clone().static_read_txn()
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
    pub fn map_size(&self) -> usize {
        self.env.info().map_size
    }

    pub fn try_clone_inner_file(&self) -> heed::Result<File> {
        self.env.try_clone_inner_file()
    }

    pub fn copy_to_file(&self, file: &mut File, option: CompactionOption) -> Result<()> {
        self.env.copy_to_file(file, option).map_err(Into::into)
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

    /* version */

    /// Writes the version of the database.
    pub(crate) fn put_version(
        &self,
        wtxn: &mut RwTxn<'_>,
        (major, minor, patch): (u32, u32, u32),
    ) -> heed::Result<()> {
        self.main.remap_types::<Str, VersionCodec>().put(
            wtxn,
            main_key::VERSION_KEY,
            &(major, minor, patch),
        )
    }

    /// Get the version of the database. `None` if it was never set.
    pub fn get_version(&self, rtxn: &RoTxn<'_>) -> heed::Result<Option<(u32, u32, u32)>> {
        self.main.remap_types::<Str, VersionCodec>().get(rtxn, main_key::VERSION_KEY)
    }

    /* vector store */
    /// Writes the vector store
    pub(crate) fn put_vector_store(
        &self,
        wtxn: &mut RwTxn<'_>,
        backend: VectorStoreBackend,
    ) -> Result<()> {
        Ok(self.main.remap_types::<Str, SerdeJson<VectorStoreBackend>>().put(
            wtxn,
            main_key::VECTOR_STORE_BACKEND,
            &backend,
        )?)
    }

    pub fn get_vector_store(&self, rtxn: &RoTxn<'_>) -> Result<Option<VectorStoreBackend>> {
        Ok(self
            .main
            .remap_types::<Str, SerdeJson<VectorStoreBackend>>()
            .get(rtxn, main_key::VECTOR_STORE_BACKEND)?)
    }

    pub(crate) fn delete_vector_store(&self, wtxn: &mut RwTxn<'_>) -> Result<bool> {
        Ok(self
            .main
            .remap_types::<Str, SerdeJson<VectorStoreBackend>>()
            .delete(wtxn, main_key::VECTOR_STORE_BACKEND)?)
    }

    /* documents ids */

    /// Writes the documents ids that corresponds to the user-ids-documents-ids FST.
    pub(crate) fn put_documents_ids(
        &self,
        wtxn: &mut RwTxn<'_>,
        docids: &RoaringBitmap,
    ) -> heed::Result<()> {
        self.main.remap_types::<Str, RoaringBitmapCodec>().put(
            wtxn,
            main_key::DOCUMENTS_IDS_KEY,
            docids,
        )
    }

    /// Returns the internal documents ids.
    pub fn documents_ids(&self, rtxn: &RoTxn<'_>) -> heed::Result<RoaringBitmap> {
        Ok(self
            .main
            .remap_types::<Str, RoaringBitmapCodec>()
            .get(rtxn, main_key::DOCUMENTS_IDS_KEY)?
            .unwrap_or_default())
    }

    /// Returns the number of documents indexed in the database.
    pub fn number_of_documents(&self, rtxn: &RoTxn<'_>) -> Result<u64> {
        let count = self
            .main
            .remap_types::<Str, RoaringBitmapLenCodec>()
            .get(rtxn, main_key::DOCUMENTS_IDS_KEY)?;
        Ok(count.unwrap_or_default())
    }

    /// Writes the stats of the documents database.
    pub fn put_documents_stats(
        &self,
        wtxn: &mut RwTxn<'_>,
        stats: DatabaseStats,
    ) -> heed::Result<()> {
        self.main.remap_types::<Str, SerdeJson<DatabaseStats>>().put(
            wtxn,
            main_key::DOCUMENTS_STATS,
            &stats,
        )
    }

    /// Returns the stats of the documents database.
    pub fn documents_stats(&self, rtxn: &RoTxn<'_>) -> heed::Result<Option<DatabaseStats>> {
        self.main
            .remap_types::<Str, SerdeJson<DatabaseStats>>()
            .get(rtxn, main_key::DOCUMENTS_STATS)
    }

    /* primary key */

    /// Writes the documents primary key, this is the field name that is used to store the id.
    pub(crate) fn put_primary_key(
        &self,
        wtxn: &mut RwTxn<'_>,
        primary_key: &str,
    ) -> heed::Result<()> {
        self.set_updated_at(wtxn, &time::OffsetDateTime::now_utc())?;
        self.main.remap_types::<Str, Str>().put(wtxn, main_key::PRIMARY_KEY_KEY, primary_key)
    }

    /// Deletes the primary key of the documents, this can be done to reset indexes settings.
    pub(crate) fn delete_primary_key(&self, wtxn: &mut RwTxn<'_>) -> heed::Result<bool> {
        self.main.remap_key_type::<Str>().delete(wtxn, main_key::PRIMARY_KEY_KEY)
    }

    /// Returns the documents primary key, `None` if it hasn't been defined.
    pub fn primary_key<'t>(&self, rtxn: &'t RoTxn<'_>) -> heed::Result<Option<&'t str>> {
        self.main.remap_types::<Str, Str>().get(rtxn, main_key::PRIMARY_KEY_KEY)
    }

    /* external documents ids */

    /// Returns the external documents ids map which associate the external ids
    /// with the internal ids (i.e. `u32`).
    pub fn external_documents_ids(&self) -> ExternalDocumentsIds {
        ExternalDocumentsIds::new(self.external_documents_ids)
    }

    /* fields ids map */

    /// Writes the fields ids map which associate the documents keys with an internal field id
    /// (i.e. `u8`), this field id is used to identify fields in the obkv documents.
    pub(crate) fn put_fields_ids_map(
        &self,
        wtxn: &mut RwTxn<'_>,
        map: &FieldsIdsMap,
    ) -> heed::Result<()> {
        self.main.remap_types::<Str, SerdeJson<FieldsIdsMap>>().put(
            wtxn,
            main_key::FIELDS_IDS_MAP_KEY,
            map,
        )
    }

    /// Returns the fields ids map which associate the documents keys with an internal field id
    /// (i.e. `u8`), this field id is used to identify fields in the obkv documents.
    pub fn fields_ids_map(&self, rtxn: &RoTxn<'_>) -> heed::Result<FieldsIdsMap> {
        Ok(self
            .main
            .remap_types::<Str, SerdeJson<FieldsIdsMap>>()
            .get(rtxn, main_key::FIELDS_IDS_MAP_KEY)?
            .unwrap_or_default())
    }

    /// Returns the fields ids map with metadata.
    ///
    /// This structure is not yet stored in the index, and is generated on the fly.
    pub fn fields_ids_map_with_metadata(&self, rtxn: &RoTxn<'_>) -> Result<FieldIdMapWithMetadata> {
        Ok(FieldIdMapWithMetadata::new(
            self.fields_ids_map(rtxn)?,
            MetadataBuilder::from_index(self, rtxn)?,
        ))
    }

    /* fieldids weights map */
    // This maps the fields ids to their weights.
    // Their weights is defined by the ordering of the searchable attributes.

    /// Writes the fieldids weights map which associates the field ids to their weights
    pub(crate) fn put_fieldids_weights_map(
        &self,
        wtxn: &mut RwTxn<'_>,
        map: &FieldidsWeightsMap,
    ) -> heed::Result<()> {
        self.main.remap_types::<Str, SerdeJson<_>>().put(
            wtxn,
            main_key::FIELDIDS_WEIGHTS_MAP_KEY,
            map,
        )
    }

    /// Get the fieldids weights map which associates the field ids to their weights
    pub fn fieldids_weights_map(&self, rtxn: &RoTxn<'_>) -> heed::Result<FieldidsWeightsMap> {
        self.main
            .remap_types::<Str, SerdeJson<_>>()
            .get(rtxn, main_key::FIELDIDS_WEIGHTS_MAP_KEY)?
            .map(Ok)
            .unwrap_or_else(|| {
                Ok(FieldidsWeightsMap::from_field_id_map_without_searchable(
                    &self.fields_ids_map(rtxn)?,
                ))
            })
    }

    /// Delete the fieldsids weights map
    pub fn delete_fieldids_weights_map(&self, wtxn: &mut RwTxn<'_>) -> heed::Result<bool> {
        self.main.remap_key_type::<Str>().delete(wtxn, main_key::FIELDIDS_WEIGHTS_MAP_KEY)
    }

    pub fn max_searchable_attribute_weight(&self, rtxn: &RoTxn<'_>) -> Result<Option<Weight>> {
        let user_defined_searchable_fields = self.user_defined_searchable_fields(rtxn)?;
        if let Some(user_defined_searchable_fields) = user_defined_searchable_fields {
            if !user_defined_searchable_fields.contains(&"*") {
                return Ok(Some(user_defined_searchable_fields.len().saturating_sub(1) as Weight));
            }
        }

        Ok(None)
    }

    pub fn searchable_fields_and_weights<'a>(
        &self,
        rtxn: &'a RoTxn<'a>,
    ) -> Result<Vec<(Cow<'a, str>, FieldId, Weight)>> {
        let fid_map = self.fields_ids_map(rtxn)?;
        let weight_map = self.fieldids_weights_map(rtxn)?;
        let searchable = self.searchable_fields(rtxn)?;

        searchable
            .into_iter()
            .map(|field| -> Result<_> {
                let fid = fid_map.id(&field).ok_or_else(|| FieldIdMapMissingEntry::FieldName {
                    field_name: field.to_string(),
                    process: "searchable_fields_and_weights",
                })?;
                let weight = weight_map
                    .weight(fid)
                    .ok_or(InternalError::FieldidsWeightsMapMissingEntry { key: fid })?;

                Ok((field, fid, weight))
            })
            .collect()
    }

    /* geo rtree */

    /// Writes the provided `rtree` which associates coordinates to documents ids.
    pub(crate) fn put_geo_rtree(
        &self,
        wtxn: &mut RwTxn<'_>,
        rtree: &RTree<GeoPoint>,
    ) -> heed::Result<()> {
        self.main.remap_types::<Str, SerdeBincode<RTree<GeoPoint>>>().put(
            wtxn,
            main_key::GEO_RTREE_KEY,
            rtree,
        )
    }

    /// Delete the `rtree` which associates coordinates to documents ids.
    pub(crate) fn delete_geo_rtree(&self, wtxn: &mut RwTxn<'_>) -> heed::Result<bool> {
        self.main.remap_key_type::<Str>().delete(wtxn, main_key::GEO_RTREE_KEY)
    }

    /// Returns the `rtree` which associates coordinates to documents ids.
    pub fn geo_rtree(&self, rtxn: &RoTxn<'_>) -> Result<Option<RTree<GeoPoint>>> {
        match self
            .main
            .remap_types::<Str, SerdeBincode<RTree<GeoPoint>>>()
            .get(rtxn, main_key::GEO_RTREE_KEY)?
        {
            Some(rtree) => Ok(Some(rtree)),
            None => Ok(None),
        }
    }

    /* geo faceted */

    /// Writes the documents ids that are faceted with a _geo field.
    pub(crate) fn put_geo_faceted_documents_ids(
        &self,
        wtxn: &mut RwTxn<'_>,
        docids: &RoaringBitmap,
    ) -> heed::Result<()> {
        self.main.remap_types::<Str, RoaringBitmapCodec>().put(
            wtxn,
            main_key::GEO_FACETED_DOCUMENTS_IDS_KEY,
            docids,
        )
    }

    /// Delete the documents ids that are faceted with a _geo field.
    pub(crate) fn delete_geo_faceted_documents_ids(
        &self,
        wtxn: &mut RwTxn<'_>,
    ) -> heed::Result<bool> {
        self.main.remap_key_type::<Str>().delete(wtxn, main_key::GEO_FACETED_DOCUMENTS_IDS_KEY)
    }

    /// Retrieve all the documents ids that are faceted with a _geo field.
    pub fn geo_faceted_documents_ids(&self, rtxn: &RoTxn<'_>) -> heed::Result<RoaringBitmap> {
        match self
            .main
            .remap_types::<Str, RoaringBitmapCodec>()
            .get(rtxn, main_key::GEO_FACETED_DOCUMENTS_IDS_KEY)?
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
        wtxn: &mut RwTxn<'_>,
        distribution: &FieldDistribution,
    ) -> heed::Result<()> {
        self.main.remap_types::<Str, SerdeJson<FieldDistribution>>().put(
            wtxn,
            main_key::FIELD_DISTRIBUTION_KEY,
            distribution,
        )
    }

    /// Returns the field distribution which associates every field name with
    /// the number of times it occurs in the documents.
    pub fn field_distribution(&self, rtxn: &RoTxn<'_>) -> heed::Result<FieldDistribution> {
        Ok(self
            .main
            .remap_types::<Str, SerdeJson<FieldDistribution>>()
            .get(rtxn, main_key::FIELD_DISTRIBUTION_KEY)?
            .unwrap_or_default())
    }

    /* displayed fields */

    /// Writes the fields that must be displayed in the defined order.
    /// There must be not be any duplicate field id.
    pub(crate) fn put_displayed_fields(
        &self,
        wtxn: &mut RwTxn<'_>,
        fields: &[&str],
    ) -> heed::Result<()> {
        self.main.remap_types::<Str, SerdeBincode<&[&str]>>().put(
            wtxn,
            main_key::DISPLAYED_FIELDS_KEY,
            &fields,
        )
    }

    /// Deletes the displayed fields ids, this will make the engine to display
    /// all the documents attributes in the order of the `FieldsIdsMap`.
    pub(crate) fn delete_displayed_fields(&self, wtxn: &mut RwTxn<'_>) -> heed::Result<bool> {
        self.main.remap_key_type::<Str>().delete(wtxn, main_key::DISPLAYED_FIELDS_KEY)
    }

    /// Returns the displayed fields in the order they were set by the user. If it returns
    /// `None` it means that all the attributes are set as displayed in the order of the `FieldsIdsMap`.
    pub fn displayed_fields<'t>(&self, rtxn: &'t RoTxn<'_>) -> heed::Result<Option<Vec<&'t str>>> {
        self.main
            .remap_types::<Str, SerdeBincode<Vec<&'t str>>>()
            .get(rtxn, main_key::DISPLAYED_FIELDS_KEY)
    }

    /// Identical to `displayed_fields`, but returns the ids instead.
    pub fn displayed_fields_ids(&self, rtxn: &RoTxn<'_>) -> Result<Option<Vec<FieldId>>> {
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

    /* remove hidden fields */
    pub fn remove_hidden_fields(
        &self,
        rtxn: &RoTxn<'_>,
        fields: impl IntoIterator<Item = impl AsRef<str>>,
    ) -> Result<(BTreeSet<String>, bool)> {
        let mut valid_fields =
            fields.into_iter().map(|f| f.as_ref().to_string()).collect::<BTreeSet<String>>();

        let fields_len = valid_fields.len();

        if let Some(dn) = self.displayed_fields(rtxn)? {
            let displayable_names = dn.iter().map(|s| s.to_string()).collect();
            valid_fields = &valid_fields & &displayable_names;
        }

        let hidden_fields = fields_len > valid_fields.len();
        Ok((valid_fields, hidden_fields))
    }

    /* searchable fields */

    /// Write the user defined searchable fields and generate the real searchable fields from the specified fields ids map.
    pub(crate) fn put_all_searchable_fields_from_fields_ids_map(
        &self,
        wtxn: &mut RwTxn<'_>,
        user_fields: &[&str],
        fields_ids_map: &FieldIdMapWithMetadata,
    ) -> Result<()> {
        // We can write the user defined searchable fields as-is.
        self.put_user_defined_searchable_fields(wtxn, user_fields)?;

        let mut weights = FieldidsWeightsMap::default();

        // Now we generate the real searchable fields:
        let mut real_fields = Vec::new();
        for (id, field_from_map, metadata) in fields_ids_map.iter() {
            if let Some(weight) = metadata.searchable_weight() {
                real_fields.push(field_from_map);
                weights.insert(id, weight);
            }
        }

        self.put_searchable_fields(wtxn, &real_fields)?;
        self.put_fieldids_weights_map(wtxn, &weights)?;

        Ok(())
    }

    pub(crate) fn delete_all_searchable_fields(&self, wtxn: &mut RwTxn<'_>) -> heed::Result<bool> {
        let did_delete_searchable = self.delete_searchable_fields(wtxn)?;
        let did_delete_user_defined = self.delete_user_defined_searchable_fields(wtxn)?;
        self.delete_fieldids_weights_map(wtxn)?;
        Ok(did_delete_searchable || did_delete_user_defined)
    }

    /// Writes the searchable fields, when this list is specified, only these are indexed.
    fn put_searchable_fields(&self, wtxn: &mut RwTxn<'_>, fields: &[&str]) -> heed::Result<()> {
        self.main.remap_types::<Str, SerdeBincode<&[&str]>>().put(
            wtxn,
            main_key::SEARCHABLE_FIELDS_KEY,
            &fields,
        )
    }

    /// Deletes the searchable fields, when no fields are specified, all fields are indexed.
    fn delete_searchable_fields(&self, wtxn: &mut RwTxn<'_>) -> heed::Result<bool> {
        self.main.remap_key_type::<Str>().delete(wtxn, main_key::SEARCHABLE_FIELDS_KEY)
    }

    /// Returns the searchable fields, those are the fields that are indexed,
    pub fn searchable_fields<'t>(&self, rtxn: &'t RoTxn<'_>) -> heed::Result<Vec<Cow<'t, str>>> {
        self.main
            .remap_types::<Str, SerdeBincode<Vec<&'t str>>>()
            .get(rtxn, main_key::SEARCHABLE_FIELDS_KEY)?
            .map(|fields| Ok(fields.into_iter().map(Cow::Borrowed).collect()))
            .unwrap_or_else(|| {
                Ok(self
                    .fields_ids_map(rtxn)?
                    .names()
                    .filter(|name| !crate::is_faceted_by(name, RESERVED_VECTORS_FIELD_NAME))
                    .map(|field| Cow::Owned(field.to_string()))
                    .collect())
            })
    }

    /// Identical to `searchable_fields`, but returns the ids instead.
    pub fn searchable_fields_ids(&self, rtxn: &RoTxn<'_>) -> Result<Vec<FieldId>> {
        let fields = self.searchable_fields(rtxn)?;
        let fields_ids_map = self.fields_ids_map(rtxn)?;
        let mut fields_ids = Vec::new();
        for name in fields {
            if let Some(field_id) = fields_ids_map.id(&name) {
                fields_ids.push(field_id);
            }
        }
        Ok(fields_ids)
    }

    /// Writes the searchable fields, when this list is specified, only these are indexed.
    pub(crate) fn put_user_defined_searchable_fields(
        &self,
        wtxn: &mut RwTxn<'_>,
        fields: &[&str],
    ) -> heed::Result<()> {
        self.main.remap_types::<Str, SerdeBincode<_>>().put(
            wtxn,
            main_key::USER_DEFINED_SEARCHABLE_FIELDS_KEY,
            &fields,
        )
    }

    /// Deletes the searchable fields, when no fields are specified, all fields are indexed.
    pub(crate) fn delete_user_defined_searchable_fields(
        &self,
        wtxn: &mut RwTxn<'_>,
    ) -> heed::Result<bool> {
        self.main.remap_key_type::<Str>().delete(wtxn, main_key::USER_DEFINED_SEARCHABLE_FIELDS_KEY)
    }

    /// Returns the user defined searchable fields.
    pub fn user_defined_searchable_fields<'t>(
        &self,
        rtxn: &'t RoTxn<'t>,
    ) -> heed::Result<Option<Vec<&'t str>>> {
        self.main
            .remap_types::<Str, SerdeBincode<Vec<_>>>()
            .get(rtxn, main_key::USER_DEFINED_SEARCHABLE_FIELDS_KEY)
    }

    /// Identical to `user_defined_searchable_fields`, but returns ids instead.
    pub fn user_defined_searchable_fields_ids(
        &self,
        rtxn: &RoTxn<'_>,
    ) -> Result<Option<Vec<FieldId>>> {
        match self.user_defined_searchable_fields(rtxn)? {
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

    /* filterable fields */

    /// Writes the filterable attributes rules in the database.
    pub(crate) fn put_filterable_attributes_rules(
        &self,
        wtxn: &mut RwTxn<'_>,
        fields: &[FilterableAttributesRule],
    ) -> heed::Result<()> {
        self.main.remap_types::<Str, SerdeJson<_>>().put(
            wtxn,
            main_key::FILTERABLE_FIELDS_KEY,
            &fields,
        )
    }

    /// Deletes the filterable attributes rules in the database.
    pub(crate) fn delete_filterable_attributes_rules(
        &self,
        wtxn: &mut RwTxn<'_>,
    ) -> heed::Result<bool> {
        self.main.remap_key_type::<Str>().delete(wtxn, main_key::FILTERABLE_FIELDS_KEY)
    }

    /// Returns the filterable attributes rules.
    pub fn filterable_attributes_rules(
        &self,
        rtxn: &RoTxn<'_>,
    ) -> heed::Result<Vec<FilterableAttributesRule>> {
        Ok(self
            .main
            .remap_types::<Str, SerdeJson<_>>()
            .get(rtxn, main_key::FILTERABLE_FIELDS_KEY)?
            .unwrap_or_default())
    }

    /* sortable fields */

    /// Writes the sortable fields names in the database.
    pub(crate) fn put_sortable_fields(
        &self,
        wtxn: &mut RwTxn<'_>,
        fields: &HashSet<String>,
    ) -> heed::Result<()> {
        self.main.remap_types::<Str, SerdeJson<_>>().put(
            wtxn,
            main_key::SORTABLE_FIELDS_KEY,
            fields,
        )
    }

    /// Deletes the sortable fields ids in the database.
    pub(crate) fn delete_sortable_fields(&self, wtxn: &mut RwTxn<'_>) -> heed::Result<bool> {
        self.main.remap_key_type::<Str>().delete(wtxn, main_key::SORTABLE_FIELDS_KEY)
    }

    /// Returns the sortable fields names.
    pub fn sortable_fields(&self, rtxn: &RoTxn<'_>) -> heed::Result<HashSet<String>> {
        Ok(self
            .main
            .remap_types::<Str, SerdeJson<_>>()
            .get(rtxn, main_key::SORTABLE_FIELDS_KEY)?
            .unwrap_or_default())
    }

    /// Identical to `sortable_fields`, but returns ids instead.
    pub fn sortable_fields_ids(&self, rtxn: &RoTxn<'_>) -> Result<HashSet<FieldId>> {
        let fields = self.sortable_fields(rtxn)?;
        let fields_ids_map = self.fields_ids_map(rtxn)?;
        Ok(fields.into_iter().filter_map(|name| fields_ids_map.id(&name)).collect())
    }

    /// Returns true if the geo feature is enabled.
    pub fn is_geo_enabled(&self, rtxn: &RoTxn<'_>) -> Result<bool> {
        let geo_filter = self.is_geo_filtering_enabled(rtxn)?;
        let geo_sortable = self.is_geo_sorting_enabled(rtxn)?;
        Ok(geo_filter || geo_sortable)
    }

    /// Returns true if the geo sorting feature is enabled.
    pub fn is_geo_sorting_enabled(&self, rtxn: &RoTxn<'_>) -> Result<bool> {
        let geo_sortable = self.sortable_fields(rtxn)?.contains(RESERVED_GEO_FIELD_NAME);
        Ok(geo_sortable)
    }

    /// Returns true if the geo filtering feature is enabled.
    pub fn is_geo_filtering_enabled(&self, rtxn: &RoTxn<'_>) -> Result<bool> {
        let geo_filter =
            self.filterable_attributes_rules(rtxn)?.iter().any(|field| field.has_geo());
        Ok(geo_filter)
    }

    /// Returns true if the geo sorting feature is enabled.
    pub fn is_geojson_filtering_enabled(&self, rtxn: &RoTxn<'_>) -> Result<bool> {
        let geojson_filter =
            self.filterable_attributes_rules(rtxn)?.iter().any(|field| field.has_geojson());
        Ok(geojson_filter)
    }

    pub fn asc_desc_fields(&self, rtxn: &RoTxn<'_>) -> Result<HashSet<String>> {
        let asc_desc_fields = self
            .criteria(rtxn)?
            .into_iter()
            .filter_map(|criterion| match criterion {
                Criterion::Asc(field) | Criterion::Desc(field) => Some(field),
                _otherwise => None,
            })
            .collect();

        Ok(asc_desc_fields)
    }

    /* faceted documents ids */

    /// Retrieve all the documents which contain this field id set as null
    pub fn null_faceted_documents_ids(
        &self,
        rtxn: &RoTxn<'_>,
        field_id: FieldId,
    ) -> heed::Result<RoaringBitmap> {
        match self.facet_id_is_null_docids.get(rtxn, &field_id)? {
            Some(docids) => Ok(docids),
            None => Ok(RoaringBitmap::new()),
        }
    }

    /// Retrieve all the documents which contain this field id and that is considered empty
    pub fn empty_faceted_documents_ids(
        &self,
        rtxn: &RoTxn<'_>,
        field_id: FieldId,
    ) -> heed::Result<RoaringBitmap> {
        match self.facet_id_is_empty_docids.get(rtxn, &field_id)? {
            Some(docids) => Ok(docids),
            None => Ok(RoaringBitmap::new()),
        }
    }

    /// Retrieve all the documents which contain this field id
    pub fn exists_faceted_documents_ids(
        &self,
        rtxn: &RoTxn<'_>,
        field_id: FieldId,
    ) -> heed::Result<RoaringBitmap> {
        match self.facet_id_exists_docids.get(rtxn, &field_id)? {
            Some(docids) => Ok(docids),
            None => Ok(RoaringBitmap::new()),
        }
    }

    /* distinct field */

    pub(crate) fn put_distinct_field(
        &self,
        wtxn: &mut RwTxn<'_>,
        distinct_field: &str,
    ) -> heed::Result<()> {
        self.main.remap_types::<Str, Str>().put(wtxn, main_key::DISTINCT_FIELD_KEY, distinct_field)
    }

    pub fn distinct_field<'a>(&self, rtxn: &'a RoTxn<'_>) -> heed::Result<Option<&'a str>> {
        self.main.remap_types::<Str, Str>().get(rtxn, main_key::DISTINCT_FIELD_KEY)
    }

    pub(crate) fn delete_distinct_field(&self, wtxn: &mut RwTxn<'_>) -> heed::Result<bool> {
        self.main.remap_key_type::<Str>().delete(wtxn, main_key::DISTINCT_FIELD_KEY)
    }

    /* criteria */

    pub(crate) fn put_criteria(
        &self,
        wtxn: &mut RwTxn<'_>,
        criteria: &[Criterion],
    ) -> heed::Result<()> {
        self.main.remap_types::<Str, SerdeJson<&[Criterion]>>().put(
            wtxn,
            main_key::CRITERIA_KEY,
            &criteria,
        )
    }

    pub(crate) fn delete_criteria(&self, wtxn: &mut RwTxn<'_>) -> heed::Result<bool> {
        self.main.remap_key_type::<Str>().delete(wtxn, main_key::CRITERIA_KEY)
    }

    pub fn criteria(&self, rtxn: &RoTxn<'_>) -> heed::Result<Vec<Criterion>> {
        match self
            .main
            .remap_types::<Str, SerdeJson<Vec<Criterion>>>()
            .get(rtxn, main_key::CRITERIA_KEY)?
        {
            Some(criteria) => Ok(criteria),
            None => Ok(default_criteria()),
        }
    }

    /* words fst */

    /// Writes the FST which is the words dictionary of the engine.
    pub(crate) fn put_words_fst<A: AsRef<[u8]>>(
        &self,
        wtxn: &mut RwTxn<'_>,
        fst: &fst::Set<A>,
    ) -> heed::Result<()> {
        self.main.remap_types::<Str, Bytes>().put(
            wtxn,
            main_key::WORDS_FST_KEY,
            fst.as_fst().as_bytes(),
        )
    }

    /// Returns the FST which is the words dictionary of the engine.
    pub fn words_fst<'t>(&self, rtxn: &'t RoTxn<'_>) -> Result<fst::Set<Cow<'t, [u8]>>> {
        match self.main.remap_types::<Str, Bytes>().get(rtxn, main_key::WORDS_FST_KEY)? {
            Some(bytes) => Ok(fst::Set::new(bytes)?.map_data(Cow::Borrowed)?),
            None => Ok(fst::Set::default().map_data(Cow::Owned)?),
        }
    }

    /* stop words */

    pub(crate) fn put_stop_words<A: AsRef<[u8]>>(
        &self,
        wtxn: &mut RwTxn<'_>,
        fst: &fst::Set<A>,
    ) -> heed::Result<()> {
        self.main.remap_types::<Str, Bytes>().put(
            wtxn,
            main_key::STOP_WORDS_KEY,
            fst.as_fst().as_bytes(),
        )
    }

    pub(crate) fn delete_stop_words(&self, wtxn: &mut RwTxn<'_>) -> heed::Result<bool> {
        self.main.remap_key_type::<Str>().delete(wtxn, main_key::STOP_WORDS_KEY)
    }

    pub fn stop_words<'t>(&self, rtxn: &'t RoTxn<'t>) -> Result<Option<fst::Set<&'t [u8]>>> {
        match self.main.remap_types::<Str, Bytes>().get(rtxn, main_key::STOP_WORDS_KEY)? {
            Some(bytes) => Ok(Some(fst::Set::new(bytes)?)),
            None => Ok(None),
        }
    }

    /* non separator tokens */

    pub(crate) fn put_non_separator_tokens(
        &self,
        wtxn: &mut RwTxn<'_>,
        set: &BTreeSet<String>,
    ) -> heed::Result<()> {
        self.main.remap_types::<Str, SerdeBincode<_>>().put(
            wtxn,
            main_key::NON_SEPARATOR_TOKENS_KEY,
            set,
        )
    }

    pub(crate) fn delete_non_separator_tokens(&self, wtxn: &mut RwTxn<'_>) -> heed::Result<bool> {
        self.main.remap_key_type::<Str>().delete(wtxn, main_key::NON_SEPARATOR_TOKENS_KEY)
    }

    pub fn non_separator_tokens(&self, rtxn: &RoTxn<'_>) -> Result<Option<BTreeSet<String>>> {
        Ok(self
            .main
            .remap_types::<Str, SerdeBincode<BTreeSet<String>>>()
            .get(rtxn, main_key::NON_SEPARATOR_TOKENS_KEY)?)
    }

    /* separator tokens */

    pub(crate) fn put_separator_tokens(
        &self,
        wtxn: &mut RwTxn<'_>,
        set: &BTreeSet<String>,
    ) -> heed::Result<()> {
        self.main.remap_types::<Str, SerdeBincode<_>>().put(
            wtxn,
            main_key::SEPARATOR_TOKENS_KEY,
            set,
        )
    }

    pub(crate) fn delete_separator_tokens(&self, wtxn: &mut RwTxn<'_>) -> heed::Result<bool> {
        self.main.remap_key_type::<Str>().delete(wtxn, main_key::SEPARATOR_TOKENS_KEY)
    }

    pub fn separator_tokens(&self, rtxn: &RoTxn<'_>) -> Result<Option<BTreeSet<String>>> {
        Ok(self
            .main
            .remap_types::<Str, SerdeBincode<BTreeSet<String>>>()
            .get(rtxn, main_key::SEPARATOR_TOKENS_KEY)?)
    }

    /* separators easing method */

    pub fn allowed_separators(&self, rtxn: &RoTxn<'_>) -> Result<Option<BTreeSet<String>>> {
        let default_separators =
            charabia::separators::DEFAULT_SEPARATORS.iter().map(|s| s.to_string());
        let mut separators: Option<BTreeSet<_>> = None;
        if let Some(mut separator_tokens) = self.separator_tokens(rtxn)? {
            separator_tokens.extend(default_separators.clone());
            separators = Some(separator_tokens);
        }

        if let Some(non_separator_tokens) = self.non_separator_tokens(rtxn)? {
            separators = separators
                .or_else(|| Some(default_separators.collect()))
                .map(|separators| &separators - &non_separator_tokens);
        }

        Ok(separators)
    }

    /* dictionary */

    pub(crate) fn put_dictionary(
        &self,
        wtxn: &mut RwTxn<'_>,
        set: &BTreeSet<String>,
    ) -> heed::Result<()> {
        self.main.remap_types::<Str, SerdeBincode<_>>().put(wtxn, main_key::DICTIONARY_KEY, set)
    }

    pub(crate) fn delete_dictionary(&self, wtxn: &mut RwTxn<'_>) -> heed::Result<bool> {
        self.main.remap_key_type::<Str>().delete(wtxn, main_key::DICTIONARY_KEY)
    }

    pub fn dictionary(&self, rtxn: &RoTxn<'_>) -> Result<Option<BTreeSet<String>>> {
        Ok(self
            .main
            .remap_types::<Str, SerdeBincode<BTreeSet<String>>>()
            .get(rtxn, main_key::DICTIONARY_KEY)?)
    }

    /* synonyms */

    pub(crate) fn put_synonyms(
        &self,
        wtxn: &mut RwTxn<'_>,
        synonyms: &HashMap<Vec<String>, Vec<Vec<String>>>,
        user_defined_synonyms: &BTreeMap<String, Vec<String>>,
    ) -> heed::Result<()> {
        self.main.remap_types::<Str, SerdeBincode<_>>().put(
            wtxn,
            main_key::SYNONYMS_KEY,
            synonyms,
        )?;
        self.main.remap_types::<Str, SerdeBincode<_>>().put(
            wtxn,
            main_key::USER_DEFINED_SYNONYMS_KEY,
            user_defined_synonyms,
        )
    }

    pub(crate) fn delete_synonyms(&self, wtxn: &mut RwTxn<'_>) -> heed::Result<bool> {
        self.main.remap_key_type::<Str>().delete(wtxn, main_key::SYNONYMS_KEY)?;
        self.main.remap_key_type::<Str>().delete(wtxn, main_key::USER_DEFINED_SYNONYMS_KEY)
    }

    pub fn user_defined_synonyms(
        &self,
        rtxn: &RoTxn<'_>,
    ) -> heed::Result<BTreeMap<String, Vec<String>>> {
        Ok(self
            .main
            .remap_types::<Str, SerdeBincode<_>>()
            .get(rtxn, main_key::USER_DEFINED_SYNONYMS_KEY)?
            .unwrap_or_default())
    }

    pub fn synonyms(
        &self,
        rtxn: &RoTxn<'_>,
    ) -> heed::Result<HashMap<Vec<String>, Vec<Vec<String>>>> {
        Ok(self
            .main
            .remap_types::<Str, SerdeBincode<_>>()
            .get(rtxn, main_key::SYNONYMS_KEY)?
            .unwrap_or_default())
    }

    pub fn words_synonyms<S: AsRef<str>>(
        &self,
        rtxn: &RoTxn<'_>,
        words: &[S],
    ) -> heed::Result<Option<Vec<Vec<String>>>> {
        let words: Vec<_> = words.iter().map(|s| s.as_ref().to_owned()).collect();
        Ok(self.synonyms(rtxn)?.remove(&words))
    }

    /* words prefixes fst */

    /// Writes the FST which is the words prefixes dictionary of the engine.
    pub(crate) fn put_words_prefixes_fst<A: AsRef<[u8]>>(
        &self,
        wtxn: &mut RwTxn<'_>,
        fst: &fst::Set<A>,
    ) -> heed::Result<()> {
        self.main.remap_types::<Str, Bytes>().put(
            wtxn,
            main_key::WORDS_PREFIXES_FST_KEY,
            fst.as_fst().as_bytes(),
        )
    }

    pub(crate) fn delete_words_prefixes_fst(&self, wtxn: &mut RwTxn<'_>) -> heed::Result<bool> {
        self.main.remap_key_type::<Str>().delete(wtxn, main_key::WORDS_PREFIXES_FST_KEY)
    }

    /// Returns the FST which is the words prefixes dictionary of the engine.
    pub fn words_prefixes_fst<'t>(&self, rtxn: &'t RoTxn<'t>) -> Result<fst::Set<Cow<'t, [u8]>>> {
        match self.main.remap_types::<Str, Bytes>().get(rtxn, main_key::WORDS_PREFIXES_FST_KEY)? {
            Some(bytes) => Ok(fst::Set::new(bytes)?.map_data(Cow::Borrowed)?),
            None => Ok(fst::Set::default().map_data(Cow::Owned)?),
        }
    }

    /* word documents count */

    /// Returns the number of documents ids associated with the given word,
    /// it is much faster than deserializing the bitmap and getting the length of it.
    pub fn word_documents_count(&self, rtxn: &RoTxn<'_>, word: &str) -> heed::Result<Option<u64>> {
        self.word_docids.remap_data_type::<RoaringBitmapLenCodec>().get(rtxn, word)
    }

    /* documents */

    /// Returns a document by using the document id.
    pub fn document<'t>(&self, rtxn: &'t RoTxn, id: DocumentId) -> Result<&'t obkv::KvReaderU16> {
        self.documents
            .get(rtxn, &id)?
            .ok_or(UserError::UnknownInternalDocumentId { document_id: id })
            .map_err(Into::into)
    }

    /// Returns an iterator over the requested documents. The next item will be an error if a document is missing.
    pub fn iter_documents<'a, 't: 'a>(
        &'a self,
        rtxn: &'t RoTxn<'t>,
        ids: impl IntoIterator<Item = DocumentId> + 'a,
    ) -> Result<impl Iterator<Item = Result<(DocumentId, &'t obkv::KvReaderU16)>> + 'a> {
        Ok(ids.into_iter().map(move |id| {
            let kv = self
                .documents
                .get(rtxn, &id)?
                .ok_or(UserError::UnknownInternalDocumentId { document_id: id })?;
            Ok((id, kv))
        }))
    }

    /// Returns a [`Vec`] of the requested documents. Returns an error if a document is missing.
    pub fn documents<'t>(
        &self,
        rtxn: &'t RoTxn<'t>,
        ids: impl IntoIterator<Item = DocumentId>,
    ) -> Result<Vec<(DocumentId, &'t obkv::KvReaderU16)>> {
        self.iter_documents(rtxn, ids)?.collect()
    }

    /// Returns an iterator over all the documents in the index.
    pub fn all_documents<'a, 't: 'a>(
        &'a self,
        rtxn: &'t RoTxn<'t>,
    ) -> Result<impl Iterator<Item = Result<(DocumentId, &'t obkv::KvReaderU16)>> + 'a> {
        self.iter_documents(rtxn, self.documents_ids(rtxn)?)
    }

    pub fn external_id_of<'a, 't: 'a>(
        &'a self,
        rtxn: &'t RoTxn<'t>,
        ids: impl IntoIterator<Item = DocumentId> + 'a,
    ) -> Result<impl IntoIterator<Item = Result<String>> + 'a> {
        let fields = self.fields_ids_map(rtxn)?;

        // uses precondition "never called on an empty index"
        let primary_key = self.primary_key(rtxn)?.ok_or(InternalError::DatabaseMissingEntry {
            db_name: db_name::MAIN,
            key: Some(main_key::PRIMARY_KEY_KEY),
        })?;
        let primary_key = PrimaryKey::new(primary_key, &fields).ok_or_else(|| {
            InternalError::FieldIdMapMissingEntry(crate::FieldIdMapMissingEntry::FieldName {
                field_name: primary_key.to_owned(),
                process: "external_id_of",
            })
        })?;
        Ok(self.iter_documents(rtxn, ids)?.map(move |entry| -> Result<_> {
            let (_docid, obkv) = entry?;
            match primary_key.document_id(obkv, &fields)? {
                Ok(document_id) => Ok(document_id),
                Err(_) => Err(InternalError::DocumentsError(
                    crate::documents::Error::InvalidDocumentFormat,
                )
                .into()),
            }
        }))
    }

    pub fn facets_distribution<'a>(&'a self, rtxn: &'a RoTxn<'a>) -> FacetDistribution<'a> {
        FacetDistribution::new(rtxn, self)
    }

    pub fn search<'a>(&'a self, rtxn: &'a RoTxn<'a>) -> Search<'a> {
        Search::new(rtxn, self)
    }

    /// Returns the index creation time.
    pub fn created_at(&self, rtxn: &RoTxn<'_>) -> Result<time::OffsetDateTime> {
        Ok(self
            .main
            .remap_types::<Str, SerdeJson<OffsetDateTime>>()
            .get(rtxn, main_key::CREATED_AT_KEY)?
            .ok_or(InternalError::DatabaseMissingEntry {
                db_name: db_name::MAIN,
                key: Some(main_key::CREATED_AT_KEY),
            })?
            .0)
    }

    /// Returns the index last updated time.
    pub fn updated_at(&self, rtxn: &RoTxn<'_>) -> Result<time::OffsetDateTime> {
        Ok(self
            .main
            .remap_types::<Str, SerdeJson<OffsetDateTime>>()
            .get(rtxn, main_key::UPDATED_AT_KEY)?
            .ok_or(InternalError::DatabaseMissingEntry {
                db_name: db_name::MAIN,
                key: Some(main_key::UPDATED_AT_KEY),
            })?
            .0)
    }

    pub fn set_updated_at(
        &self,
        wtxn: &mut RwTxn<'_>,
        time: &time::OffsetDateTime,
    ) -> heed::Result<()> {
        self.main.remap_types::<Str, SerdeJson<OffsetDateTime>>().put(
            wtxn,
            main_key::UPDATED_AT_KEY,
            &OffsetDateTime(*time),
        )
    }

    pub fn authorize_typos(&self, txn: &RoTxn<'_>) -> heed::Result<bool> {
        // It is not possible to put a bool in heed with OwnedType, so we put a u8 instead. We
        // identify 0 as being false, and anything else as true. The absence of a value is true,
        // because by default, we authorize typos.
        match self.main.remap_types::<Str, U8>().get(txn, main_key::AUTHORIZE_TYPOS)? {
            Some(0) => Ok(false),
            _ => Ok(true),
        }
    }

    pub(crate) fn put_authorize_typos(&self, txn: &mut RwTxn<'_>, flag: bool) -> heed::Result<()> {
        // It is not possible to put a bool in heed with OwnedType, so we put a u8 instead. We
        // identify 0 as being false, and anything else as true. The absence of a value is true,
        // because by default, we authorize typos.
        self.main.remap_types::<Str, U8>().put(txn, main_key::AUTHORIZE_TYPOS, &(flag as u8))?;

        Ok(())
    }

    pub fn min_word_len_one_typo(&self, txn: &RoTxn<'_>) -> heed::Result<u8> {
        // It is not possible to put a bool in heed with OwnedType, so we put a u8 instead. We
        // identify 0 as being false, and anything else as true. The absence of a value is true,
        // because by default, we authorize typos.
        Ok(self
            .main
            .remap_types::<Str, U8>()
            .get(txn, main_key::ONE_TYPO_WORD_LEN)?
            .unwrap_or(DEFAULT_MIN_WORD_LEN_ONE_TYPO))
    }

    pub(crate) fn put_min_word_len_one_typo(
        &self,
        txn: &mut RwTxn<'_>,
        val: u8,
    ) -> heed::Result<()> {
        // It is not possible to put a bool in heed with OwnedType, so we put a u8 instead. We
        // identify 0 as being false, and anything else as true. The absence of a value is true,
        // because by default, we authorize typos.
        self.main.remap_types::<Str, U8>().put(txn, main_key::ONE_TYPO_WORD_LEN, &val)?;
        Ok(())
    }

    pub fn min_word_len_two_typos(&self, txn: &RoTxn<'_>) -> heed::Result<u8> {
        // It is not possible to put a bool in heed with OwnedType, so we put a u8 instead. We
        // identify 0 as being false, and anything else as true. The absence of a value is true,
        // because by default, we authorize typos.
        Ok(self
            .main
            .remap_types::<Str, U8>()
            .get(txn, main_key::TWO_TYPOS_WORD_LEN)?
            .unwrap_or(DEFAULT_MIN_WORD_LEN_TWO_TYPOS))
    }

    pub(crate) fn put_min_word_len_two_typos(
        &self,
        txn: &mut RwTxn<'_>,
        val: u8,
    ) -> heed::Result<()> {
        // It is not possible to put a bool in heed with OwnedType, so we put a u8 instead. We
        // identify 0 as being false, and anything else as true. The absence of a value is true,
        // because by default, we authorize typos.
        self.main.remap_types::<Str, U8>().put(txn, main_key::TWO_TYPOS_WORD_LEN, &val)?;
        Ok(())
    }

    /// List the words on which typo are not allowed
    pub fn exact_words<'t>(&self, txn: &'t RoTxn<'t>) -> Result<Option<fst::Set<Cow<'t, [u8]>>>> {
        match self.main.remap_types::<Str, Bytes>().get(txn, main_key::EXACT_WORDS)? {
            Some(bytes) => Ok(Some(fst::Set::new(bytes)?.map_data(Cow::Borrowed)?)),
            None => Ok(None),
        }
    }

    pub(crate) fn put_exact_words<A: AsRef<[u8]>>(
        &self,
        txn: &mut RwTxn<'_>,
        words: &fst::Set<A>,
    ) -> Result<()> {
        self.main.remap_types::<Str, Bytes>().put(
            txn,
            main_key::EXACT_WORDS,
            words.as_fst().as_bytes(),
        )?;
        Ok(())
    }

    /// Returns the exact attributes: attributes for which typo is disallowed.
    pub fn exact_attributes<'t>(&self, txn: &'t RoTxn<'t>) -> Result<Vec<&'t str>> {
        Ok(self
            .main
            .remap_types::<Str, SerdeBincode<Vec<&str>>>()
            .get(txn, main_key::EXACT_ATTRIBUTES)?
            .unwrap_or_default())
    }

    /// Returns the list of exact attributes field ids.
    pub fn exact_attributes_ids(&self, txn: &RoTxn<'_>) -> Result<HashSet<FieldId>> {
        let attrs = self.exact_attributes(txn)?;
        let fid_map = self.fields_ids_map(txn)?;
        Ok(attrs.iter().filter_map(|attr| fid_map.id(attr)).collect())
    }

    /// Writes the exact attributes to the database.
    pub(crate) fn put_exact_attributes(&self, txn: &mut RwTxn<'_>, attrs: &[&str]) -> Result<()> {
        self.main.remap_types::<Str, SerdeBincode<&[&str]>>().put(
            txn,
            main_key::EXACT_ATTRIBUTES,
            &attrs,
        )?;
        Ok(())
    }

    /// Clears the exact attributes from the store.
    pub(crate) fn delete_exact_attributes(&self, txn: &mut RwTxn<'_>) -> heed::Result<bool> {
        self.main.remap_key_type::<Str>().delete(txn, main_key::EXACT_ATTRIBUTES)
    }

    pub fn max_values_per_facet(&self, txn: &RoTxn<'_>) -> heed::Result<Option<u64>> {
        self.main.remap_types::<Str, BEU64>().get(txn, main_key::MAX_VALUES_PER_FACET)
    }

    pub(crate) fn put_max_values_per_facet(
        &self,
        txn: &mut RwTxn<'_>,
        val: u64,
    ) -> heed::Result<()> {
        self.main.remap_types::<Str, BEU64>().put(txn, main_key::MAX_VALUES_PER_FACET, &val)
    }

    pub(crate) fn delete_max_values_per_facet(&self, txn: &mut RwTxn<'_>) -> heed::Result<bool> {
        self.main.remap_key_type::<Str>().delete(txn, main_key::MAX_VALUES_PER_FACET)
    }

    pub fn sort_facet_values_by(&self, txn: &RoTxn<'_>) -> heed::Result<OrderByMap> {
        let orders = self
            .main
            .remap_types::<Str, SerdeJson<OrderByMap>>()
            .get(txn, main_key::SORT_FACET_VALUES_BY)?
            .unwrap_or_default();
        Ok(orders)
    }

    pub(crate) fn put_sort_facet_values_by(
        &self,
        txn: &mut RwTxn<'_>,
        val: &OrderByMap,
    ) -> heed::Result<()> {
        self.main.remap_types::<Str, SerdeJson<_>>().put(txn, main_key::SORT_FACET_VALUES_BY, &val)
    }

    pub(crate) fn delete_sort_facet_values_by(&self, txn: &mut RwTxn<'_>) -> heed::Result<bool> {
        self.main.remap_key_type::<Str>().delete(txn, main_key::SORT_FACET_VALUES_BY)
    }

    pub fn pagination_max_total_hits(&self, txn: &RoTxn<'_>) -> heed::Result<Option<u64>> {
        self.main.remap_types::<Str, BEU64>().get(txn, main_key::PAGINATION_MAX_TOTAL_HITS)
    }

    pub(crate) fn put_pagination_max_total_hits(
        &self,
        txn: &mut RwTxn<'_>,
        val: u64,
    ) -> heed::Result<()> {
        self.main.remap_types::<Str, BEU64>().put(txn, main_key::PAGINATION_MAX_TOTAL_HITS, &val)
    }

    pub(crate) fn delete_pagination_max_total_hits(
        &self,
        txn: &mut RwTxn<'_>,
    ) -> heed::Result<bool> {
        self.main.remap_key_type::<Str>().delete(txn, main_key::PAGINATION_MAX_TOTAL_HITS)
    }

    pub fn proximity_precision(&self, txn: &RoTxn<'_>) -> heed::Result<Option<ProximityPrecision>> {
        self.main
            .remap_types::<Str, SerdeBincode<ProximityPrecision>>()
            .get(txn, main_key::PROXIMITY_PRECISION)
    }

    pub(crate) fn put_proximity_precision(
        &self,
        txn: &mut RwTxn<'_>,
        val: ProximityPrecision,
    ) -> heed::Result<()> {
        self.main.remap_types::<Str, SerdeBincode<ProximityPrecision>>().put(
            txn,
            main_key::PROXIMITY_PRECISION,
            &val,
        )
    }

    pub(crate) fn delete_proximity_precision(&self, txn: &mut RwTxn<'_>) -> heed::Result<bool> {
        self.main.remap_key_type::<Str>().delete(txn, main_key::PROXIMITY_PRECISION)
    }

    pub fn prefix_search(&self, txn: &RoTxn<'_>) -> heed::Result<Option<PrefixSearch>> {
        self.main.remap_types::<Str, SerdeBincode<PrefixSearch>>().get(txn, main_key::PREFIX_SEARCH)
    }

    pub(crate) fn put_prefix_search(
        &self,
        txn: &mut RwTxn<'_>,
        val: PrefixSearch,
    ) -> heed::Result<()> {
        self.main.remap_types::<Str, SerdeBincode<PrefixSearch>>().put(
            txn,
            main_key::PREFIX_SEARCH,
            &val,
        )
    }

    pub(crate) fn delete_prefix_search(&self, txn: &mut RwTxn<'_>) -> heed::Result<bool> {
        self.main.remap_key_type::<Str>().delete(txn, main_key::PREFIX_SEARCH)
    }

    pub fn facet_search(&self, txn: &RoTxn<'_>) -> heed::Result<bool> {
        self.main
            .remap_types::<Str, SerdeBincode<bool>>()
            .get(txn, main_key::FACET_SEARCH)
            .map(|v| v.unwrap_or(true))
    }

    pub(crate) fn put_facet_search(&self, txn: &mut RwTxn<'_>, val: bool) -> heed::Result<()> {
        self.main.remap_types::<Str, SerdeBincode<bool>>().put(txn, main_key::FACET_SEARCH, &val)
    }

    pub(crate) fn delete_facet_search(&self, txn: &mut RwTxn<'_>) -> heed::Result<bool> {
        self.main.remap_key_type::<Str>().delete(txn, main_key::FACET_SEARCH)
    }

    pub fn chat_config(&self, txn: &RoTxn<'_>) -> heed::Result<ChatConfig> {
        self.main
            .remap_types::<Str, SerdeJson<_>>()
            .get(txn, main_key::CHAT)
            .map(|o| o.unwrap_or_default())
    }

    pub(crate) fn put_chat_config(
        &self,
        txn: &mut RwTxn<'_>,
        val: &ChatConfig,
    ) -> heed::Result<()> {
        self.main.remap_types::<Str, SerdeJson<_>>().put(txn, main_key::CHAT, &val)
    }

    pub(crate) fn delete_chat_config(&self, txn: &mut RwTxn<'_>) -> heed::Result<bool> {
        self.main.remap_key_type::<Str>().delete(txn, main_key::CHAT)
    }

    pub fn localized_attributes_rules(
        &self,
        rtxn: &RoTxn<'_>,
    ) -> heed::Result<Option<Vec<LocalizedAttributesRule>>> {
        self.main
            .remap_types::<Str, SerdeJson<Vec<LocalizedAttributesRule>>>()
            .get(rtxn, main_key::LOCALIZED_ATTRIBUTES_RULES)
    }

    pub(crate) fn put_localized_attributes_rules(
        &self,
        txn: &mut RwTxn<'_>,
        val: Vec<LocalizedAttributesRule>,
    ) -> heed::Result<()> {
        self.main.remap_types::<Str, SerdeJson<Vec<LocalizedAttributesRule>>>().put(
            txn,
            main_key::LOCALIZED_ATTRIBUTES_RULES,
            &val,
        )
    }

    pub(crate) fn delete_localized_attributes_rules(
        &self,
        txn: &mut RwTxn<'_>,
    ) -> heed::Result<bool> {
        self.main.remap_key_type::<Str>().delete(txn, main_key::LOCALIZED_ATTRIBUTES_RULES)
    }

    pub(crate) fn put_search_cutoff(&self, wtxn: &mut RwTxn<'_>, cutoff: u64) -> heed::Result<()> {
        self.main.remap_types::<Str, BEU64>().put(wtxn, main_key::SEARCH_CUTOFF, &cutoff)
    }

    pub fn search_cutoff(&self, rtxn: &RoTxn<'_>) -> Result<Option<u64>> {
        Ok(self.main.remap_types::<Str, BEU64>().get(rtxn, main_key::SEARCH_CUTOFF)?)
    }

    pub(crate) fn delete_search_cutoff(&self, wtxn: &mut RwTxn<'_>) -> heed::Result<bool> {
        self.main.remap_key_type::<Str>().delete(wtxn, main_key::SEARCH_CUTOFF)
    }

    pub fn embedding_configs(&self) -> IndexEmbeddingConfigs {
        IndexEmbeddingConfigs::new(self.main, self.embedder_category_id)
    }

    pub fn embeddings(
        &self,
        rtxn: &RoTxn<'_>,
        docid: DocumentId,
    ) -> Result<BTreeMap<String, EmbeddingsWithMetadata>> {
        let mut res = BTreeMap::new();
        let embedders = self.embedding_configs();
        let backend = self.get_vector_store(rtxn)?.unwrap_or_default();

        for config in embedders.embedding_configs(rtxn)? {
            let embedder_info = embedders.embedder_info(rtxn, &config.name)?.unwrap();
            let has_fragments = config.config.embedder_options.has_fragments();
            let reader = VectorStore::new(
                backend,
                self.vector_store,
                embedder_info.embedder_id,
                config.config.quantized(),
            );
            let embeddings = reader.item_vectors(rtxn, docid)?;
            let regenerate = embedder_info.embedding_status.must_regenerate(docid);
            res.insert(
                config.name.to_owned(),
                EmbeddingsWithMetadata { embeddings, regenerate, has_fragments },
            );
        }
        Ok(res)
    }

    pub fn prefix_settings(&self, rtxn: &RoTxn<'_>) -> Result<PrefixSettings> {
        let compute_prefixes = self.prefix_search(rtxn)?.unwrap_or_default();
        Ok(PrefixSettings { compute_prefixes, max_prefix_length: 4, prefix_count_threshold: 100 })
    }

    pub fn vector_store_stats(&self, rtxn: &RoTxn<'_>) -> Result<VectorStoreStats> {
        let mut stats = VectorStoreStats::default();
        let embedding_configs = self.embedding_configs();
        let backend = self.get_vector_store(rtxn)?.unwrap_or_default();

        for config in embedding_configs.embedding_configs(rtxn)? {
            let embedder_id = embedding_configs.embedder_id(rtxn, &config.name)?.unwrap();
            let reader = VectorStore::new(
                backend,
                self.vector_store,
                embedder_id,
                config.config.quantized(),
            );
            reader.aggregate_stats(rtxn, &mut stats)?;
        }
        Ok(stats)
    }

    /// Check if the word is indexed in the index.
    ///
    /// This function checks if the word is indexed in the index by looking at the word_docids and exact_word_docids.
    ///
    /// # Arguments
    ///
    /// * `rtxn`: The read transaction.
    /// * `word`: The word to check.
    pub fn contains_word(&self, rtxn: &RoTxn<'_>, word: &str) -> Result<bool> {
        Ok(self.word_docids.remap_data_type::<DecodeIgnore>().get(rtxn, word)?.is_some()
            || self.exact_word_docids.remap_data_type::<DecodeIgnore>().get(rtxn, word)?.is_some())
    }

    /// Returns the sizes in bytes of each of the index database at the given rtxn.
    pub fn database_sizes(&self, rtxn: &RoTxn<'_>) -> heed::Result<IndexMap<&'static str, usize>> {
        let Self {
            env: _,
            main,
            external_documents_ids,
            word_docids,
            exact_word_docids,
            word_prefix_docids,
            exact_word_prefix_docids,
            word_pair_proximity_docids,
            word_position_docids,
            word_fid_docids,
            word_prefix_position_docids,
            word_prefix_fid_docids,
            field_id_word_count_docids,
            facet_id_f64_docids,
            facet_id_string_docids,
            facet_id_normalized_string_strings,
            facet_id_string_fst,
            facet_id_exists_docids,
            facet_id_is_null_docids,
            facet_id_is_empty_docids,
            field_id_docid_facet_f64s,
            field_id_docid_facet_strings,
            vector_store,
            embedder_category_id,
            cellulite,
            documents,
        } = self;

        fn compute_size(stats: DatabaseStat) -> usize {
            let DatabaseStat {
                page_size,
                depth: _,
                branch_pages,
                leaf_pages,
                overflow_pages,
                entries: _,
            } = stats;

            (branch_pages + leaf_pages + overflow_pages) * page_size as usize
        }

        let mut sizes = IndexMap::new();
        sizes.insert("main", main.stat(rtxn).map(compute_size)?);
        sizes
            .insert("external_documents_ids", external_documents_ids.stat(rtxn).map(compute_size)?);
        sizes.insert("word_docids", word_docids.stat(rtxn).map(compute_size)?);
        sizes.insert("exact_word_docids", exact_word_docids.stat(rtxn).map(compute_size)?);
        sizes.insert("word_prefix_docids", word_prefix_docids.stat(rtxn).map(compute_size)?);
        sizes.insert(
            "exact_word_prefix_docids",
            exact_word_prefix_docids.stat(rtxn).map(compute_size)?,
        );
        sizes.insert(
            "word_pair_proximity_docids",
            word_pair_proximity_docids.stat(rtxn).map(compute_size)?,
        );
        sizes.insert("word_position_docids", word_position_docids.stat(rtxn).map(compute_size)?);
        sizes.insert("word_fid_docids", word_fid_docids.stat(rtxn).map(compute_size)?);
        sizes.insert(
            "word_prefix_position_docids",
            word_prefix_position_docids.stat(rtxn).map(compute_size)?,
        );
        sizes
            .insert("word_prefix_fid_docids", word_prefix_fid_docids.stat(rtxn).map(compute_size)?);
        sizes.insert(
            "field_id_word_count_docids",
            field_id_word_count_docids.stat(rtxn).map(compute_size)?,
        );
        sizes.insert("facet_id_f64_docids", facet_id_f64_docids.stat(rtxn).map(compute_size)?);
        sizes
            .insert("facet_id_string_docids", facet_id_string_docids.stat(rtxn).map(compute_size)?);
        sizes.insert(
            "facet_id_normalized_string_strings",
            facet_id_normalized_string_strings.stat(rtxn).map(compute_size)?,
        );
        sizes.insert("facet_id_string_fst", facet_id_string_fst.stat(rtxn).map(compute_size)?);
        sizes
            .insert("facet_id_exists_docids", facet_id_exists_docids.stat(rtxn).map(compute_size)?);
        sizes.insert(
            "facet_id_is_null_docids",
            facet_id_is_null_docids.stat(rtxn).map(compute_size)?,
        );
        sizes.insert(
            "facet_id_is_empty_docids",
            facet_id_is_empty_docids.stat(rtxn).map(compute_size)?,
        );
        sizes.insert(
            "field_id_docid_facet_f64s",
            field_id_docid_facet_f64s.stat(rtxn).map(compute_size)?,
        );
        sizes.insert(
            "field_id_docid_facet_strings",
            field_id_docid_facet_strings.stat(rtxn).map(compute_size)?,
        );
        sizes.insert("vector_store", vector_store.stat(rtxn).map(compute_size)?);
        sizes.insert("embedder_category_id", embedder_category_id.stat(rtxn).map(compute_size)?);
        sizes.insert("documents", documents.stat(rtxn).map(compute_size)?);

        // Cellulite
        const _CELLULITE_DB_CHECK: () = {
            if Cellulite::nb_dbs() != 4 {
                panic!("Cellulite database count has changed, please update the code accordingly.")
            }
        };
        sizes.insert("cellulite_item", cellulite.item_db_stats(rtxn).map(compute_size)?);
        sizes.insert("cellulite_cell", cellulite.cell_db_stats(rtxn).map(compute_size)?);
        sizes.insert("cellulite_update", cellulite.update_db_stats(rtxn).map(compute_size)?);
        sizes.insert("cellulite_metadata", cellulite.metadata_db_stats(rtxn).map(compute_size)?);

        Ok(sizes)
    }
}

pub struct EmbeddingsWithMetadata {
    pub embeddings: Vec<Embedding>,
    pub regenerate: bool,
    pub has_fragments: bool,
}

#[derive(Debug, Default, Deserialize, Serialize)]
pub struct ChatConfig {
    pub description: String,
    /// Contains the document template and max template length.
    pub prompt: PromptData,
    pub search_parameters: SearchParameters,
}

#[derive(Debug, Default, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SearchParameters {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub hybrid: Option<HybridQuery>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub limit: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sort: Option<Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub distinct: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub matching_strategy: Option<MatchingStrategy>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub attributes_to_search_on: Option<Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ranking_score_threshold: Option<RankingScoreThreshold>,
}

#[derive(Debug, Clone, Copy, Default, Deserialize, Serialize, PartialEq, Deserr, ToSchema)]
#[deserr(try_from(f64) = TryFrom::try_from -> InvalidSettingsRankingScoreThreshold)]
pub struct RankingScoreThreshold(f64);

impl RankingScoreThreshold {
    pub fn as_f64(&self) -> f64 {
        self.0
    }
}

impl TryFrom<f64> for RankingScoreThreshold {
    type Error = InvalidSettingsRankingScoreThreshold;

    fn try_from(value: f64) -> StdResult<Self, Self::Error> {
        if !(0.0..=1.0).contains(&value) {
            Err(InvalidSettingsRankingScoreThreshold)
        } else {
            Ok(RankingScoreThreshold(value))
        }
    }
}

#[derive(Debug)]
pub struct InvalidSettingsRankingScoreThreshold;

impl Error for InvalidSettingsRankingScoreThreshold {}

impl fmt::Display for InvalidSettingsRankingScoreThreshold {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "the value of `rankingScoreThreshold` is invalid, expected a float between `0.0` and `1.0`."
        )
    }
}

#[derive(Debug, Clone, Default, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct HybridQuery {
    pub semantic_ratio: f32,
    pub embedder: String,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct PrefixSettings {
    pub prefix_count_threshold: usize,
    pub max_prefix_length: usize,
    pub compute_prefixes: PrefixSearch,
}

/// This is unfortunately a duplication of the struct in <meilisearch/src/search/mod.rs>.
/// The reason why it is duplicated is because milli cannot depend on meilisearch. It would be cyclic imports.
#[derive(Default, Debug, Copy, Clone, PartialEq, Eq, Deserr, ToSchema, Serialize, Deserialize)]
#[deserr(rename_all = camelCase)]
#[serde(rename_all = "camelCase")]
pub enum MatchingStrategy {
    /// Remove query words from last to first
    #[default]
    Last,
    /// All query words are mandatory
    All,
    /// Remove query words from the most frequent to the least
    Frequency,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(rename_all = "camelCase")]
pub enum PrefixSearch {
    #[default]
    IndexingTime,
    Disabled,
}

#[derive(Debug)]
pub enum RollbackOutcome {
    VersionMismatch {
        requested_version: (u32, u32, u32),
        rollback_version: Option<(u32, u32, u32)>,
    },
    Rollback,
    NoRollback,
}

impl RollbackOutcome {
    pub fn succeeded(&self) -> bool {
        matches!(self, RollbackOutcome::Rollback | RollbackOutcome::NoRollback)
    }
}

impl std::fmt::Display for RollbackOutcome {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RollbackOutcome::VersionMismatch { requested_version, rollback_version: Some(rollback_version) } => write!(f, "cannot rollback to the requested version\n  - note: requested version is v{}.{}.{}\n  - note: only possible to rollback to v{}.{}.{}",
              requested_version.0, requested_version.1, requested_version.2, rollback_version.0, rollback_version.1, rollback_version.2),
            RollbackOutcome::VersionMismatch { requested_version, rollback_version: None } => write!(f, "cannot rollback to the requested version\n  - note: requested version is v{}.{}.{}\n  - note: only possible to rollback to an unknown version",
                requested_version.0, requested_version.1, requested_version.2),
            RollbackOutcome::Rollback => f.write_str("rollback complete"),
            RollbackOutcome::NoRollback => f.write_str("no rollback necessary"),
        }
    }
}

#[derive(Serialize, Deserialize)]
#[serde(transparent)]
struct OffsetDateTime(#[serde(with = "time::serde::rfc3339")] time::OffsetDateTime);

#[cfg(test)]
#[path = "test_index.rs"]
pub(crate) mod tests;
