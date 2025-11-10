use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::convert::TryInto;
use std::num::NonZeroUsize;
use std::result::Result as StdResult;
use std::sync::Arc;

use charabia::{Normalize, Tokenizer, TokenizerBuilder};
use deserr::{DeserializeError, Deserr};
use itertools::{merge_join_by, EitherOrBoth, Itertools};
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use time::OffsetDateTime;

use super::chat::ChatSearchParams;
use super::del_add::{DelAdd, DelAddOperation};
use super::index_documents::{IndexDocumentsConfig, Transform};
use super::{ChatSettings, IndexerConfig};
use crate::attribute_patterns::PatternMatch;
use crate::constants::{RESERVED_GEOJSON_FIELD_NAME, RESERVED_GEO_FIELD_NAME};
use crate::criterion::Criterion;
use crate::disabled_typos_terms::DisabledTyposTerms;
use crate::error::UserError::{self, InvalidChatSettingsDocumentTemplateMaxBytes};
use crate::fields_ids_map::metadata::{FieldIdMapWithMetadata, MetadataBuilder};
use crate::filterable_attributes_rules::match_faceted_field;
use crate::index::{
    ChatConfig, PrefixSearch, SearchParameters, DEFAULT_MIN_WORD_LEN_ONE_TYPO,
    DEFAULT_MIN_WORD_LEN_TWO_TYPOS,
};
use crate::order_by_map::OrderByMap;
use crate::progress::{EmbedderStats, Progress, VariableNameStep};
use crate::prompt::{default_max_bytes, default_template_text, PromptData};
use crate::proximity::ProximityPrecision;
use crate::update::index_documents::IndexDocumentsMethod;
use crate::update::new::indexer::reindex;
use crate::update::new::steps::SettingsIndexerStep;
use crate::update::{IndexDocuments, UpdateIndexingStep};
use crate::vector::db::{FragmentConfigs, IndexEmbeddingConfig};
use crate::vector::embedder::{openai, rest};
use crate::vector::json_template::JsonTemplate;
use crate::vector::settings::{
    EmbedderAction, EmbedderSource, EmbeddingSettings, EmbeddingValidationContext, NestingContext,
    ReindexAction, SubEmbeddingSettings, WriteBackToDocuments,
};
use crate::vector::{
    Embedder, EmbeddingConfig, RuntimeEmbedder, RuntimeEmbedders, RuntimeFragment,
    VectorStoreBackend,
};
use crate::{
    ChannelCongestion, FieldId, FilterableAttributesRule, Index, LocalizedAttributesRule, Result,
};

#[derive(Debug, Clone, PartialEq, Eq, Copy)]
pub enum Setting<T> {
    Set(T),
    Reset,
    NotSet,
}

impl<T, E> Deserr<E> for Setting<T>
where
    T: Deserr<E>,
    E: DeserializeError,
{
    fn deserialize_from_value<V: deserr::IntoValue>(
        value: deserr::Value<V>,
        location: deserr::ValuePointerRef<'_>,
    ) -> std::result::Result<Self, E> {
        match value {
            deserr::Value::Null => Ok(Setting::Reset),
            _ => T::deserialize_from_value(value, location).map(Setting::Set),
        }
    }
}

impl<T> Default for Setting<T> {
    fn default() -> Self {
        Self::NotSet
    }
}

impl<T> Setting<T> {
    pub fn set(self) -> Option<T> {
        match self {
            Self::Set(value) => Some(value),
            _ => None,
        }
    }

    pub fn some_or_not_set(option: Option<T>) -> Self {
        match option {
            Some(value) => Setting::Set(value),
            None => Setting::NotSet,
        }
    }

    pub const fn as_ref(&self) -> Setting<&T> {
        match *self {
            Self::Set(ref value) => Setting::Set(value),
            Self::Reset => Setting::Reset,
            Self::NotSet => Setting::NotSet,
        }
    }

    pub const fn is_not_set(&self) -> bool {
        matches!(self, Self::NotSet)
    }

    pub const fn is_reset(&self) -> bool {
        matches!(self, Self::Reset)
    }

    /// If `Self` is `Reset`, then map self to `Set` with the provided `val`.
    pub fn or_reset(self, val: T) -> Self {
        match self {
            Self::Reset => Self::Set(val),
            otherwise => otherwise,
        }
    }

    /// Returns other if self is not set.
    pub fn or(self, other: Self) -> Self {
        match self {
            Setting::Set(_) | Setting::Reset => self,
            Setting::NotSet => other,
        }
    }

    /// Returns `true` if applying the new setting changed this setting
    pub fn apply(&mut self, new: Self) -> bool
    where
        T: PartialEq + Eq,
    {
        if let Setting::NotSet = new {
            return false;
        }
        if self == &new {
            return false;
        }
        *self = new;
        true
    }
}

impl<T: Serialize> Serialize for Setting<T> {
    fn serialize<S>(&self, serializer: S) -> StdResult<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match self {
            Self::Set(value) => Some(value),
            // Usually not_set isn't serialized by setting skip_serializing_if field attribute
            Self::NotSet | Self::Reset => None,
        }
        .serialize(serializer)
    }
}

impl<'de, T: Deserialize<'de>> Deserialize<'de> for Setting<T> {
    fn deserialize<D>(deserializer: D) -> StdResult<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        Deserialize::deserialize(deserializer).map(|x| match x {
            Some(x) => Self::Set(x),
            None => Self::Reset, // Reset is forced by sending null value
        })
    }
}

pub struct Settings<'a, 't, 'i> {
    wtxn: &'t mut heed::RwTxn<'i>,
    index: &'i Index,

    indexer_config: &'a IndexerConfig,

    searchable_fields: Setting<Vec<String>>,
    displayed_fields: Setting<Vec<String>>,
    filterable_fields: Setting<Vec<FilterableAttributesRule>>,
    sortable_fields: Setting<HashSet<String>>,
    criteria: Setting<Vec<Criterion>>,
    stop_words: Setting<BTreeSet<String>>,
    non_separator_tokens: Setting<BTreeSet<String>>,
    separator_tokens: Setting<BTreeSet<String>>,
    dictionary: Setting<BTreeSet<String>>,
    distinct_field: Setting<String>,
    synonyms: Setting<BTreeMap<String, Vec<String>>>,
    primary_key: Setting<String>,
    authorize_typos: Setting<bool>,
    disable_on_numbers: Setting<bool>,
    min_word_len_two_typos: Setting<u8>,
    min_word_len_one_typo: Setting<u8>,
    exact_words: Setting<BTreeSet<String>>,
    /// Attributes on which typo tolerance is disabled.
    exact_attributes: Setting<HashSet<String>>,
    max_values_per_facet: Setting<usize>,
    sort_facet_values_by: Setting<OrderByMap>,
    pagination_max_total_hits: Setting<usize>,
    proximity_precision: Setting<ProximityPrecision>,
    embedder_settings: Setting<BTreeMap<String, Setting<EmbeddingSettings>>>,
    search_cutoff: Setting<u64>,
    localized_attributes_rules: Setting<Vec<LocalizedAttributesRule>>,
    prefix_search: Setting<PrefixSearch>,
    facet_search: Setting<bool>,
    chat: Setting<ChatSettings>,
    vector_store: Setting<VectorStoreBackend>,
}

impl<'a, 't, 'i> Settings<'a, 't, 'i> {
    pub fn new(
        wtxn: &'t mut heed::RwTxn<'i>,
        index: &'i Index,
        indexer_config: &'a IndexerConfig,
    ) -> Settings<'a, 't, 'i> {
        Settings {
            wtxn,
            index,
            searchable_fields: Setting::NotSet,
            displayed_fields: Setting::NotSet,
            filterable_fields: Setting::NotSet,
            sortable_fields: Setting::NotSet,
            criteria: Setting::NotSet,
            stop_words: Setting::NotSet,
            non_separator_tokens: Setting::NotSet,
            separator_tokens: Setting::NotSet,
            dictionary: Setting::NotSet,
            distinct_field: Setting::NotSet,
            synonyms: Setting::NotSet,
            primary_key: Setting::NotSet,
            authorize_typos: Setting::NotSet,
            disable_on_numbers: Setting::NotSet,
            exact_words: Setting::NotSet,
            min_word_len_two_typos: Setting::NotSet,
            min_word_len_one_typo: Setting::NotSet,
            exact_attributes: Setting::NotSet,
            max_values_per_facet: Setting::NotSet,
            sort_facet_values_by: Setting::NotSet,
            pagination_max_total_hits: Setting::NotSet,
            proximity_precision: Setting::NotSet,
            embedder_settings: Setting::NotSet,
            search_cutoff: Setting::NotSet,
            localized_attributes_rules: Setting::NotSet,
            prefix_search: Setting::NotSet,
            facet_search: Setting::NotSet,
            chat: Setting::NotSet,
            vector_store: Setting::NotSet,
            indexer_config,
        }
    }

    pub fn reset_searchable_fields(&mut self) {
        self.searchable_fields = Setting::Reset;
    }

    pub fn set_searchable_fields(&mut self, names: Vec<String>) {
        self.searchable_fields = Setting::Set(names);
    }

    pub fn reset_displayed_fields(&mut self) {
        self.displayed_fields = Setting::Reset;
    }

    pub fn set_displayed_fields(&mut self, names: Vec<String>) {
        self.displayed_fields = Setting::Set(names);
    }

    pub fn reset_filterable_fields(&mut self) {
        self.filterable_fields = Setting::Reset;
    }

    pub fn set_filterable_fields(&mut self, rules: Vec<FilterableAttributesRule>) {
        self.filterable_fields = Setting::Set(rules);
    }

    pub fn set_sortable_fields(&mut self, names: HashSet<String>) {
        self.sortable_fields = Setting::Set(names);
    }

    pub fn reset_sortable_fields(&mut self) {
        self.sortable_fields = Setting::Reset;
    }

    pub fn reset_criteria(&mut self) {
        self.criteria = Setting::Reset;
    }

    pub fn set_criteria(&mut self, criteria: Vec<Criterion>) {
        self.criteria = Setting::Set(criteria);
    }

    pub fn reset_stop_words(&mut self) {
        self.stop_words = Setting::Reset;
    }

    pub fn set_stop_words(&mut self, stop_words: BTreeSet<String>) {
        self.stop_words =
            if stop_words.is_empty() { Setting::Reset } else { Setting::Set(stop_words) }
    }

    pub fn reset_non_separator_tokens(&mut self) {
        self.non_separator_tokens = Setting::Reset;
    }

    pub fn set_non_separator_tokens(&mut self, non_separator_tokens: BTreeSet<String>) {
        self.non_separator_tokens = if non_separator_tokens.is_empty() {
            Setting::Reset
        } else {
            Setting::Set(non_separator_tokens)
        }
    }

    pub fn reset_separator_tokens(&mut self) {
        self.separator_tokens = Setting::Reset;
    }

    pub fn set_separator_tokens(&mut self, separator_tokens: BTreeSet<String>) {
        self.separator_tokens = if separator_tokens.is_empty() {
            Setting::Reset
        } else {
            Setting::Set(separator_tokens)
        }
    }

    pub fn reset_dictionary(&mut self) {
        self.dictionary = Setting::Reset;
    }

    pub fn set_dictionary(&mut self, dictionary: BTreeSet<String>) {
        self.dictionary =
            if dictionary.is_empty() { Setting::Reset } else { Setting::Set(dictionary) }
    }

    pub fn reset_distinct_field(&mut self) {
        self.distinct_field = Setting::Reset;
    }

    pub fn set_distinct_field(&mut self, distinct_field: String) {
        self.distinct_field = Setting::Set(distinct_field);
    }

    pub fn reset_synonyms(&mut self) {
        self.synonyms = Setting::Reset;
    }

    pub fn set_synonyms(&mut self, synonyms: BTreeMap<String, Vec<String>>) {
        self.synonyms = if synonyms.is_empty() { Setting::Reset } else { Setting::Set(synonyms) }
    }

    pub fn reset_primary_key(&mut self) {
        self.primary_key = Setting::Reset;
    }

    pub fn set_primary_key(&mut self, primary_key: String) {
        self.primary_key = Setting::Set(primary_key);
    }

    pub fn set_authorize_typos(&mut self, val: bool) {
        self.authorize_typos = Setting::Set(val);
    }

    pub fn reset_authorize_typos(&mut self) {
        self.authorize_typos = Setting::Reset;
    }

    pub fn set_min_word_len_two_typos(&mut self, val: u8) {
        self.min_word_len_two_typos = Setting::Set(val);
    }

    pub fn reset_min_word_len_two_typos(&mut self) {
        self.min_word_len_two_typos = Setting::Reset;
    }

    pub fn set_min_word_len_one_typo(&mut self, val: u8) {
        self.min_word_len_one_typo = Setting::Set(val);
    }

    pub fn reset_min_word_len_one_typo(&mut self) {
        self.min_word_len_one_typo = Setting::Reset;
    }

    pub fn set_disable_on_numbers(&mut self, disable_on_numbers: bool) {
        self.disable_on_numbers = Setting::Set(disable_on_numbers);
    }

    pub fn reset_disable_on_numbers(&mut self) {
        self.disable_on_numbers = Setting::Reset;
    }

    pub fn set_exact_words(&mut self, words: BTreeSet<String>) {
        self.exact_words = Setting::Set(words);
    }

    pub fn reset_exact_words(&mut self) {
        self.exact_words = Setting::Reset;
    }

    pub fn set_exact_attributes(&mut self, attrs: HashSet<String>) {
        self.exact_attributes = Setting::Set(attrs);
    }

    pub fn reset_exact_attributes(&mut self) {
        self.exact_attributes = Setting::Reset;
    }

    pub fn set_max_values_per_facet(&mut self, value: usize) {
        self.max_values_per_facet = Setting::Set(value);
    }

    pub fn reset_max_values_per_facet(&mut self) {
        self.max_values_per_facet = Setting::Reset;
    }

    pub fn set_sort_facet_values_by(&mut self, value: OrderByMap) {
        self.sort_facet_values_by = Setting::Set(value);
    }

    pub fn reset_sort_facet_values_by(&mut self) {
        self.sort_facet_values_by = Setting::Reset;
    }

    pub fn set_pagination_max_total_hits(&mut self, value: usize) {
        self.pagination_max_total_hits = Setting::Set(value);
    }

    pub fn reset_pagination_max_total_hits(&mut self) {
        self.pagination_max_total_hits = Setting::Reset;
    }

    pub fn set_proximity_precision(&mut self, value: ProximityPrecision) {
        self.proximity_precision = Setting::Set(value);
    }

    pub fn reset_proximity_precision(&mut self) {
        self.proximity_precision = Setting::Reset;
    }

    pub fn set_embedder_settings(&mut self, value: BTreeMap<String, Setting<EmbeddingSettings>>) {
        self.embedder_settings = Setting::Set(value);
    }

    pub fn reset_embedder_settings(&mut self) {
        self.embedder_settings = Setting::Reset;
    }

    pub fn set_search_cutoff(&mut self, value: u64) {
        self.search_cutoff = Setting::Set(value);
    }

    pub fn reset_search_cutoff(&mut self) {
        self.search_cutoff = Setting::Reset;
    }

    pub fn set_localized_attributes_rules(&mut self, value: Vec<LocalizedAttributesRule>) {
        self.localized_attributes_rules = Setting::Set(value);
    }

    pub fn reset_localized_attributes_rules(&mut self) {
        self.localized_attributes_rules = Setting::Reset;
    }

    pub fn set_prefix_search(&mut self, value: PrefixSearch) {
        self.prefix_search = Setting::Set(value);
    }

    pub fn reset_prefix_search(&mut self) {
        self.prefix_search = Setting::Reset;
    }

    pub fn set_facet_search(&mut self, value: bool) {
        self.facet_search = Setting::Set(value);
    }

    pub fn reset_facet_search(&mut self) {
        self.facet_search = Setting::Reset;
    }

    pub fn set_chat(&mut self, value: ChatSettings) {
        self.chat = Setting::Set(value);
    }

    pub fn reset_chat(&mut self) {
        self.chat = Setting::Reset;
    }

    pub fn set_vector_store(&mut self, value: VectorStoreBackend) {
        self.vector_store = Setting::Set(value);
    }

    pub fn reset_vector_store(&mut self) {
        self.vector_store = Setting::Reset;
    }

    #[tracing::instrument(
        level = "trace"
        skip(self, progress_callback, should_abort, settings_diff, embedder_stats),
        target = "indexing::documents"
    )]
    fn reindex<FP, FA>(
        &mut self,
        progress_callback: &FP,
        should_abort: &FA,
        settings_diff: InnerIndexSettingsDiff,
        embedder_stats: &Arc<EmbedderStats>,
    ) -> Result<()>
    where
        FP: Fn(UpdateIndexingStep) + Sync,
        FA: Fn() -> bool + Sync,
    {
        // if the settings are set before any document update, we don't need to do anything, and
        // will set the primary key during the first document addition.
        if self.index.number_of_documents(self.wtxn)? == 0 {
            return Ok(());
        }

        let transform = Transform::new(
            self.wtxn,
            self.index,
            self.indexer_config,
            IndexDocumentsMethod::ReplaceDocuments,
            false,
        )?;

        // We clear the databases and remap the documents fields based on the new `FieldsIdsMap`.
        let output = transform.prepare_for_documents_reindexing(self.wtxn, settings_diff)?;

        // We index the generated `TransformOutput` which must contain
        // all the documents with fields in the newly defined searchable order.
        let indexing_builder = IndexDocuments::new(
            self.wtxn,
            self.index,
            self.indexer_config,
            IndexDocumentsConfig::default(),
            &progress_callback,
            &should_abort,
            embedder_stats,
        )?;

        indexing_builder.execute_raw(output)?;

        Ok(())
    }

    fn update_displayed(&mut self) -> Result<bool> {
        match self.displayed_fields {
            Setting::Set(ref fields) => {
                // fields are deduplicated, only the first occurrence is taken into account
                let names: Vec<_> = fields.iter().unique().map(String::as_str).collect();
                self.index.put_displayed_fields(self.wtxn, &names)?;
            }
            Setting::Reset => {
                self.index.delete_displayed_fields(self.wtxn)?;
            }
            Setting::NotSet => return Ok(false),
        }
        Ok(true)
    }

    fn update_distinct_field(&mut self) -> Result<bool> {
        match self.distinct_field {
            Setting::Set(ref attr) => {
                self.index.put_distinct_field(self.wtxn, attr)?;
            }
            Setting::Reset => {
                self.index.delete_distinct_field(self.wtxn)?;
            }
            Setting::NotSet => return Ok(false),
        }
        Ok(true)
    }

    /// Updates the index's searchable attributes.
    fn update_user_defined_searchable_attributes(&mut self) -> Result<bool> {
        match self.searchable_fields {
            Setting::Set(ref fields) => {
                // Check to see if the searchable fields changed before doing anything else
                let old_fields = self.index.user_defined_searchable_fields(self.wtxn)?;
                let did_change = {
                    let new_fields = fields.iter().map(String::as_str).collect::<Vec<_>>();
                    old_fields.is_none_or(|old| new_fields != old)
                };
                if !did_change {
                    return Ok(false);
                }

                // fields are deduplicated, only the first occurrence is taken into account
                let names = fields.iter().unique().map(String::as_str).collect::<Vec<_>>();

                self.index.put_user_defined_searchable_fields(self.wtxn, &names)?;
                Ok(true)
            }
            Setting::Reset => Ok(self.index.delete_all_searchable_fields(self.wtxn)?),
            Setting::NotSet => Ok(false),
        }
    }

    fn update_stop_words(&mut self) -> Result<bool> {
        match self.stop_words {
            Setting::Set(ref stop_words) => {
                let current = self.index.stop_words(self.wtxn)?;

                // Apply an unlossy normalization on stop_words
                let stop_words: BTreeSet<String> = stop_words
                    .iter()
                    .map(|w| w.as_str().normalize(&Default::default()).into_owned())
                    .collect();

                // since we can't compare a BTreeSet with an FST we are going to convert the
                // BTreeSet to an FST and then compare bytes per bytes the two FSTs.
                let fst = fst::Set::from_iter(stop_words.into_iter())?;

                // Does the new FST differ from the previous one?
                if current
                    .is_none_or(|current| current.as_fst().as_bytes() != fst.as_fst().as_bytes())
                {
                    // we want to re-create our FST.
                    self.index.put_stop_words(self.wtxn, &fst)?;
                    Ok(true)
                } else {
                    Ok(false)
                }
            }
            Setting::Reset => Ok(self.index.delete_stop_words(self.wtxn)?),
            Setting::NotSet => Ok(false),
        }
    }

    fn update_non_separator_tokens(&mut self) -> Result<bool> {
        let changes = match self.non_separator_tokens {
            Setting::Set(ref non_separator_tokens) => {
                let current = self.index.non_separator_tokens(self.wtxn)?;

                // Does the new list differ from the previous one?
                if current.is_none_or(|current| &current != non_separator_tokens) {
                    self.index.put_non_separator_tokens(self.wtxn, non_separator_tokens)?;
                    true
                } else {
                    false
                }
            }
            Setting::Reset => self.index.delete_non_separator_tokens(self.wtxn)?,
            Setting::NotSet => false,
        };

        // the synonyms must be updated if non separator tokens have been updated.
        if changes && self.synonyms == Setting::NotSet {
            self.synonyms = Setting::Set(self.index.user_defined_synonyms(self.wtxn)?);
        }

        Ok(changes)
    }

    fn update_separator_tokens(&mut self) -> Result<bool> {
        let changes = match self.separator_tokens {
            Setting::Set(ref separator_tokens) => {
                let current = self.index.separator_tokens(self.wtxn)?;

                // Does the new list differ from the previous one?
                if current.is_none_or(|current| &current != separator_tokens) {
                    self.index.put_separator_tokens(self.wtxn, separator_tokens)?;
                    true
                } else {
                    false
                }
            }
            Setting::Reset => self.index.delete_separator_tokens(self.wtxn)?,
            Setting::NotSet => false,
        };

        // the synonyms must be updated if separator tokens have been updated.
        if changes && self.synonyms == Setting::NotSet {
            self.synonyms = Setting::Set(self.index.user_defined_synonyms(self.wtxn)?);
        }

        Ok(changes)
    }

    fn update_dictionary(&mut self) -> Result<bool> {
        let changes = match self.dictionary {
            Setting::Set(ref dictionary) => {
                let current = self.index.dictionary(self.wtxn)?;

                // Does the new list differ from the previous one?
                if current.is_none_or(|current| &current != dictionary) {
                    self.index.put_dictionary(self.wtxn, dictionary)?;
                    true
                } else {
                    false
                }
            }
            Setting::Reset => self.index.delete_dictionary(self.wtxn)?,
            Setting::NotSet => false,
        };

        // the synonyms must be updated if dictionary has been updated.
        if changes && self.synonyms == Setting::NotSet {
            self.synonyms = Setting::Set(self.index.user_defined_synonyms(self.wtxn)?);
        }

        Ok(changes)
    }

    fn update_synonyms(&mut self) -> Result<bool> {
        match self.synonyms {
            Setting::Set(ref user_synonyms) => {
                fn normalize(tokenizer: &Tokenizer<'_>, text: &str) -> Vec<String> {
                    tokenizer
                        .tokenize(text)
                        .filter_map(|token| {
                            if token.is_word() && !token.lemma().is_empty() {
                                Some(token.lemma().to_string())
                            } else {
                                None
                            }
                        })
                        .collect::<Vec<_>>()
                }

                let mut builder = TokenizerBuilder::new();
                let stop_words = self.index.stop_words(self.wtxn)?;
                if let Some(ref stop_words) = stop_words {
                    builder.stop_words(stop_words);
                }

                let separators = self.index.allowed_separators(self.wtxn)?;
                let separators: Option<Vec<_>> =
                    separators.as_ref().map(|x| x.iter().map(String::as_str).collect());
                if let Some(ref separators) = separators {
                    builder.separators(separators);
                }

                let dictionary = self.index.dictionary(self.wtxn)?;
                let dictionary: Option<Vec<_>> =
                    dictionary.as_ref().map(|x| x.iter().map(String::as_str).collect());
                if let Some(ref dictionary) = dictionary {
                    builder.words_dict(dictionary);
                }

                let tokenizer = builder.build();

                let mut new_synonyms = HashMap::new();
                for (word, synonyms) in user_synonyms {
                    // Normalize both the word and associated synonyms.
                    let normalized_word = normalize(&tokenizer, word);
                    let normalized_synonyms: Vec<_> = synonyms
                        .iter()
                        .map(|synonym| normalize(&tokenizer, synonym))
                        .filter(|synonym| !synonym.is_empty())
                        .collect();

                    // Store the normalized synonyms under the normalized word,
                    // merging the possible duplicate words.
                    if !normalized_word.is_empty() && !normalized_synonyms.is_empty() {
                        let entry = new_synonyms.entry(normalized_word).or_insert_with(Vec::new);
                        entry.extend(normalized_synonyms.into_iter());
                    }
                }

                // Make sure that we don't have duplicate synonyms.
                new_synonyms.iter_mut().for_each(|(_, synonyms)| {
                    synonyms.sort_unstable();
                    synonyms.dedup();
                });

                let old_synonyms = self.index.synonyms(self.wtxn)?;

                if new_synonyms != old_synonyms {
                    self.index.put_synonyms(self.wtxn, &new_synonyms, user_synonyms)?;
                    Ok(true)
                } else {
                    Ok(false)
                }
            }
            Setting::Reset => Ok(self.index.delete_synonyms(self.wtxn)?),
            Setting::NotSet => Ok(false),
        }
    }

    fn update_exact_attributes(&mut self) -> Result<bool> {
        match self.exact_attributes {
            Setting::Set(ref attrs) => {
                let old_attrs = self.index.exact_attributes(self.wtxn)?;
                let old_attrs = old_attrs.into_iter().map(String::from).collect::<HashSet<_>>();

                if attrs != &old_attrs {
                    let attrs = attrs.iter().map(String::as_str).collect::<Vec<_>>();
                    self.index.put_exact_attributes(self.wtxn, &attrs)?;
                    Ok(true)
                } else {
                    Ok(false)
                }
            }
            Setting::Reset => Ok(self.index.delete_exact_attributes(self.wtxn)?),
            Setting::NotSet => Ok(false),
        }
    }

    fn update_filterable(&mut self) -> Result<()> {
        match self.filterable_fields {
            Setting::Set(ref fields) => {
                self.index.put_filterable_attributes_rules(self.wtxn, fields)?;
            }
            Setting::Reset => {
                self.index.delete_filterable_attributes_rules(self.wtxn)?;
            }
            Setting::NotSet => (),
        }
        Ok(())
    }

    fn update_sortable(&mut self) -> Result<()> {
        match self.sortable_fields {
            Setting::Set(ref fields) => {
                let mut new_fields = HashSet::new();
                for name in fields {
                    new_fields.insert(name.clone());
                }
                self.index.put_sortable_fields(self.wtxn, &new_fields)?;
            }
            Setting::Reset => {
                self.index.delete_sortable_fields(self.wtxn)?;
            }
            Setting::NotSet => (),
        }
        Ok(())
    }

    fn update_criteria(&mut self) -> Result<()> {
        match &self.criteria {
            Setting::Set(criteria) => {
                self.index.put_criteria(self.wtxn, criteria)?;
            }
            Setting::Reset => {
                self.index.delete_criteria(self.wtxn)?;
            }
            Setting::NotSet => (),
        }
        Ok(())
    }

    fn update_primary_key(&mut self) -> Result<()> {
        match self.primary_key {
            Setting::Set(ref primary_key) => {
                if self.index.number_of_documents(self.wtxn)? == 0 {
                    let mut fields_ids_map = self.index.fields_ids_map(self.wtxn)?;
                    fields_ids_map.insert(primary_key).ok_or(UserError::AttributeLimitReached)?;
                    self.index.put_fields_ids_map(self.wtxn, &fields_ids_map)?;
                    self.index.put_primary_key(self.wtxn, primary_key)?;
                    Ok(())
                } else {
                    let curr_primary_key = self.index.primary_key(self.wtxn)?.unwrap().to_string();
                    if primary_key == &curr_primary_key {
                        Ok(())
                    } else {
                        Err(UserError::PrimaryKeyCannotBeChanged(curr_primary_key).into())
                    }
                }
            }
            Setting::Reset => {
                if self.index.number_of_documents(self.wtxn)? == 0 {
                    self.index.delete_primary_key(self.wtxn)?;
                    Ok(())
                } else {
                    let primary_key = self.index.primary_key(self.wtxn)?.unwrap();
                    Err(UserError::PrimaryKeyCannotBeChanged(primary_key.to_string()).into())
                }
            }
            Setting::NotSet => Ok(()),
        }
    }

    fn update_authorize_typos(&mut self) -> Result<()> {
        match self.authorize_typos {
            Setting::Set(flag) => {
                self.index.put_authorize_typos(self.wtxn, flag)?;
                Ok(())
            }
            Setting::Reset => {
                self.index.put_authorize_typos(self.wtxn, true)?;
                Ok(())
            }
            Setting::NotSet => Ok(()),
        }
    }

    fn update_min_typo_word_len(&mut self) -> Result<()> {
        let one = self.min_word_len_one_typo.or_reset(DEFAULT_MIN_WORD_LEN_ONE_TYPO);
        let two = self.min_word_len_two_typos.or_reset(DEFAULT_MIN_WORD_LEN_TWO_TYPOS);
        match (one, two) {
            (Setting::Set(one), Setting::Set(two)) => {
                if one > two {
                    return Err(UserError::InvalidMinTypoWordLenSetting(one, two).into());
                } else {
                    self.index.put_min_word_len_one_typo(self.wtxn, one)?;
                    self.index.put_min_word_len_two_typos(self.wtxn, two)?;
                }
            }
            (Setting::Set(one), _) => {
                let two = self.index.min_word_len_two_typos(self.wtxn)?;
                if one > two {
                    return Err(UserError::InvalidMinTypoWordLenSetting(one, two).into());
                } else {
                    self.index.put_min_word_len_one_typo(self.wtxn, one)?;
                }
            }
            (_, Setting::Set(two)) => {
                let one = self.index.min_word_len_one_typo(self.wtxn)?;
                if one > two {
                    return Err(UserError::InvalidMinTypoWordLenSetting(one, two).into());
                } else {
                    self.index.put_min_word_len_two_typos(self.wtxn, two)?;
                }
            }
            _ => (),
        }

        Ok(())
    }

    fn update_disabled_typos_terms(&mut self) -> Result<()> {
        let mut disabled_typos_terms = self.index.disabled_typos_terms(self.wtxn)?;
        match self.disable_on_numbers {
            Setting::Set(disable_on_numbers) => {
                disabled_typos_terms.disable_on_numbers = disable_on_numbers;
            }
            Setting::Reset => {
                disabled_typos_terms.disable_on_numbers =
                    DisabledTyposTerms::default().disable_on_numbers;
            }
            Setting::NotSet => (),
        }

        self.index.put_disabled_typos_terms(self.wtxn, &disabled_typos_terms)?;
        Ok(())
    }

    fn update_exact_words(&mut self) -> Result<()> {
        match self.exact_words {
            Setting::Set(ref mut words) => {
                fn normalize(tokenizer: &Tokenizer<'_>, text: &str) -> String {
                    tokenizer.tokenize(text).map(|token| token.lemma().to_string()).collect()
                }

                let mut builder = TokenizerBuilder::new();
                let stop_words = self.index.stop_words(self.wtxn)?;
                if let Some(ref stop_words) = stop_words {
                    builder.stop_words(stop_words);
                }
                let tokenizer = builder.build();

                let mut words: Vec<_> =
                    words.iter().map(|word| normalize(&tokenizer, word)).collect();

                // normalization could reorder words
                words.sort_unstable();

                let words = fst::Set::from_iter(words.iter())?;
                self.index.put_exact_words(self.wtxn, &words)?;
            }
            Setting::Reset => {
                self.index.put_exact_words(self.wtxn, &fst::Set::default())?;
            }
            Setting::NotSet => (),
        }

        Ok(())
    }

    fn update_max_values_per_facet(&mut self) -> Result<()> {
        match self.max_values_per_facet {
            Setting::Set(max) => {
                self.index.put_max_values_per_facet(self.wtxn, max as u64)?;
            }
            Setting::Reset => {
                self.index.delete_max_values_per_facet(self.wtxn)?;
            }
            Setting::NotSet => (),
        }

        Ok(())
    }

    fn update_sort_facet_values_by(&mut self) -> Result<()> {
        match self.sort_facet_values_by.as_ref() {
            Setting::Set(value) => {
                self.index.put_sort_facet_values_by(self.wtxn, value)?;
            }
            Setting::Reset => {
                self.index.delete_sort_facet_values_by(self.wtxn)?;
            }
            Setting::NotSet => (),
        }

        Ok(())
    }

    fn update_pagination_max_total_hits(&mut self) -> Result<()> {
        match self.pagination_max_total_hits {
            Setting::Set(max) => {
                self.index.put_pagination_max_total_hits(self.wtxn, max as u64)?;
            }
            Setting::Reset => {
                self.index.delete_pagination_max_total_hits(self.wtxn)?;
            }
            Setting::NotSet => (),
        }

        Ok(())
    }

    fn update_proximity_precision(&mut self) -> Result<bool> {
        let changed = match self.proximity_precision {
            Setting::Set(new) => {
                let old = self.index.proximity_precision(self.wtxn)?;
                if old == Some(new) {
                    false
                } else {
                    self.index.put_proximity_precision(self.wtxn, new)?;
                    old.is_some() || new != ProximityPrecision::default()
                }
            }
            Setting::Reset => self.index.delete_proximity_precision(self.wtxn)?,
            Setting::NotSet => false,
        };

        Ok(changed)
    }

    fn update_prefix_search(&mut self) -> Result<bool> {
        let changed = match self.prefix_search {
            Setting::Set(new) => {
                let old = self.index.prefix_search(self.wtxn)?;
                if old == Some(new) {
                    false
                } else {
                    self.index.put_prefix_search(self.wtxn, new)?;
                    old.is_some() || new != PrefixSearch::default()
                }
            }
            Setting::Reset => self.index.delete_prefix_search(self.wtxn)?,
            Setting::NotSet => false,
        };

        Ok(changed)
    }

    fn update_facet_search(&mut self) -> Result<bool> {
        let changed = match self.facet_search {
            Setting::Set(new) => {
                let old = self.index.facet_search(self.wtxn)?;
                if old == new {
                    false
                } else {
                    self.index.put_facet_search(self.wtxn, new)?;
                    true
                }
            }
            Setting::Reset => self.index.delete_facet_search(self.wtxn)?,
            Setting::NotSet => false,
        };

        Ok(changed)
    }

    fn update_embedding_configs(&mut self) -> Result<BTreeMap<String, EmbedderAction>> {
        match std::mem::take(&mut self.embedder_settings) {
            Setting::Set(configs) => self.update_embedding_configs_set(configs),
            Setting::Reset => {
                let embedders = self.index.embedding_configs();
                // all vectors should be written back to documents
                let old_configs = embedders.embedding_configs(self.wtxn)?;
                let remove_all: Result<BTreeMap<String, EmbedderAction>> = old_configs
                    .into_iter()
                    .map(|IndexEmbeddingConfig { name, config, fragments: _ }| -> Result<_> {
                        let embedder_info = embedders.embedder_info(self.wtxn, &name)?.ok_or(
                            crate::InternalError::DatabaseMissingEntry {
                                db_name: crate::index::db_name::VECTOR_EMBEDDER_CATEGORY_ID,
                                key: None,
                            },
                        )?;
                        Ok((
                            name,
                            EmbedderAction::with_write_back(
                                WriteBackToDocuments {
                                    embedder_id: embedder_info.embedder_id,
                                    user_provided: embedder_info
                                        .embedding_status
                                        .into_user_provided(),
                                },
                                config.quantized(),
                            ),
                        ))
                    })
                    .collect();

                let remove_all = remove_all?;

                self.index.embedder_category_id.clear(self.wtxn)?;
                embedders.delete_embedding_configs(self.wtxn)?;
                Ok(remove_all)
            }
            Setting::NotSet => Ok(Default::default()),
        }
    }

    fn update_embedding_configs_set(
        &mut self,
        configs: BTreeMap<String, Setting<EmbeddingSettings>>,
    ) -> Result<BTreeMap<String, EmbedderAction>> {
        use crate::vector::settings::SettingsDiff;
        let embedders = self.index.embedding_configs();
        let old_configs = embedders.embedding_configs(self.wtxn)?;
        let old_configs: BTreeMap<String, (EmbeddingSettings, FragmentConfigs)> = old_configs
            .into_iter()
            .map(|IndexEmbeddingConfig { name, config, fragments }| {
                (name, (config.into(), fragments))
            })
            .collect();
        let mut updated_configs = BTreeMap::new();
        let mut embedder_actions = BTreeMap::new();
        for joined in old_configs
            .into_iter()
            .merge_join_by(configs.into_iter(), |(left, _), (right, _)| left.cmp(right))
        {
            match joined {
                // updated config
                EitherOrBoth::Both((name, (old, mut fragments)), (_, new)) => {
                    let was_quantized = old.binary_quantized.set().unwrap_or_default();
                    let settings_diff = SettingsDiff::from_settings(&name, old, new)?;
                    match settings_diff {
                        SettingsDiff::Remove => {
                            let info = embedders.remove_embedder(self.wtxn, &name)?.ok_or(
                                crate::InternalError::DatabaseMissingEntry {
                                    db_name: crate::index::db_name::VECTOR_EMBEDDER_CATEGORY_ID,
                                    key: None,
                                },
                            )?;
                            tracing::debug!(
                                embedder = name,
                                user_provided = info.embedding_status.user_provided_docids().len(),
                                "removing embedder"
                            );
                            embedder_actions.insert(
                                name,
                                EmbedderAction::with_write_back(
                                    WriteBackToDocuments {
                                        embedder_id: info.embedder_id,
                                        user_provided: info.embedding_status.into_user_provided(),
                                    },
                                    was_quantized,
                                ),
                            );
                        }
                        SettingsDiff::Reindex { action, updated_settings, quantize } => {
                            let mut remove_fragments = None;
                            let updated_settings = Setting::Set(updated_settings);
                            if let ReindexAction::RegenerateFragments(regenerate_fragments) =
                                &action
                            {
                                let it = regenerate_fragments
                                    .iter()
                                    .filter(|(_, action)| {
                                        matches!(
                                            action,
                                            crate::vector::settings::RegenerateFragment::Remove
                                        )
                                    })
                                    .map(|(name, _)| name.as_str());

                                remove_fragments = fragments.remove_fragments(it);

                                let it = regenerate_fragments
                                    .iter()
                                    .filter(|(_, action)| {
                                        matches!(
                                            action,
                                            crate::vector::settings::RegenerateFragment::Add
                                        )
                                    })
                                    .map(|(name, _)| name.clone());
                                fragments.add_new_fragments(it)?;
                            } else {
                                // needs full reindex of fragments
                                fragments = FragmentConfigs::new();
                                fragments.add_new_fragments(
                                    crate::vector::settings::fragments_from_settings(
                                        &updated_settings,
                                    ),
                                )?;
                            }
                            tracing::debug!(embedder = name, ?action, "reindex embedder");

                            let embedder_action =
                                EmbedderAction::with_reindex(action, was_quantized)
                                    .with_is_being_quantized(quantize);

                            let embedder_action = if let Some(remove_fragments) = remove_fragments {
                                embedder_action.with_remove_fragments(remove_fragments)
                            } else {
                                embedder_action
                            };

                            embedder_actions.insert(name.clone(), embedder_action);
                            let new = validate_embedding_settings(
                                updated_settings,
                                &name,
                                EmbeddingValidationContext::FullSettings,
                            )?;
                            updated_configs.insert(name, (new, fragments));
                        }
                        SettingsDiff::UpdateWithoutReindex { updated_settings, quantize } => {
                            tracing::debug!(embedder = name, "update without reindex embedder");
                            let new = validate_embedding_settings(
                                Setting::Set(updated_settings),
                                &name,
                                EmbeddingValidationContext::FullSettings,
                            )?;
                            if quantize {
                                embedder_actions.insert(
                                    name.clone(),
                                    EmbedderAction::default().with_is_being_quantized(true),
                                );
                            }
                            updated_configs.insert(name, (new, fragments));
                        }
                    }
                }
                // unchanged config
                EitherOrBoth::Left((name, (setting, fragments))) => {
                    tracing::debug!(embedder = name, "unchanged embedder");
                    updated_configs.insert(name, (Setting::Set(setting), fragments));
                }
                // new config
                EitherOrBoth::Right((name, mut setting)) => {
                    tracing::debug!(embedder = name, "new embedder");
                    // if we are asked to reset an embedder that doesn't exist, just ignore it
                    if setting.is_reset() {
                        continue;
                    }
                    // apply the default source in case the source was not set so that it gets validated
                    crate::vector::settings::EmbeddingSettings::apply_default_source(&mut setting);
                    crate::vector::settings::EmbeddingSettings::apply_default_openai_model(
                        &mut setting,
                    );
                    let setting = validate_embedding_settings(
                        setting,
                        &name,
                        EmbeddingValidationContext::FullSettings,
                    )?;
                    embedder_actions.insert(
                        name.clone(),
                        EmbedderAction::with_reindex(ReindexAction::FullReindex, false),
                    );
                    let mut fragments = FragmentConfigs::new();
                    fragments.add_new_fragments(
                        crate::vector::settings::fragments_from_settings(&setting),
                    )?;
                    updated_configs.insert(name, (setting, fragments));
                }
            }
        }
        embedders.add_new_embedders(
            self.wtxn,
            embedder_actions
                .iter()
                // ignore actions that are not possible for a new embedder, most critically deleted embedders
                .filter(|(_, action)| matches!(action.reindex(), Some(ReindexAction::FullReindex)))
                .map(|(name, _)| name.as_str()),
            updated_configs.len(),
        )?;

        let updated_configs: Vec<IndexEmbeddingConfig> = updated_configs
            .into_iter()
            .filter_map(|(name, (config, fragments))| match config {
                Setting::Set(config) => {
                    Some(IndexEmbeddingConfig { name, config: config.into(), fragments })
                }
                Setting::Reset => None,
                Setting::NotSet => Some(IndexEmbeddingConfig {
                    name,
                    config: EmbeddingSettings::default().into(),
                    fragments: Default::default(),
                }),
            })
            .collect();
        if updated_configs.is_empty() {
            embedders.delete_embedding_configs(self.wtxn)?;
        } else {
            embedders.put_embedding_configs(self.wtxn, updated_configs)?;
        }
        Ok(embedder_actions)
    }

    fn update_search_cutoff(&mut self) -> Result<bool> {
        let changed = match self.search_cutoff {
            Setting::Set(new) => {
                let old = self.index.search_cutoff(self.wtxn)?;
                if old == Some(new) {
                    false
                } else {
                    self.index.put_search_cutoff(self.wtxn, new)?;
                    true
                }
            }
            Setting::Reset => self.index.delete_search_cutoff(self.wtxn)?,
            Setting::NotSet => false,
        };

        Ok(changed)
    }

    fn update_localized_attributes_rules(&mut self) -> Result<()> {
        match &self.localized_attributes_rules {
            Setting::Set(new) => {
                let old = self.index.localized_attributes_rules(self.wtxn)?;
                if old.as_ref() != Some(new) {
                    self.index.put_localized_attributes_rules(self.wtxn, new.clone())?;
                }
            }
            Setting::Reset => {
                self.index.delete_localized_attributes_rules(self.wtxn)?;
            }
            Setting::NotSet => (),
        }

        Ok(())
    }

    fn update_chat_config(&mut self) -> Result<bool> {
        match &mut self.chat {
            Setting::Set(ChatSettings {
                description: new_description,
                document_template: new_document_template,
                document_template_max_bytes: new_document_template_max_bytes,
                search_parameters: new_search_parameters,
            }) => {
                let ChatConfig { description, prompt, search_parameters } =
                    self.index.chat_config(self.wtxn)?;

                let description = match new_description {
                    Setting::Set(new) => new.clone(),
                    Setting::Reset => Default::default(),
                    Setting::NotSet => description,
                };

                let prompt = PromptData {
                    template: match new_document_template {
                        Setting::Set(new) => new.clone(),
                        Setting::Reset => default_template_text().to_string(),
                        Setting::NotSet => prompt.template.clone(),
                    },
                    max_bytes: match new_document_template_max_bytes {
                        Setting::Set(m) => Some(
                            NonZeroUsize::new(*m)
                                .ok_or(InvalidChatSettingsDocumentTemplateMaxBytes)?,
                        ),
                        Setting::Reset => Some(default_max_bytes()),
                        Setting::NotSet => prompt.max_bytes,
                    },
                };

                let search_parameters = match new_search_parameters {
                    Setting::Set(sp) => {
                        let ChatSearchParams {
                            hybrid,
                            limit,
                            sort,
                            distinct,
                            matching_strategy,
                            attributes_to_search_on,
                            ranking_score_threshold,
                        } = sp;

                        SearchParameters {
                            hybrid: match hybrid {
                                Setting::Set(hybrid) => Some(crate::index::HybridQuery {
                                    semantic_ratio: *hybrid.semantic_ratio,
                                    embedder: hybrid.embedder.clone(),
                                }),
                                Setting::Reset => None,
                                Setting::NotSet => search_parameters.hybrid.clone(),
                            },
                            limit: match limit {
                                Setting::Set(limit) => Some(*limit),
                                Setting::Reset => None,
                                Setting::NotSet => search_parameters.limit,
                            },
                            sort: match sort {
                                Setting::Set(sort) => Some(sort.clone()),
                                Setting::Reset => None,
                                Setting::NotSet => search_parameters.sort.clone(),
                            },
                            distinct: match distinct {
                                Setting::Set(distinct) => Some(distinct.clone()),
                                Setting::Reset => None,
                                Setting::NotSet => search_parameters.distinct.clone(),
                            },
                            matching_strategy: match matching_strategy {
                                Setting::Set(matching_strategy) => Some(*matching_strategy),
                                Setting::Reset => None,
                                Setting::NotSet => search_parameters.matching_strategy,
                            },
                            attributes_to_search_on: match attributes_to_search_on {
                                Setting::Set(attributes_to_search_on) => {
                                    Some(attributes_to_search_on.clone())
                                }
                                Setting::Reset => None,
                                Setting::NotSet => {
                                    search_parameters.attributes_to_search_on.clone()
                                }
                            },
                            ranking_score_threshold: match ranking_score_threshold {
                                Setting::Set(rst) => Some(*rst),
                                Setting::Reset => None,
                                Setting::NotSet => search_parameters.ranking_score_threshold,
                            },
                        }
                    }
                    Setting::Reset => Default::default(),
                    Setting::NotSet => search_parameters,
                };

                self.index.put_chat_config(
                    self.wtxn,
                    &ChatConfig { description, prompt, search_parameters },
                )?;

                Ok(true)
            }
            Setting::Reset => self.index.delete_chat_config(self.wtxn).map_err(Into::into),
            Setting::NotSet => Ok(false),
        }
    }

    fn legacy_execute<FP, FA>(
        mut self,
        progress_callback: FP,
        should_abort: FA,
        embedder_stats: Arc<EmbedderStats>,
    ) -> Result<()>
    where
        FP: Fn(UpdateIndexingStep) + Sync,
        FA: Fn() -> bool + Sync,
    {
        self.index.set_updated_at(self.wtxn, &OffsetDateTime::now_utc())?;

        let old_inner_settings = InnerIndexSettings::from_index(self.index, self.wtxn, None)?;

        // never trigger re-indexing
        self.update_displayed()?;
        self.update_distinct_field()?;
        self.update_criteria()?;
        self.update_primary_key()?;
        self.update_authorize_typos()?;
        self.update_min_typo_word_len()?;
        self.update_exact_words()?;
        self.update_max_values_per_facet()?;
        self.update_sort_facet_values_by()?;
        self.update_pagination_max_total_hits()?;
        self.update_search_cutoff()?;

        // could trigger re-indexing
        self.update_filterable()?;
        self.update_sortable()?;
        self.update_stop_words()?;
        self.update_non_separator_tokens()?;
        self.update_separator_tokens()?;
        self.update_dictionary()?;
        self.update_synonyms()?;
        self.update_user_defined_searchable_attributes()?;
        self.update_exact_attributes()?;
        self.update_proximity_precision()?;
        self.update_prefix_search()?;
        self.update_facet_search()?;
        self.update_localized_attributes_rules()?;
        self.update_disabled_typos_terms()?;
        self.update_chat_config()?;

        let embedding_config_updates = self.update_embedding_configs()?;

        let mut new_inner_settings = InnerIndexSettings::from_index(self.index, self.wtxn, None)?;
        new_inner_settings.recompute_searchables(self.wtxn, self.index)?;

        let primary_key_id = self
            .index
            .primary_key(self.wtxn)?
            .and_then(|name| new_inner_settings.fields_ids_map.id(name));
        let settings_update_only = true;
        let inner_settings_diff = InnerIndexSettingsDiff::new(
            old_inner_settings,
            new_inner_settings,
            primary_key_id,
            embedding_config_updates,
            settings_update_only,
        );

        if inner_settings_diff.any_reindexing_needed() {
            self.reindex(&progress_callback, &should_abort, inner_settings_diff, &embedder_stats)?;
        }

        Ok(())
    }

    fn execute_vector_backend<'indexer, MSP>(
        &mut self,
        must_stop_processing: &'indexer MSP,
        progress: &'indexer Progress,
    ) -> Result<()>
    where
        MSP: Fn() -> bool + Sync,
    {
        let old_backend = self.index.get_vector_store(self.wtxn)?.unwrap_or_default();

        let new_backend = match self.vector_store {
            Setting::Set(new_backend) => {
                self.index.put_vector_store(self.wtxn, new_backend)?;
                new_backend
            }
            Setting::Reset => {
                self.index.delete_vector_store(self.wtxn)?;
                VectorStoreBackend::default()
            }
            Setting::NotSet => return Ok(()),
        };

        if old_backend == new_backend {
            return Ok(());
        }

        let embedders = self.index.embedding_configs();
        let embedding_configs = embedders.embedding_configs(self.wtxn)?;
        enum VectorStoreBackendChangeIndex {}
        let embedder_count = embedding_configs.len();

        let rtxn = self.index.read_txn()?;

        for (i, config) in embedding_configs.into_iter().enumerate() {
            if must_stop_processing() {
                return Err(crate::InternalError::AbortedIndexation.into());
            }
            let embedder_name = &config.name;
            progress.update_progress(VariableNameStep::<VectorStoreBackendChangeIndex>::new(
                format!("Changing vector store backend for embedder `{embedder_name}`"),
                i as u32,
                embedder_count as u32,
            ));
            let quantized = config.config.quantized();
            let embedder_id = embedders.embedder_id(self.wtxn, &config.name)?.unwrap();
            let vector_store = crate::vector::VectorStore::new(
                old_backend,
                self.index.vector_store,
                embedder_id,
                quantized,
            );

            vector_store.change_backend(
                &rtxn,
                self.wtxn,
                progress.clone(),
                must_stop_processing,
                self.indexer_config.max_memory,
            )?;
        }

        Ok(())
    }

    pub fn execute<'indexer, MSP>(
        mut self,
        must_stop_processing: &'indexer MSP,
        progress: &'indexer Progress,
        embedder_stats: Arc<EmbedderStats>,
    ) -> Result<Option<ChannelCongestion>>
    where
        MSP: Fn() -> bool + Sync,
    {
        progress.update_progress(SettingsIndexerStep::ChangingVectorStore);
        // execute any pending vector store backend change
        self.execute_vector_backend(must_stop_processing, progress)?;

        // force the old indexer if the environment says so
        if self.indexer_config.experimental_no_edition_2024_for_settings {
            progress.update_progress(SettingsIndexerStep::UsingStableIndexer);
            return self
                .legacy_execute(
                    |indexing_step| tracing::debug!(update = ?indexing_step),
                    must_stop_processing,
                    embedder_stats,
                )
                .map(|_| None);
        }

        // only use the new indexer when only the embedder possibly changed
        if let Self {
            searchable_fields: Setting::NotSet,
            displayed_fields: Setting::NotSet,
            filterable_fields: Setting::NotSet,
            sortable_fields: Setting::NotSet,
            criteria: Setting::NotSet,
            stop_words: Setting::NotSet,
            non_separator_tokens: Setting::NotSet,
            separator_tokens: Setting::NotSet,
            dictionary: Setting::NotSet,
            distinct_field: Setting::NotSet,
            synonyms: Setting::NotSet,
            primary_key: Setting::NotSet,
            authorize_typos: Setting::NotSet,
            min_word_len_two_typos: Setting::NotSet,
            min_word_len_one_typo: Setting::NotSet,
            exact_words: Setting::NotSet,
            exact_attributes: Setting::NotSet,
            max_values_per_facet: Setting::NotSet,
            sort_facet_values_by: Setting::NotSet,
            pagination_max_total_hits: Setting::NotSet,
            proximity_precision: Setting::NotSet,
            embedder_settings: _,
            search_cutoff: Setting::NotSet,
            localized_attributes_rules: Setting::NotSet,
            prefix_search: Setting::NotSet,
            facet_search: Setting::NotSet,
            disable_on_numbers: Setting::NotSet,
            chat: Setting::NotSet,
            vector_store: Setting::NotSet,
            wtxn: _,
            index: _,
            indexer_config: _,
        } = &self
        {
            progress.update_progress(SettingsIndexerStep::UsingExperimentalIndexer);

            self.index.set_updated_at(self.wtxn, &OffsetDateTime::now_utc())?;

            let old_inner_settings = InnerIndexSettings::from_index(self.index, self.wtxn, None)?;

            // Update index settings
            let embedding_config_updates = self.update_embedding_configs()?;
            self.update_user_defined_searchable_attributes()?;

            let mut new_inner_settings =
                InnerIndexSettings::from_index(self.index, self.wtxn, None)?;
            new_inner_settings.recompute_searchables(self.wtxn, self.index)?;

            let primary_key_id = self
                .index
                .primary_key(self.wtxn)?
                .and_then(|name| new_inner_settings.fields_ids_map.id(name));
            let settings_update_only = true;
            let inner_settings_diff = InnerIndexSettingsDiff::new(
                old_inner_settings,
                new_inner_settings,
                primary_key_id,
                embedding_config_updates,
                settings_update_only,
            );

            if self.index.number_of_documents(self.wtxn)? > 0 {
                reindex(
                    self.wtxn,
                    self.index,
                    &self.indexer_config.thread_pool,
                    self.indexer_config.grenad_parameters(),
                    &inner_settings_diff,
                    must_stop_processing,
                    progress,
                    embedder_stats,
                )
                .map(Some)
            } else {
                Ok(None)
            }
        } else {
            progress.update_progress(SettingsIndexerStep::UsingStableIndexer);

            self.legacy_execute(
                |indexing_step| tracing::debug!(update = ?indexing_step),
                must_stop_processing,
                embedder_stats,
            )
            .map(|_| None)
        }
    }
}

pub struct InnerIndexSettingsDiff {
    pub(crate) old: InnerIndexSettings,
    pub(crate) new: InnerIndexSettings,
    pub(crate) primary_key_id: Option<FieldId>,
    pub(crate) embedding_config_updates: BTreeMap<String, EmbedderAction>,
    pub(crate) settings_update_only: bool,
    /// The set of only the additional searchable fields.
    /// If any other searchable field has been modified, is set to None.
    pub(crate) only_additional_fields: Option<HashSet<String>>,
    fragment_diffs: BTreeMap<String, Vec<(Option<usize>, usize)>>,

    // Cache the check to see if all the stop_words, allowed_separators, dictionary,
    // exact_attributes, proximity_precision are different.
    pub(crate) cache_reindex_searchable_without_user_defined: bool,
    // Cache the check to see if the user_defined_searchables are different.
    pub(crate) cache_user_defined_searchables: bool,
    // Cache the check to see if the exact_attributes are different.
    pub(crate) cache_exact_attributes: bool,
}

impl InnerIndexSettingsDiff {
    #[tracing::instrument(level = "trace", skip_all, target = "indexing::settings")]
    pub(crate) fn new(
        old_settings: InnerIndexSettings,
        new_settings: InnerIndexSettings,
        primary_key_id: Option<FieldId>,
        mut embedding_config_updates: BTreeMap<String, EmbedderAction>,
        settings_update_only: bool,
    ) -> Self {
        let only_additional_fields = match (
            &old_settings.user_defined_searchable_attributes,
            &new_settings.user_defined_searchable_attributes,
        ) {
            (None, None) | (Some(_), None) | (None, Some(_)) => None, // None means *
            (Some(old), Some(new)) => {
                let old: HashSet<_> = old.iter().cloned().collect();
                let new: HashSet<_> = new.iter().cloned().collect();
                if old.difference(&new).next().is_none() {
                    // if no field has been removed return only the additional ones
                    Some(&new - &old).filter(|x| !x.is_empty())
                } else {
                    None
                }
            }
        };

        let cache_reindex_searchable_without_user_defined = {
            old_settings.stop_words.as_ref().map(|set| set.as_fst().as_bytes())
                != new_settings.stop_words.as_ref().map(|set| set.as_fst().as_bytes())
                || old_settings.allowed_separators != new_settings.allowed_separators
                || old_settings.dictionary != new_settings.dictionary
                || old_settings.proximity_precision != new_settings.proximity_precision
                || old_settings.prefix_search != new_settings.prefix_search
                || old_settings.localized_attributes_rules
                    != new_settings.localized_attributes_rules
                || old_settings.disabled_typos_terms != new_settings.disabled_typos_terms
        };

        let cache_exact_attributes = old_settings.exact_attributes != new_settings.exact_attributes;

        // Check if any searchable field has been added or removed form the list,
        // Changing the order should not be considered as a change for reindexing.
        let cache_user_defined_searchables = match (
            &old_settings.user_defined_searchable_attributes,
            &new_settings.user_defined_searchable_attributes,
        ) {
            (Some(old), Some(new)) => {
                let old: BTreeSet<_> = old.iter().collect();
                let new: BTreeSet<_> = new.iter().collect();

                old != new
            }
            (None, None) => false,
            _otherwise => true,
        };

        // if the user-defined searchables changed, then we need to reindex prompts.
        if cache_user_defined_searchables {
            for (embedder_name, runtime) in new_settings.runtime_embedders.inner_as_ref() {
                let was_quantized = old_settings
                    .runtime_embedders
                    .get(embedder_name)
                    .is_some_and(|conf| conf.is_quantized);
                // skip embedders that don't use document templates
                if !runtime.embedder.uses_document_template() {
                    continue;
                }

                // note: this could currently be entry.or_insert(..), but we're future-proofing with an explicit match
                // this always makes the code clearer by explicitly handling the cases
                match embedding_config_updates.entry(embedder_name.clone()) {
                    std::collections::btree_map::Entry::Vacant(entry) => {
                        entry.insert(EmbedderAction::with_reindex(
                            ReindexAction::RegeneratePrompts,
                            was_quantized,
                        ));
                    }
                    std::collections::btree_map::Entry::Occupied(mut entry) => {
                        // future-proofing, make sure to destructure here so that any new field is taken into account in this case
                        // case in point: adding `remove_fragments` was detected.
                        let EmbedderAction {
                            was_quantized: _,
                            is_being_quantized: _,
                            write_back, // We are deleting this embedder, so no point in regeneration
                            reindex,
                            remove_fragments: _,
                        } = entry.get_mut();

                        // fixup reindex to make sure we regenerate all fragments
                        *reindex = match reindex.take() {
                            Some(reindex) => Some(reindex), // We are at least regenerating prompts
                            None => {
                                if write_back.is_none() {
                                    Some(ReindexAction::RegeneratePrompts) // quantization case
                                } else {
                                    None
                                }
                            }
                        };
                    }
                };
            }
        }

        // build the fragment diffs
        let mut fragment_diffs = BTreeMap::new();
        for (embedder_name, embedder_action) in &embedding_config_updates {
            let Some(new_embedder) = new_settings.runtime_embedders.get(embedder_name) else {
                continue;
            };
            let regenerate_fragments =
                if let Some(ReindexAction::RegenerateFragments(regenerate_fragments)) =
                    embedder_action.reindex()
                {
                    either::Either::Left(
                        regenerate_fragments
                            .iter()
                            .filter(|(_, action)| {
                                !matches!(
                                    action,
                                    crate::vector::settings::RegenerateFragment::Remove
                                )
                            })
                            .map(|(name, _)| name),
                    )
                } else {
                    either::Either::Right(
                        new_embedder.fragments().iter().map(|fragment| &fragment.name),
                    )
                };

            let old_embedder = old_settings.runtime_embedders.get(embedder_name);

            let mut fragments = Vec::new();
            for fragment_name in regenerate_fragments {
                let Ok(new) = new_embedder
                    .fragments()
                    .binary_search_by_key(&fragment_name, |fragment| &fragment.name)
                else {
                    continue;
                };
                let old = old_embedder.as_ref().and_then(|old_embedder| {
                    old_embedder
                        .fragments()
                        .binary_search_by_key(&fragment_name, |fragment| &fragment.name)
                        .ok()
                });
                fragments.push((old, new));
            }
            fragment_diffs.insert(embedder_name.clone(), fragments);
        }

        InnerIndexSettingsDiff {
            old: old_settings,
            new: new_settings,
            primary_key_id,
            fragment_diffs,
            embedding_config_updates,
            settings_update_only,
            only_additional_fields,
            cache_reindex_searchable_without_user_defined,
            cache_user_defined_searchables,
            cache_exact_attributes,
        }
    }

    pub fn any_reindexing_needed(&self) -> bool {
        self.reindex_searchable()
            || self.reindex_facets()
            || self.reindex_vectors()
            || self.reindex_geojson()
    }

    pub fn reindex_searchable(&self) -> bool {
        self.cache_reindex_searchable_without_user_defined
            || self.cache_exact_attributes
            || self.cache_user_defined_searchables
    }

    pub fn reindex_proximities(&self) -> bool {
        // if any searchable settings force the reindexing
        (self.cache_reindex_searchable_without_user_defined || self.cache_user_defined_searchables)
        // and if any settings needs the proximity database created
            && (self.old.proximity_precision == ProximityPrecision::ByAttribute
                || self.new.proximity_precision == ProximityPrecision::ByAttribute)
    }

    pub fn reindex_searchable_id(&self, id: FieldId) -> Option<DelAddOperation> {
        if self.cache_reindex_searchable_without_user_defined || self.cache_exact_attributes {
            Some(DelAddOperation::DeletionAndAddition)
        } else if let Some(only_additional_fields) = &self.only_additional_fields {
            let additional_field = self.new.fields_ids_map.name(id).unwrap();
            if only_additional_fields.contains(additional_field) {
                Some(DelAddOperation::Addition)
            } else {
                None
            }
        } else if self.cache_user_defined_searchables {
            Some(DelAddOperation::DeletionAndAddition)
        } else {
            None
        }
    }

    /// List the faceted fields from the inner fid map.
    /// This is used to list the faceted fields when we are reindexing,
    /// but it can't be used in document addition because the field id map must be exhaustive.
    pub fn list_faceted_fields_from_fid_map(&self, del_add: DelAdd) -> BTreeSet<FieldId> {
        let settings = match del_add {
            DelAdd::Deletion => &self.old,
            DelAdd::Addition => &self.new,
        };

        settings
            .fields_ids_map
            .iter_id_metadata()
            .filter(|(_, metadata)| metadata.is_faceted(&settings.filterable_attributes_rules))
            .map(|(id, _)| id)
            .collect()
    }

    pub fn facet_fids_changed(&self) -> bool {
        for eob in merge_join_by(
            self.old.fields_ids_map.iter().filter(|(_, _, metadata)| {
                metadata.is_faceted(&self.old.filterable_attributes_rules)
            }),
            self.new.fields_ids_map.iter().filter(|(_, _, metadata)| {
                metadata.is_faceted(&self.new.filterable_attributes_rules)
            }),
            |(old_fid, _, _), (new_fid, _, _)| old_fid.cmp(new_fid),
        ) {
            match eob {
                // If there is a difference, we need to reindex facet databases.
                EitherOrBoth::Left(_) | EitherOrBoth::Right(_) => return true,
                // If the field is faceted in both old and new settings, we check the facet-searchable and facet level database.
                EitherOrBoth::Both((_, _, old_metadata), (_, _, new_metadata)) => {
                    // Check if the field is facet-searchable in the old and new settings.
                    // If there is a difference, we need to reindex facet-search database.
                    let old_filterable_features = old_metadata
                        .filterable_attributes_features(&self.old.filterable_attributes_rules);
                    let new_filterable_features = new_metadata
                        .filterable_attributes_features(&self.new.filterable_attributes_rules);
                    let is_old_facet_searchable =
                        old_filterable_features.is_facet_searchable() && self.old.facet_search;
                    let is_new_facet_searchable =
                        new_filterable_features.is_facet_searchable() && self.new.facet_search;
                    if is_old_facet_searchable != is_new_facet_searchable {
                        return true;
                    }

                    // Check if the field needs a facet level database in the old and new settings.
                    // If there is a difference, we need to reindex facet level databases.
                    let old_facet_level_database = old_metadata
                        .require_facet_level_database(&self.old.filterable_attributes_rules);
                    let new_facet_level_database = new_metadata
                        .require_facet_level_database(&self.new.filterable_attributes_rules);
                    if old_facet_level_database != new_facet_level_database {
                        return true;
                    }
                }
            }
        }

        false
    }

    pub fn global_facet_settings_changed(&self) -> bool {
        self.old.localized_attributes_rules != self.new.localized_attributes_rules
            || self.old.facet_search != self.new.facet_search
    }

    pub fn reindex_facets(&self) -> bool {
        self.facet_fids_changed() || self.global_facet_settings_changed()
    }

    pub fn reindex_vectors(&self) -> bool {
        !self.embedding_config_updates.is_empty()
    }

    pub fn reindex_geojson(&self) -> bool {
        self.old.filterable_attributes_rules.iter().any(|rule| rule.has_geojson())
            != self.new.filterable_attributes_rules.iter().any(|rule| rule.has_geojson())
    }

    pub fn settings_update_only(&self) -> bool {
        self.settings_update_only
    }

    pub fn run_geo_indexing(&self) -> bool {
        self.old.geo_fields_ids != self.new.geo_fields_ids
            || (!self.settings_update_only && self.new.geo_fields_ids.is_some())
    }

    pub fn run_geojson_indexing(&self) -> bool {
        self.old.geojson_fid != self.new.geojson_fid
            || (!self.settings_update_only && self.new.geojson_fid.is_some())
    }
}

#[derive(Clone)]
pub(crate) struct InnerIndexSettings {
    pub stop_words: Option<fst::Set<Vec<u8>>>,
    pub allowed_separators: Option<BTreeSet<String>>,
    pub dictionary: Option<BTreeSet<String>>,
    pub fields_ids_map: FieldIdMapWithMetadata,
    pub localized_attributes_rules: Vec<LocalizedAttributesRule>,
    pub filterable_attributes_rules: Vec<FilterableAttributesRule>,
    pub asc_desc_fields: HashSet<String>,
    pub distinct_field: Option<String>,
    pub user_defined_searchable_attributes: Option<Vec<String>>,
    pub sortable_fields: HashSet<String>,
    pub exact_attributes: HashSet<FieldId>,
    pub disabled_typos_terms: DisabledTyposTerms,
    pub proximity_precision: ProximityPrecision,
    pub runtime_embedders: RuntimeEmbedders,
    pub embedder_category_id: HashMap<String, u8>,
    pub geo_fields_ids: Option<(FieldId, FieldId)>,
    pub geojson_fid: Option<FieldId>,
    pub prefix_search: PrefixSearch,
    pub facet_search: bool,
}

impl InnerIndexSettings {
    pub fn from_index(
        index: &Index,
        rtxn: &heed::RoTxn<'_>,
        runtime_embedders: Option<RuntimeEmbedders>,
    ) -> Result<Self> {
        let stop_words = index.stop_words(rtxn)?;
        let stop_words = stop_words.map(|sw| sw.map_data(Vec::from).unwrap());
        let allowed_separators = index.allowed_separators(rtxn)?;
        let dictionary = index.dictionary(rtxn)?;
        let mut fields_ids_map = index.fields_ids_map(rtxn)?;
        let exact_attributes = index.exact_attributes_ids(rtxn)?;
        let proximity_precision = index.proximity_precision(rtxn)?.unwrap_or_default();
        let runtime_embedders = match runtime_embedders {
            Some(embedding_configs) => embedding_configs,
            None => embedders(index.embedding_configs().embedding_configs(rtxn)?)?,
        };
        let embedder_category_id = index
            .embedding_configs()
            .iter_embedder_id(rtxn)?
            .map(|r| r.map(|(k, v)| (k.to_string(), v)))
            .collect::<heed::Result<_>>()?;
        let prefix_search = index.prefix_search(rtxn)?.unwrap_or_default();
        let facet_search = index.facet_search(rtxn)?;
        let geo_fields_ids = match fields_ids_map.id(RESERVED_GEO_FIELD_NAME) {
            Some(_) if index.is_geo_enabled(rtxn)? => {
                // if `_geo` is faceted then we get the `lat` and `lng`
                let field_ids = fields_ids_map
                    .insert("_geo.lat")
                    .zip(fields_ids_map.insert("_geo.lng"))
                    .ok_or(UserError::AttributeLimitReached)?;
                Some(field_ids)
            }
            _ => None,
        };
        let geo_json_fid = fields_ids_map.id(RESERVED_GEOJSON_FIELD_NAME);
        let localized_attributes_rules =
            index.localized_attributes_rules(rtxn)?.unwrap_or_default();
        let filterable_attributes_rules = index.filterable_attributes_rules(rtxn)?;
        let sortable_fields = index.sortable_fields(rtxn)?;
        let asc_desc_fields = index.asc_desc_fields(rtxn)?;
        let distinct_field = index.distinct_field(rtxn)?.map(|f| f.to_string());
        let user_defined_searchable_attributes = index
            .user_defined_searchable_fields(rtxn)?
            .map(|fields| fields.into_iter().map(|f| f.to_string()).collect());
        let builder = MetadataBuilder::from_index(index, rtxn)?;
        let fields_ids_map = FieldIdMapWithMetadata::new(fields_ids_map, builder);
        let disabled_typos_terms = index.disabled_typos_terms(rtxn)?;
        Ok(Self {
            stop_words,
            allowed_separators,
            dictionary,
            fields_ids_map,
            localized_attributes_rules,
            filterable_attributes_rules,
            asc_desc_fields,
            distinct_field,
            user_defined_searchable_attributes,
            sortable_fields,
            exact_attributes,
            proximity_precision,
            runtime_embedders,
            embedder_category_id,
            geo_fields_ids,
            geojson_fid: geo_json_fid,
            prefix_search,
            facet_search,
            disabled_typos_terms,
        })
    }

    pub fn match_faceted_field(&self, field: &str) -> PatternMatch {
        match_faceted_field(
            field,
            &self.filterable_attributes_rules,
            &self.sortable_fields,
            &self.asc_desc_fields,
            &self.distinct_field,
        )
    }

    // find and insert the new field ids
    pub fn recompute_searchables(
        &mut self,
        wtxn: &mut heed::RwTxn<'_>,
        index: &Index,
    ) -> Result<()> {
        let searchable_fields = self
            .user_defined_searchable_attributes
            .as_ref()
            .map(|searchable| searchable.iter().map(|s| s.as_str()).collect::<Vec<_>>());

        // in case new fields were introduced we're going to recreate the searchable fields.
        if let Some(searchable_fields) = searchable_fields {
            index.put_all_searchable_fields_from_fields_ids_map(
                wtxn,
                &searchable_fields,
                &self.fields_ids_map,
            )?;
        }

        Ok(())
    }
}

fn embedders(embedding_configs: Vec<IndexEmbeddingConfig>) -> Result<RuntimeEmbedders> {
    let res: Result<_> = embedding_configs
        .into_iter()
        .map(
            |IndexEmbeddingConfig {
                 name,
                 config: EmbeddingConfig { embedder_options, prompt, quantized },
                 fragments,
             }| {
                let document_template = prompt.try_into().map_err(crate::Error::from)?;

                let embedder =
                    // cache_cap: no cache needed for indexing purposes
                    Arc::new(Embedder::new(embedder_options.clone(), 0)
                        .map_err(crate::vector::Error::from)
                        .map_err(crate::Error::from)?);

                let fragments = fragments
                    .into_inner()
                    .into_iter()
                    .map(|fragment| {
                        let template = JsonTemplate::new(
                            embedder_options.fragment(&fragment.name).unwrap().clone(),
                        )
                        .unwrap();

                        RuntimeFragment { name: fragment.name, id: fragment.id, template }
                    })
                    .collect();

                Ok((
                    name,
                    Arc::new(RuntimeEmbedder::new(
                        embedder,
                        document_template,
                        fragments,
                        quantized.unwrap_or_default(),
                    )),
                ))
            },
        )
        .collect();
    res.map(RuntimeEmbedders::new)
}

fn validate_prompt(
    name: &str,
    new_prompt: Setting<String>,
    max_bytes: Setting<usize>,
) -> Result<Setting<String>> {
    match new_prompt {
        Setting::Set(template) => {
            let max_bytes = match max_bytes.set() {
                Some(max_bytes) => NonZeroUsize::new(max_bytes).ok_or_else(|| {
                    crate::error::UserError::InvalidSettingsDocumentTemplateMaxBytes {
                        embedder_name: name.to_owned(),
                    }
                })?,
                None => default_max_bytes(),
            };

            // validate
            let template = crate::prompt::Prompt::new(
                template,
                // always specify a max_bytes
                Some(max_bytes),
            )
            .map(|prompt| crate::prompt::PromptData::from(prompt).template)
            .map_err(|inner| UserError::InvalidPromptForEmbeddings(name.to_owned(), inner))?;

            Ok(Setting::Set(template))
        }
        new => Ok(new),
    }
}

pub fn validate_embedding_settings(
    settings: Setting<EmbeddingSettings>,
    name: &str,
    context: EmbeddingValidationContext,
) -> Result<Setting<EmbeddingSettings>> {
    let Setting::Set(settings) = settings else { return Ok(settings) };
    let EmbeddingSettings {
        source,
        model,
        revision,
        pooling,
        api_key,
        dimensions,
        document_template,
        document_template_max_bytes,
        url,
        indexing_fragments,
        search_fragments,
        request,
        response,
        search_embedder,
        mut indexing_embedder,
        distribution,
        headers,
        binary_quantized: binary_quantize,
    } = settings;

    let document_template = validate_prompt(name, document_template, document_template_max_bytes)?;

    if let Some(0) = dimensions.set() {
        return Err(crate::error::UserError::InvalidSettingsDimensions {
            embedder_name: name.to_owned(),
        }
        .into());
    }

    if let Some(url) = url.as_ref().set() {
        url::Url::parse(url).map_err(|error| crate::error::UserError::InvalidUrl {
            embedder_name: name.to_owned(),
            inner_error: error,
            url: url.to_owned(),
        })?;
    }

    // used below
    enum WithFragments {
        Yes {
            indexing_fragments: BTreeMap<String, serde_json::Value>,
            search_fragments: BTreeMap<String, serde_json::Value>,
        },
        No,
        Maybe,
    }

    let with_fragments = {
        let has_reset = matches!(indexing_fragments, Setting::Reset)
            || matches!(search_fragments, Setting::Reset);
        let indexing_fragments: BTreeMap<_, _> = indexing_fragments
            .as_ref()
            .set()
            .iter()
            .flat_map(|map| map.iter())
            .filter_map(|(name, fragment)| {
                Some((name.clone(), fragment.as_ref().map(|fragment| fragment.value.clone())?))
            })
            .collect();
        let search_fragments: BTreeMap<_, _> = search_fragments
            .as_ref()
            .set()
            .iter()
            .flat_map(|map| map.iter())
            .filter_map(|(name, fragment)| {
                Some((name.clone(), fragment.as_ref().map(|fragment| fragment.value.clone())?))
            })
            .collect();

        let has_fragments = !indexing_fragments.is_empty() || !search_fragments.is_empty();

        if context == EmbeddingValidationContext::FullSettings {
            let are_fragments_inconsistent =
                indexing_fragments.is_empty() ^ search_fragments.is_empty();
            if are_fragments_inconsistent {
                return Err(crate::vector::error::NewEmbedderError::rest_inconsistent_fragments(
                    indexing_fragments.is_empty(),
                    indexing_fragments,
                    search_fragments,
                ))
                .map_err(|error| crate::UserError::VectorEmbeddingError(error.into()).into());
            }
        }
        if has_fragments {
            if context == EmbeddingValidationContext::SettingsPartialUpdate
                && matches!(document_template, Setting::Set(_))
            {
                return Err(
                    crate::vector::error::NewEmbedderError::rest_document_template_and_fragments(
                        indexing_fragments.len(),
                        search_fragments.len(),
                    ),
                )
                .map_err(|error| crate::UserError::VectorEmbeddingError(error.into()).into());
            }
            WithFragments::Yes { indexing_fragments, search_fragments }
        } else if has_reset || context == EmbeddingValidationContext::FullSettings {
            WithFragments::No
        } else {
            // if we are working with partial settings, the user could have changed only the `request` and not given again the fragments
            WithFragments::Maybe
        }
    };
    if let Some(request) = request.as_ref().set() {
        let request = match with_fragments {
            WithFragments::Yes { indexing_fragments, search_fragments } => {
                rest::RequestData::new(request.to_owned(), indexing_fragments, search_fragments)
                    .map_err(|error| crate::UserError::VectorEmbeddingError(error.into()))
            }
            WithFragments::No => {
                rest::RequestData::new(request.to_owned(), Default::default(), Default::default())
                    .map_err(|error| crate::UserError::VectorEmbeddingError(error.into()))
            }
            WithFragments::Maybe => {
                let mut indexing_fragments = BTreeMap::new();
                indexing_fragments.insert("test".to_string(), serde_json::json!("test"));
                rest::RequestData::new(request.to_owned(), indexing_fragments, Default::default())
                    .or_else(|_| {
                        rest::RequestData::new(
                            request.to_owned(),
                            Default::default(),
                            Default::default(),
                        )
                    })
                    .map_err(|error| crate::UserError::VectorEmbeddingError(error.into()))
            }
        }?;
        if let Some(response) = response.as_ref().set() {
            rest::Response::new(response.to_owned(), &request)
                .map_err(|error| crate::UserError::VectorEmbeddingError(error.into()))?;
        }
    }

    let Some(inferred_source) = source.set() else {
        // we are validating the fused settings, so we always have a source
        return Ok(Setting::Set(EmbeddingSettings {
            source,
            model,
            revision,
            pooling,
            api_key,
            dimensions,
            document_template,
            document_template_max_bytes,
            url,
            indexing_fragments,
            search_fragments,
            request,
            response,
            search_embedder,
            indexing_embedder,
            distribution,
            headers,
            binary_quantized: binary_quantize,
        }));
    };
    EmbeddingSettings::check_settings(
        name,
        inferred_source,
        NestingContext::NotNested,
        &model,
        &revision,
        &pooling,
        &dimensions,
        &api_key,
        &url,
        &indexing_fragments,
        &search_fragments,
        &request,
        &response,
        &document_template,
        &document_template_max_bytes,
        &headers,
        &search_embedder,
        &indexing_embedder,
        &binary_quantize,
        &distribution,
    )?;
    match inferred_source {
        EmbedderSource::OpenAi => {
            if let Setting::Set(model) = &model {
                let model = openai::EmbeddingModel::from_name(model.as_str()).ok_or(
                    crate::error::UserError::InvalidOpenAiModel {
                        embedder_name: name.to_owned(),
                        model: model.clone(),
                    },
                )?;
                if let Setting::Set(dimensions) = dimensions {
                    if !model.supports_overriding_dimensions()
                        && dimensions != model.default_dimensions()
                    {
                        return Err(crate::error::UserError::InvalidOpenAiModelDimensions {
                            embedder_name: name.to_owned(),
                            model: model.name(),
                            dimensions,
                            expected_dimensions: model.default_dimensions(),
                        }
                        .into());
                    }
                    if dimensions > model.default_dimensions() {
                        return Err(crate::error::UserError::InvalidOpenAiModelDimensionsMax {
                            embedder_name: name.to_owned(),
                            model: model.name(),
                            dimensions,
                            max_dimensions: model.default_dimensions(),
                        }
                        .into());
                    }
                }
            }
        }
        EmbedderSource::Ollama
        | EmbedderSource::HuggingFace
        | EmbedderSource::UserProvided
        | EmbedderSource::Rest => {}
        EmbedderSource::Composite => {
            if let Setting::Set(embedder) = &search_embedder {
                if let Some(source) = embedder.source.set() {
                    let search_embedder = match embedder.search_embedder.clone() {
                        Setting::Set(search_embedder) => Setting::Set(deserialize_sub_embedder(
                            search_embedder,
                            name,
                            NestingContext::Search,
                        )?),
                        Setting::Reset => Setting::Reset,
                        Setting::NotSet => Setting::NotSet,
                    };
                    let indexing_embedder = match embedder.indexing_embedder.clone() {
                        Setting::Set(indexing_embedder) => Setting::Set(deserialize_sub_embedder(
                            indexing_embedder,
                            name,
                            NestingContext::Search,
                        )?),
                        Setting::Reset => Setting::Reset,
                        Setting::NotSet => Setting::NotSet,
                    };
                    EmbeddingSettings::check_nested_source(name, source, NestingContext::Search)?;
                    EmbeddingSettings::check_settings(
                        name,
                        source,
                        NestingContext::Search,
                        &embedder.model,
                        &embedder.revision,
                        &embedder.pooling,
                        &embedder.dimensions,
                        &embedder.api_key,
                        &embedder.url,
                        &embedder.indexing_fragments,
                        &embedder.search_fragments,
                        &embedder.request,
                        &embedder.response,
                        &embedder.document_template,
                        &embedder.document_template_max_bytes,
                        &embedder.headers,
                        &search_embedder,
                        &indexing_embedder,
                        &embedder.binary_quantized,
                        &embedder.distribution,
                    )?;
                } else {
                    return Err(UserError::MissingSourceForNested {
                        embedder_name: NestingContext::Search.embedder_name_with_context(name),
                    }
                    .into());
                }
            }

            indexing_embedder = if let Setting::Set(mut embedder) = indexing_embedder {
                embedder.document_template = validate_prompt(
                    name,
                    embedder.document_template,
                    embedder.document_template_max_bytes,
                )?;

                if let Some(source) = embedder.source.set() {
                    let search_embedder = match embedder.search_embedder.clone() {
                        Setting::Set(search_embedder) => Setting::Set(deserialize_sub_embedder(
                            search_embedder,
                            name,
                            NestingContext::Indexing,
                        )?),
                        Setting::Reset => Setting::Reset,
                        Setting::NotSet => Setting::NotSet,
                    };
                    let indexing_embedder = match embedder.indexing_embedder.clone() {
                        Setting::Set(indexing_embedder) => Setting::Set(deserialize_sub_embedder(
                            indexing_embedder,
                            name,
                            NestingContext::Indexing,
                        )?),
                        Setting::Reset => Setting::Reset,
                        Setting::NotSet => Setting::NotSet,
                    };
                    EmbeddingSettings::check_nested_source(name, source, NestingContext::Indexing)?;
                    EmbeddingSettings::check_settings(
                        name,
                        source,
                        NestingContext::Indexing,
                        &embedder.model,
                        &embedder.revision,
                        &embedder.pooling,
                        &embedder.dimensions,
                        &embedder.api_key,
                        &embedder.url,
                        &embedder.indexing_fragments,
                        &embedder.search_fragments,
                        &embedder.request,
                        &embedder.response,
                        &embedder.document_template,
                        &embedder.document_template_max_bytes,
                        &embedder.headers,
                        &search_embedder,
                        &indexing_embedder,
                        &embedder.binary_quantized,
                        &embedder.distribution,
                    )?;
                } else {
                    return Err(UserError::MissingSourceForNested {
                        embedder_name: NestingContext::Indexing.embedder_name_with_context(name),
                    }
                    .into());
                }
                Setting::Set(embedder)
            } else {
                indexing_embedder
            };
        }
    }
    Ok(Setting::Set(EmbeddingSettings {
        source,
        model,
        revision,
        pooling,
        api_key,
        dimensions,
        document_template,
        document_template_max_bytes,
        url,
        indexing_fragments,
        search_fragments,
        request,
        response,
        search_embedder,
        indexing_embedder,
        distribution,
        headers,
        binary_quantized: binary_quantize,
    }))
}

fn deserialize_sub_embedder(
    sub_embedder: serde_json::Value,
    embedder_name: &str,
    context: NestingContext,
) -> std::result::Result<SubEmbeddingSettings, UserError> {
    match deserr::deserialize::<_, _, deserr::errors::JsonError>(sub_embedder) {
        Ok(sub_embedder) => Ok(sub_embedder),
        Err(error) => {
            let message = format!("{error}{}", context.nesting_embedders());
            Err(UserError::InvalidSettingsEmbedder {
                embedder_name: context.embedder_name_with_context(embedder_name),
                message,
            })
        }
    }
}

/// Implement this trait for the settings delta type.
/// This is used in the new settings update flow and will allow to easily replace the old settings delta type: `InnerIndexSettingsDiff`.
pub trait SettingsDelta {
    fn new_embedders(&self) -> &RuntimeEmbedders;
    fn old_embedders(&self) -> &RuntimeEmbedders;
    fn new_embedder_category_id(&self) -> &HashMap<String, u8>;
    fn embedder_actions(&self) -> &BTreeMap<String, EmbedderAction>;
    fn try_for_each_fragment_diff<F, E>(
        &self,
        embedder_name: &str,
        for_each: F,
    ) -> std::result::Result<(), E>
    where
        F: FnMut(FragmentDiff) -> std::result::Result<(), E>;
    fn new_fields_ids_map(&self) -> &FieldIdMapWithMetadata;
}

pub struct FragmentDiff<'a> {
    pub old: Option<&'a RuntimeFragment>,
    pub new: &'a RuntimeFragment,
}

impl SettingsDelta for InnerIndexSettingsDiff {
    fn new_embedders(&self) -> &RuntimeEmbedders {
        &self.new.runtime_embedders
    }

    fn old_embedders(&self) -> &RuntimeEmbedders {
        &self.old.runtime_embedders
    }

    fn new_embedder_category_id(&self) -> &HashMap<String, u8> {
        &self.new.embedder_category_id
    }

    fn embedder_actions(&self) -> &BTreeMap<String, EmbedderAction> {
        &self.embedding_config_updates
    }

    fn new_fields_ids_map(&self) -> &FieldIdMapWithMetadata {
        &self.new.fields_ids_map
    }

    fn try_for_each_fragment_diff<F, E>(
        &self,
        embedder_name: &str,
        mut for_each: F,
    ) -> std::result::Result<(), E>
    where
        F: FnMut(FragmentDiff) -> std::result::Result<(), E>,
    {
        let Some(fragment_diff) = self.fragment_diffs.get(embedder_name) else { return Ok(()) };
        for (old, new) in fragment_diff {
            let Some(new_runtime) = self.new.runtime_embedders.get(embedder_name) else {
                continue;
            };

            let new = new_runtime.fragments().get(*new).unwrap();

            match old {
                Some(old) => {
                    if let Some(old_runtime) = self.old.runtime_embedders.get(embedder_name) {
                        let old = &old_runtime.fragments().get(*old).unwrap();
                        for_each(FragmentDiff { old: Some(old), new })?;
                    } else {
                        for_each(FragmentDiff { old: None, new })?;
                    }
                }
                None => for_each(FragmentDiff { old: None, new })?,
            };
        }
        Ok(())
    }
}

#[cfg(test)]
#[path = "test_settings.rs"]
mod tests;
