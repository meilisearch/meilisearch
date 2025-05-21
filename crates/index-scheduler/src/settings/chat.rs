use std::collections::{BTreeMap, BTreeSet};
use std::convert::Infallible;
use std::fmt;
use std::marker::PhantomData;
use std::num::NonZeroUsize;
use std::ops::{ControlFlow, Deref};
use std::str::FromStr;

use deserr::{DeserializeError, Deserr, ErrorKind, MergeWithError, ValuePointerRef};
use fst::IntoStreamer;
use milli::disabled_typos_terms::DisabledTyposTerms;
use milli::index::{IndexEmbeddingConfig, PrefixSearch};
use milli::proximity::ProximityPrecision;
use milli::update::Setting;
use milli::{FilterableAttributesRule, Index};
use serde::{Deserialize, Serialize, Serializer};
use utoipa::ToSchema;

use crate::deserr::DeserrJsonError;
use crate::error::deserr_codes::*;
use crate::heed::RoTxn;
use crate::IndexScheduler;

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq, Deserr, ToSchema)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
#[deserr(deny_unknown_fields, rename_all = camelCase, where_predicate = __Deserr_E: deserr::MergeWithError<DeserrJsonError<InvalidSettingsTypoTolerance>>)]
pub struct PromptsSettings {
    #[serde(default, skip_serializing_if = "Setting::is_not_set")]
    #[deserr(default)]
    #[schema(value_type = Option<String>)]
    pub system: Setting<String>,

    #[serde(default, skip_serializing_if = "Setting::is_not_set")]
    #[deserr(default, error = DeserrJsonError<InvalidSettingsTypoTolerance>)]
    #[schema(value_type = Option<MinWordSizeTyposSetting>, example = json!({ "oneTypo": 5, "twoTypo": 9 }))]
    pub search_description: Setting<String>,

    #[serde(default, skip_serializing_if = "Setting::is_not_set")]
    #[deserr(default)]
    #[schema(value_type = Option<String>)]
    pub search_q_param: Setting<String>,

    #[serde(default, skip_serializing_if = "Setting::is_not_set")]
    #[deserr(default)]
    #[schema(value_type = Option<String>)]
    pub pre_query: Setting<String>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq, Deserr, ToSchema)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub enum ChatSource {
    #[default]
    OpenAi,
}

/// Holds all the settings for an index. `T` can either be `Checked` if they represents settings
/// whose validity is guaranteed, or `Unchecked` if they need to be validated. In the later case, a
/// call to `check` will return a `Settings<Checked>` from a `Settings<Unchecked>`.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq, Deserr, ToSchema)]
#[serde(
    deny_unknown_fields,
    rename_all = "camelCase",
    bound(serialize = "T: Serialize", deserialize = "T: Deserialize<'static>")
)]
#[deserr(error = DeserrJsonError, rename_all = camelCase, deny_unknown_fields)]
#[schema(rename_all = "camelCase")]
pub struct ChatSettings<T> {
    #[serde(default, skip_serializing_if = "Setting::is_not_set")]
    #[deserr(default, error = DeserrJsonError<InvalidSettingsDisplayedAttributes>)]
    #[schema(value_type = Option<String>)]
    pub source: Setting<ChatSource>,

    #[serde(default, skip_serializing_if = "Setting::is_not_set")]
    #[deserr(default, error = DeserrJsonError<InvalidSettingsSearchableAttributes>)]
    #[schema(value_type = Option<String>)]
    pub base_api: Setting<String>,

    #[serde(default, skip_serializing_if = "Setting::is_not_set")]
    #[deserr(default, error = DeserrJsonError<InvalidSettingsSearchableAttributes>)]
    #[schema(value_type = Option<String>)]
    pub api_key: Setting<String>,

    #[serde(default, skip_serializing_if = "Setting::is_not_set")]
    #[deserr(default, error = DeserrJsonError<InvalidSettingsFilterableAttributes>)]
    #[schema(value_type = Option<PromptsSettings>)]
    pub prompts: Setting<PromptsSettings>,

    #[serde(skip)]
    #[deserr(skip)]
    pub _kind: PhantomData<T>,
}

impl<T> ChatSettings<T> {
    pub fn hide_secrets(&mut self) {
        match &mut self.api_key {
            Setting::Set(key) => Self::hide_secrets(key),
            Setting::Reset => todo!(),
            Setting::NotSet => todo!(),
        }
    }

    fn hide_secret(secret: &mut String) {
        match secret.len() {
            x if x < 10 => {
                secret.replace_range(.., "XXX...");
            }
            x if x < 20 => {
                secret.replace_range(2.., "XXXX...");
            }
            x if x < 30 => {
                secret.replace_range(3.., "XXXXX...");
            }
            _x => {
                secret.replace_range(5.., "XXXXXX...");
            }
        }
    }
}

impl ChatSettings<Checked> {
    pub fn cleared() -> ChatSettings<Checked> {
        ChatSettings {
            source: Setting::Reset,
            base_api: Setting::Reset,
            api_key: Setting::Reset,
            prompts: Setting::Reset,
            _kind: PhantomData,
        }
    }

    pub fn into_unchecked(self) -> ChatSettings<Unchecked> {
        let Self { source, base_api, api_key, prompts, _kind } = self;
        ChatSettings { source, base_api, api_key, prompts, _kind: PhantomData }
    }
}

impl ChatSettings<Unchecked> {
    pub fn check(self) -> ChatSettings<Checked> {
        ChatSettings {
            source: self.source,
            base_api: self.base_api,
            api_key: self.api_key,
            prompts: self.prompts,
            _kind: PhantomData,
        }
    }

    pub fn validate(self) -> Result<Self, milli::Error> {
        self.validate_prompt_settings()?;
        self.validate_global_settings()
    }

    fn validate_global_settings(mut self) -> Result<Self, milli::Error> {
        // Check that the ApiBase is a valid URL
        Ok(self)
    }

    fn validate_prompt_settings(mut self) -> Result<Self, milli::Error> {
        // TODO
        // let Setting::Set(mut configs) = self.embedders else { return Ok(self) };
        // for (name, config) in configs.iter_mut() {
        //     let config_to_check = std::mem::take(config);
        //     let checked_config =
        //         milli::update::validate_embedding_settings(config_to_check.inner, name)?;
        //     *config = SettingEmbeddingSettings { inner: checked_config };
        // }
        // self.embedders = Setting::Set(configs);
        Ok(self)
    }

    pub fn merge(&mut self, other: &Self) {
        // For most settings only the latest version is kept
        *self = Self {
            source: other.source.or(self.source),
            base_api: other.base_api.or(self.base_api),
            api_key: other.api_key.or(self.api_key),
            prompts: match (self.prompts, other.prompts) {
                (Setting::NotSet, set) | (set, Setting::NotSet) => set,
                (Setting::Set(_) | Setting::Reset, Setting::Reset) => Setting::Reset,
                (Setting::Reset, Setting::Set(set)) => Setting::Set(set),
                // If both are set we must merge the prompts settings
                (Setting::Set(this), Setting::Set(other)) => Setting::Set(PromptsSettings {
                    system: other.system.or(system),
                    search_description: other.search_description.or(search_description),
                    search_q_param: other.search_q_param.or(search_q_param),
                    pre_query: other.pre_query.or(pre_query),
                }),
            },

            _kind: PhantomData,
        }
    }
}

pub fn apply_settings_to_builder(
    settings: &ChatSettings<Checked>,
    // TODO we must not store this into milli but in the index scheduler
    builder: &mut milli::update::Settings,
) {
    let ChatSettings { source, base_api, api_key, prompts, _kind } = settings;

    match source.deref() {
        Setting::Set(ref names) => builder.set_searchable_fields(names.clone()),
        Setting::Reset => builder.reset_searchable_fields(),
        Setting::NotSet => (),
    }

    match displayed_attributes.deref() {
        Setting::Set(ref names) => builder.set_displayed_fields(names.clone()),
        Setting::Reset => builder.reset_displayed_fields(),
        Setting::NotSet => (),
    }

    match filterable_attributes {
        Setting::Set(ref facets) => {
            builder.set_filterable_fields(facets.clone().into_iter().collect())
        }
        Setting::Reset => builder.reset_filterable_fields(),
        Setting::NotSet => (),
    }

    match sortable_attributes {
        Setting::Set(ref fields) => builder.set_sortable_fields(fields.iter().cloned().collect()),
        Setting::Reset => builder.reset_sortable_fields(),
        Setting::NotSet => (),
    }

    match ranking_rules {
        Setting::Set(ref criteria) => {
            builder.set_criteria(criteria.iter().map(|c| c.clone().into()).collect())
        }
        Setting::Reset => builder.reset_criteria(),
        Setting::NotSet => (),
    }

    match stop_words {
        Setting::Set(ref stop_words) => builder.set_stop_words(stop_words.clone()),
        Setting::Reset => builder.reset_stop_words(),
        Setting::NotSet => (),
    }

    match non_separator_tokens {
        Setting::Set(ref non_separator_tokens) => {
            builder.set_non_separator_tokens(non_separator_tokens.clone())
        }
        Setting::Reset => builder.reset_non_separator_tokens(),
        Setting::NotSet => (),
    }

    match separator_tokens {
        Setting::Set(ref separator_tokens) => {
            builder.set_separator_tokens(separator_tokens.clone())
        }
        Setting::Reset => builder.reset_separator_tokens(),
        Setting::NotSet => (),
    }

    match dictionary {
        Setting::Set(ref dictionary) => builder.set_dictionary(dictionary.clone()),
        Setting::Reset => builder.reset_dictionary(),
        Setting::NotSet => (),
    }

    match synonyms {
        Setting::Set(ref synonyms) => builder.set_synonyms(synonyms.clone().into_iter().collect()),
        Setting::Reset => builder.reset_synonyms(),
        Setting::NotSet => (),
    }

    match distinct_attribute {
        Setting::Set(ref attr) => builder.set_distinct_field(attr.clone()),
        Setting::Reset => builder.reset_distinct_field(),
        Setting::NotSet => (),
    }

    match proximity_precision {
        Setting::Set(ref precision) => builder.set_proximity_precision((*precision).into()),
        Setting::Reset => builder.reset_proximity_precision(),
        Setting::NotSet => (),
    }

    match localized_attributes_rules {
        Setting::Set(ref rules) => builder
            .set_localized_attributes_rules(rules.iter().cloned().map(|r| r.into()).collect()),
        Setting::Reset => builder.reset_localized_attributes_rules(),
        Setting::NotSet => (),
    }

    match typo_tolerance {
        Setting::Set(ref value) => {
            match value.enabled {
                Setting::Set(val) => builder.set_autorize_typos(val),
                Setting::Reset => builder.reset_authorize_typos(),
                Setting::NotSet => (),
            }

            match value.min_word_size_for_typos {
                Setting::Set(ref setting) => {
                    match setting.one_typo {
                        Setting::Set(val) => builder.set_min_word_len_one_typo(val),
                        Setting::Reset => builder.reset_min_word_len_one_typo(),
                        Setting::NotSet => (),
                    }
                    match setting.two_typos {
                        Setting::Set(val) => builder.set_min_word_len_two_typos(val),
                        Setting::Reset => builder.reset_min_word_len_two_typos(),
                        Setting::NotSet => (),
                    }
                }
                Setting::Reset => {
                    builder.reset_min_word_len_one_typo();
                    builder.reset_min_word_len_two_typos();
                }
                Setting::NotSet => (),
            }

            match value.disable_on_words {
                Setting::Set(ref words) => {
                    builder.set_exact_words(words.clone());
                }
                Setting::Reset => builder.reset_exact_words(),
                Setting::NotSet => (),
            }

            match value.disable_on_attributes {
                Setting::Set(ref words) => {
                    builder.set_exact_attributes(words.iter().cloned().collect())
                }
                Setting::Reset => builder.reset_exact_attributes(),
                Setting::NotSet => (),
            }

            match value.disable_on_numbers {
                Setting::Set(val) => builder.set_disable_on_numbers(val),
                Setting::Reset => builder.reset_disable_on_numbers(),
                Setting::NotSet => (),
            }
        }
        Setting::Reset => {
            // all typo settings need to be reset here.
            builder.reset_authorize_typos();
            builder.reset_min_word_len_one_typo();
            builder.reset_min_word_len_two_typos();
            builder.reset_exact_words();
            builder.reset_exact_attributes();
        }
        Setting::NotSet => (),
    }

    match faceting {
        Setting::Set(FacetingSettings { max_values_per_facet, sort_facet_values_by }) => {
            match max_values_per_facet {
                Setting::Set(val) => builder.set_max_values_per_facet(*val),
                Setting::Reset => builder.reset_max_values_per_facet(),
                Setting::NotSet => (),
            }
            match sort_facet_values_by {
                Setting::Set(val) => builder.set_sort_facet_values_by(
                    val.iter().map(|(name, order)| (name.clone(), (*order).into())).collect(),
                ),
                Setting::Reset => builder.reset_sort_facet_values_by(),
                Setting::NotSet => (),
            }
        }
        Setting::Reset => {
            builder.reset_max_values_per_facet();
            builder.reset_sort_facet_values_by();
        }
        Setting::NotSet => (),
    }

    match pagination {
        Setting::Set(ref value) => match value.max_total_hits {
            Setting::Set(val) => builder.set_pagination_max_total_hits(val),
            Setting::Reset => builder.reset_pagination_max_total_hits(),
            Setting::NotSet => (),
        },
        Setting::Reset => builder.reset_pagination_max_total_hits(),
        Setting::NotSet => (),
    }

    match embedders {
        Setting::Set(value) => builder.set_embedder_settings(
            value.iter().map(|(k, v)| (k.clone(), v.inner.clone())).collect(),
        ),
        Setting::Reset => builder.reset_embedder_settings(),
        Setting::NotSet => (),
    }

    match search_cutoff_ms {
        Setting::Set(cutoff) => builder.set_search_cutoff(*cutoff),
        Setting::Reset => builder.reset_search_cutoff(),
        Setting::NotSet => (),
    }

    match prefix_search {
        Setting::Set(prefix_search) => {
            builder.set_prefix_search(PrefixSearch::from(*prefix_search))
        }
        Setting::Reset => builder.reset_prefix_search(),
        Setting::NotSet => (),
    }

    match facet_search {
        Setting::Set(facet_search) => builder.set_facet_search(*facet_search),
        Setting::Reset => builder.reset_facet_search(),
        Setting::NotSet => (),
    }

    match chat {
        Setting::Set(chat) => builder.set_chat(chat.clone()),
        Setting::Reset => builder.reset_chat(),
        Setting::NotSet => (),
    }
}

pub enum SecretPolicy {
    RevealSecrets,
    HideSecrets,
}

pub fn settings(
    index_scheduler: &IndexScheduler,
    rtxn: &RoTxn,
    secret_policy: SecretPolicy,
) -> Result<Settings<Checked>, milli::Error> {
    let mut settings = index_scheduler.chat_settings(rtxn)?;
    if let SecretPolicy::HideSecrets = secret_policy {
        settings.hide_secrets()
    }
    Ok(settings)
}
