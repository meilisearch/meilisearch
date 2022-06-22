use jayson::DeserializeFromValue;
use log::{debug, info, trace};
use milli::documents::DocumentBatchReader;
use milli::update::{
    DocumentAdditionResult, DocumentDeletionResult, IndexDocumentsConfig, IndexDocumentsMethod,
    Setting,
};
use milli::Criterion;
use serde::{Deserialize, Serialize, Serializer};
use std::collections::{BTreeMap, BTreeSet};
use std::convert::Infallible;
use std::num::NonZeroUsize;
use uuid::Uuid;

use super::error::Result;
use super::index::{Index, IndexMeta};
use crate::update_file_store::UpdateFileStore;

fn serialize_with_wildcard<S>(
    field: &Setting<Vec<String>>,
    s: S,
) -> std::result::Result<S::Ok, S::Error>
where
    S: Serializer,
{
    let wildcard = vec!["*".to_string()];
    match field {
        Setting::Set(value) => Some(value),
        Setting::Reset => Some(&wildcard),
        Setting::NotSet => None,
    }
    .serialize(s)
}

#[cfg_attr(test, derive(proptest_derive::Arbitrary))]
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, DeserializeFromValue)]
#[serde(deny_unknown_fields)]
#[serde(rename_all = "camelCase")]
pub struct MinWordSizeTyposSetting {
    #[cfg_attr(test, proptest(strategy = "test::setting_strategy()"))]
    #[serde(default, skip_serializing_if = "Setting::is_not_set")]
    pub one_typo: Setting<u8>,
    #[cfg_attr(test, proptest(strategy = "test::setting_strategy()"))]
    #[serde(default, skip_serializing_if = "Setting::is_not_set")]
    pub two_typos: Setting<u8>,
}

#[cfg_attr(test, derive(proptest_derive::Arbitrary))]
#[derive(
    Debug, Clone, Default, Serialize, Deserialize, PartialEq, jayson::DeserializeFromValue,
)]
#[serde(deny_unknown_fields)]
#[serde(rename_all = "camelCase")]
pub struct TypoSettings {
    #[cfg_attr(test, proptest(strategy = "test::setting_strategy()"))]
    #[serde(default, skip_serializing_if = "Setting::is_not_set")]
    pub enabled: Setting<bool>,
    #[cfg_attr(test, proptest(strategy = "test::setting_strategy()"))]
    #[serde(default, skip_serializing_if = "Setting::is_not_set")]
    pub min_word_size_for_typos: Setting<MinWordSizeTyposSetting>,
    #[cfg_attr(test, proptest(strategy = "test::setting_strategy()"))]
    #[serde(default, skip_serializing_if = "Setting::is_not_set")]
    pub disable_on_words: Setting<BTreeSet<String>>,
    #[cfg_attr(test, proptest(strategy = "test::setting_strategy()"))]
    #[serde(default, skip_serializing_if = "Setting::is_not_set")]
    pub disable_on_attributes: Setting<BTreeSet<String>>,
}

#[cfg_attr(test, derive(proptest_derive::Arbitrary))]
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, DeserializeFromValue)]
#[jayson(rename_all = camelCase, deny_unknown_fields)]
#[serde(deny_unknown_fields)]
#[serde(rename_all = "camelCase")]
pub struct FacetingSettings {
    #[cfg_attr(test, proptest(strategy = "test::setting_strategy()"))]
    #[serde(default, skip_serializing_if = "Setting::is_not_set")]
    pub max_values_per_facet: Setting<usize>,
}

#[cfg_attr(test, derive(proptest_derive::Arbitrary))]
#[derive(
    Debug, Clone, Default, Serialize, Deserialize, PartialEq, jayson::DeserializeFromValue,
)]
#[jayson(rename_all = camelCase, deny_unknown_fields)]
#[serde(deny_unknown_fields)]
#[serde(rename_all = "camelCase")]
pub struct PaginationSettings {
    #[cfg_attr(test, proptest(strategy = "test::setting_strategy()"))]
    #[serde(default, skip_serializing_if = "Setting::is_not_set")]
    pub limited_to: Setting<usize>,
}

/// Holds all the settings for an index. The settings are validated by the implementation of
/// jayson::DeserializeFromValue.
#[derive(
    Debug, Clone, Default, Serialize, Deserialize, PartialEq, jayson::DeserializeFromValue,
)]
#[jayson(rename_all = camelCase, deny_unknown_fields, validate = check_settings -> Infallible)]
#[serde(deny_unknown_fields)]
#[serde(rename_all = "camelCase")]
#[cfg_attr(test, derive(proptest_derive::Arbitrary))]
pub struct Settings {
    #[serde(
        default,
        serialize_with = "serialize_with_wildcard",
        skip_serializing_if = "Setting::is_not_set"
    )]
    #[cfg_attr(test, proptest(strategy = "test::setting_strategy()"))]
    pub displayed_attributes: Setting<Vec<String>>,

    #[serde(
        default,
        serialize_with = "serialize_with_wildcard",
        skip_serializing_if = "Setting::is_not_set"
    )]
    #[cfg_attr(test, proptest(strategy = "test::setting_strategy()"))]
    pub searchable_attributes: Setting<Vec<String>>,

    #[serde(default, skip_serializing_if = "Setting::is_not_set")]
    #[cfg_attr(test, proptest(strategy = "test::setting_strategy()"))]
    pub filterable_attributes: Setting<BTreeSet<String>>,
    #[serde(default, skip_serializing_if = "Setting::is_not_set")]
    #[cfg_attr(test, proptest(strategy = "test::setting_strategy()"))]
    pub sortable_attributes: Setting<BTreeSet<String>>,
    #[serde(default, skip_serializing_if = "Setting::is_not_set")]
    #[cfg_attr(test, proptest(strategy = "test::criteria_setting_strategy()"))]
    #[jayson(needs_predicate)]
    pub ranking_rules: Setting<Vec<Criterion>>,
    #[serde(default, skip_serializing_if = "Setting::is_not_set")]
    #[cfg_attr(test, proptest(strategy = "test::setting_strategy()"))]
    pub stop_words: Setting<BTreeSet<String>>,
    #[serde(default, skip_serializing_if = "Setting::is_not_set")]
    #[cfg_attr(test, proptest(strategy = "test::setting_strategy()"))]
    pub synonyms: Setting<BTreeMap<String, Vec<String>>>,
    #[serde(default, skip_serializing_if = "Setting::is_not_set")]
    #[cfg_attr(test, proptest(strategy = "test::setting_strategy()"))]
    pub distinct_attribute: Setting<String>,
    #[serde(default, skip_serializing_if = "Setting::is_not_set")]
    #[cfg_attr(test, proptest(strategy = "test::setting_strategy()"))]
    pub typo_tolerance: Setting<TypoSettings>,
    #[serde(default, skip_serializing_if = "Setting::is_not_set")]
    #[cfg_attr(test, proptest(strategy = "test::setting_strategy()"))]
    pub faceting: Setting<FacetingSettings>,
    #[serde(default, skip_serializing_if = "Setting::is_not_set")]
    #[cfg_attr(test, proptest(strategy = "test::setting_strategy()"))]
    pub pagination: Setting<PaginationSettings>,
}

impl Settings {
    pub fn cleared() -> Settings {
        Settings {
            displayed_attributes: Setting::Reset,
            searchable_attributes: Setting::Reset,
            filterable_attributes: Setting::Reset,
            sortable_attributes: Setting::Reset,
            ranking_rules: Setting::Reset,
            stop_words: Setting::Reset,
            synonyms: Setting::Reset,
            distinct_attribute: Setting::Reset,
            typo_tolerance: Setting::Reset,
            faceting: Setting::Reset,
            pagination: Setting::Reset,
        }
    }
}
fn check_settings(settings: Settings) -> std::result::Result<Settings, Infallible> {
    let Settings {
        displayed_attributes,
        searchable_attributes,
        filterable_attributes,
        sortable_attributes,
        ranking_rules,
        stop_words,
        synonyms,
        distinct_attribute,
        typo_tolerance,
        faceting,
        pagination,
    } = settings;

    let displayed_attributes = match displayed_attributes {
        Setting::Set(fields) => {
            if fields.iter().any(|f| f == "*") {
                Setting::Reset
            } else {
                Setting::Set(fields)
            }
        }
        otherwise => otherwise,
    };

    let searchable_attributes = match searchable_attributes {
        Setting::Set(fields) => {
            if fields.iter().any(|f| f == "*") {
                Setting::Reset
            } else {
                Setting::Set(fields)
            }
        }
        otherwise => otherwise,
    };

    Ok(Settings {
        displayed_attributes,
        searchable_attributes,
        filterable_attributes,
        sortable_attributes,
        ranking_rules,
        stop_words,
        synonyms,
        distinct_attribute,
        typo_tolerance,
        faceting,
        pagination,
    })
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[serde(rename_all = "camelCase")]
pub struct Facets {
    pub level_group_size: Option<NonZeroUsize>,
    pub min_level_size: Option<NonZeroUsize>,
}

impl Index {
    fn update_primary_key_txn<'a, 'b>(
        &'a self,
        txn: &mut milli::heed::RwTxn<'a, 'b>,
        primary_key: String,
    ) -> Result<IndexMeta> {
        let mut builder = milli::update::Settings::new(txn, self, self.indexer_config.as_ref());
        builder.set_primary_key(primary_key);
        builder.execute(|_| ())?;
        let meta = IndexMeta::new_txn(self, txn)?;

        Ok(meta)
    }

    pub fn update_primary_key(&self, primary_key: String) -> Result<IndexMeta> {
        let mut txn = self.write_txn()?;
        let res = self.update_primary_key_txn(&mut txn, primary_key)?;
        txn.commit()?;

        Ok(res)
    }

    /// Deletes `ids` from the index, and returns how many documents were deleted.
    pub fn delete_documents(&self, ids: &[String]) -> Result<DocumentDeletionResult> {
        let mut txn = self.write_txn()?;
        let mut builder = milli::update::DeleteDocuments::new(&mut txn, self)?;

        // We ignore unexisting document ids
        ids.iter().for_each(|id| {
            builder.delete_external_id(id);
        });

        let deleted = builder.execute()?;

        txn.commit()?;

        Ok(deleted)
    }

    pub fn clear_documents(&self) -> Result<()> {
        let mut txn = self.write_txn()?;
        milli::update::ClearDocuments::new(&mut txn, self).execute()?;
        txn.commit()?;

        Ok(())
    }

    pub fn update_documents(
        &self,
        method: IndexDocumentsMethod,
        primary_key: Option<String>,
        file_store: UpdateFileStore,
        contents: impl IntoIterator<Item = Uuid>,
    ) -> Result<DocumentAdditionResult> {
        trace!("performing document addition");
        let mut txn = self.write_txn()?;

        if let Some(primary_key) = primary_key {
            if self.primary_key(&txn)?.is_none() {
                self.update_primary_key_txn(&mut txn, primary_key)?;
            }
        }

        let config = IndexDocumentsConfig {
            update_method: method,
            ..Default::default()
        };

        let indexing_callback = |indexing_step| debug!("update: {:?}", indexing_step);
        let mut builder = milli::update::IndexDocuments::new(
            &mut txn,
            self,
            self.indexer_config.as_ref(),
            config,
            indexing_callback,
        )?;

        for content_uuid in contents.into_iter() {
            let content_file = file_store.get_update(content_uuid)?;
            let reader = DocumentBatchReader::from_reader(content_file)?;
            builder.add_documents(reader)?;
        }

        let addition = builder.execute()?;

        txn.commit()?;

        info!("document addition done: {:?}", addition);

        Ok(addition)
    }

    pub fn update_settings(&self, settings: &Settings) -> Result<()> {
        // We must use the write transaction of the update here.
        let mut txn = self.write_txn()?;
        let mut builder =
            milli::update::Settings::new(&mut txn, self, self.indexer_config.as_ref());

        apply_settings_to_builder(settings, &mut builder);

        builder.execute(|indexing_step| debug!("update: {:?}", indexing_step))?;

        txn.commit()?;

        Ok(())
    }
}

pub fn apply_settings_to_builder(settings: &Settings, builder: &mut milli::update::Settings) {
    match settings.searchable_attributes {
        Setting::Set(ref names) => builder.set_searchable_fields(names.clone()),
        Setting::Reset => builder.reset_searchable_fields(),
        Setting::NotSet => (),
    }

    match settings.displayed_attributes {
        Setting::Set(ref names) => builder.set_displayed_fields(names.clone()),
        Setting::Reset => builder.reset_displayed_fields(),
        Setting::NotSet => (),
    }

    match settings.filterable_attributes {
        Setting::Set(ref facets) => {
            builder.set_filterable_fields(facets.clone().into_iter().collect())
        }
        Setting::Reset => builder.reset_filterable_fields(),
        Setting::NotSet => (),
    }

    match settings.sortable_attributes {
        Setting::Set(ref fields) => builder.set_sortable_fields(fields.iter().cloned().collect()),
        Setting::Reset => builder.reset_sortable_fields(),
        Setting::NotSet => (),
    }

    match settings.ranking_rules {
        Setting::Set(ref criteria) => builder.set_criteria(criteria.clone()),
        Setting::Reset => builder.reset_criteria(),
        Setting::NotSet => (),
    }

    match settings.stop_words {
        Setting::Set(ref stop_words) => builder.set_stop_words(stop_words.clone()),
        Setting::Reset => builder.reset_stop_words(),
        Setting::NotSet => (),
    }

    match settings.synonyms {
        Setting::Set(ref synonyms) => builder.set_synonyms(synonyms.clone().into_iter().collect()),
        Setting::Reset => builder.reset_synonyms(),
        Setting::NotSet => (),
    }

    match settings.distinct_attribute {
        Setting::Set(ref attr) => builder.set_distinct_field(attr.clone()),
        Setting::Reset => builder.reset_distinct_field(),
        Setting::NotSet => (),
    }

    match settings.typo_tolerance {
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

    match settings.faceting {
        Setting::Set(ref value) => match value.max_values_per_facet {
            Setting::Set(val) => builder.set_max_values_per_facet(val),
            Setting::Reset => builder.reset_max_values_per_facet(),
            Setting::NotSet => (),
        },
        Setting::Reset => builder.reset_max_values_per_facet(),
        Setting::NotSet => (),
    }

    match settings.pagination {
        Setting::Set(ref value) => match value.limited_to {
            Setting::Set(val) => builder.set_pagination_limited_to(val),
            Setting::Reset => builder.reset_pagination_limited_to(),
            Setting::NotSet => (),
        },
        Setting::Reset => builder.reset_pagination_limited_to(),
        Setting::NotSet => (),
    }
}

#[cfg(test)]
pub(crate) mod test {
    use super::*;
    use meilisearch_types::error::MeiliDeserError;
    use milli::Criterion;
    use proptest::prelude::*;
    use serde_json::json;

    fn criteria_strategy() -> impl Strategy<Value = Vec<Criterion>> {
        proptest::collection::vec(
            prop_oneof![
                Just(Criterion::Words),
                Just(Criterion::Typo),
                Just(Criterion::Proximity),
                Just(Criterion::Attribute),
                Just(Criterion::Sort),
                Just(Criterion::Exactness),
                any::<String>().prop_map(Criterion::Asc),
                any::<String>().prop_map(Criterion::Desc),
            ],
            0..100,
        )
    }

    pub(super) fn criteria_setting_strategy() -> impl Strategy<Value = Setting<Vec<Criterion>>> {
        prop_oneof![
            Just(Setting::NotSet),
            Just(Setting::Reset),
            criteria_strategy().prop_map(Setting::Set),
        ]
    }
    pub(super) fn setting_strategy<T: Arbitrary + Clone>() -> impl Strategy<Value = Setting<T>> {
        prop_oneof![
            Just(Setting::NotSet),
            Just(Setting::Reset),
            any::<T>().prop_map(Setting::Set)
        ]
    }

    #[test]
    fn test_setting_check() {
        let j = json!({
            "filterableAttributes": ["a", "b"],
            "searchableAttributes": ["*", "b"],
        });
        let settings: Settings = jayson::deserialize::<_, _, MeiliDeserError>(j).unwrap();
        assert_eq!(
            settings.filterable_attributes,
            Setting::Set([String::from("a"), String::from("b")].into_iter().collect())
        );
        assert_eq!(settings.searchable_attributes, Setting::Reset);

        let j = json!({
            "displayedAttributes": ["c", "*"],
        });
        let settings: Settings = jayson::deserialize::<_, _, MeiliDeserError>(j).unwrap();
        assert_eq!(settings.displayed_attributes, Setting::Reset);

        let j = json!({
            "filterableAttributes": ["*"],
        });
        let settings: Settings = jayson::deserialize::<_, _, MeiliDeserError>(j).unwrap();
        assert_eq!(
            settings.filterable_attributes,
            Setting::Set([String::from("*")].into_iter().collect())
        );
    }
}
