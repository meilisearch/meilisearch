use std::collections::{BTreeMap, BTreeSet};
use std::marker::PhantomData;
use std::num::NonZeroUsize;

use log::{debug, info, trace};
use milli::documents::DocumentBatchReader;
use milli::update::{
    DocumentAdditionResult, DocumentDeletionResult, IndexDocumentsConfig, IndexDocumentsMethod,
    Setting,
};
use serde::{Deserialize, Serialize, Serializer};
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

#[derive(Clone, Default, Debug, Serialize, PartialEq)]
pub struct Checked;

#[derive(Clone, Default, Debug, Serialize, Deserialize, PartialEq)]
pub struct Unchecked;

#[cfg_attr(test, derive(proptest_derive::Arbitrary))]
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
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
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
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
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
#[serde(rename_all = "camelCase")]
pub struct FacetingSettings {
    #[cfg_attr(test, proptest(strategy = "test::setting_strategy()"))]
    #[serde(default, skip_serializing_if = "Setting::is_not_set")]
    pub max_values_per_facet: Setting<usize>,
}

#[cfg_attr(test, derive(proptest_derive::Arbitrary))]
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
#[serde(rename_all = "camelCase")]
pub struct PaginationSettings {
    #[cfg_attr(test, proptest(strategy = "test::setting_strategy()"))]
    #[serde(default, skip_serializing_if = "Setting::is_not_set")]
    pub max_total_hits: Setting<usize>,
}

/// Holds all the settings for an index. `T` can either be `Checked` if they represents settings
/// whose validity is guaranteed, or `Unchecked` if they need to be validated. In the later case, a
/// call to `check` will return a `Settings<Checked>` from a `Settings<Unchecked>`.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
#[serde(rename_all = "camelCase")]
#[serde(bound(serialize = "T: Serialize", deserialize = "T: Deserialize<'static>"))]
#[cfg_attr(test, derive(proptest_derive::Arbitrary))]
pub struct Settings<T> {
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
    #[cfg_attr(test, proptest(strategy = "test::setting_strategy()"))]
    pub ranking_rules: Setting<Vec<String>>,
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

    #[serde(skip)]
    pub _kind: PhantomData<T>,
}

impl Settings<Checked> {
    pub fn cleared() -> Settings<Checked> {
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
            _kind: PhantomData,
        }
    }

    pub fn into_unchecked(self) -> Settings<Unchecked> {
        let Self {
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
            ..
        } = self;

        Settings {
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
            _kind: PhantomData,
        }
    }
}

impl Settings<Unchecked> {
    pub fn check(self) -> Settings<Checked> {
        let displayed_attributes = match self.displayed_attributes {
            Setting::Set(fields) => {
                if fields.iter().any(|f| f == "*") {
                    Setting::Reset
                } else {
                    Setting::Set(fields)
                }
            }
            otherwise => otherwise,
        };

        let searchable_attributes = match self.searchable_attributes {
            Setting::Set(fields) => {
                if fields.iter().any(|f| f == "*") {
                    Setting::Reset
                } else {
                    Setting::Set(fields)
                }
            }
            otherwise => otherwise,
        };

        Settings {
            displayed_attributes,
            searchable_attributes,
            filterable_attributes: self.filterable_attributes,
            sortable_attributes: self.sortable_attributes,
            ranking_rules: self.ranking_rules,
            stop_words: self.stop_words,
            synonyms: self.synonyms,
            distinct_attribute: self.distinct_attribute,
            typo_tolerance: self.typo_tolerance,
            faceting: self.faceting,
            pagination: self.pagination,
            _kind: PhantomData,
        }
    }
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

    pub fn update_settings(&self, settings: &Settings<Checked>) -> Result<()> {
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

pub fn apply_settings_to_builder(
    settings: &Settings<Checked>,
    builder: &mut milli::update::Settings,
) {
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
        Setting::Set(ref value) => match value.max_total_hits {
            Setting::Set(val) => builder.set_pagination_max_total_hits(val),
            Setting::Reset => builder.reset_pagination_max_total_hits(),
            Setting::NotSet => (),
        },
        Setting::Reset => builder.reset_pagination_max_total_hits(),
        Setting::NotSet => (),
    }
}

#[cfg(test)]
pub(crate) mod test {
    use proptest::prelude::*;

    use super::*;

    pub(super) fn setting_strategy<T: Arbitrary + Clone>() -> impl Strategy<Value = Setting<T>> {
        prop_oneof![
            Just(Setting::NotSet),
            Just(Setting::Reset),
            any::<T>().prop_map(Setting::Set)
        ]
    }

    #[test]
    fn test_setting_check() {
        // test no changes
        let settings = Settings {
            displayed_attributes: Setting::Set(vec![String::from("hello")]),
            searchable_attributes: Setting::Set(vec![String::from("hello")]),
            filterable_attributes: Setting::NotSet,
            sortable_attributes: Setting::NotSet,
            ranking_rules: Setting::NotSet,
            stop_words: Setting::NotSet,
            synonyms: Setting::NotSet,
            distinct_attribute: Setting::NotSet,
            typo_tolerance: Setting::NotSet,
            faceting: Setting::NotSet,
            pagination: Setting::NotSet,
            _kind: PhantomData::<Unchecked>,
        };

        let checked = settings.clone().check();
        assert_eq!(settings.displayed_attributes, checked.displayed_attributes);
        assert_eq!(
            settings.searchable_attributes,
            checked.searchable_attributes
        );

        // test wildcard
        // test no changes
        let settings = Settings {
            displayed_attributes: Setting::Set(vec![String::from("*")]),
            searchable_attributes: Setting::Set(vec![String::from("hello"), String::from("*")]),
            filterable_attributes: Setting::NotSet,
            sortable_attributes: Setting::NotSet,
            ranking_rules: Setting::NotSet,
            stop_words: Setting::NotSet,
            synonyms: Setting::NotSet,
            distinct_attribute: Setting::NotSet,
            typo_tolerance: Setting::NotSet,
            faceting: Setting::NotSet,
            pagination: Setting::NotSet,
            _kind: PhantomData::<Unchecked>,
        };

        let checked = settings.check();
        assert_eq!(checked.displayed_attributes, Setting::Reset);
        assert_eq!(checked.searchable_attributes, Setting::Reset);
    }
}
