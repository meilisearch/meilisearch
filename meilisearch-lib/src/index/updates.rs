use std::collections::{BTreeMap, BTreeSet};
use std::marker::PhantomData;
use std::num::NonZeroUsize;

use log::{debug, info, trace};
use milli::documents::DocumentBatchReader;
use milli::update::{
    DocumentAdditionResult, DocumentDeletionResult, IndexDocumentsMethod, Setting,
};
use serde::{Deserialize, Serialize, Serializer};
use uuid::Uuid;

use super::error::Result;
use super::index::{Index, IndexMeta};

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

/// Holds all the settings for an index. `T` can either be `Checked` if they represents settings
/// whose validity is guaranteed, or `Unchecked` if they need to be validated. In the later case, a
/// call to `check` will return a `Settings<Checked>` from a `Settings<Unchecked>`.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
#[serde(rename_all = "camelCase")]
#[serde(bound(serialize = "T: Serialize", deserialize = "T: Deserialize<'static>"))]
pub struct Settings<T> {
    #[serde(
        default,
        serialize_with = "serialize_with_wildcard",
        skip_serializing_if = "Setting::is_not_set"
    )]
    pub displayed_attributes: Setting<Vec<String>>,

    #[serde(
        default,
        serialize_with = "serialize_with_wildcard",
        skip_serializing_if = "Setting::is_not_set"
    )]
    pub searchable_attributes: Setting<Vec<String>>,

    #[serde(default, skip_serializing_if = "Setting::is_not_set")]
    pub filterable_attributes: Setting<BTreeSet<String>>,
    #[serde(default, skip_serializing_if = "Setting::is_not_set")]
    pub sortable_attributes: Setting<BTreeSet<String>>,
    #[serde(default, skip_serializing_if = "Setting::is_not_set")]
    pub ranking_rules: Setting<Vec<String>>,
    #[serde(default, skip_serializing_if = "Setting::is_not_set")]
    pub stop_words: Setting<BTreeSet<String>>,
    #[serde(default, skip_serializing_if = "Setting::is_not_set")]
    pub synonyms: Setting<BTreeMap<String, Vec<String>>>,
    #[serde(default, skip_serializing_if = "Setting::is_not_set")]
    pub distinct_attribute: Setting<String>,

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
        txn: &mut heed::RwTxn<'a, 'b>,
        primary_key: String,
    ) -> Result<IndexMeta> {
        let mut builder = self.update_handler.update_builder().settings(txn, self);
        builder.set_primary_key(primary_key);
        builder.execute(|_| ())?;
        let meta = IndexMeta::new_txn(self, &txn)?;

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
        let mut builder = self
            .update_handler
            .update_builder()
            .delete_documents(&mut txn, self)?;

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
        self.update_handler
            .update_builder()
            .clear_documents(&mut txn, self)
            .execute()?;

        txn.commit()?;

        Ok(())
    }

    pub fn update_documents(
        &self,
        method: IndexDocumentsMethod,
        content_uuid: Uuid,
        primary_key: Option<String>,
    ) -> Result<DocumentAdditionResult> {
        trace!("performing document addition");
        let mut txn = self.write_txn()?;

        if let Some(primary_key) = primary_key {
            self.update_primary_key_txn(&mut txn, primary_key)?;
        }

        let indexing_callback = |indexing_step| debug!("update: {:?}", indexing_step);

        let content_file = self.update_file_store.get_update(content_uuid).unwrap();
        let reader = DocumentBatchReader::from_reader(content_file).unwrap();

        let mut builder = self
            .update_handler
            .update_builder()
            .index_documents(&mut txn, self);
        builder.index_documents_method(method);
        let addition = builder.execute(reader, indexing_callback)?;

        txn.commit()?;

        info!("document addition done: {:?}", addition);

        Ok(addition)
    }

    pub fn update_settings(&self, settings: &Settings<Checked>) -> Result<()> {
        // We must use the write transaction of the update here.
        let mut txn = self.write_txn()?;
        let mut builder = self
            .update_handler
            .update_builder()
            .settings(&mut txn, self);

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
}

#[cfg(test)]
mod test {
    use quickcheck::Arbitrary;

    use super::*;

    #[derive(Clone)]
    struct ArbitrarySetting<T>(Setting<T>);

    impl<T: Arbitrary> ArbitrarySetting<T> {
        fn into_inner(self) -> Setting<T> {
            self.0
        }
    }

    impl<T: Arbitrary> Arbitrary for ArbitrarySetting<T> {
        fn arbitrary(g: &mut quickcheck::Gen) -> Self {
            let rand = g.choose(&[1, 2, 3]).unwrap();
            match rand {
                1 => Self(Setting::Set(T::arbitrary(g))),
                2 => Self(Setting::Reset),
                3 => Self(Setting::NotSet),
                _ => unreachable!(),
            }
        }
    }

    impl<T: Clone + 'static> Arbitrary for Settings<T> {
        fn arbitrary(g: &mut quickcheck::Gen) -> Self {
            Settings {
                displayed_attributes: ArbitrarySetting::arbitrary(g).into_inner(),
                searchable_attributes: ArbitrarySetting::arbitrary(g).into_inner(),
                filterable_attributes: ArbitrarySetting::arbitrary(g).into_inner(),
                sortable_attributes: ArbitrarySetting::arbitrary(g).into_inner(),
                ranking_rules: ArbitrarySetting::arbitrary(g).into_inner(),
                stop_words: ArbitrarySetting::arbitrary(g).into_inner(),
                synonyms: ArbitrarySetting::arbitrary(g).into_inner(),
                distinct_attribute: ArbitrarySetting::arbitrary(g).into_inner(),
                _kind: PhantomData,
            }
        }
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
            _kind: PhantomData::<Unchecked>,
        };

        let checked = settings.check();
        assert_eq!(checked.displayed_attributes, Setting::Reset);
        assert_eq!(checked.searchable_attributes, Setting::Reset);
    }
}
