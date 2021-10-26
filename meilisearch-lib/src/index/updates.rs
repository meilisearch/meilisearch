use std::collections::{BTreeMap, BTreeSet};
use std::marker::PhantomData;
use std::num::NonZeroUsize;

use log::{debug, info, trace};
use milli::documents::DocumentBatchReader;
use milli::update::{IndexDocumentsMethod, Setting, UpdateBuilder};
use serde::{Deserialize, Serialize, Serializer};
use uuid::Uuid;

use crate::index_controller::updates::status::{Failed, Processed, Processing, UpdateResult};
use crate::Update;

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

#[derive(Clone, Default, Debug, Serialize)]
pub struct Checked;

#[derive(Clone, Default, Debug, Serialize, Deserialize)]
pub struct Unchecked;

/// Holds all the settings for an index. `T` can either be `Checked` if they represents settings
/// whose validity is guaranteed, or `Unchecked` if they need to be validated. In the later case, a
/// call to `check` will return a `Settings<Checked>` from a `Settings<Unchecked>`.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
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
    pub fn handle_update(&self, update: Processing) -> std::result::Result<Processed, Failed> {
        let update_id = update.id();
        let update_builder = self.update_handler.update_builder(update_id);
        let result = (|| {
            let mut txn = self.write_txn()?;
            let result = match update.meta() {
                Update::DocumentAddition {
                    primary_key,
                    content_uuid,
                    method,
                } => self.update_documents(
                    &mut txn,
                    *method,
                    *content_uuid,
                    update_builder,
                    primary_key.as_deref(),
                ),
                Update::Settings(settings) => {
                    let settings = settings.clone().check();
                    self.update_settings(&mut txn, &settings, update_builder)
                }
                Update::ClearDocuments => {
                    let builder = update_builder.clear_documents(&mut txn, self);
                    let _count = builder.execute()?;
                    Ok(UpdateResult::Other)
                }
                Update::DeleteDocuments(ids) => {
                    let mut builder = update_builder.delete_documents(&mut txn, self)?;

                    // We ignore unexisting document ids
                    ids.iter().for_each(|id| {
                        builder.delete_external_id(id);
                    });

                    let deleted = builder.execute()?;
                    Ok(UpdateResult::DocumentDeletion { deleted })
                }
            };
            if result.is_ok() {
                txn.commit()?;
            }
            result
        })();

        if let Update::DocumentAddition { content_uuid, .. } = update.from.meta() {
            let _ = self.update_file_store.delete(*content_uuid);
        }

        match result {
            Ok(result) => Ok(update.process(result)),
            Err(e) => Err(update.fail(e)),
        }
    }

    pub fn update_primary_key(&self, primary_key: Option<String>) -> Result<IndexMeta> {
        match primary_key {
            Some(primary_key) => {
                let mut txn = self.write_txn()?;
                let mut builder = UpdateBuilder::new(0).settings(&mut txn, self);
                builder.set_primary_key(primary_key);
                builder.execute(|_, _| ())?;
                let meta = IndexMeta::new_txn(self, &txn)?;
                txn.commit()?;
                Ok(meta)
            }
            None => {
                let meta = IndexMeta::new(self)?;
                Ok(meta)
            }
        }
    }

    fn update_documents<'a, 'b>(
        &'a self,
        txn: &mut heed::RwTxn<'a, 'b>,
        method: IndexDocumentsMethod,
        content_uuid: Uuid,
        update_builder: UpdateBuilder,
        primary_key: Option<&str>,
    ) -> Result<UpdateResult> {
        trace!("performing document addition");

        // Set the primary key if not set already, ignore if already set.
        if let (None, Some(primary_key)) = (self.primary_key(txn)?, primary_key) {
            let mut builder = UpdateBuilder::new(0).settings(txn, self);
            builder.set_primary_key(primary_key.to_string());
            builder.execute(|_, _| ())?;
        }

        let indexing_callback =
            |indexing_step, update_id| debug!("update {}: {:?}", update_id, indexing_step);

        let content_file = self.update_file_store.get_update(content_uuid).unwrap();
        let reader = DocumentBatchReader::from_reader(content_file).unwrap();

        let mut builder = update_builder.index_documents(txn, self);
        builder.index_documents_method(method);
        let addition = builder.execute(reader, indexing_callback)?;

        info!("document addition done: {:?}", addition);

        Ok(UpdateResult::DocumentsAddition(addition))
    }

    fn update_settings<'a, 'b>(
        &'a self,
        txn: &mut heed::RwTxn<'a, 'b>,
        settings: &Settings<Checked>,
        update_builder: UpdateBuilder,
    ) -> Result<UpdateResult> {
        // We must use the write transaction of the update here.
        let mut builder = update_builder.settings(txn, self);

        apply_settings_to_builder(settings, &mut builder);

        builder.execute(|indexing_step, update_id| {
            debug!("update {}: {:?}", update_id, indexing_step)
        })?;

        Ok(UpdateResult::Other)
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
    use super::*;

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
