use std::collections::{BTreeMap, BTreeSet};
use std::io;
use std::marker::PhantomData;
use std::num::NonZeroUsize;

use flate2::read::GzDecoder;
use log::{debug, info, trace};
use milli::update::{IndexDocumentsMethod, Setting, UpdateBuilder, UpdateFormat};
use serde::{Deserialize, Serialize, Serializer};

use crate::index_controller::UpdateResult;

use super::error::Result;
use super::Index;

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
    pub fn update_documents(
        &self,
        format: UpdateFormat,
        method: IndexDocumentsMethod,
        content: Option<impl io::Read>,
        update_builder: UpdateBuilder,
        primary_key: Option<&str>,
    ) -> Result<UpdateResult> {
        let mut txn = self.write_txn()?;
        let result = self.update_documents_txn(
            &mut txn,
            format,
            method,
            content,
            update_builder,
            primary_key,
        )?;
        txn.commit()?;
        Ok(result)
    }

    pub fn update_documents_txn<'a, 'b>(
        &'a self,
        txn: &mut heed::RwTxn<'a, 'b>,
        format: UpdateFormat,
        method: IndexDocumentsMethod,
        content: Option<impl io::Read>,
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

        let mut builder = update_builder.index_documents(txn, self);
        builder.update_format(format);
        builder.index_documents_method(method);

        let indexing_callback =
            |indexing_step, update_id| debug!("update {}: {:?}", update_id, indexing_step);

        let gzipped = false;
        let addition = match content {
            Some(content) if gzipped => {
                builder.execute(GzDecoder::new(content), indexing_callback)?
            }
            Some(content) => builder.execute(content, indexing_callback)?,
            None => builder.execute(std::io::empty(), indexing_callback)?,
        };

        info!("document addition done: {:?}", addition);

        Ok(UpdateResult::DocumentsAddition(addition))
    }

    pub fn clear_documents(&self, update_builder: UpdateBuilder) -> Result<UpdateResult> {
        // We must use the write transaction of the update here.
        let mut wtxn = self.write_txn()?;
        let builder = update_builder.clear_documents(&mut wtxn, self);

        let _count = builder.execute()?;

        wtxn.commit()
            .and(Ok(UpdateResult::Other))
            .map_err(Into::into)
    }

    pub fn update_settings_txn<'a, 'b>(
        &'a self,
        txn: &mut heed::RwTxn<'a, 'b>,
        settings: &Settings<Checked>,
        update_builder: UpdateBuilder,
    ) -> Result<UpdateResult> {
        // We must use the write transaction of the update here.
        let mut builder = update_builder.settings(txn, self);

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
            Setting::Set(ref fields) => {
                builder.set_sortable_fields(fields.iter().cloned().collect())
            }
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
            Setting::Set(ref synonyms) => {
                builder.set_synonyms(synonyms.clone().into_iter().collect())
            }
            Setting::Reset => builder.reset_synonyms(),
            Setting::NotSet => (),
        }

        match settings.distinct_attribute {
            Setting::Set(ref attr) => builder.set_distinct_field(attr.clone()),
            Setting::Reset => builder.reset_distinct_field(),
            Setting::NotSet => (),
        }

        builder.execute(|indexing_step, update_id| {
            debug!("update {}: {:?}", update_id, indexing_step)
        })?;

        Ok(UpdateResult::Other)
    }

    pub fn update_settings(
        &self,
        settings: &Settings<Checked>,
        update_builder: UpdateBuilder,
    ) -> Result<UpdateResult> {
        let mut txn = self.write_txn()?;
        let result = self.update_settings_txn(&mut txn, settings, update_builder)?;
        txn.commit()?;
        Ok(result)
    }

    pub fn delete_documents(
        &self,
        document_ids: &[String],
        update_builder: UpdateBuilder,
    ) -> Result<UpdateResult> {
        let mut txn = self.write_txn()?;
        let mut builder = update_builder.delete_documents(&mut txn, self)?;

        // We ignore unexisting document ids
        document_ids.iter().for_each(|id| {
            builder.delete_external_id(id);
        });

        let deleted = builder.execute()?;
        txn.commit()
            .and(Ok(UpdateResult::DocumentDeletion { deleted }))
            .map_err(Into::into)
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
