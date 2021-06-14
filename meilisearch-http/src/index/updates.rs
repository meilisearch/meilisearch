use std::collections::{BTreeSet, BTreeMap, HashSet};
use std::io;
use std::marker::PhantomData;
use std::num::NonZeroUsize;

use flate2::read::GzDecoder;
use log::info;
use milli::update::{IndexDocumentsMethod, UpdateBuilder, UpdateFormat};
use serde::{Deserialize, Serialize, Serializer};

use crate::index::error::IndexError;
use crate::index_controller::UpdateResult;

use super::error::Result;
use super::{deserialize_some, Index};

fn serialize_with_wildcard<S>(
    field: &Option<Option<Vec<String>>>,
    s: S,
) -> std::result::Result<S::Ok, S::Error>
where
    S: Serializer,
{
    let wildcard = vec!["*".to_string()];
    s.serialize_some(&field.as_ref().map(|o| o.as_ref().unwrap_or(&wildcard)))
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
        deserialize_with = "deserialize_some",
        serialize_with = "serialize_with_wildcard",
        skip_serializing_if = "Option::is_none"
    )]
    pub displayed_attributes: Option<Option<Vec<String>>>,

    #[serde(
        default,
        deserialize_with = "deserialize_some",
        serialize_with = "serialize_with_wildcard",
        skip_serializing_if = "Option::is_none"
    )]
    pub searchable_attributes: Option<Option<Vec<String>>>,

    #[serde(
        default,
        deserialize_with = "deserialize_some",
        skip_serializing_if = "Option::is_none"
    )]
    pub filterable_attributes: Option<Option<HashSet<String>>>,

    #[serde(
        default,
        deserialize_with = "deserialize_some",
        skip_serializing_if = "Option::is_none"
    )]
    pub ranking_rules: Option<Option<Vec<String>>>,
    #[serde(
        default,
        deserialize_with = "deserialize_some",
        skip_serializing_if = "Option::is_none"
    )]
    pub stop_words: Option<Option<BTreeSet<String>>>,
    #[serde(
        default,
        deserialize_with = "deserialize_some",
        skip_serializing_if = "Option::is_none"
    )]
    pub synonyms: Option<Option<BTreeMap<String, Vec<String>>>>,
    #[serde(
        default,
        deserialize_with = "deserialize_some",
        skip_serializing_if = "Option::is_none"
    )]
    pub distinct_attribute: Option<Option<String>>,

    #[serde(skip)]
    pub _kind: PhantomData<T>,
}

impl Settings<Checked> {
    pub fn cleared() -> Settings<Checked> {
        Settings {
            displayed_attributes: Some(None),
            searchable_attributes: Some(None),
            filterable_attributes: Some(None),
            ranking_rules: Some(None),
            stop_words: Some(None),
            synonyms: Some(None),
            distinct_attribute: Some(None),
            _kind: PhantomData,
        }
    }

    pub fn into_unchecked(self) -> Settings<Unchecked> {
        let Self {
            displayed_attributes,
            searchable_attributes,
            filterable_attributes,
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
            ranking_rules,
            stop_words,
            synonyms,
            distinct_attribute,
            _kind: PhantomData,
        }
    }
}

impl Settings<Unchecked> {
    pub fn check(mut self) -> Settings<Checked> {
        let displayed_attributes = match self.displayed_attributes.take() {
            Some(Some(fields)) => {
                if fields.iter().any(|f| f == "*") {
                    Some(None)
                } else {
                    Some(Some(fields))
                }
            }
            otherwise => otherwise,
        };

        let searchable_attributes = match self.searchable_attributes.take() {
            Some(Some(fields)) => {
                if fields.iter().any(|f| f == "*") {
                    Some(None)
                } else {
                    Some(Some(fields))
                }
            }
            otherwise => otherwise,
        };

        Settings {
            displayed_attributes,
            searchable_attributes,
            filterable_attributes: self.filterable_attributes,
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
        info!("performing document addition");

        // Set the primary key if not set already, ignore if already set.
        if let (None, Some(primary_key)) = (self.primary_key(txn)?, primary_key) {
            let mut builder = UpdateBuilder::new(0)
                .settings(txn, &self);
            builder.set_primary_key(primary_key.to_string());
            builder.execute(|_, _| ())
                .map_err(|e| IndexError::Internal(Box::new(e)))?;
        }

        let mut builder = update_builder.index_documents(txn, self);
        builder.update_format(format);
        builder.index_documents_method(method);

        let indexing_callback =
            |indexing_step, update_id| info!("update {}: {:?}", update_id, indexing_step);

        let gzipped = false;
        let addition = match content {
            Some(content) if gzipped => builder
                .execute(GzDecoder::new(content), indexing_callback)
                .map_err(|e| IndexError::Internal(e.into()))?,
            Some(content) => builder
                .execute(content, indexing_callback)
                .map_err(|e| IndexError::Internal(e.into()))?,
            None => builder
                .execute(std::io::empty(), indexing_callback)
                .map_err(|e| IndexError::Internal(e.into()))?,
        };

        info!("document addition done: {:?}", addition);

        Ok(UpdateResult::DocumentsAddition(addition))
    }

    pub fn clear_documents(&self, update_builder: UpdateBuilder) -> Result<UpdateResult> {
        // We must use the write transaction of the update here.
        let mut wtxn = self.write_txn()?;
        let builder = update_builder.clear_documents(&mut wtxn, self);

        match builder.execute() {
            Ok(_count) => wtxn
                .commit()
                .and(Ok(UpdateResult::Other))
                .map_err(Into::into),
            Err(e) => Err(IndexError::Internal(Box::new(e))),
        }
    }

    pub fn update_settings_txn<'a, 'b>(
        &'a self,
        txn: &mut heed::RwTxn<'a, 'b>,
        settings: &Settings<Checked>,
        update_builder: UpdateBuilder,
    ) -> Result<UpdateResult> {
        // We must use the write transaction of the update here.
        let mut builder = update_builder.settings(txn, self);

        if let Some(ref names) = settings.searchable_attributes {
            match names {
                Some(names) => builder.set_searchable_fields(names.clone()),
                None => builder.reset_searchable_fields(),
            }
        }

        if let Some(ref names) = settings.displayed_attributes {
            match names {
                Some(names) => builder.set_displayed_fields(names.clone()),
                None => builder.reset_displayed_fields(),
            }
        }

        if let Some(ref facet_types) = settings.filterable_attributes {
            let facet_types = facet_types.clone().unwrap_or_else(HashSet::new);
            builder.set_filterable_fields(facet_types);
        }

        if let Some(ref criteria) = settings.ranking_rules {
            match criteria {
                Some(criteria) => builder.set_criteria(criteria.clone()),
                None => builder.reset_criteria(),
            }
        }

        if let Some(ref stop_words) = settings.stop_words {
            match stop_words {
                Some(stop_words) => builder.set_stop_words(stop_words.clone()),
                None => builder.reset_stop_words(),
            }
        }

        if let Some(ref synonyms) = settings.synonyms {
            match synonyms {
                Some(synonyms) => builder.set_synonyms(synonyms.clone().into_iter().collect()),
                None => builder.reset_synonyms(),
            }
        }

        if let Some(ref distinct_attribute) = settings.distinct_attribute {
            match distinct_attribute {
                Some(attr) => builder.set_distinct_field(attr.clone()),
                None => builder.reset_distinct_field(),
            }
        }

        builder.execute(|indexing_step, update_id| {
            info!("update {}: {:?}", update_id, indexing_step)
        })
        .map_err(|e| IndexError::Internal(e.into()))?;

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
        let mut builder = update_builder.delete_documents(&mut txn, self)
            .map_err(|e| IndexError::Internal(e.into()))?;

        // We ignore unexisting document ids
        document_ids.iter().for_each(|id| {
            builder.delete_external_id(id);
        });

        match builder.execute() {
            Ok(deleted) => txn
                .commit()
                .and(Ok(UpdateResult::DocumentDeletion { deleted }))
                .map_err(Into::into),
            Err(e) => Err(IndexError::Internal(Box::new(e))),
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_setting_check() {
        // test no changes
        let settings = Settings {
            displayed_attributes: Some(Some(vec![String::from("hello")])),
            searchable_attributes: Some(Some(vec![String::from("hello")])),
            filterable_attributes: None,
            ranking_rules: None,
            stop_words: None,
            synonyms: None,
            distinct_attribute: None,
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
            displayed_attributes: Some(Some(vec![String::from("*")])),
            searchable_attributes: Some(Some(vec![String::from("hello"), String::from("*")])),
            filterable_attributes: None,
            ranking_rules: None,
            stop_words: None,
            synonyms: None,
            distinct_attribute: None,
            _kind: PhantomData::<Unchecked>,
        };

        let checked = settings.check();
        assert_eq!(checked.displayed_attributes, Some(None));
        assert_eq!(checked.searchable_attributes, Some(None));
    }
}
