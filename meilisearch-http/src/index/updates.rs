use std::collections::{BTreeSet, HashMap};
use std::io;
use std::marker::PhantomData;
use std::num::NonZeroUsize;

use flate2::read::GzDecoder;
use log::info;
use milli::update::{IndexDocumentsMethod, UpdateBuilder, UpdateFormat};
use serde::{Deserialize, Serialize, Serializer};

use crate::index_controller::UpdateResult;

use super::{deserialize_some, Index};

fn serialize_with_wildcard<S>(field: &Option<Option<Vec<String>>>, s: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    let wildcard = vec!["*".to_string()];
    s.serialize_some(&field.as_ref().map(|o| o.as_ref().unwrap_or(&wildcard)))
}

#[derive(Clone, Default, Debug)]
pub struct Checked;
#[derive(Clone, Default, Debug)]
pub struct Unchecked;

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[serde(rename_all = "camelCase")]
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
    pub attributes_for_faceting: Option<Option<HashMap<String, String>>>,

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
    pub distinct_attribute: Option<Option<String>>,

    #[serde(skip)]
    pub _kind: PhantomData<T>,
}

impl Settings<Checked> {
    pub fn cleared() -> Settings<Checked> {
        Settings {
            displayed_attributes: Some(None),
            searchable_attributes: Some(None),
            attributes_for_faceting: Some(None),
            ranking_rules: Some(None),
            stop_words: Some(None),
            distinct_attribute: Some(None),
            _kind: PhantomData,
        }
    }

    pub fn into_unchecked(self) -> Settings<Unchecked> {
        let Self {
            displayed_attributes,
            searchable_attributes,
            attributes_for_faceting,
            ranking_rules,
            stop_words,
            distinct_attribute,
            ..
        } = self;

        Settings {
            displayed_attributes,
            searchable_attributes,
            attributes_for_faceting,
            ranking_rules,
            stop_words,
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
            attributes_for_faceting: self.attributes_for_faceting,
            ranking_rules: self.ranking_rules,
            stop_words: self.stop_words,
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
    ) -> anyhow::Result<UpdateResult> {
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
    ) -> anyhow::Result<UpdateResult> {
        info!("performing document addition");

        // Set the primary key if not set already, ignore if already set.
        if let (None, Some(ref primary_key)) = (self.primary_key(txn)?, primary_key) {
            self.put_primary_key(txn, primary_key)?;
        }

        let mut builder = update_builder.index_documents(txn, self);
        builder.update_format(format);
        builder.index_documents_method(method);

        //let indexing_callback =
        //|indexing_step, update_id| info!("update {}: {:?}", update_id, indexing_step);

        let indexing_callback = |_, _| ();

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

    pub fn clear_documents(&self, update_builder: UpdateBuilder) -> anyhow::Result<UpdateResult> {
        // We must use the write transaction of the update here.
        let mut wtxn = self.write_txn()?;
        let builder = update_builder.clear_documents(&mut wtxn, self);

        match builder.execute() {
            Ok(_count) => wtxn
                .commit()
                .and(Ok(UpdateResult::Other))
                .map_err(Into::into),
            Err(e) => Err(e),
        }
    }

    pub fn update_settings_txn<'a, 'b>(
        &'a self,
        txn: &mut heed::RwTxn<'a, 'b>,
        settings: &Settings<Checked>,
        update_builder: UpdateBuilder,
    ) -> anyhow::Result<UpdateResult> {
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

        if let Some(ref facet_types) = settings.attributes_for_faceting {
            let facet_types = facet_types.clone().unwrap_or_else(HashMap::new);
            builder.set_faceted_fields(facet_types);
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
                _ => builder.reset_stop_words(),
            }
        }

        if let Some(ref distinct_attribute) = settings.distinct_attribute {
            match distinct_attribute {
                Some(attr) => builder.set_distinct_attribute(attr.clone()),
                None => builder.reset_distinct_attribute(),
            }
        }

        builder.execute(|indexing_step, update_id| {
            info!("update {}: {:?}", update_id, indexing_step)
        })?;

        Ok(UpdateResult::Other)
    }

    pub fn update_settings(
        &self,
        settings: &Settings<Checked>,
        update_builder: UpdateBuilder,
    ) -> anyhow::Result<UpdateResult> {
        let mut txn = self.write_txn()?;
        let result = self.update_settings_txn(&mut txn, settings, update_builder)?;
        txn.commit()?;
        Ok(result)
    }

    pub fn delete_documents(
        &self,
        document_ids: Option<impl io::Read>,
        update_builder: UpdateBuilder,
    ) -> anyhow::Result<UpdateResult> {
        let ids = match document_ids {
            Some(reader) => serde_json::from_reader(reader)?,
            None => Vec::<String>::new(),
        };
        let mut txn = self.write_txn()?;
        let mut builder = update_builder.delete_documents(&mut txn, self)?;

        // We ignore unexisting document ids
        ids.iter().for_each(|id| {
            builder.delete_external_id(id);
        });

        match builder.execute() {
            Ok(deleted) => txn
                .commit()
                .and(Ok(UpdateResult::DocumentDeletion { deleted }))
                .map_err(Into::into),
            Err(e) => Err(e),
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
            attributes_for_faceting: None,
            ranking_rules: None,
            stop_words: None,
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
            attributes_for_faceting: None,
            ranking_rules: None,
            stop_words: None,
            distinct_attribute: None,
            _kind: PhantomData::<Unchecked>,
        };

        let checked = settings.check();
        assert_eq!(checked.displayed_attributes, Some(None));
        assert_eq!(checked.searchable_attributes, Some(None));
    }
}
