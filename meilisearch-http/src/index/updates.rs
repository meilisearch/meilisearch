use std::collections::{BTreeSet, HashMap};
use std::io;
use std::num::NonZeroUsize;
use std::marker::PhantomData;

use flate2::read::GzDecoder;
use log::info;
use milli::update::{IndexDocumentsMethod, UpdateBuilder, UpdateFormat};
use serde::{de::Deserializer, Deserialize, Serialize};

use super::Index;
use crate::index_controller::UpdateResult;

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
        skip_serializing_if = "Option::is_none"
    )]
    pub displayed_attributes: Option<Option<Vec<String>>>,

    #[serde(
        default,
        deserialize_with = "deserialize_some",
        skip_serializing_if = "Option::is_none"
    )]
    pub searchable_attributes: Option<Option<Vec<String>>>,

    #[serde(default)]
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
}

impl Settings<Unchecked> {
    pub fn check(self) -> Settings<Checked> {
        todo!()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[serde(rename_all = "camelCase")]
pub struct Facets {
    pub level_group_size: Option<NonZeroUsize>,
    pub min_level_size: Option<NonZeroUsize>,
}

fn deserialize_some<'de, T, D>(deserializer: D) -> Result<Option<T>, D::Error>
where
    T: Deserialize<'de>,
    D: Deserializer<'de>,
{
    Deserialize::deserialize(deserializer).map(Some)
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
        info!("performing document addition");
        // We must use the write transaction of the update here.
        let mut wtxn = self.write_txn()?;

        // Set the primary key if not set already, ignore if already set.
        if let (None, Some(ref primary_key)) = (self.primary_key(&wtxn)?, primary_key) {
            self.put_primary_key(&mut wtxn, primary_key)?;
        }

        let mut builder = update_builder.index_documents(&mut wtxn, self);
        builder.update_format(format);
        builder.index_documents_method(method);

        let indexing_callback =
            |indexing_step, update_id| info!("update {}: {:?}", update_id, indexing_step);

        let gzipped = false;
        let result = match content {
            Some(content) if gzipped => builder.execute(GzDecoder::new(content), indexing_callback),
            Some(content) => builder.execute(content, indexing_callback),
            None => builder.execute(std::io::empty(), indexing_callback),
        };

        info!("document addition done: {:?}", result);

        result.and_then(|addition_result| {
            wtxn.commit()
                .and(Ok(UpdateResult::DocumentsAddition(addition_result)))
                .map_err(Into::into)
        })
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

    pub fn update_settings(
        &self,
        settings: &Settings<Checked>,
        update_builder: UpdateBuilder,
    ) -> anyhow::Result<UpdateResult> {
        // We must use the write transaction of the update here.
        let mut wtxn = self.write_txn()?;
        let mut builder = update_builder.settings(&mut wtxn, self);

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

        let result = builder
            .execute(|indexing_step, update_id| info!("update {}: {:?}", update_id, indexing_step));

        match result {
            Ok(()) => wtxn
                .commit()
                .and(Ok(UpdateResult::Other))
                .map_err(Into::into),
            Err(e) => Err(e),
        }
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
