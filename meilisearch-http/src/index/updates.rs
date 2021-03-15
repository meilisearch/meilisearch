use std::collections::HashMap;
use std::io;
use std::num::NonZeroUsize;

use flate2::read::GzDecoder;
use log::info;
use milli::update::{UpdateFormat, IndexDocumentsMethod, UpdateBuilder, DocumentAdditionResult};
use serde::{Serialize, Deserialize, de::Deserializer};

use super::Index;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum UpdateResult {
    DocumentsAddition(DocumentAdditionResult),
    DocumentDeletion { deleted: u64 },
    Other,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[serde(rename_all = "camelCase")]
pub struct Settings {
    #[serde(
        default,
        deserialize_with = "deserialize_some",
        skip_serializing_if = "Option::is_none",
    )]
    pub displayed_attributes: Option<Option<Vec<String>>>,

    #[serde(
        default,
        deserialize_with = "deserialize_some",
        skip_serializing_if = "Option::is_none",
    )]
    pub searchable_attributes: Option<Option<Vec<String>>>,

    #[serde(default)]
    pub attributes_for_faceting: Option<Option<HashMap<String, String>>>,

    #[serde(
        default,
        deserialize_with = "deserialize_some",
        skip_serializing_if = "Option::is_none",
    )]
    pub ranking_rules: Option<Option<Vec<String>>>,
}

impl Settings {
    pub fn cleared() -> Self {
        Self {
            displayed_attributes: Some(None),
            searchable_attributes: Some(None),
            attributes_for_faceting: Some(None),
            ranking_rules: Some(None),
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

fn deserialize_some<'de, T, D>(deserializer: D) -> Result<Option<T>, D::Error>
where T: Deserialize<'de>,
      D: Deserializer<'de>
{
    Deserialize::deserialize(deserializer).map(Some)
}

impl Index {
    pub fn update_documents(
        &self,
        format: UpdateFormat,
        method: IndexDocumentsMethod,
        content: impl io::Read,
        update_builder: UpdateBuilder,
        primary_key: Option<&str>,
    ) -> anyhow::Result<UpdateResult> {
        info!("performing document addition");
        // We must use the write transaction of the update here.
        let mut wtxn = self.write_txn()?;

        // Set the primary key if not set already, ignore if already set.
        match (self.primary_key(&wtxn)?, primary_key) {
            (None, Some(ref primary_key)) => {
                self.put_primary_key(&mut wtxn, primary_key)?;
            }
            _ => (),
        }

        let mut builder = update_builder.index_documents(&mut wtxn, self);
        builder.update_format(format);
        builder.index_documents_method(method);

        let gzipped = false;
        let reader = if gzipped {
            Box::new(GzDecoder::new(content))
        } else {
            Box::new(content) as Box<dyn io::Read>
        };

        let result = builder.execute(reader, |indexing_step, update_id| {
            info!("update {}: {:?}", update_id, indexing_step)
        });

        info!("document addition done: {:?}", result);

        match result {
            Ok(addition_result) => wtxn
                .commit()
                .and(Ok(UpdateResult::DocumentsAddition(addition_result)))
                .map_err(Into::into),
            Err(e) => Err(e.into()),
        }
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
            Err(e) => Err(e.into()),
        }
    }

    pub fn update_settings(
        &self,
        settings: &Settings,
        update_builder: UpdateBuilder,
    ) -> anyhow::Result<UpdateResult> {
        // We must use the write transaction of the update here.
        let mut wtxn = self.write_txn()?;
        let mut builder = update_builder.settings(&mut wtxn, self);

        // We transpose the settings JSON struct into a real setting update.
        if let Some(ref names) = settings.searchable_attributes {
            match names {
                Some(names) => builder.set_searchable_fields(names.clone()),
                None => builder.reset_searchable_fields(),
            }
        }

        // We transpose the settings JSON struct into a real setting update.
        if let Some(ref names) = settings.displayed_attributes {
            match names {
                Some(names) => builder.set_displayed_fields(names.clone()),
                None => builder.reset_displayed_fields(),
            }
        }

        // We transpose the settings JSON struct into a real setting update.
        if let Some(ref facet_types) = settings.attributes_for_faceting {
            let facet_types = facet_types.clone().unwrap_or_else(|| HashMap::new());
            builder.set_faceted_fields(facet_types);
        }

        // We transpose the settings JSON struct into a real setting update.
        if let Some(ref criteria) = settings.ranking_rules {
            match criteria {
                Some(criteria) => builder.set_criteria(criteria.clone()),
                None => builder.reset_criteria(),
            }
        }

        let result = builder
            .execute(|indexing_step, update_id| info!("update {}: {:?}", update_id, indexing_step));

        match result {
            Ok(()) => wtxn
                .commit()
                .and(Ok(UpdateResult::Other))
                .map_err(Into::into),
            Err(e) => Err(e.into()),
        }
    }

    pub fn update_facets(
        &self,
        levels: &Facets,
        update_builder: UpdateBuilder,
    ) -> anyhow::Result<UpdateResult> {
        // We must use the write transaction of the update here.
        let mut wtxn = self.write_txn()?;
        let mut builder = update_builder.facets(&mut wtxn, self);
        if let Some(value) = levels.level_group_size {
            builder.level_group_size(value);
        }
        if let Some(value) = levels.min_level_size {
            builder.min_level_size(value);
        }
        match builder.execute() {
            Ok(()) => wtxn
                .commit()
                .and(Ok(UpdateResult::Other))
                .map_err(Into::into),
            Err(e) => Err(e.into()),
        }
    }

    pub fn delete_documents(
        &self,
        document_ids: impl io::Read,
        update_builder: UpdateBuilder,
    ) -> anyhow::Result<UpdateResult> {
        let ids: Vec<String> = serde_json::from_reader(document_ids)?;
        let mut txn = self.write_txn()?;
        let mut builder = update_builder.delete_documents(&mut txn, self)?;

        // We ignore unexisting document ids
        ids.iter().for_each(|id| { builder.delete_external_id(id); });

        match builder.execute() {
            Ok(deleted) => txn
                .commit()
                .and(Ok(UpdateResult::DocumentDeletion { deleted }))
                .map_err(Into::into),
            Err(e) => Err(e.into())
        }
    }
}
