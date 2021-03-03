pub mod actor_index_controller;
mod updates;

use std::collections::HashMap;
use std::num::NonZeroUsize;

use anyhow::Result;
use chrono::{DateTime, Utc};
use milli::update::{IndexDocumentsMethod, UpdateFormat, DocumentAdditionResult};
use serde::{Serialize, Deserialize, de::Deserializer};
use uuid::Uuid;

pub use updates::{Processed, Processing, Failed};

pub type UpdateStatus = updates::UpdateStatus<UpdateMeta, UpdateResult, String>;

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct IndexMetadata {
    uuid: Uuid,
    created_at: DateTime<Utc>,
    updated_at: DateTime<Utc>,
    primary_key: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum UpdateMeta {
    DocumentsAddition {
        method: IndexDocumentsMethod,
        format: UpdateFormat,
        primary_key: Option<String>,
    },
    ClearDocuments,
    DeleteDocuments,
    Settings(Settings),
    Facets(Facets),
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
    pub faceted_attributes: Option<Option<HashMap<String, String>>>,

    #[serde(
        default,
        deserialize_with = "deserialize_some",
        skip_serializing_if = "Option::is_none",
    )]
    pub criteria: Option<Option<Vec<String>>>,
}

impl Settings {
    pub fn cleared() -> Self {
        Self {
            displayed_attributes: Some(None),
            searchable_attributes: Some(None),
            faceted_attributes: Some(None),
            criteria: Some(None),
        }
    }
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum UpdateResult {
    DocumentsAddition(DocumentAdditionResult),
    DocumentDeletion { deleted: usize },
    Other,
}

#[derive(Clone, Debug)]
pub struct IndexSettings {
    pub name: Option<String>,
    pub primary_key: Option<String>,
}
