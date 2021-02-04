mod local_index_controller;
mod updates;

pub use local_index_controller::LocalIndexController;

use std::collections::HashMap;
use std::num::NonZeroUsize;
use std::sync::Arc;

use anyhow::Result;
use milli::Index;
use milli::update::{IndexDocumentsMethod, UpdateFormat, DocumentAdditionResult};
use serde::{Serialize, Deserialize, de::Deserializer};

pub use updates::{Processed, Processing, Failed};

pub type UpdateStatus = updates::UpdateStatus<UpdateMeta, UpdateResult, String>;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum UpdateMeta {
    DocumentsAddition { method: IndexDocumentsMethod, format: UpdateFormat },
    ClearDocuments,
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
    Other,
}

/// The `IndexController` is in charge of the access to the underlying indices. It splits the logic
/// for read access which is provided thanks to an handle to the index, and write access which must
/// be provided. This allows the implementer to define the behaviour of write accesses to the
/// indices, and abstract the scheduling of the updates. The implementer must be able to provide an
/// instance of `IndexStore`
pub trait IndexController {

    /*
     * Write operations
     *
     * Logic for the write operation need to be provided by the implementer, since they can be made
     * asynchronous thanks to an update_store for example.
     *
     * */

    /// Perform document addition on the database. If the provided index does not exist, it will be
    /// created when the addition is applied to the index.
    fn add_documents<S: AsRef<str>>(
        &self,
        index: S,
        method: IndexDocumentsMethod,
        format: UpdateFormat,
        data: &[u8],
    ) -> anyhow::Result<UpdateStatus>;

    /// Updates an index settings. If the index does not exist, it will be created when the update
    /// is applied to the index.
    fn update_settings<S: AsRef<str>>(&self, index_uid: S, settings: Settings) -> anyhow::Result<UpdateStatus>;

    /// Create an index with the given `index_uid`.
    fn create_index<S: AsRef<str>>(&self, index_uid: S) -> Result<()>;

    /// Delete index with the given `index_uid`, attempting to close it beforehand.
    fn delete_index<S: AsRef<str>>(&self, index_uid: S) -> Result<()>;

    /// Swap two indexes, concretely, it simply swaps the index the names point to.
    fn swap_indices<S1: AsRef<str>, S2: AsRef<str>>(&self, index1_uid: S1, index2_uid: S2) -> Result<()>;

    /// Apply an update to the given index. This method can be called when an update is ready to be
    /// processed
    fn handle_update<S: AsRef<str>>(
        &self,
        _index: S,
        _update_id: u64,
        _meta: Processing<UpdateMeta>,
        _content: &[u8]
    ) -> Result<Processed<UpdateMeta, UpdateResult>, Failed<UpdateMeta, String>> {
        todo!()
    }

    /// Returns, if it exists, the `Index` with the povided name.
    fn index(&self, name: impl AsRef<str>) -> anyhow::Result<Option<Arc<Index>>>;

    fn update_status(&self, index: impl AsRef<str>, id: u64) -> anyhow::Result<Option<UpdateStatus>>;
    fn all_update_status(&self, index: impl AsRef<str>) -> anyhow::Result<Vec<UpdateStatus>>;
}
