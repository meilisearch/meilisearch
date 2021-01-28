mod update_store;
mod index_store;
mod update_handler;

use index_store::IndexStore;

use std::path::Path;
use std::sync::Arc;

use milli::Index;

use crate::option::IndexerOpts;
use super::IndexController;

pub struct LocalIndexController {
    indexes: IndexStore,
}

impl LocalIndexController {
    pub fn new(path: impl AsRef<Path>, opt: IndexerOpts) -> anyhow::Result<Self> {
        let indexes = IndexStore::new(path, opt)?;
        Ok(Self { indexes })
    }
}

impl IndexController for LocalIndexController {
    fn add_documents<S: AsRef<str>>(
        &self,
        _index: S,
        _method: milli::update::IndexDocumentsMethod,
        _format: milli::update::UpdateFormat,
        _data: &[u8],
    ) -> anyhow::Result<super::UpdateStatusResponse> {
        todo!()
    }

    fn update_settings<S: AsRef<str>>(&self, _index_uid: S, _settings: super::Settings) -> anyhow::Result<super::UpdateStatusResponse> {
        todo!()
    }

    fn create_index<S: AsRef<str>>(&self, _index_uid: S) -> anyhow::Result<()> {
        todo!()
    }

    fn delete_index<S: AsRef<str>>(&self, _index_uid: S) -> anyhow::Result<()> {
        todo!()
    }

    fn swap_indices<S1: AsRef<str>, S2: AsRef<str>>(&self, _index1_uid: S1, _index2_uid: S2) -> anyhow::Result<()> {
        todo!()
    }

    fn index(&self, name: impl AsRef<str>) -> anyhow::Result<Option<Arc<Index>>> {
        let index = self.indexes.index(name)?.map(|(i, _)| i);
        Ok(index)
    }
}
