use std::ops::Deref;

use super::{IndexStore, IndexController};

pub struct UpdateStore {
    index_store: IndexStore,
}

impl Deref for UpdateStore {
    type Target = IndexStore;

    fn deref(&self) -> &Self::Target {
        &self.index_store
    }
}

impl UpdateStore {
    pub fn new(index_store: IndexStore) -> Self {
        Self { index_store }
    }
}

impl IndexController for UpdateStore {
    fn add_documents<S: AsRef<str>>(
        &self,
        _index: S,
        _method: milli::update::IndexDocumentsMethod,
        _format: milli::update::UpdateFormat,
        _data: &[u8],
    ) -> anyhow::Result<crate::index_controller::UpdateStatusResponse> {
        todo!()
    }

    fn update_settings<S: AsRef<str>>(&self, _index_uid: S, _settings: crate::index_controller::Settings) -> anyhow::Result<crate::index_controller::UpdateStatusResponse> {
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
}
