mod update_store;
mod index_store;
mod update_handler;

use index_store::IndexStore;

use std::path::Path;
use std::sync::Arc;

use milli::Index;
use anyhow::bail;
use itertools::Itertools;

use crate::option::IndexerOpts;
use super::IndexController;
use super::updates::UpdateStatus;
use super::{UpdateMeta, UpdateResult};

pub struct LocalIndexController {
    indexes: IndexStore,
    update_db_size: u64,
    index_db_size: u64,
}

impl LocalIndexController {
    pub fn new(
        path: impl AsRef<Path>,
        opt: IndexerOpts,
        index_db_size: u64,
        update_db_size: u64,
    ) -> anyhow::Result<Self> {
        let indexes = IndexStore::new(path, opt)?;
        Ok(Self { indexes, index_db_size, update_db_size })
    }
}

impl IndexController for LocalIndexController {
    fn add_documents<S: AsRef<str>>(
        &self,
        index: S,
        method: milli::update::IndexDocumentsMethod,
        format: milli::update::UpdateFormat,
        data: &[u8],
    ) -> anyhow::Result<UpdateStatus<UpdateMeta, UpdateResult, String>> {
        let (_, update_store) = self.indexes.get_or_create_index(&index, self.update_db_size, self.index_db_size)?;
        let meta = UpdateMeta::DocumentsAddition { method, format };
        let pending = update_store.register_update(meta, data).unwrap();
        Ok(pending.into())
    }

    fn update_settings<S: AsRef<str>>(
        &self,
        index: S,
        settings: super::Settings
    ) -> anyhow::Result<UpdateStatus<UpdateMeta, UpdateResult, String>> {
        let (_, update_store) = self.indexes.get_or_create_index(&index, self.update_db_size, self.index_db_size)?;
        let meta = UpdateMeta::Settings(settings);
        let pending = update_store.register_update(meta, &[]).unwrap();
        Ok(pending.into())
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

    fn update_status(&self, index: impl AsRef<str>, id: u64) -> anyhow::Result<Option<UpdateStatus<UpdateMeta, UpdateResult, String>>> {
        match self.indexes.index(&index)? {
            Some((_, update_store)) => Ok(update_store.meta(id)?),
            None => bail!("index {:?} doesn't exist", index.as_ref()),
        }
    }

    fn all_update_status(&self, index: impl AsRef<str>) -> anyhow::Result<Vec<UpdateStatus<UpdateMeta, UpdateResult, String>>> {
        match self.indexes.index(index)? {
            Some((_, update_store)) => {
                let updates = update_store.iter_metas(|processing, processed, pending, aborted, failed| {
                    Ok(processing
                        .map(UpdateStatus::from)
                        .into_iter()
                        .chain(pending.filter_map(|p| p.ok()).map(|(_, u)| UpdateStatus::from(u)))
                        .chain(aborted.filter_map(Result::ok).map(|(_, u)| UpdateStatus::from(u)))
                        .chain(processed.filter_map(Result::ok).map(|(_, u)| UpdateStatus::from(u)))
                        .chain(failed.filter_map(Result::ok).map(|(_, u)| UpdateStatus::from(u)))
                        .sorted_by(|a, b| a.id().cmp(&b.id()))
                        .collect())
                })?;
                Ok(updates)
            }
            None => Ok(Vec::new())
        }

    }
}
