mod update_store;
mod index_store;
mod update_handler;

use std::path::Path;
use std::sync::Arc;

use anyhow::{bail, Context};
use itertools::Itertools;
use milli::Index;

use crate::option::IndexerOpts;
use index_store::IndexStore;
use super::IndexController;
use super::updates::UpdateStatus;
use super::{UpdateMeta, UpdateResult, IndexMetadata, IndexSettings};

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

    fn create_index(&self, index_settings: IndexSettings) -> anyhow::Result<IndexMetadata> {
        let index_name = index_settings.name.context("Missing name for index")?;
        let (index, _, meta) = self.indexes.create_index(&index_name, self.update_db_size, self.index_db_size)?;
        if let Some(ref primary_key) = index_settings.primary_key {
            if let Err(e) = update_primary_key(index, primary_key).context("error creating index") {
                // TODO: creating index could not be completed, delete everything.
                Err(e)?
            }
        }

        let meta = IndexMetadata {
            name: index_name,
            uuid: meta.uuid.clone(),
            created_at: meta.created_at,
            updated_at: meta.created_at,
            primary_key: index_settings.primary_key,
        };

        Ok(meta)
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
        match self.indexes.index(&index)? {
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
            None => bail!("index {} doesn't exist.", index.as_ref()),
        }

    }

    fn list_indexes(&self) -> anyhow::Result<Vec<IndexMetadata>> {
        let metas = self.indexes.list_indexes()?;
        let mut output_meta = Vec::new();
        for (name, meta, primary_key) in metas {
            let created_at = meta.created_at;
            let uuid = meta.uuid;
            let updated_at = self
                .all_update_status(&name)?
                .iter()
                .filter_map(|u| u.processed().map(|u| u.processed_at))
                .max()
                .unwrap_or(created_at);

            let index_meta = IndexMetadata {
                name,
                created_at,
                updated_at,
                uuid,
                primary_key,
            };
            output_meta.push(index_meta);
        }
        Ok(output_meta)
    }

    fn update_index(&self, name: impl AsRef<str>, index_settings: IndexSettings) -> anyhow::Result<IndexMetadata> {
        if index_settings.name.is_some() {
            bail!("can't udpate an index name.")
        }

        let (primary_key, meta) = match index_settings.primary_key {
            Some(ref primary_key) => {
                self.indexes
                    .update_index(&name, |index| {
                        let mut txn = index.write_txn()?;
                        if index.primary_key(&txn)?.is_some() {
                            bail!("primary key already exists.")
                        }
                        index.put_primary_key(&mut txn, primary_key)?;
                        txn.commit()?;
                        Ok(Some(primary_key.clone()))
                    })?
            },
            None => {
                let (index, meta) = self.indexes
                    .index_with_meta(&name)?
                    .with_context(|| format!("index {:?} doesn't exist.", name.as_ref()))?;
                let primary_key = index
                    .primary_key(&index.read_txn()?)?
                    .map(String::from);
                (primary_key, meta)
            },
        };

        Ok(IndexMetadata {
            name: name.as_ref().to_string(),
            uuid: meta.uuid.clone(),
            created_at: meta.created_at,
            updated_at: meta.updated_at,
            primary_key,
        })
    }
}

fn update_primary_key(index: impl AsRef<Index>, primary_key: impl AsRef<str>) -> anyhow::Result<()> {
    let index = index.as_ref();
    let mut txn = index.write_txn()?;
    if index.primary_key(&txn)?.is_some() {
        bail!("primary key already set.")
    }
    index.put_primary_key(&mut txn, primary_key.as_ref())?;
    txn.commit()?;
    Ok(())
}

#[cfg(test)]
mod test {
    use super::*;
    use tempfile::tempdir;
    use crate::make_index_controller_tests;

    make_index_controller_tests!({
        let options = IndexerOpts::default();
        let path = tempdir().unwrap();
        let size = 4096 * 100;
        LocalIndexController::new(path, options, size, size).unwrap()
    });
}
