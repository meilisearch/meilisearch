use heed::RwTxn;

use super::UpgradeIndex;
use crate::update::upgrade::UpgradeParams;
use crate::vector::{VectorStore, VectorStoreBackend};
use crate::{Index, Result};

/// Convert old Annoy vector stores to Hannoy ones
pub(super) struct ConvertArroyToHannoy();

impl UpgradeIndex for ConvertArroyToHannoy {
    fn upgrade(
        &self,
        wtxn: &mut RwTxn,
        index: &Index,
        UpgradeParams { progress, must_stop_processing, .. }: UpgradeParams<'_>,
    ) -> Result<bool> {
        let embedders = index.embedding_configs();
        let backend = index.get_vector_store(wtxn)?.unwrap_or_default();
        if backend == VectorStoreBackend::Hannoy {
            return Ok(false);
        }

        let rtxn = index.read_txn()?;

        for config in embedders.embedding_configs(wtxn)? {
            let embedder_info = embedders.embedder_info(wtxn, &config.name)?.unwrap();
            let vector_store = VectorStore::new(
                backend,
                index.vector_store,
                embedder_info.embedder_id,
                config.config.quantized(),
            );

            vector_store.change_backend(
                &rtxn,
                wtxn,
                progress.clone(),
                &|| must_stop_processing.get(),
                None,
            )?;
        }

        index.put_vector_store(wtxn, VectorStoreBackend::Hannoy)?;

        Ok(false)
    }

    fn must_upgrade(&self, initial_version: (u32, u32, u32)) -> bool {
        initial_version < (1, 37, 0)
    }

    fn description(&self) -> &'static str {
        "Migrates Arroy vector storage to Hannoy format"
    }
}

pub struct AddShards {}

impl super::UpgradeIndex for AddShards {
    fn must_upgrade(&self, initial_version: (u32, u32, u32)) -> bool {
        initial_version < (1, 38, 0)
    }

    fn upgrade(
        &self,
        wtxn: &mut heed::RwTxn,
        index: &crate::Index,
        UpgradeParams { shards, .. }: UpgradeParams<'_>,
    ) -> crate::Result<bool> {
        let Some(shards) = shards else {
            return Ok(false);
        };

        // before this upgrade, there is at most one shard owned by the remote.
        // if we find it, we can associate all docids to that shard.
        let Some(own_shard) = shards.as_sorted_slice().iter().find(|shard| shard.is_own) else {
            return Ok(false);
        };

        let shard_docids = index.shard_docids();

        let docids = index.documents_ids(wtxn)?;

        shard_docids.put_docids(wtxn, &own_shard.name, &docids)?;
        Ok(false)
    }

    fn description(&self) -> &'static str {
        "adding shards to network objects"
    }
}
