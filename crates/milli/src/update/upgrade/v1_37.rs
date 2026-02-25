use heed::RwTxn;

use super::UpgradeIndex;
use crate::progress::Progress;
use crate::vector::{VectorStore, VectorStoreBackend};
use crate::{Index, MustStopProcessing, Result};

/// Convert old Annoy vector stores to Hannoy ones
pub(super) struct ConvertArroyToHannoy();

impl UpgradeIndex for ConvertArroyToHannoy {
    fn upgrade(
        &self,
        wtxn: &mut RwTxn,
        index: &Index,
        must_stop_processing: &MustStopProcessing,
        progress: Progress,
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
