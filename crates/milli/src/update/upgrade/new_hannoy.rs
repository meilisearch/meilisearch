use heed::RwTxn;

use super::UpgradeIndex;
use crate::progress::Progress;
use crate::vector::VectorStore;
use crate::{Index, Result};

#[allow(non_camel_case_types)]
pub(super) struct Latest_V1_18_New_Hannoy();

impl UpgradeIndex for Latest_V1_18_New_Hannoy {
    fn upgrade(
        &self,
        wtxn: &mut RwTxn,
        index: &Index,
        _original: (u32, u32, u32),
        progress: Progress,
    ) -> Result<bool> {
        let embedding_configs = index.embedding_configs();
        let backend = index.get_vector_store(wtxn)?;
        for config in embedding_configs.embedding_configs(wtxn)? {
            // TODO use the embedder name to display progress
            /// REMOVE THIS FILE, IMPLEMENT CONVERSION AS A SETTING CHANGE
            let quantized = config.config.quantized();
            let embedder_id = embedding_configs.embedder_id(wtxn, &config.name)?.unwrap();
            let mut vector_store =
                VectorStore::new(backend, index.vector_store, embedder_id, quantized);
            vector_store.change_backend(wtxn, progress.clone())?;
        }

        Ok(false)
    }

    fn target_version(&self) -> (u32, u32, u32) {
        (1, 22, 0)
    }
}
