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
        let index_version = index.get_version(wtxn)?.unwrap();
        for config in embedding_configs.embedding_configs(wtxn)? {
            // TODO use the embedder name to display progress
            let quantized = config.config.quantized();
            let embedder_id = embedding_configs.embedder_id(wtxn, &config.name)?.unwrap();
            let vector_store =
                VectorStore::new(index_version, index.vector_store, embedder_id, quantized);
            vector_store.convert_from_arroy(wtxn, progress.clone())?;
        }

        Ok(false)
    }

    fn target_version(&self) -> (u32, u32, u32) {
        (1, 22, 0)
    }
}
