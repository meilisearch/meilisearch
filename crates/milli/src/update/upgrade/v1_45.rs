use std::ops::Not as _;

use heed::RwTxn;

use super::v1_37::{change_quantized_config, ConvertArroyToHannoy};
use super::UpgradeIndex;
use crate::update::upgrade::UpgradeParams;
use crate::vector::VectorStore;
use crate::{Index, Result};

/// Fix the desync of internal Arroy and Hannoy vector stores
pub(super) struct FixVectorStoreConfig();

impl UpgradeIndex for FixVectorStoreConfig {
    fn upgrade(&self, wtxn: &mut RwTxn, index: &Index, _: UpgradeParams<'_>) -> Result<bool> {
        let embedders = index.embedding_configs();
        let backend = index.get_vector_store(wtxn)?.unwrap_or_default();

        let mut configs = embedders.embedding_configs(wtxn)?;
        for config in &mut configs {
            let embedder_info = embedders.embedder_info(wtxn, &config.name)?.unwrap();

            let vector_store = VectorStore::new(
                backend,
                index.vector_store,
                embedder_info.embedder_id,
                config.config.quantized(),
            );

            let detected = vector_store.clean_stores(wtxn)?;
            config.config.quantized = change_quantized_config(config.config.quantized, detected);
        }

        embedders.put_embedding_configs(wtxn, configs)?;

        Ok(false)
    }

    fn must_upgrade(&self, initial_version: (u32, u32, u32)) -> bool {
        (ConvertArroyToHannoy {}).must_upgrade(initial_version).not()
            && initial_version < (1, 45, 2)
    }

    fn description(&self) -> &'static str {
        "Fix vector stores config desync"
    }
}
