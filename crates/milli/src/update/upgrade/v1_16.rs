use heed::types::{SerdeJson, Str};
use heed::RwTxn;

use super::UpgradeIndex;
use crate::progress::Progress;
use crate::vector::db::{EmbedderInfo, EmbeddingStatus};
use crate::{Index, InternalError, Result};

#[allow(non_camel_case_types)]
pub(super) struct Latest_V1_15_To_V1_16_0();

impl UpgradeIndex for Latest_V1_15_To_V1_16_0 {
    fn upgrade(
        &self,
        wtxn: &mut RwTxn,
        index: &Index,
        _original: (u32, u32, u32),
        _progress: Progress,
    ) -> Result<bool> {
        let v1_15_indexing_configs = index
            .main
            .remap_types::<Str, SerdeJson<Vec<super::v1_15::IndexEmbeddingConfig>>>()
            .get(wtxn, crate::index::main_key::EMBEDDING_CONFIGS)?
            .unwrap_or_default();

        let embedders = index.embedding_configs();
        for config in v1_15_indexing_configs {
            let embedder_id = embedders.embedder_id(wtxn, &config.name)?.ok_or(
                InternalError::DatabaseMissingEntry {
                    db_name: crate::index::db_name::VECTOR_EMBEDDER_CATEGORY_ID,
                    key: None,
                },
            )?;
            let info = EmbedderInfo {
                embedder_id,
                // v1.15 used not to make a difference between `user_provided` and `! regenerate`.
                embedding_status: EmbeddingStatus::from_user_provided(config.user_provided),
            };
            embedders.put_embedder_info(wtxn, &config.name, &info)?;
        }

        Ok(false)
    }

    fn target_version(&self) -> (u32, u32, u32) {
        (1, 16, 0)
    }
}
