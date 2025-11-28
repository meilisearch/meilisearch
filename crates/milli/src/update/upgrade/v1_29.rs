use heed::RwTxn;

use super::UpgradeIndex;
use crate::progress::Progress;
use crate::vector::VectorStoreBackend;
use crate::{Index, Result};

#[allow(non_camel_case_types)]
pub(super) struct Latest_V1_28_To_V1_29_0();

impl UpgradeIndex for Latest_V1_28_To_V1_29_0 {
    fn upgrade(
        &self,
        wtxn: &mut RwTxn,
        index: &Index,
        _original: (u32, u32, u32),
        _progress: Progress,
    ) -> Result<bool> {
        if index.get_vector_store(wtxn)?.is_none() {
            index.put_vector_store(wtxn, VectorStoreBackend::Arroy)?;
        }

        Ok(false)
    }

    fn target_version(&self) -> (u32, u32, u32) {
        (1, 29, 0)
    }
}
