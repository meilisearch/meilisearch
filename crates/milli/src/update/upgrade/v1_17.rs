use heed::RwTxn;

use super::UpgradeIndex;
use crate::progress::Progress;
use crate::{Index, Result};

#[allow(non_camel_case_types)]
pub(super) struct Latest_V1_16_To_V1_17_0();

impl UpgradeIndex for Latest_V1_16_To_V1_17_0 {
    fn upgrade(
        &self,
        _wtxn: &mut RwTxn,
        _index: &Index,
        _original: (u32, u32, u32),
        _progress: Progress,
    ) -> Result<bool> {
        Ok(false)
    }

    fn target_version(&self) -> (u32, u32, u32) {
        (1, 17, 0)
    }
}
