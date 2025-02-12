use heed::RwTxn;

use super::UpgradeIndex;
use crate::constants::{VERSION_MAJOR, VERSION_MINOR, VERSION_PATCH};
use crate::progress::Progress;
use crate::{Index, Result};

#[allow(non_camel_case_types)]
pub(super) struct V1_13_0_To_Current();

impl UpgradeIndex for V1_13_0_To_Current {
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
        (
            VERSION_MAJOR.parse().unwrap(),
            VERSION_MINOR.parse().unwrap(),
            VERSION_PATCH.parse().unwrap(),
        )
    }
}
