use heed::RwTxn;

use crate::constants::{VERSION_MAJOR, VERSION_MINOR, VERSION_PATCH};
use crate::progress::Progress;
use crate::{make_enum_progress, Index, Result};

use super::UpgradeIndex;

#[allow(non_camel_case_types)]
pub(super) struct V1_12_To_V1_12_3 {}

impl UpgradeIndex for V1_12_To_V1_12_3 {
    fn upgrade(
        &self,
        wtxn: &mut RwTxn,
        index: &Index,
        _original: (u32, u32, u32),
        progress: Progress,
    ) -> Result<bool> {
        make_enum_progress! {
            enum FieldDistribution {
                RebuildingFieldDistribution,
            }
        };
        progress.update_progress(FieldDistribution::RebuildingFieldDistribution);
        crate::update::new::reindex::field_distribution(index, wtxn, &progress)?;
        Ok(true)
    }

    fn target_version(&self) -> (u32, u32, u32) {
        (1, 12, 3)
    }
}

#[allow(non_camel_case_types)]
pub(super) struct V1_12_3_To_Current();

impl UpgradeIndex for V1_12_3_To_Current {
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
