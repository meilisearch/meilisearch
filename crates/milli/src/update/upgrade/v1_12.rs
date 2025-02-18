use heed::RwTxn;

use super::UpgradeIndex;
use crate::progress::Progress;
use crate::{make_enum_progress, Index, Result};

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
pub(super) struct V1_12_3_To_V1_13_0 {}

impl UpgradeIndex for V1_12_3_To_V1_13_0 {
    fn upgrade(
        &self,
        _wtxn: &mut RwTxn,
        _index: &Index,
        _original: (u32, u32, u32),
        _progress: Progress,
    ) -> Result<bool> {
        // recompute the indexes stats
        Ok(true)
    }

    fn target_version(&self) -> (u32, u32, u32) {
        (1, 13, 0)
    }
}
