use heed::RwTxn;

use super::UpgradeIndex;
use crate::progress::Progress;
use crate::{make_enum_progress, Index, Result};

pub(super) struct FixFieldDistribution {}

impl UpgradeIndex for FixFieldDistribution {
    fn upgrade(&self, wtxn: &mut RwTxn, index: &Index, progress: Progress) -> Result<bool> {
        make_enum_progress! {
            enum FieldDistribution {
                RebuildingFieldDistribution,
            }
        };
        progress.update_progress(FieldDistribution::RebuildingFieldDistribution);
        crate::update::new::reindex::field_distribution(index, wtxn, &progress)?;
        Ok(true)
    }

    fn must_upgrade(&self, initial_version: (u32, u32, u32)) -> bool {
        initial_version < (1, 12, 3)
    }

    fn description(&self) -> &'static str {
        "Recomputing field distribution which was wrong before v1.12.3"
    }
}

pub(super) struct RecomputeStats {}

impl UpgradeIndex for RecomputeStats {
    fn upgrade(&self, _wtxn: &mut RwTxn, _index: &Index, _progress: Progress) -> Result<bool> {
        // recompute the indexes stats
        Ok(true)
    }

    fn must_upgrade(&self, initial_version: (u32, u32, u32)) -> bool {
        initial_version < (1, 13, 0)
    }

    fn description(&self) -> &'static str {
        "Recomputing stats"
    }
}
