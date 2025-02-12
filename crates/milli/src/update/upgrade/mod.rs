mod v1_12;
mod v1_13;

use heed::RwTxn;
use v1_12::{V1_12_3_To_V1_13_0, V1_12_To_V1_12_3};
use v1_13::V1_13_0_To_Current;

use crate::progress::{Progress, VariableNameStep};
use crate::{Index, InternalError, Result};

trait UpgradeIndex {
    /// Returns true if the index scheduler must regenerate its cached stats
    fn upgrade(
        &self,
        wtxn: &mut RwTxn,
        index: &Index,
        original: (u32, u32, u32),
        progress: Progress,
    ) -> Result<bool>;
    fn target_version(&self) -> (u32, u32, u32);
}

/// Return true if the cached stats of the index must be regenerated
pub fn upgrade(
    wtxn: &mut RwTxn,
    index: &Index,
    db_version: (u32, u32, u32),
    progress: Progress,
) -> Result<bool> {
    let from = index.get_version(wtxn)?.unwrap_or(db_version);
    let upgrade_functions: &[&dyn UpgradeIndex] =
        &[&V1_12_To_V1_12_3 {}, &V1_12_3_To_V1_13_0 {}, &V1_13_0_To_Current()];

    let start = match from {
        (1, 12, 0..=2) => 0,
        (1, 12, 3..) => 1,
        (1, 13, 0) => 2,
        // We must handle the current version in the match because in case of a failure some index may have been upgraded but not other.
        (1, 13, _) => return Ok(false),
        (major, minor, patch) => {
            return Err(InternalError::CannotUpgradeToVersion(major, minor, patch).into())
        }
    };

    enum UpgradeVersion {}
    let upgrade_path = &upgrade_functions[start..];

    let mut current_version = from;

    let mut regenerate_stats = false;
    for (i, upgrade) in upgrade_path.iter().enumerate() {
        let target = upgrade.target_version();
        progress.update_progress(VariableNameStep::<UpgradeVersion>::new(
            format!(
                "Upgrading from v{}.{}.{} to v{}.{}.{}",
                current_version.0,
                current_version.1,
                current_version.2,
                target.0,
                target.1,
                target.2
            ),
            i as u32,
            upgrade_path.len() as u32,
        ));
        regenerate_stats |= upgrade.upgrade(wtxn, index, from, progress.clone())?;
        index.put_version(wtxn, target)?;
        current_version = target;
    }

    Ok(regenerate_stats)
}
