mod v1_12;

use heed::RwTxn;
use v1_12::{v1_12_3_to_v1_13, v1_12_to_v1_12_3};

use crate::progress::{Progress, VariableNameStep};
use crate::{Index, InternalError, Result};

/// Return true if the cached stats of the index must be regenerated
pub fn upgrade(wtxn: &mut RwTxn, index: &Index, progress: Progress) -> Result<bool> {
    let from = index.get_version(wtxn)?;
    let upgrade_functions = [
        (
            v1_12_to_v1_12_3 as fn(&mut RwTxn, &Index, Progress) -> Result<bool>,
            "Upgrading from v1.12.(0/1/2) to v1.12.3",
        ),
        (
            v1_12_3_to_v1_13 as fn(&mut RwTxn, &Index, Progress) -> Result<bool>,
            "Upgrading from v1.12.3+ to v1.13",
        ),
    ];

    let start = match from {
        // If there was no version it means we're coming from the v1.12
        None | Some((1, 12, 0..=2)) => 0,
        Some((1, 12, 3..)) => 1,
        // We must handle the current version in the match because in case of a failure some index may have been upgraded but not other.
        Some((1, 13, _)) => return Ok(false),
        Some((major, minor, patch)) => {
            return Err(InternalError::CannotUpgradeToVersion(major, minor, patch).into())
        }
    };

    enum UpgradeVersion {}
    let upgrade_path = &upgrade_functions[start..];

    let mut regenerate_stats = false;
    for (i, (upgrade_function, upgrade_msg)) in upgrade_path.iter().enumerate() {
        progress.update_progress(VariableNameStep::<UpgradeVersion>::new(
            upgrade_msg.to_string(),
            i as u32,
            upgrade_path.len() as u32,
        ));
        regenerate_stats |= (upgrade_function)(wtxn, index, progress.clone())?;
    }

    Ok(regenerate_stats)
}
