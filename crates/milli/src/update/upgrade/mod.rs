use heed::RwTxn;

use crate::progress::{Progress, VariableNameStep};
use crate::{Index, InternalError, Result};

/// Return true if the cached stats of the index must be regenerated
pub fn upgrade(wtxn: &mut RwTxn, index: &Index, progress: Progress) -> Result<bool> {
    let from = index.get_version(wtxn)?;
    let upgrade_functions =
        [(v1_12_to_v1_13 as fn(&Index, Progress) -> Result<()>, "Upgrading from v1.12 to v1.13")];

    let (start, regenerate_stats) = match from {
        // If there was no version it means we're coming from the v1.12
        None | Some((1, 12, _)) => (0, false),
        // We must handle the current version in the match because in case of a failure some index may have been upgraded but not other.
        Some((1, 13, _)) => return Ok(false),
        Some((major, minor, patch)) => {
            return Err(InternalError::CannotUpgradeToVersion(major, minor, patch).into())
        }
    };

    enum UpgradeVersion {}
    let upgrade_path = &upgrade_functions[start..];

    for (i, (upgrade_function, upgrade_msg)) in upgrade_path.iter().enumerate() {
        progress.update_progress(VariableNameStep::<UpgradeVersion>::new(
            upgrade_msg.to_string(),
            i as u32,
            upgrade_path.len() as u32,
        ));
        (upgrade_function)(index, progress.clone())?;
    }

    Ok(regenerate_stats)
}

fn v1_12_to_v1_13(_index: &Index, _progress: Progress) -> Result<()> {
    Ok(())
}
