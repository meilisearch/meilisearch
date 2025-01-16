use crate::progress::{Progress, VariableNameStep};
use crate::{Index, InternalError, Result};

pub fn upgrade(index: &Index, progress: Progress) -> Result<()> {
    let wtxn = index.env.write_txn()?;
    let from = index.get_version(&wtxn)?;
    let upgrade_functions =
        [(v1_12_to_v1_13 as fn(&Index, Progress) -> Result<()>, "Upgrading from v1.12 to v1.13")];

    let start = match from {
        // If there was no version it means we're coming from the base version specified by the index-scheduler
        None | Some((1, 12, _)) => 0,
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

    Ok(())
}

fn v1_12_to_v1_13(_index: &Index, _progress: Progress) -> Result<()> {
    Ok(())
}
