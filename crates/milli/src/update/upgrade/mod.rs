mod v1_12;
mod v1_13;
mod v1_14;
mod v1_15;
mod v1_16;
mod v1_32;
mod v1_30_1;

use heed::RwTxn;
use v1_12::{FixFieldDistribution, RecomputeStats};
use v1_13::AddNewStats;
use v1_14::UpgradeArroyVersion;
use v1_15::RecomputeWordFst;
use v1_16::SwitchToMultimodal;
use v1_30_1::RebuildHannoyGraph;
use v1_32::CleanupFidBasedDatabases;

use crate::constants::{VERSION_MAJOR, VERSION_MINOR, VERSION_PATCH};
use crate::progress::{Progress, VariableNameStep};
use crate::{Index, InternalError, Result};

trait UpgradeIndex {
    /// Returns `true` if `upgrade` should be called when the index started with version `initial_version`.
    fn must_upgrade(&self, initial_version: (u32, u32, u32)) -> bool;

    /// Returns `true` if the index scheduler must regenerate its cached stats.
    fn upgrade(&self, wtxn: &mut RwTxn, index: &Index, progress: Progress) -> Result<bool>;

    /// Description of the upgrade for progress display purposes.
    fn description(&self) -> &'static str;
}

const UPGRADE_FUNCTIONS: &[&dyn UpgradeIndex] = &[
    &FixFieldDistribution {},
    &RecomputeStats {},
    &AddNewStats {},
    &UpgradeArroyVersion {},
    &RecomputeWordFst {},
    &SwitchToMultimodal {},
    &CleanupFidBasedDatabases {},
    &RebuildHannoyGraph,
];

/// Return true if the cached stats of the index must be regenerated
pub fn upgrade<MSP>(
    wtxn: &mut RwTxn,
    index: &Index,
    db_version: (u32, u32, u32),
    must_stop_processing: MSP,
    progress: Progress,
) -> Result<bool>
where
    MSP: Fn() -> bool + Sync,
{
    let upgrade_functions = UPGRADE_FUNCTIONS;

    let initial_version = index.get_version(wtxn)?.unwrap_or(db_version);

    enum UpgradeVersion {}

    let mut regenerate_stats = false;
    for (i, upgrade) in upgrade_functions.iter().enumerate() {
        if (must_stop_processing)() {
            return Err(crate::Error::InternalError(InternalError::AbortedIndexation));
        }
        if upgrade.must_upgrade(initial_version) {
            regenerate_stats |= upgrade.upgrade(wtxn, index, progress.clone())?;
            progress.update_progress(VariableNameStep::<UpgradeVersion>::new(
                upgrade.description(),
                i as u32,
                upgrade_functions.len() as u32,
            ));
        } else {
            progress.update_progress(VariableNameStep::<UpgradeVersion>::new(
                "Skipping migration that must not be applied",
                i as u32,
                upgrade_functions.len() as u32,
            ));
        }
    }

    index.put_version(wtxn, (VERSION_MAJOR, VERSION_MINOR, VERSION_PATCH))?;

    Ok(regenerate_stats)
}
