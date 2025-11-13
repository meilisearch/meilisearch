mod v1_12;
mod v1_13;
mod v1_14;
mod v1_15;
mod v1_16;

use heed::RwTxn;
use v1_12::{V1_12_3_To_V1_13_0, V1_12_To_V1_12_3};
use v1_13::{V1_13_0_To_V1_13_1, V1_13_1_To_Latest_V1_13};
use v1_14::Latest_V1_13_To_Latest_V1_14;
use v1_15::Latest_V1_14_To_Latest_V1_15;
use v1_16::Latest_V1_15_To_V1_16_0;

use crate::constants::{VERSION_MAJOR, VERSION_MINOR, VERSION_PATCH};
use crate::progress::{Progress, VariableNameStep};
use crate::{Index, InternalError, Result};

trait UpgradeIndex {
    /// Returns `true` if the index scheduler must regenerate its cached stats.
    fn upgrade(
        &self,
        wtxn: &mut RwTxn,
        index: &Index,
        original: (u32, u32, u32),
        progress: Progress,
    ) -> Result<bool>;
    fn target_version(&self) -> (u32, u32, u32);
}

const UPGRADE_FUNCTIONS: &[&dyn UpgradeIndex] = &[
    &V1_12_To_V1_12_3 {},
    &V1_12_3_To_V1_13_0 {},
    &V1_13_0_To_V1_13_1 {},
    &V1_13_1_To_Latest_V1_13 {},
    &Latest_V1_13_To_Latest_V1_14 {},
    &Latest_V1_14_To_Latest_V1_15 {},
    &Latest_V1_15_To_V1_16_0 {},
    &ToTargetNoOp { target: (1, 18, 0) },
    &ToTargetNoOp { target: (1, 19, 0) },
    &ToTargetNoOp { target: (1, 20, 0) },
    &ToTargetNoOp { target: (1, 21, 0) },
    &ToTargetNoOp { target: (1, 22, 0) },
    &ToTargetNoOp { target: (1, 23, 0) },
    &ToTargetNoOp { target: (1, 24, 0) },
    &ToTargetNoOp { target: (1, 25, 0) },
    &ToTargetNoOp { target: (1, 26, 0) },
    // This is the last upgrade function, it will be called when the index is up to date.
    // any other upgrade function should be added before this one.
    &ToCurrentNoOp {},
];

/// Causes a compile-time error if the argument is not in range of `0..UPGRADE_FUNCTIONS.len()`
macro_rules! function_index {
    ($start:expr) => {{
        const _CHECK_INDEX: () = {
            if $start >= $crate::update::upgrade::UPGRADE_FUNCTIONS.len() {
                panic!("upgrade functions out of range")
            }
        };

        $start
    }};
}

const fn start(from: (u32, u32, u32)) -> Option<usize> {
    let start = match from {
        (1, 12, 0..=2) => function_index!(0),
        (1, 12, 3..) => function_index!(1),
        (1, 13, 0) => function_index!(2),
        (1, 13, _) => function_index!(4),
        (1, 14, _) => function_index!(5),
        // We must handle the current version in the match because in case of a failure some index may have been upgraded but not other.
        (1, 15, _) => function_index!(6),
        (1, 16, _) | (1, 17, _) => function_index!(7),
        (1, 18, _) => function_index!(8),
        (1, 19, _) => function_index!(9),
        (1, 20, _) => function_index!(10),
        (1, 21, _) => function_index!(11),
        (1, 22, _) => function_index!(12),
        (1, 23, _) => function_index!(13),
        (1, 24, _) => function_index!(14),
        (1, 25, _) => function_index!(15),
        (1, 26, _) => function_index!(16),
        // We deliberately don't add a placeholder with (VERSION_MAJOR, VERSION_MINOR, VERSION_PATCH) here to force manually
        // considering dumpless upgrade.
        (_major, _minor, _patch) => return None,
    };

    Some(start)
}

/// Causes a compile-time error if the latest package cannot be upgraded.
///
/// This serves as a reminder to consider the proper dumpless upgrade implementation when changing the package version.
const _CHECK_PACKAGE_CAN_UPGRADE: () = {
    if start((VERSION_MAJOR, VERSION_MINOR, VERSION_PATCH)).is_none() {
        panic!("cannot upgrade from latest package version")
    }
};

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
    let from = index.get_version(wtxn)?.unwrap_or(db_version);

    let start =
        start(from).ok_or_else(|| InternalError::CannotUpgradeToVersion(from.0, from.1, from.2))?;

    enum UpgradeVersion {}
    let upgrade_path = &UPGRADE_FUNCTIONS[start..];

    let mut current_version = from;
    let mut regenerate_stats = false;
    for (i, upgrade) in upgrade_path.iter().enumerate() {
        if (must_stop_processing)() {
            return Err(crate::Error::InternalError(InternalError::AbortedIndexation));
        }
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

#[allow(non_camel_case_types)]
struct ToCurrentNoOp {}

impl UpgradeIndex for ToCurrentNoOp {
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
        (VERSION_MAJOR, VERSION_MINOR, VERSION_PATCH)
    }
}

/// Perform no operation during the upgrade except changing to the specified target version.
#[allow(non_camel_case_types)]
struct ToTargetNoOp {
    pub target: (u32, u32, u32),
}

impl UpgradeIndex for ToTargetNoOp {
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
        self.target
    }
}
