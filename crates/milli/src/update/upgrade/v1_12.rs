use heed::RwTxn;

use crate::progress::Progress;
use crate::{make_enum_progress, Index, Result};

// The field distribution was not computed correctly in the v1.12 until the v1.12.3
pub(super) fn v1_12_to_v1_12_3(
    wtxn: &mut RwTxn,
    index: &Index,
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

pub(super) fn v1_12_3_to_v1_13(
    _wtxn: &mut RwTxn,
    _index: &Index,
    _progress: Progress,
) -> Result<bool> {
    Ok(false)
}
