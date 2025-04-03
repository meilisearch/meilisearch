use heed::RwTxn;

use super::UpgradeIndex;
use crate::constants::{VERSION_MAJOR, VERSION_MINOR, VERSION_PATCH};
use crate::database_stats::DatabaseStats;
use crate::progress::Progress;
use crate::{make_enum_progress, Index, Result};

#[allow(non_camel_case_types)]
pub(super) struct V1_13_0_To_V1_13_1();

impl UpgradeIndex for V1_13_0_To_V1_13_1 {
    fn upgrade(
        &self,
        wtxn: &mut RwTxn,
        index: &Index,
        _original: (u32, u32, u32),
        progress: Progress,
    ) -> Result<bool> {
        make_enum_progress! {
            enum DocumentsStats {
                CreatingDocumentsStats,
            }
        };

        // Create the new documents stats.
        progress.update_progress(DocumentsStats::CreatingDocumentsStats);
        let stats = DatabaseStats::new(index.documents.remap_types(), wtxn)?;
        index.put_documents_stats(wtxn, stats)?;

        Ok(true)
    }

    fn target_version(&self) -> (u32, u32, u32) {
        (1, 13, 1)
    }
}

#[allow(non_camel_case_types)]
pub(super) struct V1_13_1_To_Latest_V1_13();

impl UpgradeIndex for V1_13_1_To_Latest_V1_13 {
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
