use heed::RwTxn;

use super::UpgradeIndex;
use crate::database_stats::DatabaseStats;
use crate::progress::Progress;
use crate::{make_enum_progress, Index, MustStopProcessing, Result};

pub(super) struct AddNewStats();

impl UpgradeIndex for AddNewStats {
    fn upgrade(
        &self,
        wtxn: &mut RwTxn,
        index: &Index,
        _must_stop_processing: &MustStopProcessing,
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

    fn must_upgrade(&self, initial_version: (u32, u32, u32)) -> bool {
        initial_version < (1, 13, 1)
    }

    fn description(&self) -> &'static str {
        "Computing newly introduced document stats"
    }
}
