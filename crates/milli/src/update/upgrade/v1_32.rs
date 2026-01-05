use std::collections::BTreeSet;

use heed::RwTxn;

use super::UpgradeIndex;
use crate::progress::Progress;
use crate::{make_enum_progress, Index, Result};

pub(super) struct CleanupFidBasedDatabases();

impl UpgradeIndex for CleanupFidBasedDatabases {
    fn upgrade(&self, wtxn: &mut RwTxn, index: &Index, progress: Progress) -> Result<bool> {
        make_enum_progress! {
            enum CleanupFidBasedDatabases {
                RetrievingFidsToDelete,
                DeletingFidBasedDatabases,
            }
        };

        // Force-delete the fid-based databases for the fids that are not searchable.
        // This is a sanity cleanup step to ensure that the database is not corrupted.
        progress.update_progress(CleanupFidBasedDatabases::RetrievingFidsToDelete);
        let fid_map = index.fields_ids_map_with_metadata(wtxn)?;
        let fids_to_delete: BTreeSet<_> = fid_map
            .iter()
            .filter_map(|(id, _, metadata)| if !metadata.is_searchable() { Some(id) } else { None })
            .collect();

        if !fids_to_delete.is_empty() {
            progress.update_progress(CleanupFidBasedDatabases::DeletingFidBasedDatabases);
            crate::update::new::indexer::delete_old_fid_based_databases_from_fids(
                wtxn,
                index,
                &|| false,
                &fids_to_delete,
                &progress,
            )?;
        }

        Ok(false)
    }
    fn must_upgrade(&self, initial_version: (u32, u32, u32)) -> bool {
        initial_version < (1, 32, 0)
    }

    fn description(&self) -> &'static str {
        "Cleaning up the fid-based databases"
    }
}
