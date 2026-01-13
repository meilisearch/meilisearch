use arroy::distances::Cosine;
use heed::RwTxn;

use super::UpgradeIndex;
use crate::progress::Progress;
use crate::{make_enum_progress, Index, MustStopProcessing, Result};

pub(super) struct UpgradeArroyVersion();

impl UpgradeIndex for UpgradeArroyVersion {
    fn upgrade(
        &self,
        wtxn: &mut RwTxn,
        index: &Index,
        _must_stop_processing: &MustStopProcessing,
        progress: Progress,
    ) -> Result<bool> {
        make_enum_progress! {
            enum VectorStore {
                UpdateInternalVersions,
            }
        };

        progress.update_progress(VectorStore::UpdateInternalVersions);

        let rtxn = index.read_txn()?;
        arroy::upgrade::from_0_5_to_0_6::<Cosine>(
            &rtxn,
            index.vector_store.remap_types(),
            wtxn,
            index.vector_store.remap_types(),
        )?;

        Ok(false)
    }

    fn must_upgrade(&self, initial_version: (u32, u32, u32)) -> bool {
        initial_version < (1, 14, 0)
    }

    fn description(&self) -> &'static str {
        "Updating vector store with an internal version"
    }
}
