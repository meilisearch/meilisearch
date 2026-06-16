use heed::RwTxn;

use super::{UpgradeIndex, UpgradeParams};
use crate::update::new::indexer::recompute_exact_word_prefix_docids_from_database;
use crate::{Index, Result};

/// Rebuild exact-word prefix databases that were missing before exact-word deltas were tracked.
pub(super) struct RecomputeExactWordPrefixDocids();

impl UpgradeIndex for RecomputeExactWordPrefixDocids {
    fn upgrade(
        &self,
        wtxn: &mut RwTxn,
        index: &Index,
        UpgradeParams { progress, .. }: UpgradeParams<'_>,
    ) -> Result<bool> {
        recompute_exact_word_prefix_docids_from_database(index, wtxn, progress)?;
        Ok(false)
    }

    fn must_upgrade(&self, initial_version: (u32, u32, u32)) -> bool {
        initial_version < (1, 46, 0)
    }

    fn description(&self) -> &'static str {
        "Recomputing exact-word prefix databases"
    }
}
