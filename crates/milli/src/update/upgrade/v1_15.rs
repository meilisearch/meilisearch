use heed::RwTxn;
use roaring::RoaringBitmap;
use serde::Deserialize;

use super::UpgradeIndex;
use crate::progress::Progress;
use crate::update::new::indexer::recompute_word_fst_from_word_docids_database;
use crate::{Index, Result};

pub(super) struct RecomputeWordFst();

impl UpgradeIndex for RecomputeWordFst {
    fn upgrade(&self, wtxn: &mut RwTxn, index: &Index, progress: Progress) -> Result<bool> {
        // Recompute the word FST from the word docids database.
        recompute_word_fst_from_word_docids_database(index, wtxn, &progress)?;

        Ok(false)
    }
    fn must_upgrade(&self, initial_version: (u32, u32, u32)) -> bool {
        initial_version < (1, 15, 0)
    }

    fn description(&self) -> &'static str {
        "Recomputing word FST from word docids database as it was wrong before v1.15.0"
    }
}

/// Parts of v1.15 `IndexingEmbeddingConfig` that are relevant for upgrade to v1.16
///
/// # Warning
///
/// This object should not be rewritten to the DB, only read to get the name and `user_provided` roaring.
#[derive(Debug, Deserialize)]
pub struct IndexEmbeddingConfig {
    pub name: String,
    pub user_provided: RoaringBitmap,
}
