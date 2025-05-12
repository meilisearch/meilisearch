use heed::RwTxn;

use super::UpgradeIndex;
use crate::progress::Progress;
use crate::update::new::indexer::recompute_word_fst_from_word_docids_database;
use crate::{make_enum_progress, Index, Result};

#[allow(non_camel_case_types)]
pub(super) struct Latest_V1_14_To_Latest_V1_15();

impl UpgradeIndex for Latest_V1_14_To_Latest_V1_15 {
    fn upgrade(
        &self,
        wtxn: &mut RwTxn,
        index: &Index,
        _original: (u32, u32, u32),
        progress: Progress,
    ) -> Result<bool> {
        // Recompute the word FST from the word docids database.
        make_enum_progress! {
            enum TypoTolerance {
                RecomputeWordFst,
            }
        };

        progress.update_progress(TypoTolerance::RecomputeWordFst);
        recompute_word_fst_from_word_docids_database(index, wtxn)?;

        Ok(false)
    }

    fn target_version(&self) -> (u32, u32, u32) {
        (1, 15, 0)
    }
}
