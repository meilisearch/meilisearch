use meilisearch_types::milli;
use meilisearch_types::milli::progress::{Progress, VariableNameStep};

use crate::{Error, IndexScheduler, Result};

impl IndexScheduler {
    pub(super) fn process_upgrade(
        &self,
        db_version: (u32, u32, u32),
        progress: Progress,
    ) -> Result<()> {
        #[cfg(test)]
        self.maybe_fail(crate::test_utils::FailureLocation::ProcessUpgrade)?;

        enum UpgradeIndex {}
        let indexes = self.index_names()?;

        for (i, uid) in indexes.iter().enumerate() {
            progress.update_progress(VariableNameStep::<UpgradeIndex>::new(
                format!("Upgrading index `{uid}`"),
                i as u32,
                indexes.len() as u32,
            ));
            let index = self.index(uid)?;
            let mut index_wtxn = index.write_txn()?;
            let regen_stats = milli::update::upgrade::upgrade(
                &mut index_wtxn,
                &index,
                db_version,
                progress.clone(),
            )
            .map_err(|e| Error::from_milli(e, Some(uid.to_string())))?;
            if regen_stats {
                let stats = crate::index_mapper::IndexStats::new(&index, &index_wtxn)
                    .map_err(|e| Error::from_milli(e, Some(uid.to_string())))?;
                index_wtxn.commit()?;

                // Release wtxn as soon as possible because it stops us from registering tasks
                let mut index_schd_wtxn = self.env.write_txn()?;
                self.index_mapper.store_stats_of(&mut index_schd_wtxn, uid, &stats)?;
                index_schd_wtxn.commit()?;
            } else {
                index_wtxn.commit()?;
            }
        }

        Ok(())
    }
}
