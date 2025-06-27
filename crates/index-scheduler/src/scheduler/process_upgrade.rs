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

        let indexes = self.index_names()?;

        for (i, uid) in indexes.iter().enumerate() {
            let must_stop_processing = self.scheduler.must_stop_processing.clone();

            if must_stop_processing.get() {
                return Err(Error::AbortedTask);
            }
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
                || must_stop_processing.get(),
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

    pub fn process_rollback(&self, db_version: (u32, u32, u32), progress: &Progress) -> Result<()> {
        let mut wtxn = self.env.write_txn()?;
        tracing::info!(?db_version, "roll back index scheduler version");
        self.version.set_version(&mut wtxn, db_version)?;
        let db_path = self.scheduler.version_file_path.parent().unwrap();
        wtxn.commit()?;

        let indexes = self.index_names()?;

        tracing::info!("roll backing all indexes");
        for (i, uid) in indexes.iter().enumerate() {
            progress.update_progress(VariableNameStep::<UpgradeIndex>::new(
                format!("Rollbacking index `{uid}`"),
                i as u32,
                indexes.len() as u32,
            ));
            let index_schd_rtxn = self.env.read_txn()?;

            let rollback_outcome =
                self.index_mapper.rollback_index(&index_schd_rtxn, uid, db_version)?;
            if !rollback_outcome.succeeded() {
                return Err(crate::Error::RollbackFailed { index: uid.clone(), rollback_outcome });
            }
        }

        tracing::info!(?db_path, ?db_version, "roll back version file");
        meilisearch_types::versioning::create_version_file(
            db_path,
            db_version.0,
            db_version.1,
            db_version.2,
        )?;

        Ok(())
    }
}

enum UpgradeIndex {}
