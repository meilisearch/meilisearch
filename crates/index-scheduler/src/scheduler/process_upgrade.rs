use meilisearch_types::index_uid::{DsrIndex, UserIndex};
use meilisearch_types::milli;
use meilisearch_types::milli::progress::{Progress, VariableNameStep};
use meilisearch_types::milli::update::upgrade::must_upgrade_dsr;

use crate::index_mapper::IndexUid as _;
use crate::processing::UpgradeIndexesProgress;
use crate::{Error, IndexScheduler, Result};

impl IndexScheduler {
    pub(super) fn process_upgrade(
        &self,
        db_version: (u32, u32, u32),
        progress: Progress,
    ) -> Result<()> {
        #[cfg(test)]
        self.maybe_fail(crate::test_utils::FailureLocation::ProcessUpgrade)?;

        let indexes = self.user_index_names()?;
        let must_stop_processing = &self.scheduler.must_stop_processing;

        let shards = self.network().shards();

        progress.update_progress(UpgradeIndexesProgress::UpgradingUserIndexes);

        for (i, uid) in indexes.iter().enumerate() {
            if must_stop_processing.get() {
                return Err(Error::AbortedTask);
            }

            progress.update_progress(VariableNameStep::<UpgradeIndex>::new(
                format!("Upgrading index `{uid}`"),
                i as u32,
                indexes.len() as u32,
            ));
            let index = self.user_index(uid)?;
            let mut index_wtxn = index.write_txn()?;
            let regen_stats = milli::update::upgrade::upgrade(
                &mut index_wtxn,
                &index,
                db_version,
                milli::update::upgrade::UpgradeParams {
                    must_stop_processing,
                    progress: &progress,
                    shards: shards.as_ref(),
                },
            )
            .map_err(|e| Error::from_milli(e, Some(uid.to_string())))?;
            if regen_stats {
                let stats = crate::index_mapper::IndexStats::new(&index, &index_wtxn)
                    .map_err(|e| Error::from_milli(e, Some(uid.to_string())))?;
                index_wtxn.commit()?;

                // Release wtxn as soon as possible because it stops us from registering tasks
                let mut index_schd_wtxn = self.env.write_txn()?;
                let name = UserIndex::try_from_uid(uid)?;
                self.index_mapper.store_stats_of(&mut index_schd_wtxn, name, &stats)?;
                index_schd_wtxn.commit()?;
            } else {
                index_wtxn.commit()?;
            }
        }

        progress.update_progress(UpgradeIndexesProgress::UpgradingDsrIndex);

        'dsr_update: {
            let err = |err| Error::from_milli(err, Some(DsrIndex::dsr_uid().to_string()));
            let rtxn = self.env.read_txn()?;
            let index = match self.index_mapper.index(&rtxn, DsrIndex) {
                Ok(dsr_index) => dsr_index,
                Err(Error::IndexNotFound(_)) => break 'dsr_update,
                Err(err) => return Err(err),
            };
            let mut index_wtxn = index.write_txn()?;

            // get initial version **before** upgrade, which overwrites it
            let initial_version = index.get_version(&index_wtxn)?.unwrap_or(db_version);

            let regen_stats = milli::update::upgrade::upgrade(
                &mut index_wtxn,
                &index,
                db_version,
                milli::update::upgrade::UpgradeParams {
                    must_stop_processing,
                    progress: &progress,
                    shards: shards.as_ref(),
                },
            )
            .map_err(err)?;

            if must_upgrade_dsr(initial_version).map_err(err)? {
                self.apply_dsr_settings(
                    &mut index_wtxn,
                    &index,
                    &progress,
                    must_stop_processing,
                    Default::default(),
                )?;
            }

            if regen_stats {
                let stats =
                    crate::index_mapper::IndexStats::new(&index, &index_wtxn).map_err(err)?;
                index_wtxn.commit()?;

                // Release wtxn as soon as possible because it stops us from registering tasks
                let mut index_schd_wtxn = self.env.write_txn()?;
                self.index_mapper.store_stats_of(&mut index_schd_wtxn, DsrIndex, &stats)?;
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

        let indexes = self.user_index_names()?;

        tracing::info!("roll backing all indexes");
        for (i, uid) in indexes.iter().enumerate() {
            progress.update_progress(VariableNameStep::<UpgradeIndex>::new(
                format!("Rollbacking index `{uid}`"),
                i as u32,
                indexes.len() as u32,
            ));
            let index_schd_rtxn = self.env.read_txn()?;

            let name = UserIndex::try_from_uid(uid)?;

            let rollback_outcome =
                self.index_mapper.rollback_index(&index_schd_rtxn, name, db_version)?;
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
