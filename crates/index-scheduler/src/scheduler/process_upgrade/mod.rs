use meilisearch_types::{
    milli,
    milli::progress::{Progress, VariableNameStep},
};

use crate::{processing::UpgradeDatabaseProgress, Error, IndexScheduler, Result};

impl IndexScheduler {
    pub(super) fn process_upgrade(&self, progress: Progress) -> Result<()> {
        progress.update_progress(UpgradeDatabaseProgress::EnsuringCorrectnessOfTheSwap);

        enum UpgradeIndex {}
        let indexes = self.index_names()?;

        for (i, uid) in indexes.iter().enumerate() {
            progress.update_progress(VariableNameStep::<UpgradeIndex>::new(
                format!("Upgrading index `{uid}`"),
                i as u32,
                indexes.len() as u32,
            ));
            let index = self.index(uid)?;
            let mut wtxn = index.write_txn()?;
            let regenerate = milli::update::upgrade::upgrade(&mut wtxn, &index, progress.clone())
                .map_err(|e| Error::from_milli(e, Some(uid.to_string())))?;
            if regenerate {
                let stats = crate::index_mapper::IndexStats::new(&index, &wtxn)
                    .map_err(|e| Error::from_milli(e, Some(uid.to_string())))?;
                // Release wtxn as soon as possible because it stops us from registering tasks
                let mut index_schd_wtxn = self.env.write_txn()?;
                self.index_mapper.store_stats_of(&mut index_schd_wtxn, uid, &stats)?;
                index_schd_wtxn.commit()?;
            }
            wtxn.commit()?;
        }

        Ok(())
    }
}
