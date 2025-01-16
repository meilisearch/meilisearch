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
            milli::update::upgrade::upgrade(&index, progress.clone())
                .map_err(|e| Error::from_milli(e, Some(uid.to_string())))?;
        }

        Ok(())
    }
}
