mod v1_10;
mod v1_9;

use std::path::PathBuf;

use anyhow::{bail, Context};
use meilisearch_types::versioning::create_version_file;

use v1_10::v1_9_to_v1_10;

pub struct OfflineUpgrade {
    pub db_path: PathBuf,
    pub current_version: (String, String, String),
    pub target_version: (String, String, String),
}

impl OfflineUpgrade {
    pub fn upgrade(self) -> anyhow::Result<()> {
        let (current_major, current_minor, current_patch) = &self.current_version;
        let (target_major, target_minor, target_patch) = &self.target_version;

        println!("Upgrading from {current_major}.{current_minor}.{current_patch} to {target_major}.{target_minor}.{target_patch}");

        match (
            (current_major.as_str(), current_minor.as_str(), current_patch.as_str()),
            (target_major.as_str(), target_minor.as_str(), target_patch.as_str()),
        ) {
            (("1", "9", _), ("1", "10", _)) => v1_9_to_v1_10(&self.db_path)?,
            ((major, minor, _), _) if major != "1" && minor != "9" =>
                bail!("Unsupported current version {current_major}.{current_minor}.{current_patch}. Can only upgrade from v1.9"),
            (_, (major, minor, _)) if major != "1" && minor != "10" =>
                bail!("Unsupported target version {target_major}.{target_minor}.{target_patch}. Can only upgrade to v1.10"),
            _ => 
                bail!("Unsupported upgrade from {current_major}.{current_minor}.{current_patch} to {target_major}.{target_minor}.{target_patch}. Can only upgrade from v1.9 to v1.10"),
        }

        println!("Writing VERSION file");

        create_version_file(&self.db_path, target_major, target_minor, target_patch)
            .context("while writing VERSION file after the upgrade")?;

        println!("Success");

        Ok(())
    }
}
