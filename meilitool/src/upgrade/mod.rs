mod v1_10;
mod v1_11;
mod v1_9;

use std::path::{Path, PathBuf};

use anyhow::{bail, Context};
use meilisearch_types::versioning::create_version_file;

use v1_10::v1_9_to_v1_10;

use crate::upgrade::v1_11::v1_10_to_v1_11;

pub struct OfflineUpgrade {
    pub db_path: PathBuf,
    pub current_version: (String, String, String),
    pub target_version: (String, String, String),
}

impl OfflineUpgrade {
    pub fn upgrade(self) -> anyhow::Result<()> {
        let upgrade_list = [
            (v1_9_to_v1_10 as fn(&Path) -> Result<(), anyhow::Error>, "1", "10", "0"),
            (v1_10_to_v1_11, "1", "11", "0"),
        ];

        let (current_major, current_minor, current_patch) = &self.current_version;

        let start_at = match (
            current_major.as_str(),
            current_minor.as_str(),
            current_patch.as_str(),
        ) {
            ("1", "9", _) => 0,
            ("1", "10", _) => 1,
            _ => {
                bail!("Unsupported current version {current_major}.{current_minor}.{current_patch}. Can only upgrade from v1.9")
            }
        };

        let (target_major, target_minor, target_patch) = &self.target_version;

        let ends_at = match (target_major.as_str(), target_minor.as_str(), target_patch.as_str()) {
            ("1", "10", _) => 0,
            ("1", "11", _) => 1,
            (major, _, _) if major.starts_with('v') => {
                bail!("Target version must not starts with a `v`. Instead of writing `v1.9.0` write `1.9.0` for example.")
            }
            _ => {
                bail!("Unsupported target version {target_major}.{target_minor}.{target_patch}. Can only upgrade to v1.11")
            }
        };

        println!("Starting the upgrade from {current_major}.{current_minor}.{current_patch} to {target_major}.{target_minor}.{target_patch}");

        #[allow(clippy::needless_range_loop)]
        for index in start_at..=ends_at {
            let (func, major, minor, patch) = upgrade_list[index];
            (func)(&self.db_path)?;
            println!("Done");
            // We're writing the version file just in case an issue arise _while_ upgrading.
            // We don't want the DB to fail in an unknown state.
            println!("Writing VERSION file");

            create_version_file(&self.db_path, major, minor, patch)
                .context("while writing VERSION file after the upgrade")?;
        }

        println!("Success");

        Ok(())
    }
}
