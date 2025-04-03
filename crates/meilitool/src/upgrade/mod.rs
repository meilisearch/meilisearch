mod v1_10;
mod v1_11;
mod v1_12;
mod v1_9;

use std::path::{Path, PathBuf};

use anyhow::{bail, Context};
use meilisearch_types::versioning::create_version_file;
use v1_10::v1_9_to_v1_10;
use v1_12::{v1_11_to_v1_12, v1_12_to_v1_12_3};

use crate::upgrade::v1_11::v1_10_to_v1_11;

pub struct OfflineUpgrade {
    pub db_path: PathBuf,
    pub current_version: (u32, u32, u32),
    pub target_version: (u32, u32, u32),
}

impl OfflineUpgrade {
    pub fn upgrade(self) -> anyhow::Result<()> {
        // Adding a version?
        //
        // 1. Update the LAST_SUPPORTED_UPGRADE_FROM_VERSION and LAST_SUPPORTED_UPGRADE_TO_VERSION.
        // 2. Add new version to the upgrade list if necessary
        // 3. Use `no_upgrade` as index for versions that are compatible.

        if self.current_version == self.target_version {
            println!("Database is already at the target version. Exiting.");
            return Ok(());
        }

        if self.current_version > self.target_version {
            bail!(
                "Cannot downgrade from {}.{}.{} to {}.{}.{}. Downgrade not supported",
                self.current_version.0,
                self.current_version.1,
                self.current_version.2,
                self.target_version.0,
                self.target_version.1,
                self.target_version.2
            );
        }

        const FIRST_SUPPORTED_UPGRADE_FROM_VERSION: &str = "1.9.0";
        const LAST_SUPPORTED_UPGRADE_FROM_VERSION: &str = "1.12.7";
        const FIRST_SUPPORTED_UPGRADE_TO_VERSION: &str = "1.10.0";
        const LAST_SUPPORTED_UPGRADE_TO_VERSION: &str = "1.12.7";

        let upgrade_list = [
            (
                v1_9_to_v1_10 as fn(&Path, u32, u32, u32) -> Result<(), anyhow::Error>,
                "1",
                "10",
                "0",
            ),
            (v1_10_to_v1_11, "1", "11", "0"),
            (v1_11_to_v1_12, "1", "12", "0"),
            (v1_12_to_v1_12_3, "1", "12", "3"),
        ];

        let no_upgrade: usize = upgrade_list.len();

        let (current_major, current_minor, current_patch) = self.current_version;

        let start_at = match (current_major, current_minor, current_patch) {
            (1, 9, _) => 0,
            (1, 10, _) => 1,
            (1, 11, _) => 2,
            (1, 12, 0..=2) => 3,
            (1, 12, 3..=7) => no_upgrade,
            _ => {
                bail!("Unsupported current version {current_major}.{current_minor}.{current_patch}. Can only upgrade from versions in range [{}-{}]",
                      FIRST_SUPPORTED_UPGRADE_FROM_VERSION,
                      LAST_SUPPORTED_UPGRADE_FROM_VERSION);
            }
        };

        let (target_major, target_minor, target_patch) = self.target_version;

        let ends_at = match (target_major, target_minor, target_patch) {
            (1, 10, _) => 0,
            (1, 11, _) => 1,
            (1, 12, 0..=2) => 2,
            (1, 12, 3..=7) => 3,
            _ => {
                bail!("Unsupported target version {target_major}.{target_minor}.{target_patch}. Can only upgrade to versions in range [{}-{}]",
                      FIRST_SUPPORTED_UPGRADE_TO_VERSION,
                      LAST_SUPPORTED_UPGRADE_TO_VERSION);
            }
        };

        println!("Starting the upgrade from {current_major}.{current_minor}.{current_patch} to {target_major}.{target_minor}.{target_patch}");

        if start_at == no_upgrade {
            println!("No upgrade operation to perform, writing VERSION file");
            create_version_file(
                &self.db_path,
                &target_major.to_string(),
                &target_minor.to_string(),
                &target_patch.to_string(),
            )
            .context("while writing VERSION file after the upgrade")?;
            println!("Success");
            return Ok(());
        }

        #[allow(clippy::needless_range_loop)]
        for index in start_at..=ends_at {
            let (func, major, minor, patch) = upgrade_list[index];
            (func)(&self.db_path, current_major, current_minor, current_patch)?;
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
