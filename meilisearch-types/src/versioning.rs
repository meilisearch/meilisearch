use std::fs;
use std::io::{self, ErrorKind};
use std::path::Path;

/// The name of the file that contains the version of the database.
pub const VERSION_FILE_NAME: &str = "VERSION";

static VERSION_MAJOR: &str = env!("CARGO_PKG_VERSION_MAJOR");
static VERSION_MINOR: &str = env!("CARGO_PKG_VERSION_MINOR");
static VERSION_PATCH: &str = env!("CARGO_PKG_VERSION_PATCH");

/// Persists the version of the current Meilisearch binary to a VERSION file
pub fn create_version_file(db_path: &Path) -> io::Result<()> {
    let version_path = db_path.join(VERSION_FILE_NAME);
    fs::write(version_path, format!("{}.{}.{}", VERSION_MAJOR, VERSION_MINOR, VERSION_PATCH))
}

/// Ensures Meilisearch version is compatible with the database, returns an error versions mismatch.
pub fn check_version_file(db_path: &Path) -> anyhow::Result<()> {
    let version_path = db_path.join(VERSION_FILE_NAME);

    match fs::read_to_string(version_path) {
        Ok(version) => {
            let version_components = version.split('.').collect::<Vec<_>>();
            let (major, minor, patch) = match &version_components[..] {
                [major, minor, patch] => (major.to_string(), minor.to_string(), patch.to_string()),
                _ => return Err(VersionFileError::MalformedVersionFile.into()),
            };

            if major != VERSION_MAJOR || minor != VERSION_MINOR {
                return Err(VersionFileError::VersionMismatch { major, minor, patch }.into());
            }
        }
        Err(error) => {
            return match error.kind() {
                ErrorKind::NotFound => Err(VersionFileError::MissingVersionFile.into()),
                _ => Err(error.into()),
            }
        }
    }

    Ok(())
}

#[derive(thiserror::Error, Debug)]
pub enum VersionFileError {
    #[error(
        "Meilisearch (v{}) failed to infer the version of the database.
        To update Meilisearch please follow our guide on https://www.meilisearch.com/docs/learn/update_and_migration/updating.",
        env!("CARGO_PKG_VERSION").to_string()
    )]
    MissingVersionFile,
    #[error("Version file is corrupted and thus Meilisearch is unable to determine the version of the database.")]
    MalformedVersionFile,
    #[error(
        "Your database version ({major}.{minor}.{patch}) is incompatible with your current engine version ({}).\n\
        To migrate data between Meilisearch versions, please follow our guide on https://www.meilisearch.com/docs/learn/update_and_migration/updating.",
        env!("CARGO_PKG_VERSION").to_string()
    )]
    VersionMismatch { major: String, minor: String, patch: String },
}
