use std::fs;
use std::io::{ErrorKind, Write};
use std::path::Path;

use milli::heed;
use tempfile::NamedTempFile;

/// The name of the file that contains the version of the database.
pub const VERSION_FILE_NAME: &str = "VERSION";

pub static VERSION_MAJOR: &str = env!("CARGO_PKG_VERSION_MAJOR");
pub static VERSION_MINOR: &str = env!("CARGO_PKG_VERSION_MINOR");
pub static VERSION_PATCH: &str = env!("CARGO_PKG_VERSION_PATCH");

/// Persists the version of the current Meilisearch binary to a VERSION file
pub fn create_current_version_file(db_path: &Path) -> anyhow::Result<()> {
    create_version_file(db_path, VERSION_MAJOR, VERSION_MINOR, VERSION_PATCH)
}

pub fn create_version_file(
    db_path: &Path,
    major: &str,
    minor: &str,
    patch: &str,
) -> anyhow::Result<()> {
    let version_path = db_path.join(VERSION_FILE_NAME);
    // In order to persist the file later we must create it in the `data.ms` and not in `/tmp`
    let mut file = NamedTempFile::new_in(db_path)?;
    file.write_all(format!("{}.{}.{}", major, minor, patch).as_bytes())?;
    file.flush()?;
    file.persist(version_path)?;
    Ok(())
}

pub fn get_version(db_path: &Path) -> Result<(u32, u32, u32), VersionFileError> {
    let version_path = db_path.join(VERSION_FILE_NAME);

    match fs::read_to_string(version_path) {
        Ok(version) => parse_version(&version),
        Err(error) => match error.kind() {
            ErrorKind::NotFound => Err(VersionFileError::MissingVersionFile),
            _ => Err(anyhow::Error::from(error).into()),
        },
    }
}

pub fn parse_version(version: &str) -> Result<(u32, u32, u32), VersionFileError> {
    let version_components = version.trim().split('.').collect::<Vec<_>>();
    let (major, minor, patch) = match &version_components[..] {
        [major, minor, patch] => (
            major.parse().map_err(|e| VersionFileError::MalformedVersionFile {
                context: format!("Could not parse the major: {e}"),
            })?,
            minor.parse().map_err(|e| VersionFileError::MalformedVersionFile {
                context: format!("Could not parse the minor: {e}"),
            })?,
            patch.parse().map_err(|e| VersionFileError::MalformedVersionFile {
                context: format!("Could not parse the patch: {e}"),
            })?,
        ),
        _ => {
            return Err(VersionFileError::MalformedVersionFile {
                context: format!(
                    "The version contains {} parts instead of 3 (major, minor and patch)",
                    version_components.len()
                ),
            })
        }
    };
    Ok((major, minor, patch))
}

#[derive(thiserror::Error, Debug)]
pub enum VersionFileError {
    #[error(
        "Meilisearch (v{}) failed to infer the version of the database.
        To update Meilisearch please follow our guide on https://www.meilisearch.com/docs/learn/update_and_migration/updating.",
        env!("CARGO_PKG_VERSION").to_string()
    )]
    MissingVersionFile,
    #[error("Version file is corrupted and thus Meilisearch is unable to determine the version of the database. {context}")]
    MalformedVersionFile { context: String },
    #[error(
        "Your database version ({major}.{minor}.{patch}) is incompatible with your current engine version ({}).\n\
        To migrate data between Meilisearch versions, please follow our guide on https://www.meilisearch.com/docs/learn/update_and_migration/updating.",
        env!("CARGO_PKG_VERSION").to_string()
    )]
    VersionMismatch { major: u32, minor: u32, patch: u32 },
    #[error("Database version {major}.{minor}.{patch} is higher than the Meilisearch version {VERSION_MAJOR}.{VERSION_MINOR}.{VERSION_PATCH}. Downgrade is not supported")]
    DowngradeNotSupported { major: u32, minor: u32, patch: u32 },
    #[error("Database version {major}.{minor}.{patch} is too old for the experimental dumpless upgrade feature. Please generate a dump using the v{major}.{minor}.{patch} and import it in the v{VERSION_MAJOR}.{VERSION_MINOR}.{VERSION_PATCH}")]
    TooOldForAutomaticUpgrade { major: u32, minor: u32, patch: u32 },
    #[error("Error while modifying the database: {0}")]
    ErrorWhileModifyingTheDatabase(#[from] heed::Error),

    #[error(transparent)]
    AnyhowError(#[from] anyhow::Error),
}
