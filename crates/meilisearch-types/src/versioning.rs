use std::fs;
use std::io::{self, ErrorKind};
use std::path::Path;

/// The name of the file that contains the version of the database.
pub const VERSION_FILE_NAME: &str = "VERSION";

pub static VERSION_MAJOR: &str = env!("CARGO_PKG_VERSION_MAJOR");
pub static VERSION_MINOR: &str = env!("CARGO_PKG_VERSION_MINOR");
pub static VERSION_PATCH: &str = env!("CARGO_PKG_VERSION_PATCH");

/// Persists the version of the current Meilisearch binary to a VERSION file
pub fn update_version_file_for_dumpless_upgrade(
    db_path: &Path,
    from: (u32, u32, u32),
    to: (u32, u32, u32),
) -> Result<(), VersionFileError> {
    let (from_major, from_minor, from_patch) = from;
    let (to_major, to_minor, to_patch) = to;

    if from_major > to_major
        || (from_major == to_major && from_minor > to_minor)
        || (from_major == to_major && from_minor == to_minor && from_patch > to_patch)
    {
        Err(VersionFileError::DowngradeNotSupported {
            major: from_major,
            minor: from_minor,
            patch: from_patch,
        })
    } else if from_major < 1 || (from_major == to_major && from_minor < 12) {
        Err(VersionFileError::TooOldForAutomaticUpgrade {
            major: from_major,
            minor: from_minor,
            patch: from_patch,
        })
    } else {
        create_current_version_file(db_path)?;
        Ok(())
    }
}

/// Persists the version of the current Meilisearch binary to a VERSION file
pub fn create_current_version_file(db_path: &Path) -> io::Result<()> {
    create_version_file(db_path, VERSION_MAJOR, VERSION_MINOR, VERSION_PATCH)
}

pub fn create_version_file(
    db_path: &Path,
    major: &str,
    minor: &str,
    patch: &str,
) -> io::Result<()> {
    let version_path = db_path.join(VERSION_FILE_NAME);
    fs::write(version_path, format!("{}.{}.{}", major, minor, patch))
}

pub fn get_version(db_path: &Path) -> Result<(u32, u32, u32), VersionFileError> {
    let version_path = db_path.join(VERSION_FILE_NAME);

    match fs::read_to_string(version_path) {
        Ok(version) => parse_version(&version),
        Err(error) => match error.kind() {
            ErrorKind::NotFound => Err(VersionFileError::MissingVersionFile),
            _ => Err(error.into()),
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

    #[error(transparent)]
    IoError(#[from] std::io::Error),
}
