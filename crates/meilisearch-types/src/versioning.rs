use std::fs;
use std::io::{ErrorKind, Write};
use std::path::Path;

use milli::heed;
use tempfile::NamedTempFile;

/// The name of the file that contains the version of the database.
pub const VERSION_FILE_NAME: &str = "VERSION";

pub use milli::constants::{VERSION_MAJOR, VERSION_MINOR, VERSION_PATCH};

/// Persists the version of the current Meilisearch binary to a VERSION file
pub fn create_current_version_file(db_path: &Path) -> anyhow::Result<()> {
    create_version_file(db_path, VERSION_MAJOR, VERSION_MINOR, VERSION_PATCH)
}

pub fn create_version_file(
    db_path: &Path,
    major: u32,
    minor: u32,
    patch: u32,
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
        Ok(version) if version.trim().is_empty() => Err(VersionFileError::MissingVersionFile),
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_version_valid() {
        assert_eq!(parse_version("1.2.3").unwrap(), (1, 2, 3));
        assert_eq!(parse_version("  1.2.3  ").unwrap(), (1, 2, 3));
    }

    #[test]
    fn parse_version_missing_parts() {
        let err = parse_version("1.2").unwrap_err();
        assert!(matches!(err, VersionFileError::MalformedVersionFile { .. }));
    }

    #[test]
    fn parse_version_non_numeric() {
        let err = parse_version("a.b.c").unwrap_err();
        assert!(matches!(err, VersionFileError::MalformedVersionFile { .. }));
    }

    #[test]
    fn get_version_missing_file() {
        let dir = tempfile::tempdir().unwrap();
        let err = get_version(dir.path()).unwrap_err();
        assert!(matches!(err, VersionFileError::MissingVersionFile));
    }

    #[test]
    fn get_version_empty_file() {
        let dir = tempfile::tempdir().unwrap();
        fs::write(dir.path().join(VERSION_FILE_NAME), "").unwrap();
        let err = get_version(dir.path()).unwrap_err();
        assert!(matches!(err, VersionFileError::MissingVersionFile));
    }

    #[test]
    fn get_version_whitespace_only_file() {
        let dir = tempfile::tempdir().unwrap();
        fs::write(dir.path().join(VERSION_FILE_NAME), "  \n  ").unwrap();
        let err = get_version(dir.path()).unwrap_err();
        assert!(matches!(err, VersionFileError::MissingVersionFile));
    }

    #[test]
    fn get_version_valid_file() {
        let dir = tempfile::tempdir().unwrap();
        fs::write(dir.path().join(VERSION_FILE_NAME), "1.12.0").unwrap();
        assert_eq!(get_version(dir.path()).unwrap(), (1, 12, 0));
    }
}
