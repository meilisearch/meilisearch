use std::fs;
use std::io::ErrorKind;
use std::path::Path;

use self::error::VersionFileError;

mod error;

pub const VERSION_FILE_NAME: &str = "VERSION";

static VERSION_MAJOR: &str = env!("CARGO_PKG_VERSION_MAJOR");
static VERSION_MINOR: &str = env!("CARGO_PKG_VERSION_MINOR");
static VERSION_PATCH: &str = env!("CARGO_PKG_VERSION_PATCH");

// Persists the version of the current MeiliSearch binary to a VERSION file
pub fn create_version_file(db_path: &Path) -> anyhow::Result<()> {
    let version_path = db_path.join(VERSION_FILE_NAME);
    fs::write(
        version_path,
        format!("{}.{}.{}", VERSION_MAJOR, VERSION_MINOR, VERSION_PATCH),
    )?;

    Ok(())
}

// Ensures MeiliSearch version is compatible with the database, returns an error versions mismatch.
pub fn check_version_file(db_path: &Path) -> anyhow::Result<()> {
    let version_path = db_path.join(VERSION_FILE_NAME);

    match fs::read_to_string(&version_path) {
        Ok(version) => {
            let version_components = version.split('.').collect::<Vec<_>>();
            let (major, minor, patch) = match &version_components[..] {
                [major, minor, patch] => (major.to_string(), minor.to_string(), patch.to_string()),
                _ => return Err(VersionFileError::MalformedVersionFile.into()),
            };

            if major != VERSION_MAJOR || minor != VERSION_MINOR {
                return Err(VersionFileError::VersionMismatch {
                    major,
                    minor,
                    patch,
                }
                .into());
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
