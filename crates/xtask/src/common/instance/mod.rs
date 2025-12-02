use std::fmt::Display;
use std::path::PathBuf;

use serde::{Deserialize, Serialize};

mod release;

pub use release::{add_releases_to_assets, Release};

/// A binary to execute on a temporary DB.
///
/// - The URL of the binary will be in the form <http://localhost:PORT>, where `PORT`
///   is selected by the runner.
/// - The database will be temporary, cleaned before use, and will be selected by the runner.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Binary {
    /// Describes how this binary should be instantiated
    pub source: BinarySource,
    /// Extra CLI arguments to pass to the binary.
    ///
    /// Should be Meilisearch CLI options.
    pub extra_cli_args: Vec<String>,
}

impl Display for Binary {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.source)?;
        if !self.extra_cli_args.is_empty() {
            write!(f, "with arguments: {:?}", self.extra_cli_args)?;
        }
        Ok(())
    }
}

impl Binary {
    pub fn as_release(&self) -> Option<&Release> {
        if let BinarySource::Release(release) = &self.source {
            Some(release)
        } else {
            None
        }
    }

    pub fn binary_path(&self, asset_folder: &str) -> anyhow::Result<Option<PathBuf>> {
        self.source.binary_path(asset_folder)
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
/// Description of how to get a binary to instantiate.
pub enum BinarySource {
    /// Compile and run the binary from the current repository.=
    CompileFromSource { edition: Edition },
    /// Get a release from GitHub
    Release(Release),
    /// Run the binary from the specified local path.
    Path(PathBuf),
}

impl Display for BinarySource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            BinarySource::CompileFromSource { edition: Edition::Community } => {
                f.write_str("git with community edition")
            }
            BinarySource::CompileFromSource { edition: Edition::Enterprise } => {
                f.write_str("git with enterprise edition")
            }
            BinarySource::Release(release) => write!(f, "{release}"),
            BinarySource::Path(path) => write!(f, "binary at `{}`", path.display()),
        }
    }
}

impl Default for BinarySource {
    fn default() -> Self {
        Self::CompileFromSource { edition: Default::default() }
    }
}

impl BinarySource {
    fn binary_path(&self, asset_folder: &str) -> anyhow::Result<Option<PathBuf>> {
        Ok(match self {
            Self::Release(release) => Some(release.binary_path(asset_folder)?),
            Self::CompileFromSource { .. } => None,
            Self::Path(path) => Some(path.clone()),
        })
    }
}

#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize)]
pub enum Edition {
    #[default]
    Community,
    Enterprise,
}

impl Edition {
    fn binary_base(&self) -> &'static str {
        match self {
            Edition::Community => "meilisearch",
            Edition::Enterprise => "meilisearch-enterprise",
        }
    }
}
