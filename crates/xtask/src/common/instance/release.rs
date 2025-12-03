use std::collections::BTreeMap;
use std::fmt::Display;
use std::path::PathBuf;

use anyhow::Context;
use cargo_metadata::semver::Version;
use serde::{Deserialize, Serialize};

use super::Edition;
use crate::common::assets::{Asset, AssetFormat};

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct Release {
    #[serde(default)]
    pub edition: Edition,
    pub version: Version,
}

impl Display for Release {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "v{}", self.version)?;
        match self.edition {
            Edition::Community => f.write_str(" Community Edition"),
            Edition::Enterprise => f.write_str(" Enterprise Edition"),
        }
    }
}

impl Release {
    pub fn binary_path(&self, asset_folder: &str) -> anyhow::Result<PathBuf> {
        let mut asset_folder: PathBuf = asset_folder
            .parse()
            .with_context(|| format!("parsing asset folder `{asset_folder}` as a path"))?;
        asset_folder.push(self.local_filename()?);
        Ok(asset_folder)
    }

    fn local_filename(&self) -> anyhow::Result<String> {
        let version = &self.version;
        let arch = get_arch()?;
        let base = self.edition.binary_base();

        Ok(format!("{base}-{version}-{arch}"))
    }

    fn remote_filename(&self) -> anyhow::Result<String> {
        let arch = get_arch()?;
        let base = self.edition.binary_base();

        Ok(format!("{base}-{arch}"))
    }

    async fn fetch_sha256(&self) -> anyhow::Result<String> {
        let version = &self.version;
        let asset_name = self.remote_filename()?;

        // If version is lower than 1.15 there is no point in trying to get the sha256, GitHub didn't support it
        if *version < Version::parse("1.15.0")? {
            anyhow::bail!("version is lower than 1.15, sha256 not available");
        }

        #[derive(Deserialize)]
        struct GithubReleaseAsset {
            name: String,
            digest: Option<String>,
        }

        #[derive(Deserialize)]
        struct GithubRelease {
            assets: Vec<GithubReleaseAsset>,
        }

        let url = format!(
            "https://api.github.com/repos/meilisearch/meilisearch/releases/tags/v{version}"
        );

        let client = reqwest::Client::builder()
            .user_agent("Meilisearch bench xtask")
            .build()
            .context("failed to build reqwest client")?;
        let body = client.get(url).send().await?.text().await?;
        let data: GithubRelease = serde_json::from_str(&body)?;

        let digest = data
            .assets
            .into_iter()
            .find(|asset| asset.name.as_str() == asset_name.as_str())
            .with_context(|| format!("asset {asset_name} not found in release {self}"))?
            .digest
            .with_context(|| format!("asset {asset_name} has no digest"))?;

        let sha256 = digest
            .strip_prefix("sha256:")
            .map(|s| s.to_string())
            .context("invalid sha256 format")?;

        Ok(sha256)
    }

    async fn add_asset(&self, assets: &mut BTreeMap<String, Asset>) -> anyhow::Result<()> {
        let local_filename = self.local_filename()?;
        let version = &self.version;
        if assets.contains_key(&local_filename) {
            return Ok(());
        }

        let remote_filename = self.remote_filename()?;

        // Try to get the sha256 but it may fail if Github is rate limiting us
        // We hardcode some values to speed up tests and avoid hitting Github
        // Also, versions prior to 1.15 don't have sha256 available anyway
        let sha256 = match local_filename.as_str() {
            "meilisearch-1.12.0-macos-apple-silicon" => {
                Some("3b384707a5df9edf66f9157f0ddb70dcd3ac84d4887149169cf93067d06717b7".into())
            }
            "meilisearch-1.12.0-linux-amd64" => {
                Some("865a3fc222e3b3bd1f4b64346cb114b9669af691aae28d71fa68dbf39427abcf".into())
            }
            _ => match self.fetch_sha256().await {
                Ok(sha256) => Some(sha256),
                Err(err) => {
                    tracing::warn!("failed to get sha256 for release {self}: {err}");
                    None
                }
            },
        };

        let url = format!(
        "https://github.com/meilisearch/meilisearch/releases/download/v{version}/{remote_filename}"
    );

        let asset = Asset {
            local_location: Some(local_filename.clone()),
            remote_location: Some(url),
            format: AssetFormat::Raw,
            sha256,
        };

        assets.insert(local_filename, asset);

        Ok(())
    }
}

pub fn get_arch() -> anyhow::Result<&'static str> {
    // linux-aarch64
    #[cfg(all(target_os = "linux", target_arch = "aarch64"))]
    {
        Ok("linux-aarch64")
    }

    // linux-amd64
    #[cfg(all(target_os = "linux", target_arch = "x86_64"))]
    {
        Ok("linux-amd64")
    }

    // macos-amd64
    #[cfg(all(target_os = "macos", target_arch = "x86_64"))]
    {
        Ok("macos-amd64")
    }

    // macos-apple-silicon
    #[cfg(all(target_os = "macos", target_arch = "aarch64"))]
    {
        Ok("macos-apple-silicon")
    }

    // windows-amd64
    #[cfg(all(target_os = "windows", target_arch = "x86_64"))]
    {
        Ok("windows-amd64")
    }

    #[cfg(not(all(target_os = "windows", target_arch = "x86_64")))]
    #[cfg(not(all(target_os = "linux", target_arch = "aarch64")))]
    #[cfg(not(all(target_os = "linux", target_arch = "x86_64")))]
    #[cfg(not(all(target_os = "macos", target_arch = "aarch64")))]
    anyhow::bail!("unsupported platform")
}

pub async fn add_releases_to_assets(
    assets: &mut BTreeMap<String, Asset>,
    releases: impl IntoIterator<Item = &Release>,
) -> anyhow::Result<()> {
    for release in releases {
        release.add_asset(assets).await?;
    }

    Ok(())
}
