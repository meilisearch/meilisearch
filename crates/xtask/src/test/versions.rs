use std::{collections::BTreeMap, fmt::Display, path::PathBuf};

use crate::common::assets::{Asset, AssetFormat};
use anyhow::Context;
use cargo_metadata::semver::Version;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug)]
pub enum VersionOrLatest {
    Version(Version),
    Latest,
}

impl<'a> Deserialize<'a> for VersionOrLatest {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'a>,
    {
        let s: &str = Deserialize::deserialize(deserializer)?;

        if s.eq_ignore_ascii_case("latest") {
            Ok(VersionOrLatest::Latest)
        } else {
            let version = Version::parse(s).map_err(serde::de::Error::custom)?;
            Ok(VersionOrLatest::Version(version))
        }
    }
}

impl Serialize for VersionOrLatest {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        match self {
            VersionOrLatest::Version(v) => serializer.serialize_str(&v.to_string()),
            VersionOrLatest::Latest => serializer.serialize_str("latest"),
        }
    }
}

impl VersionOrLatest {
    pub fn binary_path(&self, asset_folder: &str) -> anyhow::Result<Option<PathBuf>> {
        match self {
            VersionOrLatest::Version(version) => {
                let mut asset_folder: PathBuf =
                    asset_folder.parse().context("parsing asset folder")?;
                let arch = get_arch()?;
                let local_filename = format!("meilisearch-{version}-{arch}");
                asset_folder.push(local_filename);
                Ok(Some(asset_folder))
            }
            VersionOrLatest::Latest => Ok(None),
        }
    }
}

impl Display for VersionOrLatest {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            VersionOrLatest::Version(v) => v.fmt(f),
            VersionOrLatest::Latest => write!(f, "latest"),
        }
    }
}

async fn get_sha256(version: &Version, asset_name: &str) -> anyhow::Result<String> {
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

    let url =
        format!("https://api.github.com/repos/meilisearch/meilisearch/releases/tags/v{version}");
    let client = reqwest::Client::builder()
        .user_agent("Meilisearch bench xtask")
        .build()
        .context("failed to build reqwest client")?;
    let body = client.get(url).send().await?.text().await?;
    let data: GithubRelease = serde_json::from_str(&body)?;

    let digest = data
        .assets
        .into_iter()
        .find(|asset| asset.name.as_str() == asset_name)
        .with_context(|| format!("asset {asset_name} not found in release v{version}"))?
        .digest
        .with_context(|| format!("asset {asset_name} has no digest"))?;

    let sha256 =
        digest.strip_prefix("sha256:").map(|s| s.to_string()).context("invalid sha256 format")?;

    Ok(sha256)
}

pub fn get_arch() -> anyhow::Result<&'static str> {
    let arch;

    // linux-aarch64
    #[cfg(all(target_os = "linux", target_arch = "aarch64"))]
    {
        arch = "linux-aarch64";
    }

    // linux-amd64
    #[cfg(all(target_os = "linux", target_arch = "x86_64"))]
    {
        arch = "linux-amd64";
    }

    // macos-amd64
    #[cfg(all(target_os = "macos", target_arch = "x86_64"))]
    {
        arch = "macos-amd64";
    }

    // macos-apple-silicon
    #[cfg(all(target_os = "macos", target_arch = "aarch64"))]
    {
        arch = "macos-apple-silicon";
    }

    // windows-amd64
    #[cfg(all(target_os = "windows", target_arch = "x86_64"))]
    {
        arch = "windows-amd64";
    }

    if arch.is_empty() {
        anyhow::bail!("unsupported platform");
    }

    Ok(arch)
}

async fn add_asset(assets: &mut BTreeMap<String, Asset>, version: &Version) -> anyhow::Result<()> {
    let arch = get_arch()?;
    let local_filename = format!("meilisearch-{version}-{arch}");
    if assets.contains_key(&local_filename) {
        return Ok(());
    }

    let filename = format!("meilisearch-{arch}");

    // Try to get the sha256 but it may fail if Github is rate limiting us
    // We hardcode some values to speed up tests and avoid hitting Github
    // Also, versions prior to 1.15 don't have sha256 available anyway
    let sha256 = match local_filename.as_str() {
        "meilisearch-1.12.0-macos-apple-silicon" => {
            Some(String::from("3b384707a5df9edf66f9157f0ddb70dcd3ac84d4887149169cf93067d06717b7"))
        }
        _ => match get_sha256(version, &filename).await {
            Ok(sha256) => Some(sha256),
            Err(err) => {
                tracing::warn!("failed to get sha256 for version {version}: {err}");
                None
            }
        },
    };

    let url = format!(
        "https://github.com/meilisearch/meilisearch/releases/download/v{version}/{filename}"
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

pub async fn expand_assets_with_versions(
    assets: &mut BTreeMap<String, Asset>,
    versions: &[Version],
) -> anyhow::Result<()> {
    for version in versions {
        add_asset(assets, version).await?;
    }

    Ok(())
}
