use std::collections::BTreeMap;

use crate::common::assets::{Asset, AssetFormat};
use anyhow::Context;
use cargo_metadata::semver::Version;
use serde::Deserialize;

async fn get_sha256(version: &Version, asset_name: &str) -> anyhow::Result<String> {
    #[derive(Deserialize)]
    struct GithubReleaseAsset {
        name: String,
        digest: String,
    }

    #[derive(Deserialize)]
    struct GithubRelease {
        assets: Vec<GithubReleaseAsset>,
    }

    let url =
        format!("https://api.github.com/repos/meilisearch/meilisearch/releases/tags/v{version}");
    let data: GithubRelease = reqwest::get(url).await?.json().await?;

    let digest = data
        .assets
        .into_iter()
        .find(|asset| asset.name.as_str() == asset_name)
        .with_context(|| format!("asset {asset_name} not found in release v{version}"))?
        .digest;

    let sha256 =
        digest.strip_prefix("sha256:").map(|s| s.to_string()).context("invalid sha256 format")?;

    Ok(sha256)
}

async fn add_asset(assets: &mut BTreeMap<String, Asset>, version: &Version) -> anyhow::Result<()> {
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

    let local_filename = format!("meilisearch-{version}-{arch}");
    if assets.contains_key(&local_filename) {
        return Ok(());
    }

    let filename = format!("meilisearch-{arch}");

    // Try to get the sha256 but it may fail if Github is rate limiting us
    let sha256 = match get_sha256(version, &filename).await {
        Ok(sha256) => Some(sha256),
        Err(err) => {
            eprintln!("⚠️ Warning: could not get sha256 from GitHub: {err}. Proceeding without integrity check.");
            None
        }
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
