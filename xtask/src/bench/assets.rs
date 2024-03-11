use std::collections::BTreeMap;
use std::io::{Read as _, Seek as _, Write as _};

use anyhow::{bail, Context};
use futures_util::TryStreamExt as _;
use serde::Deserialize;
use sha2::Digest;

use super::client::Client;

#[derive(Deserialize, Clone)]
pub struct Asset {
    pub local_location: Option<String>,
    pub remote_location: Option<String>,
    #[serde(default)]
    pub format: AssetFormat,
    pub sha256: Option<String>,
}

#[derive(Deserialize, Default, Copy, Clone)]
pub enum AssetFormat {
    #[default]
    Auto,
    Json,
    NdJson,
    Raw,
}

impl AssetFormat {
    pub fn to_content_type(self, filename: &str) -> &'static str {
        match self {
            AssetFormat::Auto => Self::auto_detect(filename).to_content_type(filename),
            AssetFormat::Json => "application/json",
            AssetFormat::NdJson => "application/x-ndjson",
            AssetFormat::Raw => "application/octet-stream",
        }
    }

    fn auto_detect(filename: &str) -> Self {
        let path = std::path::Path::new(filename);
        match path.extension().and_then(|extension| extension.to_str()) {
            Some(extension) if extension.eq_ignore_ascii_case("json") => Self::Json,
            Some(extension) if extension.eq_ignore_ascii_case("ndjson") => Self::NdJson,
            extension => {
                tracing::warn!(asset = filename, ?extension, "asset has format `Auto`, but extension was not recognized. Specify `Raw` format to suppress this warning.");
                AssetFormat::Raw
            }
        }
    }
}

pub fn fetch_asset(
    name: &str,
    assets: &BTreeMap<String, Asset>,
    asset_folder: &str,
) -> anyhow::Result<(std::fs::File, AssetFormat)> {
    let asset =
        assets.get(name).with_context(|| format!("could not find asset with name '{name}'"))?;
    let filename = if let Some(local_filename) = &asset.local_location {
        local_filename.clone()
    } else {
        format!("{asset_folder}/{name}")
    };

    Ok((
        std::fs::File::open(&filename)
            .with_context(|| format!("could not open asset '{name}' at '{filename}'"))?,
        asset.format,
    ))
}

#[tracing::instrument(skip(client, assets), fields(asset_count = assets.len()))]
pub async fn fetch_assets(
    client: &Client,
    assets: &BTreeMap<String, Asset>,
    asset_folder: &str,
) -> anyhow::Result<()> {
    let mut download_tasks = tokio::task::JoinSet::new();
    for (name, asset) in assets {
        // trying local
        if let Some(local) = &asset.local_location {
            match std::fs::File::open(local) {
                Ok(file) => {
                    if check_sha256(name, asset, file)? {
                        continue;
                    } else {
                        tracing::warn!(asset = name, file = local, "found local resource for asset but hash differed, skipping to asset store");
                    }
                }
                Err(error) => match error.kind() {
                    std::io::ErrorKind::NotFound => { /* file does not exist, go to remote, no need for logs */
                    }
                    _ => tracing::warn!(
                        error = &error as &dyn std::error::Error,
                        "error checking local resource, skipping to asset store"
                    ),
                },
            }
        }

        // checking asset store
        let store_filename = format!("{}/{}", asset_folder, name);

        match std::fs::File::open(&store_filename) {
            Ok(file) => {
                if check_sha256(name, asset, file)? {
                    continue;
                } else {
                    tracing::warn!(asset = name, file = store_filename, "found resource for asset in asset store, but hash differed, skipping to remote method");
                }
            }
            Err(error) => match error.kind() {
                std::io::ErrorKind::NotFound => { /* file does not exist, go to remote, no need for logs */
                }
                _ => tracing::warn!(
                    error = &error as &dyn std::error::Error,
                    "error checking resource in store, skipping to remote method"
                ),
            },
        }

        // downloading remote
        match &asset.remote_location {
            Some(location) => {
                std::fs::create_dir_all(asset_folder).with_context(|| format!("could not create asset folder at {asset_folder}"))?;
                download_tasks.spawn({
                    let client = client.clone();
                    let name = name.to_string();
                    let location = location.to_string();
                    let store_filename = store_filename.clone();
                    let asset = asset.clone();
                    download_asset(client, name, asset, location, store_filename)});
            },
            None => bail!("asset {name} has no remote location, but was not found locally or in the asset store"),
        }
    }

    while let Some(res) = download_tasks.join_next().await {
        res.context("download task panicked")?.context("download task failed")?;
    }

    Ok(())
}

fn check_sha256(name: &str, asset: &Asset, mut file: std::fs::File) -> anyhow::Result<bool> {
    let mut bytes = Vec::new();
    file.read_to_end(&mut bytes).with_context(|| format!("hashing file for asset {name}"))?;
    let mut file_hash = sha2::Sha256::new();
    file_hash.update(&bytes);
    let file_hash = file_hash.finalize();
    let file_hash = format!("{:x}", file_hash);
    tracing::debug!(hash = file_hash, "hashed local file");

    Ok(match &asset.sha256 {
        Some(hash) => {
            tracing::debug!(hash, "hash from workload");
            if hash.to_ascii_lowercase() == file_hash {
                true
            } else {
                tracing::warn!(
                    file_hash,
                    asset_hash = hash.to_ascii_lowercase(),
                    "hashes don't match"
                );
                false
            }
        }
        None => {
            tracing::warn!(sha256 = file_hash, "Skipping hash for asset {name} that doesn't have one. Please add it to workload file");
            true
        }
    })
}

#[tracing::instrument(skip(client, asset, name), fields(asset = name))]
async fn download_asset(
    client: Client,
    name: String,
    asset: Asset,
    src: String,
    dest_filename: String,
) -> anyhow::Result<()> {
    let context = || format!("failure downloading asset {name} from {src}");

    let response = client.get(&src).send().await.with_context(context)?;

    let file = std::fs::File::options()
        .create(true)
        .truncate(true)
        .write(true)
        .read(true)
        .open(&dest_filename)
        .with_context(|| format!("creating destination file {dest_filename}"))
        .with_context(context)?;

    let mut dest = std::io::BufWriter::new(
        file.try_clone().context("cloning I/O handle").with_context(context)?,
    );

    let total_len: Option<u64> = response
        .headers()
        .get(reqwest::header::CONTENT_LENGTH)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.parse().ok());

    let progress = tokio::spawn({
        let name = name.clone();
        async move {
            loop {
                match file.metadata().context("could not get file metadata") {
                    Ok(metadata) => {
                        let len = metadata.len();
                        tracing::info!(
                            asset = name,
                            downloaded_bytes = len,
                            total_bytes = total_len,
                            "asset download in progress"
                        );
                    }
                    Err(error) => {
                        tracing::warn!(%error, "could not get file metadata");
                    }
                }
                tokio::time::sleep(std::time::Duration::from_secs(60)).await;
            }
        }
    });

    let writing_context = || format!("while writing to destination file at {dest_filename}");

    let mut response = response.bytes_stream();

    while let Some(bytes) =
        response.try_next().await.context("while downloading file").with_context(context)?
    {
        dest.write_all(&bytes).with_context(writing_context).with_context(context)?;
    }

    progress.abort();

    let mut file = dest.into_inner().with_context(writing_context).with_context(context)?;

    file.rewind().context("while rewinding asset file")?;

    if !check_sha256(&name, &asset, file)? {
        bail!("asset '{name}': sha256 mismatch for file {dest_filename} downloaded from {src}")
    }

    Ok(())
}
