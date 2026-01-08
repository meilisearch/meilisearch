use std::path::{Path, PathBuf};

use base64::{engine::general_purpose, Engine as _};
use rand::{distributions::Alphanumeric, Rng};
use reqwest::Url;

use crate::error::OpenAIError;

fn create_paths<P: AsRef<Path>>(url: &Url, base_dir: P) -> (PathBuf, PathBuf) {
    let mut dir = PathBuf::from(base_dir.as_ref());
    let mut path = dir.clone();
    let segments = url.path_segments().map(|c| c.collect::<Vec<_>>());
    if let Some(segments) = segments {
        for (idx, segment) in segments.iter().enumerate() {
            if idx != segments.len() - 1 {
                dir.push(segment);
            }
            path.push(segment);
        }
    }

    (dir, path)
}

pub(crate) async fn download_url<P: AsRef<Path>>(
    url: &str,
    dir: P,
) -> Result<PathBuf, OpenAIError> {
    let parsed_url = Url::parse(url).map_err(|e| OpenAIError::FileSaveError(e.to_string()))?;
    let response = reqwest::get(url)
        .await
        .map_err(|e| OpenAIError::FileSaveError(e.to_string()))?;

    if !response.status().is_success() {
        return Err(OpenAIError::FileSaveError(format!(
            "couldn't download file, status: {}, url: {url}",
            response.status()
        )));
    }

    let (dir, file_path) = create_paths(&parsed_url, dir);

    tokio::fs::create_dir_all(dir.as_path())
        .await
        .map_err(|e| OpenAIError::FileSaveError(format!("{}, dir: {}", e, dir.display())))?;

    tokio::fs::write(
        file_path.as_path(),
        response.bytes().await.map_err(|e| {
            OpenAIError::FileSaveError(format!("{}, file path: {}", e, file_path.display()))
        })?,
    )
    .await
    .map_err(|e| OpenAIError::FileSaveError(e.to_string()))?;

    Ok(file_path)
}

pub(crate) async fn save_b64<P: AsRef<Path>>(b64: &str, dir: P) -> Result<PathBuf, OpenAIError> {
    let filename: String = rand::thread_rng()
        .sample_iter(&Alphanumeric)
        .take(10)
        .map(char::from)
        .collect();

    let filename = format!("{filename}.png");

    let path = PathBuf::from(dir.as_ref()).join(filename);

    tokio::fs::write(
        path.as_path(),
        general_purpose::STANDARD
            .decode(b64)
            .map_err(|e| OpenAIError::FileSaveError(e.to_string()))?,
    )
    .await
    .map_err(|e| OpenAIError::FileSaveError(format!("{}, path: {}", e, path.display())))?;

    Ok(path)
}
