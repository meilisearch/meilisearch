use std::io::{self, ErrorKind};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use tokio::fs::{create_dir_all, metadata};
use tokio::sync::mpsc::Receiver;
use tracing::warn;
use uuid::Uuid;

// TODO not sure I want to use it this way
use super::index_map::IndexTransferRequest;

/// It's 10 MB/s 🐌
fn fake_transfer_speed(size: u64) -> Duration {
    Duration::from_secs(size / (10 * 1024 * 1024))
}

async fn download_index(
    indexes_folder: &Path,
    fake_s3_folder: &Path,
    uuid: Uuid,
) -> Result<(), io::Error> {
    let index_path = indexes_folder.join(uuid.to_string());
    let index_path_in_s3 = fake_s3_folder.join(uuid.to_string());
    let index_size = metadata(index_path_in_s3.join("data.ms")).await?.len();
    let download_duration = fake_transfer_speed(index_size);
    tokio::time::sleep(download_duration).await;
    tokio::fs::rename(index_path_in_s3, index_path).await?;
    Ok(())
}

async fn upload_index(
    indexes_folder: &Path,
    fake_s3_folder: &Path,
    uuid: Uuid,
) -> Result<(), io::Error> {
    let index_path = indexes_folder.join(uuid.to_string());
    let index_path_in_s3 = fake_s3_folder.join(uuid.to_string());
    let index_size = metadata(index_path.join("data.ms")).await?.len();
    let upload_duration = fake_transfer_speed(index_size);
    tokio::time::sleep(upload_duration).await;
    tokio::fs::rename(index_path, index_path_in_s3).await?;
    Ok(())
}

pub async fn process_index_transfers(
    indexes: PathBuf,
    mut transfer_receiver: Receiver<IndexTransferRequest>,
) {
    // Create the folder that fakes S3
    // TODO remove this once we actually upload to S3
    let fake_s3 = indexes.join("this-is-s3");
    if let Err(e) = create_dir_all(&fake_s3).await {
        if e.kind() != ErrorKind::AlreadyExists {
            panic!("Failed to create fake S3 folder: {e}");
        }
    }

    while let Some(request) = transfer_receiver.recv().await {
        match request {
            IndexTransferRequest::Download { uuid, answer } => {
                let result = download_index(&indexes, &fake_s3, uuid).await.map_err(Arc::new);
                if answer.send(result).is_err() {
                    warn!("Couldn't send the download status of index {uuid}: channel closed");
                }
            }
            IndexTransferRequest::Upload { uuid, answer } => {
                let result = upload_index(&indexes, &fake_s3, uuid).await.map_err(Arc::new);
                if answer.send(result).is_err() {
                    warn!("Couldn't send the upload status of index {uuid}: channel closed");
                }
            }
        }
    }
}
