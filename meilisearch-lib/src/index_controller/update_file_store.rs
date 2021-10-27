use std::fs::{create_dir_all, File};
use std::io::{self, BufReader, BufWriter, Write};
use std::ops::{Deref, DerefMut};
use std::path::{Path, PathBuf};

use milli::documents::DocumentBatchReader;
use serde_json::Map;
use tempfile::{NamedTempFile, PersistError};
use uuid::Uuid;

const UPDATE_FILES_PATH: &str = "updates/updates_files";

use crate::document_formats::read_ndjson;

pub struct UpdateFile {
    path: PathBuf,
    file: NamedTempFile,
}

#[derive(Debug, thiserror::Error)]
#[error("Error while persisting update to disk: {0}")]
pub struct UpdateFileStoreError(Box<dyn std::error::Error + Sync + Send + 'static>);

type Result<T> = std::result::Result<T, UpdateFileStoreError>;

macro_rules! into_update_store_error {
    ($($other:path),*) => {
        $(
            impl From<$other> for UpdateFileStoreError {
                fn from(other: $other) -> Self {
                    Self(Box::new(other))
                }
            }
        )*
    };
}

into_update_store_error!(
    PersistError,
    io::Error,
    serde_json::Error,
    milli::documents::Error
);

impl UpdateFile {
    pub fn persist(self) -> Result<()> {
        self.file.persist(&self.path)?;
        Ok(())
    }
}

impl Deref for UpdateFile {
    type Target = NamedTempFile;

    fn deref(&self) -> &Self::Target {
        &self.file
    }
}

impl DerefMut for UpdateFile {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.file
    }
}

#[derive(Clone, Debug)]
pub struct UpdateFileStore {
    path: PathBuf,
}

impl UpdateFileStore {
    pub fn load_dump(src: impl AsRef<Path>, dst: impl AsRef<Path>) -> anyhow::Result<()> {
        let src_update_files_path = src.as_ref().join(UPDATE_FILES_PATH);
        let dst_update_files_path = dst.as_ref().join(UPDATE_FILES_PATH);

        // No update files to load
        if !src_update_files_path.exists() {
            return Ok(());
        }

        create_dir_all(&dst_update_files_path)?;

        let entries = std::fs::read_dir(src_update_files_path)?;

        for entry in entries {
            let entry = entry?;
            let update_file = BufReader::new(File::open(entry.path())?);
            let file_uuid = entry.file_name();
            let file_uuid = file_uuid
                .to_str()
                .ok_or_else(|| anyhow::anyhow!("invalid update file name"))?;
            let dst_path = dst_update_files_path.join(file_uuid);
            let dst_file = BufWriter::new(File::create(dst_path)?);
            read_ndjson(update_file, dst_file)?;
        }

        Ok(())
    }

    pub fn new(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref().join(UPDATE_FILES_PATH);
        std::fs::create_dir_all(&path)?;
        Ok(Self { path })
    }

    /// Creates a new temporary update file.
    ///
    /// A call to `persist` is needed to persist the file in the database.
    pub fn new_update(&self) -> Result<(Uuid, UpdateFile)> {
        let file = NamedTempFile::new_in(&self.path)?;
        let uuid = Uuid::new_v4();
        let path = self.path.join(uuid.to_string());
        let update_file = UpdateFile { file, path };

        Ok((uuid, update_file))
    }

    /// Returns the file corresponding to the requested uuid.
    pub fn get_update(&self, uuid: Uuid) -> Result<File> {
        let path = self.path.join(uuid.to_string());
        let file = File::open(path)?;
        Ok(file)
    }

    /// Copies the content of the update file pointed to by `uuid` to the `dst` directory.
    pub fn snapshot(&self, uuid: Uuid, dst: impl AsRef<Path>) -> Result<()> {
        let src = self.path.join(uuid.to_string());
        let mut dst = dst.as_ref().join(UPDATE_FILES_PATH);
        std::fs::create_dir_all(&dst)?;
        dst.push(uuid.to_string());
        std::fs::copy(src, dst)?;
        Ok(())
    }

    /// Peforms a dump of the given update file uuid into the provided dump path.
    pub fn dump(&self, uuid: Uuid, dump_path: impl AsRef<Path>) -> Result<()> {
        let uuid_string = uuid.to_string();
        let update_file_path = self.path.join(&uuid_string);
        let mut dst = dump_path.as_ref().join(UPDATE_FILES_PATH);
        std::fs::create_dir_all(&dst)?;
        dst.push(&uuid_string);

        let update_file = File::open(update_file_path)?;
        let mut dst_file = NamedTempFile::new_in(&dump_path)?;
        let mut document_reader = DocumentBatchReader::from_reader(update_file)?;

        let mut document_buffer = Map::new();
        // TODO: we need to find a way to do this more efficiently. (create a custom serializer
        // for jsonl for example...)
        while let Some((index, document)) = document_reader.next_document_with_index()? {
            for (field_id, content) in document.iter() {
                if let Some(field_name) = index.name(field_id) {
                    let content = serde_json::from_slice(content)?;
                    document_buffer.insert(field_name.to_string(), content);
                }
            }

            serde_json::to_writer(&mut dst_file, &document_buffer)?;
            dst_file.write_all(b"\n")?;
            document_buffer.clear();
        }

        dst_file.persist(dst)?;

        Ok(())
    }

    pub fn get_size(&self, uuid: Uuid) -> Result<u64> {
        Ok(self.get_update(uuid)?.metadata()?.len())
    }

    pub fn delete(&self, uuid: Uuid) -> Result<()> {
        let path = self.path.join(uuid.to_string());
        std::fs::remove_file(path)?;
        Ok(())
    }
}
