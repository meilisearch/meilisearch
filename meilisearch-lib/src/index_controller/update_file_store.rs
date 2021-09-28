use std::fs::{File, create_dir_all};
use std::io::{BufReader, BufWriter, Write};
use std::path::{Path, PathBuf};
use std::ops::{Deref, DerefMut};

use milli::documents::DocumentBatchReader;
use serde_json::Map;
use tempfile::NamedTempFile;
use uuid::Uuid;

const UPDATE_FILES_PATH: &str = "updates/updates_files";

use crate::document_formats::read_jsonl;

use super::error::Result;

pub struct UpdateFile {
    path: PathBuf,
    file: NamedTempFile,
}

impl UpdateFile {
    pub fn persist(self) {
        self.file.persist(&self.path).unwrap();
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

        create_dir_all(&dst_update_files_path).unwrap();

        let entries = std::fs::read_dir(src_update_files_path).unwrap();

        for entry in entries {
            let entry = entry.unwrap();
            let update_file = BufReader::new(File::open(entry.path()).unwrap());
            let file_uuid = entry.file_name();
            let file_uuid = file_uuid.to_str().ok_or_else(|| anyhow::anyhow!("invalid update file name"))?;
            let dst_path = dst_update_files_path.join(file_uuid);
            let dst_file = BufWriter::new(File::create(dst_path)?);
            read_jsonl(update_file, dst_file)?;
        }

        Ok(())
    }

    pub fn new(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref().join(UPDATE_FILES_PATH);
        std::fs::create_dir_all(&path).unwrap();
        Ok(Self { path })
    }

    /// Created a new temporary update file.
    ///
    /// A call to persist is needed to persist in the database.
    pub fn new_update(&self) -> Result<(Uuid, UpdateFile)> {
        let file  = NamedTempFile::new().unwrap();
        let uuid = Uuid::new_v4();
        let path = self.path.join(uuid.to_string());
        let update_file = UpdateFile { file, path };

        Ok((uuid, update_file))
    }

    /// Returns a the file corresponding to the requested uuid.
    pub fn get_update(&self, uuid: Uuid) -> Result<File> {
        let path = self.path.join(uuid.to_string());
        let file = File::open(path).unwrap();
        Ok(file)
    }

    /// Copies the content of the update file poited to by uuid to dst directory.
    pub fn snapshot(&self, uuid: Uuid, dst: impl AsRef<Path>) -> Result<()> {
        let src = self.path.join(uuid.to_string());
        let mut dst = dst.as_ref().join(UPDATE_FILES_PATH);
        std::fs::create_dir_all(&dst).unwrap();
        dst.push(uuid.to_string());
        std::fs::copy(src, dst).unwrap();
        Ok(())
    }

    /// Peform a dump of the given update file uuid into the provided snapshot path.
    pub fn dump(&self, uuid: Uuid, dump_path: impl AsRef<Path>) -> Result<()> {
        let uuid_string = uuid.to_string();
        let update_file_path = self.path.join(&uuid_string);
        let mut dst = dump_path.as_ref().join(UPDATE_FILES_PATH);
        std::fs::create_dir_all(&dst).unwrap();
        dst.push(&uuid_string);

        let update_file = File::open(update_file_path).unwrap();
        let mut dst_file = NamedTempFile::new().unwrap();
        let mut document_reader = DocumentBatchReader::from_reader(update_file).unwrap();

        let mut document_buffer = Map::new();
        // TODO: we need to find a way to do this more efficiently. (create a custom serializer to
        // jsonl for example...)
        while let Some((index, document)) = document_reader.next_document_with_index().unwrap() {
            for (field_id, content) in document.iter() {
                let field_name = index.get_by_left(&field_id).unwrap();
                let content = serde_json::from_slice(content).unwrap();
                document_buffer.insert(field_name.to_string(), content);
            }

            serde_json::to_writer(&mut dst_file, &document_buffer).unwrap();
            dst_file.write(b"\n").unwrap();
            document_buffer.clear();
        }

        dst_file.persist(dst).unwrap();

        Ok(())
    }
}
