use std::fs::File;
use std::path::{Path, PathBuf};
use std::ops::{Deref, DerefMut};

use tempfile::NamedTempFile;
use uuid::Uuid;

use super::error::Result;

pub struct UpdateFile {
    path: PathBuf,
    file: NamedTempFile,
}

impl UpdateFile {
    pub fn persist(self) {
        println!("persisting in {}", self.path.display());
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
    pub fn new(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref().join("updates/updates_files");
        std::fs::create_dir_all(&path).unwrap();
        Ok(Self { path })
    }

    pub fn new_update(&self) -> Result<(Uuid, UpdateFile)> {
        let file  = NamedTempFile::new().unwrap();
        let uuid = Uuid::new_v4();
        let path = self.path.join(uuid.to_string());
        let update_file = UpdateFile { file, path };

        Ok((uuid, update_file))
    }

    pub fn get_update(&self, uuid: Uuid) -> Result<File> {
        let path = self.path.join(uuid.to_string());
        println!("reading in {}", path.display());
        let file = File::open(path).unwrap();
        Ok(file)
    }
}
