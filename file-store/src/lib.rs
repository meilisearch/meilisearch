use std::collections::BTreeSet;
use std::fs::File as StdFile;
use std::ops::{Deref, DerefMut};
use std::path::{Path, PathBuf};
use std::str::FromStr;

use tempfile::NamedTempFile;
use uuid::Uuid;

const UPDATE_FILES_PATH: &str = "updates/updates_files";

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error(transparent)]
    IoError(#[from] std::io::Error),
    #[error(transparent)]
    PersistError(#[from] tempfile::PersistError),
}

pub type Result<T> = std::result::Result<T, Error>;

impl Deref for File {
    type Target = NamedTempFile;

    fn deref(&self) -> &Self::Target {
        &self.file
    }
}

impl DerefMut for File {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.file
    }
}

#[cfg_attr(test, faux::create)]
#[derive(Clone, Debug)]
pub struct FileStore {
    path: PathBuf,
}

#[cfg(not(test))]
impl FileStore {
    pub fn new(path: impl AsRef<Path>) -> Result<FileStore> {
        let path = path.as_ref().to_path_buf();
        std::fs::create_dir_all(&path)?;
        Ok(FileStore { path })
    }
}

#[cfg_attr(test, faux::methods)]
impl FileStore {
    /// Creates a new temporary update file.
    /// A call to `persist` is needed to persist the file in the database.
    pub fn new_update(&self) -> Result<(Uuid, File)> {
        let file = NamedTempFile::new_in(&self.path)?;
        let uuid = Uuid::new_v4();
        let path = self.path.join(uuid.to_string());
        let update_file = File { file, path };

        Ok((uuid, update_file))
    }

    /// Creates a new temporary update file with the given Uuid.
    /// A call to `persist` is needed to persist the file in the database.
    pub fn new_update_with_uuid(&self, uuid: u128) -> Result<(Uuid, File)> {
        let file = NamedTempFile::new_in(&self.path)?;
        let uuid = Uuid::from_u128(uuid);
        let path = self.path.join(uuid.to_string());
        let update_file = File { file, path };

        Ok((uuid, update_file))
    }

    /// Returns the file corresponding to the requested uuid.
    pub fn get_update(&self, uuid: Uuid) -> Result<StdFile> {
        let path = self.path.join(uuid.to_string());
        let file = StdFile::open(path)?;
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

    pub fn get_size(&self, uuid: Uuid) -> Result<u64> {
        Ok(self.get_update(uuid)?.metadata()?.len())
    }

    pub fn delete(&self, uuid: Uuid) -> Result<()> {
        let path = self.path.join(uuid.to_string());
        std::fs::remove_file(path)?;
        Ok(())
    }

    /// List the Uuids of the files in the FileStore
    ///
    /// This function is meant to be used by tests only.
    #[doc(hidden)]
    pub fn __all_uuids(&self) -> BTreeSet<Uuid> {
        let mut uuids = BTreeSet::new();
        for entry in self.path.read_dir().unwrap() {
            let entry = entry.unwrap();
            let uuid = Uuid::from_str(entry.file_name().to_str().unwrap()).unwrap();
            uuids.insert(uuid);
        }
        uuids
    }
}

pub struct File {
    path: PathBuf,
    file: NamedTempFile,
}

impl File {
    pub fn persist(self) -> Result<()> {
        self.file.persist(&self.path)?;
        Ok(())
    }
}
