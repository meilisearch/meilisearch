use std::fs::File;
use std::ops::{Deref, DerefMut};
use std::path::{Path, PathBuf};

use tempfile::NamedTempFile;
use uuid::Uuid;

#[cfg(not(test))]
pub use store::UpdateFileStore;
#[cfg(test)]
pub use test::MockUpdateFileStore as UpdateFileStore;

const UPDATE_FILES_PATH: &str = "updates/updates_files";

pub struct UpdateFile {
    path: PathBuf,
    file: NamedTempFile,
}

#[derive(Debug, thiserror::Error)]
pub enum UpdateFileStoreError {
    #[error("Error while persisting update to disk")]
    Error,
    #[error(transparent)]
    IoError(#[from] std::io::Error),
    #[error(transparent)]
    PersistError(#[from] tempfile::PersistError),
}

pub type Result<T> = std::result::Result<T, UpdateFileStoreError>;

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

mod store {
    use super::*;

    #[derive(Clone, Debug)]
    pub struct UpdateFileStore {
        path: PathBuf,
    }

    impl UpdateFileStore {
        pub fn new(path: impl AsRef<Path>) -> Result<Self> {
            let path = path.as_ref().join(UPDATE_FILES_PATH);
            std::fs::create_dir_all(&path)?;
            Ok(Self { path })
        }

        /// Creates a new temporary update file.
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

        pub fn get_size(&self, uuid: Uuid) -> Result<u64> {
            Ok(self.get_update(uuid)?.metadata()?.len())
        }

        pub fn delete(&self, uuid: Uuid) -> Result<()> {
            let path = self.path.join(uuid.to_string());
            std::fs::remove_file(path)?;
            Ok(())
        }
    }
}

#[cfg(test)]
mod test {
    use std::sync::Arc;

    use nelson::Mocker;

    use super::*;

    #[derive(Clone)]
    pub enum MockUpdateFileStore {
        Real(store::UpdateFileStore),
        Mock(Arc<Mocker>),
    }

    impl MockUpdateFileStore {
        pub fn mock(mocker: Mocker) -> Self {
            Self::Mock(Arc::new(mocker))
        }

        pub fn new(path: impl AsRef<Path>) -> Result<Self> {
            store::UpdateFileStore::new(path).map(Self::Real)
        }

        pub fn new_update(&self) -> Result<(Uuid, UpdateFile)> {
            match self {
                MockUpdateFileStore::Real(s) => s.new_update(),
                MockUpdateFileStore::Mock(_) => todo!(),
            }
        }

        pub fn get_update(&self, uuid: Uuid) -> Result<File> {
            match self {
                MockUpdateFileStore::Real(s) => s.get_update(uuid),
                MockUpdateFileStore::Mock(_) => todo!(),
            }
        }

        pub fn snapshot(&self, uuid: Uuid, dst: impl AsRef<Path>) -> Result<()> {
            match self {
                MockUpdateFileStore::Real(s) => s.snapshot(uuid, dst),
                MockUpdateFileStore::Mock(_) => todo!(),
            }
        }

        pub fn get_size(&self, uuid: Uuid) -> Result<u64> {
            match self {
                MockUpdateFileStore::Real(s) => s.get_size(uuid),
                MockUpdateFileStore::Mock(_) => todo!(),
            }
        }

        pub fn delete(&self, uuid: Uuid) -> Result<()> {
            match self {
                MockUpdateFileStore::Real(s) => s.delete(uuid),
                MockUpdateFileStore::Mock(mocker) => unsafe { mocker.get("delete").call(uuid) },
            }
        }
    }
}
