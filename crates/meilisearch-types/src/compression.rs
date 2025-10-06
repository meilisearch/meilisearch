use std::fs::{create_dir_all, File};
use std::io::Write;
use std::path::{Path, PathBuf};

use flate2::read::GzDecoder;
use flate2::write::GzEncoder;
use flate2::Compression;
use milli::heed::Env;
use tar::{Archive, Builder};
use tempfile::NamedTempFile;

pub fn to_tar_gz(src: impl AsRef<Path>, dest: impl AsRef<Path>) -> anyhow::Result<()> {
    let mut f = File::create(dest)?;
    let gz_encoder = GzEncoder::new(&mut f, Compression::default());
    let mut tar_encoder = Builder::new(gz_encoder);
    tar_encoder.append_dir_all(".", src)?;
    let gz_encoder = tar_encoder.into_inner()?;
    gz_encoder.finish()?;
    f.flush()?;
    Ok(())
}

pub fn from_tar_gz(src: impl AsRef<Path>, dest: impl AsRef<Path>) -> anyhow::Result<()> {
    let f = File::open(&src)?;
    let gz = GzDecoder::new(f);
    let mut ar = Archive::new(gz);
    create_dir_all(&dest)?;
    ar.unpack(&dest)?;
    Ok(())
}

pub struct PipedArchiveBuilder {
    base_path: PathBuf,
    tar_encoder: tar::Builder<GzEncoder<NamedTempFile>>,
}

impl PipedArchiveBuilder {
    pub fn new(dest_dir: PathBuf, base_path: PathBuf) -> anyhow::Result<Self> {
        let temp_archive = tempfile::NamedTempFile::new_in(&dest_dir)?;

        let gz_encoder = GzEncoder::new(temp_archive, Compression::default());
        let mut tar_encoder = Builder::new(gz_encoder);
        let base_path_in_archive = PathInArchive::from_absolute_and_base(&base_path, &base_path);
        tar_encoder.append_dir(base_path_in_archive.as_path(), &base_path)?;

        Ok(Self { base_path, tar_encoder })
    }

    /// Add a heed environment to the archive.
    ///
    /// # Errors
    ///
    /// - Errors originating with that thread:
    ///     - Heed errors, if taking a write transaction fails
    ///     - If the copy of the environment fails.
    ///     - If there is an I/O error opening the database at the environment's path.
    /// - Errors originating with another thread:
    ///     - If the cancellation thread panicked or otherwise dropped its receiver.
    ///     - If the processing thread panicked or otherwise dropped its receiver.
    pub fn add_env_to_archive<T>(&mut self, env: &Env<T>) -> anyhow::Result<()> {
        let path = env.path().to_path_buf();
        // make sure that the environment cannot change while it is being added to the archive,
        // as any concurrent change would corrupt the copy.
        let env_wtxn = env.write_txn()?;

        let dir_path_in_archive = PathInArchive::from_absolute_and_base(&path, &self.base_path);

        self.tar_encoder.append_dir(dir_path_in_archive.as_path(), &path)?;

        let path = path.join("data.mdb");
        let path_in_archive = PathInArchive::from_absolute_and_base(&path, &self.base_path);

        self.tar_encoder.append_path_with_name(&path, path_in_archive.as_path())?;

        // no change we might want to commit
        env_wtxn.abort();
        Ok(())
    }

    /// Add a file to the archive
    ///
    /// # Errors
    ///
    /// - If the processing thread panicked or otherwise dropped its receiver.
    pub fn add_file_to_archive(&mut self, path: PathBuf) -> anyhow::Result<()> {
        let path_in_archive = PathInArchive::from_absolute_and_base(&path, &self.base_path);
        self.tar_encoder.append_path_with_name(&path, path_in_archive.as_path())?;
        Ok(())
    }

    /// Add a directory name (**without its contents**) to the archive.
    ///
    /// # Errors
    ///
    /// - If the processing thread panicked or otherwise dropped its receiver.
    pub fn add_dir_to_archive(&mut self, path: PathBuf) -> anyhow::Result<()> {
        let path_in_archive = PathInArchive::from_absolute_and_base(&path, &self.base_path);

        self.tar_encoder.append_dir(path_in_archive.as_path(), &path)?;
        Ok(())
    }

    /// Finalize the archive and persists it to disk.
    ///
    /// # Errors
    ///
    /// - Originating with the current thread:
    ///     - If persisting the archive fails
    /// - Originating with another thread:
    ///     - If the cancellation thread panicked.
    ///     - If the processing thread panicked or otherwise terminated in error.
    pub fn finish(self, dest_path: &Path) -> anyhow::Result<File> {
        let gz_encoder = self.tar_encoder.into_inner()?;
        let mut temp_archive = gz_encoder.finish()?;
        temp_archive.flush()?;

        let archive = temp_archive.persist(dest_path)?;
        Ok(archive)
    }
}

struct PathInArchive(PathBuf);

impl PathInArchive {
    pub fn from_absolute_and_base(absolute: &Path, base: &Path) -> Self {
        /// FIXME
        let canonical = absolute.canonicalize().unwrap();
        let relative = match canonical.strip_prefix(base) {
            Ok(stripped) => Path::new(&".").join(stripped),
            Err(_) => absolute.to_path_buf(),
        };

        Self(relative)
    }

    pub fn as_path(&self) -> &Path {
        self.0.as_path()
    }
}
