use std::fs::{create_dir_all, File};
use std::io::{Read, Write};
use std::os::fd::AsRawFd;
use std::path::{Path, PathBuf};
use std::sync::mpsc::{Receiver, Sender};
use std::thread::JoinHandle;

use flate2::read::GzDecoder;
use flate2::write::GzEncoder;
use flate2::Compression;
use milli::heed::Env;
use tar::{Archive, Builder, Header};

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
    send: Sender<CompressionMessage>,
    join_handle: JoinHandle<anyhow::Result<File>>,
}

enum CompressionMessage {
    Env { path: PathBuf, reader: std::io::PipeReader },
    File { path: PathBuf },
    Dir { path: PathBuf },
}

impl PipedArchiveBuilder {
    pub fn new(dest_dir: PathBuf, dest_filename: String, base_path: PathBuf) -> Self {
        let (send, recv) = std::sync::mpsc::channel();
        let join_handle = std::thread::Builder::new()
            .name("piped-archive-builer".into())
            .spawn(|| Self::run(dest_dir, dest_filename, recv, base_path))
            .unwrap();
        Self { send, join_handle }
    }

    pub fn add_env_to_archive<T>(&mut self, env: &Env<T>) -> anyhow::Result<()> {
        let (reader, writer) = std::io::pipe()?;
        let path = env.path().to_path_buf();
        // make sure that the environment cannot change while it is being added to the archive,
        // as any concurrent change would corrupt the copy.
        let env_wtxn = env.write_txn()?;

        self.send.send(CompressionMessage::Env { path, reader });
        // SAFETY: the writer end of the pipe is available for write access
        unsafe { env.copy_to_fd(writer.as_raw_fd(), milli::heed::CompactionOption::Disabled)? }

        // no change we might want to commit
        env_wtxn.abort();
        Ok(())
    }

    pub fn add_file_to_archive(&mut self, path: PathBuf) -> anyhow::Result<()> {
        self.send.send(CompressionMessage::File { path });
        Ok(())
    }

    pub fn add_dir_to_archive(&mut self, path: PathBuf) -> anyhow::Result<()> {
        self.send.send(CompressionMessage::Dir { path });
        Ok(())
    }

    pub fn finish(self) -> anyhow::Result<File> {
        drop(self.send);
        /// FIXME catch panic
        let file = self.join_handle.join().unwrap()?;
        Ok(file)
    }

    fn run(
        dest_dir: PathBuf,
        dest_filename: String,
        recv: Receiver<CompressionMessage>,
        base_path: PathBuf,
    ) -> anyhow::Result<File> {
        let mut temp_archive = tempfile::NamedTempFile::new_in(&dest_dir)?;

        let gz_encoder = GzEncoder::new(&mut temp_archive, Compression::default());
        let mut tar_encoder = Builder::new(gz_encoder);
        let base_path_in_archive = PathInArchive::from_absolute_and_base(&base_path, &base_path);
        // add the root
        tar_encoder.append_dir(base_path_in_archive.as_path(), &base_path)?;
        while let Ok(message) = recv.recv() {
            match message {
                CompressionMessage::Env { path, reader } => {
                    let dir_path_in_archive =
                        PathInArchive::from_absolute_and_base(&path, &base_path);

                    tar_encoder.append_dir(dir_path_in_archive.as_path(), &path)?;

                    let path = path.join("data.mdb");
                    Self::add_to_archive(&mut tar_encoder, &path, &base_path, reader)?;
                }
                CompressionMessage::File { path } => {
                    let path_in_archive = PathInArchive::from_absolute_and_base(&path, &base_path);
                    tar_encoder.append_path_with_name(&path, path_in_archive.as_path())?;
                }
                CompressionMessage::Dir { path } => {
                    let path_in_archive = PathInArchive::from_absolute_and_base(&path, &base_path);

                    tar_encoder.append_dir(path_in_archive.as_path(), &path)?;
                }
            }
        }

        let gz_encoder = tar_encoder.into_inner()?;
        gz_encoder.finish()?;
        temp_archive.flush()?;
        let archive = temp_archive.persist(dest_dir.join(dest_filename))?;
        Ok(archive)
    }

    fn add_to_archive(
        tar_encoder: &mut Builder<impl Write>,
        path: &Path,
        base: &Path,
        reader: impl Read,
    ) -> anyhow::Result<()> {
        let stats = path.metadata()?;
        let mut header = Header::new_gnu();
        header.set_metadata_in_mode(&stats, tar::HeaderMode::Complete);
        let path_in_archive = PathInArchive::from_absolute_and_base(path, base);

        tar_encoder.append_data(&mut header, path_in_archive.as_path(), reader)?;
        Ok(())
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
