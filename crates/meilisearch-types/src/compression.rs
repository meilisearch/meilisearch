use std::fs::{create_dir_all, File};
use std::io::{PipeWriter, Read, Write};
use std::mem::ManuallyDrop;
use std::ops::DerefMut;
use std::os::fd::{AsRawFd, FromRawFd};
use std::path::{Path, PathBuf};
use std::sync::mpsc::{Receiver, RecvTimeoutError, Sender};
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
    send_compression: Sender<CompressionMessage>,
    send_cancellation: Sender<CancellationMessage>,
    processing_thread: JoinHandle<anyhow::Result<File>>,
    cancellation_thread: JoinHandle<()>,
}

enum CompressionMessage {
    Env { path: PathBuf, reader: std::io::PipeReader },
    File { path: PathBuf },
    Dir { path: PathBuf },
}

impl PipedArchiveBuilder {
    pub fn new<F>(
        dest_dir: PathBuf,
        dest_filename: String,
        base_path: PathBuf,
        must_stop_processing: F,
    ) -> Self
    where
        F: Fn() -> bool + Send + 'static,
    {
        let (send_compression, recv) = std::sync::mpsc::channel();
        let processing_thread = std::thread::Builder::new()
            .name("piped-archive-builder".into())
            .spawn(|| Self::run_processing(dest_dir, dest_filename, recv, base_path))
            .unwrap();

        let (send_cancellation, recv) = std::sync::mpsc::channel();

        let cancellation_thread = std::thread::Builder::new()
            .name("piped-archive-builder-cancellation".into())
            .spawn(|| Self::run_cancellation(must_stop_processing, recv))
            .unwrap();

        Self { send_compression, send_cancellation, processing_thread, cancellation_thread }
    }

    pub fn add_env_to_archive<T>(&mut self, env: &Env<T>) -> anyhow::Result<()> {
        let (reader, writer) = std::io::pipe()?;
        let path = env.path().to_path_buf();
        // make sure that the environment cannot change while it is being added to the archive,
        // as any concurrent change would corrupt the copy.
        let env_wtxn = env.write_txn()?;

        // SAFETY: only the cancellation thread has the actual responsibility of closing the pipe since
        // the clone is `ManuallyDrop`.
        let mut cloned_writer = unsafe {
            let writer_raw_fd = writer.as_raw_fd();
            ManuallyDrop::new(PipeWriter::from_raw_fd(writer_raw_fd))
        };

        self.send_cancellation.send(CancellationMessage::OpenedPipe { pipe: writer });

        self.send_compression.send(CompressionMessage::Env { path, reader });

        let mdb_path = env.path().join("data.mdb");
        let mut file = std::fs::File::open(&mdb_path)?;
        std::io::copy(&mut file, cloned_writer.deref_mut())?;

        self.send_cancellation.send(CancellationMessage::ClosingPipe);

        // no change we might want to commit
        env_wtxn.abort();
        Ok(())
    }

    pub fn add_file_to_archive(&mut self, path: PathBuf) -> anyhow::Result<()> {
        self.send_compression.send(CompressionMessage::File { path });
        Ok(())
    }

    pub fn add_dir_to_archive(&mut self, path: PathBuf) -> anyhow::Result<()> {
        self.send_compression.send(CompressionMessage::Dir { path });
        Ok(())
    }

    pub fn finish(self) -> anyhow::Result<File> {
        drop(self.send_cancellation);
        drop(self.send_compression);
        /// FIXME catch panics
        let file = self.processing_thread.join().unwrap()?;
        self.cancellation_thread.join().unwrap();
        Ok(file)
    }

    fn run_processing(
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

    fn run_cancellation<F>(must_stop_processing: F, recv: Receiver<CancellationMessage>)
    where
        F: Fn() -> bool + Send + 'static,
    {
        let mut current_pipe = None;
        loop {
            let next_message = match recv.recv_timeout(std::time::Duration::from_secs(60)) {
                Ok(message) => message,
                Err(RecvTimeoutError::Disconnected) => break,
                Err(RecvTimeoutError::Timeout) => {
                    if must_stop_processing() {
                        break;
                    }
                    continue;
                }
            };
            match next_message {
                CancellationMessage::OpenedPipe { pipe } => current_pipe = Some(pipe),
                CancellationMessage::ClosingPipe => current_pipe = None,
            }
        }
        drop(current_pipe);
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

enum CancellationMessage {
    OpenedPipe { pipe: PipeWriter },
    ClosingPipe,
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
