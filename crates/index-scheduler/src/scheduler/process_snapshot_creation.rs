use std::ffi::OsStr;
use std::fs;
use std::sync::atomic::Ordering;

use meilisearch_types::heed::CompactionOption;
use meilisearch_types::milli::progress::{Progress, VariableNameStep};
use meilisearch_types::tasks::{Status, Task};
use meilisearch_types::{compression, VERSION_FILE_NAME};

use crate::heed::EnvOpenOptions;
use crate::processing::{AtomicUpdateFileStep, SnapshotCreationProgress};
use crate::queue::TaskQueue;
use crate::{Error, IndexScheduler, Result};

const UPDATE_FILES_DIR_NAME: &str = "update_files";

/// # Safety
///
/// See [`EnvOpenOptions::open`].
unsafe fn remove_tasks(
    tasks: &[Task],
    dst: &std::path::Path,
    index_base_map_size: usize,
) -> Result<()> {
    let env_options = EnvOpenOptions::new();
    let mut env_options = env_options.read_txn_without_tls();
    let env = env_options.max_dbs(TaskQueue::nb_db()).map_size(index_base_map_size).open(dst)?;
    let mut wtxn = env.write_txn()?;
    let task_queue = TaskQueue::new(&env, &mut wtxn)?;

    // Destructuring to ensure the code below gets updated if a database gets added in the future.
    let TaskQueue {
        all_tasks,
        status,
        kind,
        index_tasks: _, // snapshot creation tasks are not index tasks
        canceled_by,
        enqueued_at,
        started_at,
        finished_at,
    } = task_queue;

    for task in tasks {
        all_tasks.delete(&mut wtxn, &task.uid)?;

        let mut tasks = status.get(&wtxn, &task.status)?.unwrap_or_default();
        tasks.remove(task.uid);
        status.put(&mut wtxn, &task.status, &tasks)?;

        let mut tasks = kind.get(&wtxn, &task.kind.as_kind())?.unwrap_or_default();
        tasks.remove(task.uid);
        kind.put(&mut wtxn, &task.kind.as_kind(), &tasks)?;

        canceled_by.delete(&mut wtxn, &task.uid)?;

        let timestamp = task.enqueued_at.unix_timestamp_nanos();
        let mut tasks = enqueued_at.get(&wtxn, &timestamp)?.unwrap_or_default();
        tasks.remove(task.uid);
        enqueued_at.put(&mut wtxn, &timestamp, &tasks)?;

        if let Some(task_started_at) = task.started_at {
            let timestamp = task_started_at.unix_timestamp_nanos();
            let mut tasks = started_at.get(&wtxn, &timestamp)?.unwrap_or_default();
            tasks.remove(task.uid);
            started_at.put(&mut wtxn, &timestamp, &tasks)?;
        }

        if let Some(task_finished_at) = task.finished_at {
            let timestamp = task_finished_at.unix_timestamp_nanos();
            let mut tasks = finished_at.get(&wtxn, &timestamp)?.unwrap_or_default();
            tasks.remove(task.uid);
            finished_at.put(&mut wtxn, &timestamp, &tasks)?;
        }
    }
    wtxn.commit()?;
    Ok(())
}

impl IndexScheduler {
    pub(super) fn process_snapshot(
        &self,
        progress: Progress,
        tasks: Vec<Task>,
    ) -> Result<Vec<Task>> {
        progress.update_progress(SnapshotCreationProgress::StartTheSnapshotCreation);

        match self.scheduler.s3_snapshot_options.clone() {
            Some(options) => {
                #[cfg(not(unix))]
                {
                    let _ = options;
                    panic!("Non-unix platform does not support S3 snapshotting");
                }
                #[cfg(unix)]
                self.runtime
                    .as_ref()
                    .expect("Runtime not initialized")
                    .block_on(self.process_snapshot_to_s3(progress, options, tasks))
            }
            None => self.process_snapshots_to_disk(progress, tasks),
        }
    }

    fn process_snapshots_to_disk(
        &self,
        progress: Progress,
        mut tasks: Vec<Task>,
    ) -> Result<Vec<Task>, Error> {
        fs::create_dir_all(&self.scheduler.snapshots_path)?;
        let temp_snapshot_dir = tempfile::tempdir()?;

        // 1. Snapshot the version file.
        let dst = temp_snapshot_dir.path().join(VERSION_FILE_NAME);
        fs::copy(&self.scheduler.version_file_path, dst)?;

        // 2. Snapshot the index-scheduler LMDB env
        //
        // When we call copy_to_path, LMDB opens a read transaction by itself,
        // we can't provide our own. It is an issue as we would like to know
        // the update files to copy but new ones can be enqueued between the copy
        // of the env and the new transaction we open to retrieve the enqueued tasks.
        // So we prefer opening a new transaction after copying the env and copy more
        // update files than not enough.
        //
        // Note that there cannot be any update files deleted between those
        // two read operations as the task processing is synchronous.

        // 2.1 First copy the LMDB env of the index-scheduler
        progress.update_progress(SnapshotCreationProgress::SnapshotTheIndexScheduler);
        let dst = temp_snapshot_dir.path().join("tasks");
        fs::create_dir_all(&dst)?;
        let compaction_option = if self.scheduler.experimental_no_snapshot_compaction {
            CompactionOption::Disabled
        } else {
            CompactionOption::Enabled
        };
        self.env.copy_to_path(dst.join("data.mdb"), compaction_option)?;

        // 2.2 Remove the current snapshot tasks
        //
        // This is done to ensure that the tasks are not processed again when the snapshot is imported
        //
        // # Safety
        //
        // This is safe because we open the env file we just created in a temporary directory.
        // We are sure it's not being used by any other process nor thread.
        unsafe {
            remove_tasks(&tasks, &dst, self.index_mapper.index_base_map_size)?;
        }

        // 2.3 Create a read transaction on the index-scheduler
        let rtxn = self.env.read_txn()?;

        // 2.4 Create the update files directory
        let update_files_dir = temp_snapshot_dir.path().join(UPDATE_FILES_DIR_NAME);
        fs::create_dir_all(&update_files_dir)?;

        // 2.5 Only copy the update files of the enqueued tasks
        progress.update_progress(SnapshotCreationProgress::SnapshotTheUpdateFiles);
        let enqueued = self.queue.tasks.get_status(&rtxn, Status::Enqueued)?;
        let (atomic, update_file_progress) = AtomicUpdateFileStep::new(enqueued.len() as u32);
        progress.update_progress(update_file_progress);
        for task_id in enqueued {
            let task =
                self.queue.tasks.get_task(&rtxn, task_id)?.ok_or(Error::CorruptedTaskQueue)?;
            if let Some(content_uuid) = task.content_uuid() {
                let src = self.queue.file_store.update_path(content_uuid);
                let dst = update_files_dir.join(content_uuid.to_string());
                fs::copy(src, dst)?;
            }
            atomic.fetch_add(1, Ordering::Relaxed);
        }

        // 3. Snapshot every indexes
        progress.update_progress(SnapshotCreationProgress::SnapshotTheIndexes);
        let index_mapping = self.index_mapper.index_mapping;
        let nb_indexes = index_mapping.len(&rtxn)? as u32;

        for (i, result) in index_mapping.iter(&rtxn)?.enumerate() {
            let (name, uuid) = result?;
            progress.update_progress(VariableNameStep::<SnapshotCreationProgress>::new(
                name, i as u32, nb_indexes,
            ));
            let index = self.index_mapper.index(&rtxn, name)?;
            let dst = temp_snapshot_dir.path().join("indexes").join(uuid.to_string());
            fs::create_dir_all(&dst)?;
            index
                .copy_to_path(dst.join("data.mdb"), compaction_option)
                .map_err(|e| Error::from_milli(e, Some(name.to_string())))?;
        }

        drop(rtxn);

        // 4. Snapshot the auth LMDB env
        progress.update_progress(SnapshotCreationProgress::SnapshotTheApiKeys);
        let dst = temp_snapshot_dir.path().join("auth");
        fs::create_dir_all(&dst)?;
        self.scheduler.auth_env.copy_to_path(dst.join("data.mdb"), compaction_option)?;

        // 5. Copy and tarball the flat snapshot
        progress.update_progress(SnapshotCreationProgress::CreateTheTarball);
        // 5.1 Find the original name of the database
        // TODO find a better way to get this path
        let mut base_path = self.env.path().to_owned();
        base_path.pop();
        let db_name = base_path.file_name().and_then(OsStr::to_str).unwrap_or("data.ms");

        // 5.2 Tarball the content of the snapshot in a tempfile with a .snapshot extension
        let snapshot_path = self.scheduler.snapshots_path.join(format!("{}.snapshot", db_name));
        let temp_snapshot_file = tempfile::NamedTempFile::new_in(&self.scheduler.snapshots_path)?;
        compression::to_tar_gz(temp_snapshot_dir.path(), temp_snapshot_file.path())?;
        let file = temp_snapshot_file.persist(snapshot_path)?;

        // 5.3 Change the permission to make the snapshot readonly
        let mut permissions = file.metadata()?.permissions();
        permissions.set_readonly(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            #[allow(clippy::non_octal_unix_permissions)]
            //                     rwxrwxrwx
            permissions.set_mode(0b100100100);
        }

        file.set_permissions(permissions)?;

        for task in &mut tasks {
            task.status = Status::Succeeded;
        }

        Ok(tasks)
    }

    #[cfg(unix)]
    pub(super) async fn process_snapshot_to_s3(
        &self,
        progress: Progress,
        opts: meilisearch_types::milli::update::S3SnapshotOptions,
        mut tasks: Vec<Task>,
    ) -> Result<Vec<Task>> {
        use meilisearch_types::milli::update::S3SnapshotOptions;

        let S3SnapshotOptions {
            s3_bucket_url,
            s3_bucket_region,
            s3_bucket_name,
            s3_snapshot_prefix,
            s3_access_key,
            s3_secret_key,
            s3_max_in_flight_parts,
            s3_compression_level: level,
            s3_signature_duration,
            s3_multipart_part_size,
        } = opts;

        let must_stop_processing = self.scheduler.must_stop_processing.clone();
        let retry_backoff = backoff::ExponentialBackoff::default();
        let db_name = {
            let mut base_path = self.env.path().to_owned();
            base_path.pop();
            base_path.file_name().and_then(OsStr::to_str).unwrap_or("data.ms").to_string()
        };

        let (reader, writer) = std::io::pipe()?;
        let uploader_task = tokio::spawn(multipart_stream_to_s3(
            s3_bucket_url,
            s3_bucket_region,
            s3_bucket_name,
            s3_snapshot_prefix,
            s3_access_key,
            s3_secret_key,
            s3_max_in_flight_parts,
            s3_signature_duration,
            s3_multipart_part_size,
            must_stop_processing,
            retry_backoff,
            db_name,
            reader,
        ));

        let index_scheduler = IndexScheduler::private_clone(self);
        let builder_task = tokio::task::spawn_blocking(move || {
            stream_tarball_into_pipe(progress, level, writer, index_scheduler)
        });

        let (uploader_result, builder_result) = tokio::join!(uploader_task, builder_task);

        // Check uploader result first to early return on task abortion.
        // safety: JoinHandle can return an error if the task was aborted, cancelled, or panicked.
        uploader_result.unwrap()?;
        builder_result.unwrap()?;

        for task in &mut tasks {
            task.status = Status::Succeeded;
        }

        Ok(tasks)
    }
}

/// Streams a tarball of the database content into a pipe.
#[cfg(unix)]
fn stream_tarball_into_pipe(
    progress: Progress,
    level: u32,
    writer: std::io::PipeWriter,
    index_scheduler: IndexScheduler,
) -> std::result::Result<(), Error> {
    use std::io::Write as _;
    use std::path::Path;

    let writer = flate2::write::GzEncoder::new(writer, flate2::Compression::new(level));
    let mut tarball = tar::Builder::new(writer);

    // 1. Snapshot the version file
    tarball
        .append_path_with_name(&index_scheduler.scheduler.version_file_path, VERSION_FILE_NAME)?;

    // 2. Snapshot the index scheduler LMDB env
    progress.update_progress(SnapshotCreationProgress::SnapshotTheIndexScheduler);
    let tasks_env_file = index_scheduler.env.try_clone_inner_file()?;
    let path = Path::new("tasks").join("data.mdb");
    append_file_to_tarball(&mut tarball, path, tasks_env_file)?;

    // 2.3 Create a read transaction on the index-scheduler
    let rtxn = index_scheduler.env.read_txn()?;

    // 2.4 Create the update files directory
    //     And only copy the update files of the enqueued tasks
    progress.update_progress(SnapshotCreationProgress::SnapshotTheUpdateFiles);
    let enqueued = index_scheduler.queue.tasks.get_status(&rtxn, Status::Enqueued)?;
    let (atomic, update_file_progress) = AtomicUpdateFileStep::new(enqueued.len() as u32);
    progress.update_progress(update_file_progress);

    // We create the update_files directory so that it
    // always exists even if there are no update files
    let update_files_dir = Path::new(UPDATE_FILES_DIR_NAME);
    let src_update_files_dir = {
        let mut path = index_scheduler.env.path().to_path_buf();
        path.pop();
        path.join(UPDATE_FILES_DIR_NAME)
    };
    tarball.append_dir(update_files_dir, src_update_files_dir)?;

    for task_id in enqueued {
        let task = index_scheduler
            .queue
            .tasks
            .get_task(&rtxn, task_id)?
            .ok_or(Error::CorruptedTaskQueue)?;
        if let Some(content_uuid) = task.content_uuid() {
            use std::fs::File;

            let src = index_scheduler.queue.file_store.update_path(content_uuid);
            let mut update_file = File::open(src)?;
            let path = update_files_dir.join(content_uuid.to_string());
            tarball.append_file(path, &mut update_file)?;
        }
        atomic.fetch_add(1, Ordering::Relaxed);
    }

    // 3. Snapshot every indexes
    progress.update_progress(SnapshotCreationProgress::SnapshotTheIndexes);
    let index_mapping = index_scheduler.index_mapper.index_mapping;
    let nb_indexes = index_mapping.len(&rtxn)? as u32;
    let indexes_dir = Path::new("indexes");
    let indexes_references: Vec<_> = index_scheduler
        .index_mapper
        .index_mapping
        .iter(&rtxn)?
        .map(|res| res.map_err(Error::from).map(|(name, uuid)| (name.to_string(), uuid)))
        .collect::<Result<_, Error>>()?;

    // It's prettier to use a for loop instead of the IndexMapper::try_for_each_index
    // method, especially when we need to access the UUID, local path and index number.
    for (i, (name, uuid)) in indexes_references.into_iter().enumerate() {
        progress.update_progress(VariableNameStep::<SnapshotCreationProgress>::new(
            &name, i as u32, nb_indexes,
        ));
        let path = indexes_dir.join(uuid.to_string()).join("data.mdb");
        let index = index_scheduler.index_mapper.index(&rtxn, &name)?;
        let index_file = index.try_clone_inner_file()?;
        tracing::trace!("Appending index file for {name} in {}", path.display());
        append_file_to_tarball(&mut tarball, path, index_file)?;
    }

    drop(rtxn);

    // 4. Snapshot the auth LMDB env
    progress.update_progress(SnapshotCreationProgress::SnapshotTheApiKeys);
    let auth_env_file = index_scheduler.scheduler.auth_env.try_clone_inner_file()?;
    let path = Path::new("auth").join("data.mdb");
    append_file_to_tarball(&mut tarball, path, auth_env_file)?;

    let mut gzencoder = tarball.into_inner()?;
    gzencoder.flush()?;
    gzencoder.try_finish()?;
    let mut writer = gzencoder.finish()?;
    writer.flush()?;

    Result::<_, Error>::Ok(())
}

#[cfg(unix)]
fn append_file_to_tarball<W, P>(
    tarball: &mut tar::Builder<W>,
    path: P,
    mut auth_env_file: fs::File,
) -> Result<(), Error>
where
    W: std::io::Write,
    P: AsRef<std::path::Path>,
{
    use std::io::{Seek as _, SeekFrom};

    // Note: A previous snapshot operation may have left the cursor
    //       at the end of the file so we need to seek to the start.
    auth_env_file.seek(SeekFrom::Start(0))?;
    tarball.append_file(path, &mut auth_env_file)?;
    Ok(())
}

/// Streams the content read from the given reader to S3.
#[cfg(unix)]
#[allow(clippy::too_many_arguments)]
async fn multipart_stream_to_s3(
    s3_bucket_url: String,
    s3_bucket_region: String,
    s3_bucket_name: String,
    s3_snapshot_prefix: String,
    s3_access_key: String,
    s3_secret_key: String,
    s3_max_in_flight_parts: std::num::NonZero<usize>,
    s3_signature_duration: std::time::Duration,
    s3_multipart_part_size: u64,
    must_stop_processing: super::MustStopProcessing,
    retry_backoff: backoff::exponential::ExponentialBackoff<backoff::SystemClock>,
    db_name: String,
    reader: std::io::PipeReader,
) -> Result<(), Error> {
    use std::{collections::VecDeque, os::fd::OwnedFd, path::PathBuf};

    use bytes::{Bytes, BytesMut};
    use reqwest::{Client, Response};
    use rusty_s3::S3Action as _;
    use rusty_s3::{actions::CreateMultipartUpload, Bucket, BucketError, Credentials, UrlStyle};
    use tokio::task::JoinHandle;

    let reader = OwnedFd::from(reader);
    let reader = tokio::net::unix::pipe::Receiver::from_owned_fd(reader)?;
    let s3_snapshot_prefix = PathBuf::from(s3_snapshot_prefix);
    let url =
        s3_bucket_url.parse().map_err(BucketError::ParseError).map_err(Error::S3BucketError)?;
    let bucket = Bucket::new(url, UrlStyle::Path, s3_bucket_name, s3_bucket_region)
        .map_err(Error::S3BucketError)?;
    let credential = Credentials::new(s3_access_key, s3_secret_key);

    // Note for the future (rust 1.91+): use with_added_extension, it's prettier
    let object_path = s3_snapshot_prefix.join(format!("{db_name}.snapshot"));
    // Note: It doesn't work on Windows and if a port to this platform is needed,
    //       use the slash-path crate or similar to get the correct path separator.
    let object = object_path.display().to_string();

    let action = bucket.create_multipart_upload(Some(&credential), &object);
    let url = action.sign(s3_signature_duration);

    let client = Client::new();
    let resp = client.post(url).send().await.map_err(Error::S3HttpError)?;
    let status = resp.status();

    let body = match resp.error_for_status_ref() {
        Ok(_) => resp.text().await.map_err(Error::S3HttpError)?,
        Err(_) => {
            return Err(Error::S3Error { status, body: resp.text().await.unwrap_or_default() })
        }
    };

    let multipart =
        CreateMultipartUpload::parse_response(&body).map_err(|e| Error::S3XmlError(Box::new(e)))?;
    tracing::debug!("Starting the upload of the snapshot to {object}");

    // We use this bumpalo for etags strings.
    let bump = bumpalo::Bump::new();
    let mut etags = Vec::<&str>::new();
    let mut in_flight = VecDeque::<(JoinHandle<reqwest::Result<Response>>, Bytes)>::with_capacity(
        s3_max_in_flight_parts.get(),
    );

    // Part numbers start at 1 and cannot be larger than 10k
    for part_number in 1u16.. {
        if must_stop_processing.get() {
            return Err(Error::AbortedTask);
        }

        let part_upload =
            bucket.upload_part(Some(&credential), &object, part_number, multipart.upload_id());
        let url = part_upload.sign(s3_signature_duration);

        // Wait for a buffer to be ready if there are in-flight parts that landed
        let mut buffer = if in_flight.len() >= s3_max_in_flight_parts.get() {
            let (handle, buffer) = in_flight.pop_front().expect("At least one in flight request");
            let resp = join_and_map_error(handle).await?;
            extract_and_append_etag(&bump, &mut etags, resp.headers())?;

            let mut buffer = match buffer.try_into_mut() {
                Ok(buffer) => buffer,
                Err(_) => unreachable!("All bytes references were consumed in the task"),
            };
            buffer.clear();
            buffer
        } else {
            BytesMut::with_capacity(s3_multipart_part_size as usize)
        };

        // If we successfully read enough bytes,
        // we can continue and send the buffer/part
        while buffer.len() < (s3_multipart_part_size as usize / 2) {
            // Wait for the pipe to be readable

            use std::io;
            reader.readable().await?;

            match reader.try_read_buf(&mut buffer) {
                Ok(0) => break,
                // We read some bytes but maybe not enough
                Ok(_) => continue,
                // The readiness event is a false positive.
                Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => continue,
                Err(e) => return Err(e.into()),
            }
        }

        if buffer.is_empty() {
            // Break the loop if the buffer is
            // empty after we tried to read bytes
            break;
        }

        let body = buffer.freeze();
        tracing::trace!("Sending part {part_number}");
        let task = tokio::spawn({
            let client = client.clone();
            let body = body.clone();
            backoff::future::retry(retry_backoff.clone(), move || {
                let client = client.clone();
                let url = url.clone();
                let body = body.clone();
                async move {
                    match client.put(url).body(body).send().await {
                        Ok(resp) if resp.status().is_client_error() => {
                            resp.error_for_status().map_err(backoff::Error::Permanent)
                        }
                        Ok(resp) => Ok(resp),
                        Err(e) => Err(backoff::Error::transient(e)),
                    }
                }
            })
        });
        in_flight.push_back((task, body));
    }

    for (handle, _buffer) in in_flight {
        let resp = join_and_map_error(handle).await?;
        extract_and_append_etag(&bump, &mut etags, resp.headers())?;
    }

    tracing::debug!("Finalizing the multipart upload");

    let action = bucket.complete_multipart_upload(
        Some(&credential),
        &object,
        multipart.upload_id(),
        etags.iter().map(AsRef::as_ref),
    );
    let url = action.sign(s3_signature_duration);
    let body = action.body();
    let resp = backoff::future::retry(retry_backoff, move || {
        let client = client.clone();
        let url = url.clone();
        let body = body.clone();
        async move {
            match client.post(url).body(body).send().await {
                Ok(resp) if resp.status().is_client_error() => {
                    resp.error_for_status().map_err(backoff::Error::Permanent)
                }
                Ok(resp) => Ok(resp),
                Err(e) => Err(backoff::Error::transient(e)),
            }
        }
    })
    .await
    .map_err(Error::S3HttpError)?;

    let status = resp.status();
    let body = resp.text().await.map_err(|e| Error::S3Error { status, body: e.to_string() })?;
    if status.is_success() {
        Ok(())
    } else {
        Err(Error::S3Error { status, body })
    }
}

#[cfg(unix)]
async fn join_and_map_error(
    join_handle: tokio::task::JoinHandle<Result<reqwest::Response, reqwest::Error>>,
) -> Result<reqwest::Response> {
    // safety: Panic happens if the task (JoinHandle) was aborted, cancelled, or panicked
    let request = join_handle.await.unwrap();
    let resp = request.map_err(Error::S3HttpError)?;
    match resp.error_for_status_ref() {
        Ok(_) => Ok(resp),
        Err(_) => Err(Error::S3Error {
            status: resp.status(),
            body: resp.text().await.unwrap_or_default(),
        }),
    }
}

#[cfg(unix)]
fn extract_and_append_etag<'b>(
    bump: &'b bumpalo::Bump,
    etags: &mut Vec<&'b str>,
    headers: &reqwest::header::HeaderMap,
) -> Result<()> {
    use reqwest::header::ETAG;

    let etag = headers.get(ETAG).ok_or_else(|| Error::S3XmlError("Missing ETag header".into()))?;
    let etag = etag.to_str().map_err(|e| Error::S3XmlError(Box::new(e)))?;
    etags.push(bump.alloc_str(etag));

    Ok(())
}
