use std::collections::VecDeque;
use std::env::VarError;
use std::ffi::OsStr;
use std::fs;
use std::sync::atomic::Ordering;
use std::time::Duration;

use meilisearch_types::heed::CompactionOption;
use meilisearch_types::milli::progress::{Progress, VariableNameStep};
use meilisearch_types::tasks::{Status, Task};
use meilisearch_types::{compression, VERSION_FILE_NAME};
use reqwest::header::ETAG;
use reqwest::Client;
use rusty_s3::actions::{CreateMultipartUpload, S3Action as _};
use rusty_s3::{Bucket, Credentials, UrlStyle};

use crate::heed::EnvOpenOptions;
use crate::processing::{AtomicUpdateFileStep, SnapshotCreationProgress};
use crate::queue::TaskQueue;
use crate::{Error, IndexScheduler, Result};

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

        const S3_BUCKET_URL: &str = "MEILI_S3_BUCKET_URL";
        const S3_BUCKET_REGION: &str = "MEILI_S3_BUCKET_REGION";
        const S3_BUCKET_NAME: &str = "MEILI_S3_BUCKET_NAME";
        const S3_SNAPSHOT_PREFIX: &str = "MEILI_S3_SNAPSHOT_PREFIX";
        const S3_ACCESS_KEY: &str = "MEILI_S3_ACCESS_KEY";
        const S3_SECRET_KEY: &str = "MEILI_S3_SECRET_KEY";

        let bucket_url = std::env::var(S3_BUCKET_URL).map_err(|e| (S3_BUCKET_URL, e));
        let bucket_region = std::env::var(S3_BUCKET_REGION).map_err(|e| (S3_BUCKET_REGION, e));
        let bucket_name = std::env::var(S3_BUCKET_NAME).map_err(|e| (S3_BUCKET_NAME, e));
        let snapshot_prefix =
            std::env::var(S3_SNAPSHOT_PREFIX).map_err(|e| (S3_SNAPSHOT_PREFIX, e));
        let access_key = std::env::var(S3_ACCESS_KEY).map_err(|e| (S3_ACCESS_KEY, e));
        let secret_key = std::env::var(S3_SECRET_KEY).map_err(|e| (S3_SECRET_KEY, e));
        match (bucket_url, bucket_region, bucket_name, snapshot_prefix, access_key, secret_key) {
            (
                Ok(bucket_url),
                Ok(bucket_region),
                Ok(bucket_name),
                Ok(snapshot_prefix),
                Ok(access_key),
                Ok(secret_key),
            ) => {
                let runtime = self.runtime.as_ref().expect("Runtime not initialized");
                #[cfg(not(unix))]
                panic!("Non-unix platform does not support S3 snapshotting");
                #[cfg(unix)]
                runtime.block_on(self.process_snapshot_to_s3(
                    progress,
                    bucket_url,
                    bucket_region,
                    bucket_name,
                    snapshot_prefix,
                    access_key,
                    secret_key,
                    tasks,
                ))
            }
            (
                Err((_, VarError::NotPresent)),
                Err((_, VarError::NotPresent)),
                Err((_, VarError::NotPresent)),
                Err((_, VarError::NotPresent)),
                Err((_, VarError::NotPresent)),
                Err((_, VarError::NotPresent)),
            ) => self.process_snapshots_to_disk(progress, tasks),
            (Err((var, e)), _, _, _, _, _)
            | (_, Err((var, e)), _, _, _, _)
            | (_, _, Err((var, e)), _, _, _)
            | (_, _, _, Err((var, e)), _, _)
            | (_, _, _, _, Err((var, e)), _)
            | (_, _, _, _, _, Err((var, e))) => {
                // TODO: Handle error gracefully
                panic!("Error while reading environment variables: {}: {}", var, e);
            }
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
        let update_files_dir = temp_snapshot_dir.path().join("update_files");
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
        bucket_url: String,
        bucket_region: String,
        bucket_name: String,
        snapshot_prefix: String,
        access_key: String,
        secret_key: String,
        mut tasks: Vec<Task>,
    ) -> Result<Vec<Task>> {
        use std::io;
        use std::path::Path;

        use async_compression::tokio::write::GzipEncoder;
        use async_compression::Level;
        use bytes::{Bytes, BytesMut};
        use meilisearch_types::milli::update::new::StdResult;
        use tokio::fs::File;
        use tokio::io::AsyncReadExt;
        use tokio::task::JoinHandle;

        const ONE_HOUR: Duration = Duration::from_secs(3600);
        // default part size is 250MiB
        // TODO use 375MiB
        // It must be at least twice 5MiB
        const PART_SIZE: usize = 10 * 1024 * 1024;

        // The maximum number of parts that can be uploaded in parallel.
        const S3_MAX_IN_FLIGHT_PARTS: &str = "MEILI_S3_MAX_IN_FLIGHT_PARTS";
        let max_in_flight_parts: usize = match std::env::var(S3_MAX_IN_FLIGHT_PARTS) {
            Ok(val) => val.parse().expect("Failed to parse MEILI_S3_MAX_IN_FLIGHT_PARTS"),
            Err(_) => 10,
        };

        let client = Client::new();
        // TODO Remove this unwrap
        let url = bucket_url.parse().unwrap();
        let bucket = Bucket::new(url, UrlStyle::Path, bucket_name, bucket_region).unwrap();
        let credential = Credentials::new(access_key, secret_key);
        // TODO change this and use the database name like in the original version
        let object = format!("{}/data.ms.snapshot", snapshot_prefix);

        // TODO implement exponential backoff on upload requests: https://docs.rs/backoff
        // TODO return a result with actual errors
        // TODO sign for longer than an hour?
        // TODO Use a better thing than a String for the object path
        let (writer, mut reader) = tokio::net::unix::pipe::pipe()?;
        let uploader_task = tokio::spawn(async move {
            let action = bucket.create_multipart_upload(Some(&credential), &object);
            // TODO Question: If it is only signed for an hour and a snapshot takes longer than an hour, what happens?
            //                If the part is deleted (like a TTL) we should sign it for at least 24 hours.
            let url = action.sign(ONE_HOUR);
            let resp = client.post(url).send().await.unwrap().error_for_status().unwrap();
            let body = resp.text().await.unwrap();

            let multipart = CreateMultipartUpload::parse_response(&body).unwrap();
            let mut etags = Vec::<String>::new();
            let mut in_flight = VecDeque::<(
                JoinHandle<StdResult<reqwest::Response, reqwest::Error>>,
                Bytes,
            )>::with_capacity(max_in_flight_parts);

            for part_number in 1u16.. {
                let part_upload = bucket.upload_part(
                    Some(&credential),
                    &object,
                    part_number,
                    multipart.upload_id(),
                );
                let url = part_upload.sign(ONE_HOUR);

                // Wait for a buffer to be ready if there are in-flight parts that landed
                let mut buffer = if in_flight.len() >= max_in_flight_parts {
                    let (request, buffer) = in_flight.pop_front().unwrap();
                    let mut buffer = buffer.try_into_mut().expect("Valid to convert into BytesMut");
                    let resp = request.await.unwrap().unwrap().error_for_status().unwrap();
                    let etag =
                        resp.headers().get(ETAG).expect("every UploadPart request returns an Etag");
                    // TODO use bumpalo to reduce the number of allocations
                    etags.push(etag.to_str().unwrap().to_owned());
                    buffer.clear();
                    buffer
                } else {
                    // TODO Base this on the available memory
                    BytesMut::with_capacity(PART_SIZE)
                };

                while buffer.len() < (PART_SIZE / 2) {
                    eprintln!(
                        "buffer is {:.2}% full, trying to read more",
                        buffer.len() as f32 / buffer.capacity() as f32 * 100.0
                    );

                    // Wait for the pipe to be readable
                    reader.readable().await?;

                    match reader.try_read_buf(&mut buffer) {
                        Ok(0) => break,
                        // We read some bytes but maybe not enough
                        Ok(n) => {
                            eprintln!("Read {} bytes from pipe, continuing", n);
                            continue;
                        }
                        // The readiness event is a false positive.
                        Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => {
                            eprintln!("received a WouldBlock");
                            continue;
                        }
                        Err(e) => return Err(e.into()),
                    }
                }

                eprintln!(
                    "buffer is {:.2}% full",
                    buffer.len() as f32 / buffer.capacity() as f32 * 100.0
                );

                if buffer.is_empty() {
                    eprintln!("buffer is empty, breaking part number loop");
                    // Break the loop if the buffer is empty
                    // after we tried to read bytes
                    break;
                }

                let body = buffer.freeze();
                eprintln!("Sending part {}", part_number);
                let task = tokio::spawn(client.put(url).body(body.clone()).send());
                in_flight.push_back((task, body));
            }

            for (join_handle, _buffer) in in_flight {
                let resp = join_handle.await.unwrap().unwrap().error_for_status().unwrap();
                let etag =
                    resp.headers().get(ETAG).expect("every UploadPart request returns an Etag");
                // TODO use bumpalo to reduce the number of allocations
                etags.push(etag.to_str().unwrap().to_owned());
            }

            let action = bucket.complete_multipart_upload(
                Some(&credential),
                &object,
                multipart.upload_id(),
                etags.iter().map(AsRef::as_ref),
            );
            let url = action.sign(ONE_HOUR);
            let resp = client
                .post(url)
                .body(action.body())
                .send()
                .await
                .unwrap()
                .error_for_status()
                .unwrap();

            // TODO do a better check and do not assert
            assert!(resp.status().is_success());

            Result::<_, Error>::Ok(())
        });

        // TODO not a big fan of this clone
        //      remove it and get all the necessary data from the scheduler
        let index_scheduler = IndexScheduler::private_clone(self);
        let builder_task = tokio::task::spawn(async move {
            // let compressed_writer = GzipEncoder::with_quality(writer, Level::Fastest);
            let mut tarball = tokio_tar::Builder::new(writer);

            // 1. Snapshot the version file
            tarball
                .append_path_with_name(
                    &index_scheduler.scheduler.version_file_path,
                    VERSION_FILE_NAME,
                )
                .await?;

            // 2. Snapshot the index scheduler LMDB env
            progress.update_progress(SnapshotCreationProgress::SnapshotTheIndexScheduler);
            let mut tasks_env_file =
                index_scheduler.env.try_clone_inner_file().map(File::from_std)?;
            let path = Path::new("tasks").join("data.mdb");
            // NOTE when commenting this line, the tarballl works better
            tarball.append_file(path, &mut tasks_env_file).await?;
            drop(tasks_env_file);

            // 2.3 Create a read transaction on the index-scheduler
            let rtxn = index_scheduler.env.read_txn()?;

            // 2.4 Create the update files directory
            //     And only copy the update files of the enqueued tasks
            progress.update_progress(SnapshotCreationProgress::SnapshotTheUpdateFiles);
            let enqueued = index_scheduler.queue.tasks.get_status(&rtxn, Status::Enqueued)?;
            let (atomic, update_file_progress) = AtomicUpdateFileStep::new(enqueued.len() as u32);
            progress.update_progress(update_file_progress);
            let update_files_dir = Path::new("update_files");
            for task_id in enqueued {
                let task = index_scheduler
                    .queue
                    .tasks
                    .get_task(&rtxn, task_id)?
                    .ok_or(Error::CorruptedTaskQueue)?;
                if let Some(content_uuid) = task.content_uuid() {
                    let src = index_scheduler.queue.file_store.update_path(content_uuid);
                    let mut update_file = File::open(src).await?;
                    let path = update_files_dir.join(content_uuid.to_string());
                    tarball.append_file(path, &mut update_file).await?;
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

            dbg!(&indexes_references);

            // Note that we need to collect and open all of the indexes files because
            // otherwise, using a for loop, we would have to have a Send rtxn.
            for (i, (name, uuid)) in indexes_references.into_iter().enumerate() {
                progress.update_progress(VariableNameStep::<SnapshotCreationProgress>::new(
                    &name, i as u32, nb_indexes,
                ));
                let path = indexes_dir.join(uuid.to_string()).join("data.mdb");
                let index = index_scheduler.index_mapper.index(&rtxn, &name)?;
                let mut index_file = index.try_clone_inner_file().map(File::from_std).unwrap();
                eprintln!("Appending index file for {} in {}", name, path.display());
                tarball.append_file(path, &mut index_file).await?;
            }

            drop(rtxn);

            // 4. Snapshot the auth LMDB env
            progress.update_progress(SnapshotCreationProgress::SnapshotTheApiKeys);
            let mut auth_env_file = index_scheduler
                .scheduler
                .auth_env
                .try_clone_inner_file()
                .map(File::from_std)
                .unwrap();
            let path = Path::new("auth").join("data.mdb");
            tarball.append_file(path, &mut auth_env_file).await?;

            tarball.into_inner().await?;

            Result::<_, Error>::Ok(())
        });

        let (uploader_result, builder_result) = tokio::join!(uploader_task, builder_task);

        builder_result.unwrap()?;
        uploader_result.unwrap()?;

        for task in &mut tasks {
            task.status = Status::Succeeded;
        }

        Ok(tasks)
    }
}
