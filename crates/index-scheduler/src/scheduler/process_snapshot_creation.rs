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
        const S3_ACCESS_KEY: &str = "MEILI_S3_ACCESS_KEY";
        const S3_SECRET_KEY: &str = "MEILI_S3_SECRET_KEY";

        let bucket_url = std::env::var(S3_BUCKET_URL);
        let bucket_region = std::env::var(S3_BUCKET_REGION);
        let bucket_name = std::env::var(S3_BUCKET_NAME);
        let access_key = std::env::var(S3_ACCESS_KEY);
        let secret_key = std::env::var(S3_SECRET_KEY);
        match (bucket_url, bucket_region, bucket_name, access_key, secret_key) {
            (
                Ok(bucket_url),
                Ok(bucket_region),
                Ok(bucket_name),
                Ok(access_key),
                Ok(secret_key),
            ) => {
                let runtime = self.runtime.as_ref().expect("Runtime not initialized");
                runtime.block_on(self.process_snapshot_to_s3(
                    progress,
                    bucket_url,
                    bucket_region,
                    bucket_name,
                    access_key,
                    secret_key,
                    tasks,
                ))
            }
            (
                Err(VarError::NotPresent),
                Err(VarError::NotPresent),
                Err(VarError::NotPresent),
                Err(VarError::NotPresent),
                Err(VarError::NotPresent),
            ) => self.process_snapshots_to_disk(progress, tasks),
            (Err(e), _, _, _, _)
            | (_, Err(e), _, _, _)
            | (_, _, Err(e), _, _)
            | (_, _, _, Err(e), _)
            | (_, _, _, _, Err(e)) => {
                // TODO: Handle error gracefully
                panic!("Error while reading environment variables: {}", e);
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
                let src = self.queue.file_store.get_update_path(content_uuid);
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

    pub(super) async fn process_snapshot_to_s3(
        &self,
        progress: Progress,
        bucket_url: String,
        bucket_region: String,
        bucket_name: String,
        access_key: String,
        secret_key: String,
        mut tasks: Vec<Task>,
    ) -> Result<Vec<Task>> {
        const ONE_HOUR: Duration = Duration::from_secs(3600);
        // default part size is 250MiB
        const MIN_PART_SIZE: usize = 250 * 1024 * 1024;
        // 10MiB
        const TEN_MIB: usize = 10 * 1024 * 1024;
        // The maximum number of parts that can be uploaded to a single multipart upload.
        const MAX_NUMBER_PARTS: usize = 10_000;
        // The maximum number of parts that can be uploaded in parallel.
        const S3_MAX_IN_FLIGHT_PARTS: &str = "MEILI_S3_MAX_IN_FLIGHT_PARTS";
        let max_in_flight_parts: usize = match std::env::var(S3_MAX_IN_FLIGHT_PARTS) {
            Ok(val) => val.parse().expect("Failed to parse MEILI_S3_MAX_IN_FLIGHT_PARTS"),
            Err(_) => 10,
        };

        let client = Client::new();
        // TODO Remove this unwrap
        let url = bucket_url.parse().unwrap();
        eprintln!("{url:?}");
        let bucket = Bucket::new(url, UrlStyle::Path, bucket_name, bucket_region).unwrap();
        let credential = Credentials::new(access_key, secret_key);

        let rtxn = self.read_txn()?;

        // Every part must be between 5 MB and 5 GB in size, except for the last part
        // A maximum of 10,000 parts can be uploaded to a single multipart upload.
        //
        // Part numbers can be any number from 1 to 10,000, inclusive.
        // A part number uniquely identifies a part and also defines its position within
        // the object being created. If you upload a new part using the same part number
        // that was used with a previous part, the previously uploaded part is overwritten.
        progress.update_progress(SnapshotCreationProgress::SnapshotTheIndexes);
        let index_mapping = self.index_mapper.index_mapping;
        let nb_indexes = index_mapping.len(&rtxn)? as u32;
        for (i, result) in index_mapping.iter(&rtxn)?.enumerate() {
            let (name, uuid) = result?;
            progress.update_progress(VariableNameStep::<SnapshotCreationProgress>::new(
                name, i as u32, nb_indexes,
            ));
            let index = self.index_mapper.index(&rtxn, name)?;
            let file = index
                .try_clone_inner_file()
                .map_err(|e| Error::from_milli(e, Some(name.to_string())))?;
            let mmap = unsafe { memmap2::Mmap::map(&file)? };
            mmap.advise(memmap2::Advice::Sequential)?;
            let mmap = bytes::Bytes::from_owner(mmap);

            let object = uuid.to_string();
            let action = bucket.create_multipart_upload(Some(&credential), &object);
            let url = action.sign(ONE_HOUR);
            let resp = client.post(url).send().await.unwrap().error_for_status().unwrap();
            let body = resp.text().await.unwrap();

            let multipart = CreateMultipartUpload::parse_response(&body).unwrap();
            let mut etags = Vec::<String>::new();

            let part_size = mmap.len() / MAX_NUMBER_PARTS;
            let part_size = if part_size < TEN_MIB { MIN_PART_SIZE } else { part_size };

            let mut in_flight_parts = VecDeque::with_capacity(max_in_flight_parts);
            let number_of_parts = mmap.len().div_ceil(part_size);
            for i in 0..number_of_parts {
                let part_number = u16::try_from(i).unwrap().checked_add(1).unwrap();
                let part_upload = bucket.upload_part(
                    Some(&credential),
                    &object,
                    part_number,
                    multipart.upload_id(),
                );
                let url = part_upload.sign(ONE_HOUR);

                // Make sure we do not read out of bound
                let body = if mmap.len() < part_size * (i + 1) {
                    mmap.slice(part_size * i..)
                } else {
                    mmap.slice(part_size * i..part_size * (i + 1))
                };

                let task = tokio::spawn(client.put(url).body(body).send());
                in_flight_parts.push_back(task);

                if in_flight_parts.len() == max_in_flight_parts {
                    let resp = in_flight_parts
                        .pop_front()
                        .unwrap()
                        .await
                        .unwrap()
                        .unwrap()
                        .error_for_status()
                        .unwrap();
                    let etag =
                        resp.headers().get(ETAG).expect("every UploadPart request returns an Etag");
                    // TODO use bumpalo to reduce the number of allocations
                    etags.push(etag.to_str().unwrap().to_owned());
                }
            }

            for join_handle in in_flight_parts {
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

            let body = resp.text().await.unwrap();
            // TODO remove this
            println!("it worked! {body}");
        }

        for task in &mut tasks {
            task.status = Status::Succeeded;
        }

        Ok(tasks)
    }
}
