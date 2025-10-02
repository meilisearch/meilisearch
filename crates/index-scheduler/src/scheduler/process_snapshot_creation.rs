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
        let compaction_option = if self.scheduler.experimental_no_snapshot_compaction {
            CompactionOption::Disabled
        } else {
            CompactionOption::Enabled
        };
        match compaction_option {
            CompactionOption::Enabled => self.process_snapshot_with_temp(progress, tasks),
            CompactionOption::Disabled => self.process_snapshot_with_pipe(progress, tasks),
        }
    }

    fn process_snapshot_with_temp(
        &self,
        progress: Progress,
        mut tasks: Vec<Task>,
    ) -> Result<Vec<Task>> {
        progress.update_progress(SnapshotCreationProgress::StartTheSnapshotCreation);

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

        self.env.copy_to_path(dst.join("data.mdb"), CompactionOption::Enabled)?;

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
                .copy_to_path(dst.join("data.mdb"), CompactionOption::Enabled)
                .map_err(|e| Error::from_milli(e, Some(name.to_string())))?;
        }

        drop(rtxn);

        // 4. Snapshot the auth LMDB env
        progress.update_progress(SnapshotCreationProgress::SnapshotTheApiKeys);
        let dst = temp_snapshot_dir.path().join("auth");
        fs::create_dir_all(&dst)?;
        self.scheduler.auth_env.copy_to_path(dst.join("data.mdb"), CompactionOption::Enabled)?;

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

    fn process_snapshot_with_pipe(
        &self,
        progress: Progress,
        mut tasks: Vec<Task>,
    ) -> Result<Vec<Task>> {
        progress.update_progress(SnapshotCreationProgress::StartTheSnapshotCreation);

        fs::create_dir_all(&self.scheduler.snapshots_path)?;

        // 1. Find the base path and original name of the database

        // TODO find a better way to get this path
        let mut base_path = self.env.path().to_owned();
        base_path.pop();
        let base_path = base_path;
        let db_name = base_path.file_name().and_then(OsStr::to_str).unwrap_or("data.ms");

        // 2. Start the tarball builder. The tarball will be created on another thread from piped data.

        let mut builder = compression::PipedArchiveBuilder::new(
            self.scheduler.snapshots_path.clone(),
            format!("{db_name}.snapshot"),
            base_path,
        );

        // 3. Snapshot the VERSION file
        builder.add_file_to_archive(self.scheduler.version_file_path.clone())?;

        // 4. Snapshot the index-scheduler LMDB env
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

        // 4.1 First copy the LMDB env of the index-scheduler
        progress.update_progress(SnapshotCreationProgress::SnapshotTheIndexScheduler);
        builder.add_env_to_archive(&self.env)?;

        // 4.2 Create a read transaction on the index-scheduler
        let rtxn = self.env.read_txn()?;

        // 4.3 Only copy the update files of the enqueued tasks
        progress.update_progress(SnapshotCreationProgress::SnapshotTheUpdateFiles);
        builder.add_dir_to_archive(self.queue.file_store.path().to_path_buf())?;
        let enqueued = self.queue.tasks.get_status(&rtxn, Status::Enqueued)?;
        let (atomic, update_file_progress) = AtomicUpdateFileStep::new(enqueued.len() as u32);
        progress.update_progress(update_file_progress);
        for task_id in enqueued {
            let task =
                self.queue.tasks.get_task(&rtxn, task_id)?.ok_or(Error::CorruptedTaskQueue)?;
            if let Some(content_uuid) = task.content_uuid() {
                let src = self.queue.file_store.get_update_path(content_uuid);
                builder.add_file_to_archive(src)?;
            }
            atomic.fetch_add(1, Ordering::Relaxed);
        }

        // 5. Snapshot every index
        progress.update_progress(SnapshotCreationProgress::SnapshotTheIndexes);
        builder.add_dir_to_archive(self.index_mapper.base_path().to_path_buf())?;
        let index_mapping = self.index_mapper.index_mapping;
        let nb_indexes = index_mapping.len(&rtxn)? as u32;

        for (i, result) in index_mapping.iter(&rtxn)?.enumerate() {
            let (name, _) = result?;
            progress.update_progress(VariableNameStep::<SnapshotCreationProgress>::new(
                name, i as u32, nb_indexes,
            ));
            let index = self.index_mapper.index(&rtxn, name)?;
            builder.add_env_to_archive(index.raw_env())?;
        }

        drop(rtxn);

        // 6. Snapshot the auth LMDB env
        progress.update_progress(SnapshotCreationProgress::SnapshotTheApiKeys);
        builder.add_env_to_archive(&self.scheduler.auth_env)?;

        // 7. Finalize the tarball
        progress.update_progress(SnapshotCreationProgress::CreateTheTarball);
        let file = builder.finish()?;

        // 8. Change the permission to make the snapshot readonly
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
}
