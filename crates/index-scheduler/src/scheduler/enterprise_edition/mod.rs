// Copyright © 2025 Meilisearch Some Rights Reserved
// This file is part of Meilisearch Enterprise Edition (EE).
// Use of this source code is governed by the Business Source License 1.1,
// as found in the LICENSE-EE file or at <https://mariadb.com/bsl11>

use std::time::Duration;

use bumpalo::Bump;
use meilisearch_types::milli::documents::PrimaryKey;
use meilisearch_types::milli::progress::{EmbedderStats, Progress};
use meilisearch_types::milli::update::new::indexer;
use meilisearch_types::milli::update::new::indexer::current_edition::sharding::Shards;
use meilisearch_types::milli::{self};
use meilisearch_types::network::Remote;
use meilisearch_types::tasks::network::{NetworkTopologyState, Origin};
use meilisearch_types::tasks::{KindWithContent, Status, Task};
use roaring::RoaringBitmap;

use super::create_batch::Batch;
use crate::scheduler::process_batch::ProcessBatchInfo;
use crate::scheduler::process_export::{ExportContext, ExportOptions, TargetInstance};
use crate::utils::ProcessingBatch;
use crate::{Error, IndexScheduler, Result};

impl IndexScheduler {
    pub(super) fn process_network_index_batch(
        &self,
        mut network_task: Task,
        inner_batch: Box<Batch>,
        current_batch: &mut ProcessingBatch,
        progress: Progress,
    ) -> Result<(Vec<Task>, ProcessBatchInfo)> {
        let KindWithContent::NetworkTopologyChange(network_topology_change) =
            &mut network_task.kind
        else {
            tracing::error!("unexpected network kind for network task while processing batch");
            return Err(Error::CorruptedTaskQueue);
        };

        let network = network_topology_change.network_for_state();

        let (mut tasks, info) =
            self.process_batch(*inner_batch, current_batch, progress, network)?;

        for task in &tasks {
            let Some(network) = task.network.as_ref() else {
                continue;
            };
            let Some(import) = network.import_data() else {
                continue;
            };
            if let Some(index_name) = import.index_name.as_deref() {
                network_topology_change.process_remote_tasks(
                    &import.remote_name,
                    index_name,
                    import.document_count,
                );
            }
        }
        network_task.details = Some(network_topology_change.to_details());

        tasks.push(network_task);
        Ok((tasks, info))
    }

    pub(super) fn process_network_ready(
        &self,
        mut task: Task,
        progress: Progress,
    ) -> Result<(Vec<Task>, ProcessBatchInfo)> {
        let KindWithContent::NetworkTopologyChange(network_topology_change) = &mut task.kind else {
            tracing::error!("network topology change task has the wrong kind with content");
            return Err(Error::CorruptedTaskQueue);
        };

        let Some(task_network) = &task.network else {
            tracing::error!("network topology change task has no network");
            return Err(Error::CorruptedTaskQueue);
        };

        let origin;
        let origin = match task_network.origin() {
            Some(origin) => origin,
            None => {
                let myself = network_topology_change.in_name().expect("origin is not the leader");
                origin = Origin {
                    remote_name: myself.to_string(),
                    task_uid: task.uid,
                    network_version: task_network.network_version(),
                };
                &origin
            }
        };

        let mut moved_documents = None;
        if let (Some((remotes, out_name)), Some(new_shards)) =
            (network_topology_change.export_to_process(), network_topology_change.new_shards())
        {
            moved_documents = Some(self.balance_documents(
                remotes,
                out_name,
                new_shards,
                origin,
                &progress,
                &self.scheduler.must_stop_processing,
            )?);
        }
        if let Some(moved_documents) = moved_documents {
            // we need the mut moved documents to avoid a lifetime error in the previous if let.
            network_topology_change.set_moved(moved_documents);
        }
        network_topology_change.update_state();
        if network_topology_change.state() == NetworkTopologyState::Finished {
            task.status = Status::Succeeded;
        }

        task.details = Some(network_topology_change.to_details());
        Ok((vec![task], Default::default()))
    }

    fn balance_documents<'a, I: Iterator<Item = (&'a str, &'a Remote)> + Clone>(
        &self,
        remotes: I,
        out_name: &str,
        new_shards: Shards,
        network_change_origin: &Origin,
        progress: &Progress,
        must_stop_processing: &crate::scheduler::MustStopProcessing,
    ) -> crate::Result<u64> {
        // TECHDEBT: this spawns a `ureq` agent additionally to `reqwest`. We probably want to harmonize all of this.
        let config = http_client::ureq::config::Config::builder()
            .prepare(|config| config.timeout_global(Some(Duration::from_secs(5))))
            .build();

        /// FIXME: breaks use of internal IPs
        let agent = http_client::ureq::Agent::new_with_config(
            config,
            http_client::policy::Policy::deny_all_local_ips(),
        )
        ;

        let mut indexer_alloc = Bump::new();

        let scheduler_rtxn = self.env.read_txn()?;

        let index_count = self.index_mapper.index_count(&scheduler_rtxn)?;

        // when the instance is empty, we still need to tell that to remotes, as they cannot know of that fact and will be waiting for
        // data
        if index_count == 0 {
            for (remote_name, remote) in remotes {
                let target = TargetInstance {
                    remote_name: Some(remote_name),
                    base_url: &remote.url,
                    api_key: remote.write_api_key.as_deref(),
                };

                let res = self.export_no_index(
                    target,
                    out_name,
                    network_change_origin,
                    &agent,
                    must_stop_processing,
                );

                if let Err(err) = res {
                    tracing::warn!("Could not signal not to wait documents to `{remote_name}` due to error: {err}");
                }
            }
            return Ok(0);
        }

        let mut total_moved_documents = 0;

        self.index_mapper.try_for_each_index::<(), ()>(
            &scheduler_rtxn,
            |index_uid, index| -> crate::Result<()> {
                indexer_alloc.reset();
                let err = |err| Error::from_milli(err, Some(index_uid.to_string()));
                let index_rtxn = index.read_txn()?;
                let all_docids = index.external_documents_ids();
                let mut documents_to_move_to =
                    hashbrown::HashMap::<String, RoaringBitmap>::new();
                let mut documents_to_delete = RoaringBitmap::new();

                for res in all_docids.iter(&index_rtxn)? {
                    let (external_docid, docid) = res?;
                    match new_shards.processing_shard(external_docid) {
                        Some(shard) if shard.is_own => continue,
                        Some(shard) => {
                            documents_to_move_to.entry_ref(&shard.name).or_default().insert(docid);
                        }
                        None => {
                            documents_to_delete.insert(docid);
                        }
                    }
                }

                let fields_ids_map = index.fields_ids_map(&index_rtxn)?;

                for (remote_name, remote) in remotes.clone() {
                    let documents_to_move =
                        documents_to_move_to.remove(remote_name).unwrap_or_default();

                    let target = TargetInstance {
                        remote_name: Some(remote_name),
                        base_url: &remote.url,
                        api_key: remote.write_api_key.as_deref(),
                    };
                    let options = ExportOptions {
                        index_uid,
                        payload_size: None,
                        override_settings: false,
                        export_mode: super::process_export::ExportMode::NetworkBalancing {
                            index_count,
                            export_old_remote_name: out_name,
                            network_change_origin,
                        },
                    };
                    let ctx = ExportContext {
                        index,
                        index_rtxn: &index_rtxn,
                        universe: &documents_to_move,
                        progress,
                        agent: &agent,
                        must_stop_processing,
                    };

                    let res = self.export_one_index(target, options, ctx);

                    match res {
                        Ok(_) =>{ documents_to_delete |= documents_to_move;}
                        Err(err) => {
                            tracing::warn!("Could not export documents to `{remote_name}` due to error: {err}\n  - Note: Documents will be kept");
                        }
                    }


                }

                if documents_to_delete.is_empty() {
                    return Ok(());
                }

                total_moved_documents += documents_to_delete.len();

                self.delete_documents_from_index(progress, must_stop_processing, &indexer_alloc, index_uid, index, &err, index_rtxn, documents_to_delete, fields_ids_map)
            },
        )?;

        Ok(total_moved_documents)
    }

    #[allow(clippy::too_many_arguments)]
    fn delete_documents_from_index(
        &self,
        progress: &Progress,
        must_stop_processing: &super::MustStopProcessing,
        indexer_alloc: &Bump,
        index_uid: &str,
        index: &milli::Index,
        err: &impl Fn(milli::Error) -> Error,
        index_rtxn: milli::heed::RoTxn<'_, milli::heed::WithoutTls>,
        documents_to_delete: RoaringBitmap,
        fields_ids_map: milli::FieldsIdsMap,
    ) -> std::result::Result<(), Error> {
        let mut new_fields_ids_map = fields_ids_map.clone();

        // candidates not empty => index not empty => a primary key is set
        let primary_key = index.primary_key(&index_rtxn)?.unwrap();

        let primary_key = PrimaryKey::new_or_insert(primary_key, &mut new_fields_ids_map)
            .map_err(milli::Error::from)
            .map_err(err)?;

        let mut index_wtxn = index.write_txn()?;

        let mut indexer = indexer::DocumentDeletion::new();
        indexer.delete_documents_by_docids(documents_to_delete);
        let document_changes = indexer.into_changes(indexer_alloc, primary_key);
        let embedders = index
            .embedding_configs()
            .embedding_configs(&index_wtxn)
            .map_err(milli::Error::from)
            .map_err(err)?;
        let embedders = self.embedders(index_uid.to_string(), embedders)?;
        let indexer_config = self.index_mapper.indexer_config();
        let pool = &indexer_config.thread_pool;

        indexer::index(
            &mut index_wtxn,
            index,
            pool,
            indexer_config.grenad_parameters(),
            &fields_ids_map,
            new_fields_ids_map,
            None, // document deletion never changes primary key
            &document_changes,
            embedders,
            &|| must_stop_processing.get(),
            progress,
            &EmbedderStats::default(),
        )
        .map_err(err)?;

        // update stats
        let mut mapper_wtxn = self.env.write_txn()?;
        let stats = crate::index_mapper::IndexStats::new(index, &index_wtxn).map_err(err)?;
        self.index_mapper.store_stats_of(&mut mapper_wtxn, index_uid, &stats)?;

        index_wtxn.commit()?;
        // update stats after committing changes to index
        mapper_wtxn.commit()?;

        Ok(())
    }

    #[cfg(unix)]
    async fn assume_role_with_web_identity(
        role_arn: &str,
        web_identity_token_file: &std::path::Path,
    ) -> anyhow::Result<StsCredentials> {
        use std::env::VarError;

        let token = tokio::fs::read_to_string(web_identity_token_file)
            .await
            .map_err(|e| anyhow::anyhow!("Failed to read web identity token file: {e}"))?;

        let duration: u32 =
            match std::env::var("MEILI_EXPERIMENTAL_S3_WEB_IDENTITY_TOKEN_DURATION_SECONDS") {
                Ok(s) => s.parse()?,
                Err(VarError::NotPresent) => 3600,
                Err(VarError::NotUnicode(e)) => {
                    anyhow::bail!("Invalid duration: {e:?}")
                }
            };

        let form_data = [
            ("Action", "AssumeRoleWithWebIdentity"),
            ("Version", "2011-06-15"),
            ("RoleArn", role_arn),
            ("RoleSessionName", "meilisearch-snapshot-session"),
            ("WebIdentityToken", &token),
            ("DurationSeconds", &duration.to_string()),
        ];

        let client = http_client::reqwest::Client::builder().build().unwrap();
        let response = client
            .post("https://sts.amazonaws.com/")
            .prepare(|inner| {
                inner
                    .header(http_client::reqwest::header::ACCEPT, "application/json")
                    .header(
                        http_client::reqwest::header::CONTENT_TYPE,
                        "application/x-www-form-urlencoded",
                    )
                    .form(&form_data)
            })
            .send()
            .await
            .map_err(|e| anyhow::anyhow!("Failed to send STS request: {e}"))?;

        let status = response.status();
        let body = response
            .text()
            .await
            .map_err(|e| anyhow::anyhow!("Failed to read STS response body: {e}"))?;

        if !status.is_success() {
            return Err(anyhow::anyhow!("STS request failed with status {status}: {body}"));
        }

        let sts_response: StsResponse = serde_json::from_str(&body)
            .map_err(|e| anyhow::anyhow!("Failed to deserialize STS response: {e}"))?;

        Ok(sts_response.response.result.credentials)
    }

    #[cfg(unix)]
    async fn extract_credentials_from_options(
        s3_access_key: Option<String>,
        s3_secret_key: Option<String>,
        s3_role_arn: Option<String>,
        s3_web_identity_token_file: Option<std::path::PathBuf>,
    ) -> anyhow::Result<(String, String, Option<String>)> {
        let static_credentials = s3_access_key.zip(s3_secret_key);
        let web_identity = s3_role_arn.zip(s3_web_identity_token_file);
        match (static_credentials, web_identity) {
            (Some((access_key, secret_key)), None) => Ok((access_key, secret_key, None)),
            (None, Some((role_arn, token_file))) => {
                let StsCredentials { access_key_id, secret_access_key, session_token } =
                    Self::assume_role_with_web_identity(&role_arn, &token_file).await?;
                Ok((access_key_id, secret_access_key, Some(session_token)))
            }
            (_, _) => anyhow::bail!("Clap must pass valid auth parameters"),
        }
    }

    #[cfg(unix)]
    pub(super) async fn process_snapshot_to_s3(
        &self,
        progress: Progress,
        opts: meilisearch_types::milli::update::S3SnapshotOptions,
        mut tasks: Vec<Task>,
    ) -> Result<Vec<Task>> {
        use std::ffi::OsStr;

        use meilisearch_types::milli::update::S3SnapshotOptions;

        let S3SnapshotOptions {
            s3_bucket_url,
            s3_bucket_region,
            s3_bucket_name,
            s3_snapshot_prefix,
            s3_access_key,
            s3_secret_key,
            s3_role_arn,
            s3_web_identity_token_file,
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
        let uploader_task = tokio::spawn(async move {
            let (s3_access_key, s3_secret_key, s3_token) = Self::extract_credentials_from_options(
                s3_access_key,
                s3_secret_key,
                s3_role_arn,
                s3_web_identity_token_file,
            )
            .await?;

            multipart_stream_to_s3(
                s3_bucket_url,
                s3_bucket_region,
                s3_bucket_name,
                s3_snapshot_prefix,
                s3_access_key,
                s3_secret_key,
                s3_token,
                s3_max_in_flight_parts,
                s3_signature_duration,
                s3_multipart_part_size,
                must_stop_processing,
                retry_backoff,
                db_name,
                reader,
            )
            .await
        });

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

#[cfg(unix)]
#[derive(Debug, Clone, serde::Deserialize)]
struct StsCredentials {
    #[serde(rename = "AccessKeyId")]
    access_key_id: String,
    #[serde(rename = "SecretAccessKey")]
    secret_access_key: String,
    #[serde(rename = "SessionToken")]
    session_token: String,
}

#[cfg(unix)]
#[derive(Debug, serde::Deserialize)]
struct AssumeRoleWithWebIdentityResult {
    #[serde(rename = "Credentials")]
    credentials: StsCredentials,
}

#[cfg(unix)]
#[derive(Debug, serde::Deserialize)]
struct AssumeRoleWithWebIdentityResponse {
    #[serde(rename = "AssumeRoleWithWebIdentityResult")]
    result: AssumeRoleWithWebIdentityResult,
}

#[cfg(unix)]
#[derive(Debug, serde::Deserialize)]
struct StsResponse {
    #[serde(rename = "AssumeRoleWithWebIdentityResponse")]
    response: AssumeRoleWithWebIdentityResponse,
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
    use std::sync::atomic::Ordering;

    use meilisearch_types::milli::progress::VariableNameStep;
    use meilisearch_types::VERSION_FILE_NAME;

    use crate::processing::{AtomicUpdateFileStep, SnapshotCreationProgress};
    use crate::scheduler::process_snapshot_creation::UPDATE_FILES_DIR_NAME;

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
    mut auth_env_file: std::fs::File,
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
#[allow(clippy::too_many_arguments)]
#[cfg(unix)]
async fn multipart_stream_to_s3(
    s3_bucket_url: String,
    s3_bucket_region: String,
    s3_bucket_name: String,
    s3_snapshot_prefix: String,
    s3_access_key: String,
    s3_secret_key: String,
    s3_token: Option<String>,
    s3_max_in_flight_parts: std::num::NonZero<usize>,
    s3_signature_duration: std::time::Duration,
    s3_multipart_part_size: u64,
    must_stop_processing: super::MustStopProcessing,
    retry_backoff: backoff::exponential::ExponentialBackoff<backoff::SystemClock>,
    db_name: String,
    reader: std::io::PipeReader,
) -> Result<(), Error> {
    use std::collections::VecDeque;
    use std::io;
    use std::os::fd::OwnedFd;
    use std::path::PathBuf;

    use bytes::{Bytes, BytesMut};
    use http_client::reqwest::{Client, Response};
    use rusty_s3::actions::CreateMultipartUpload;
    use rusty_s3::{Bucket, BucketError, Credentials, S3Action as _, UrlStyle};
    use tokio::task::JoinHandle;

    let reader = OwnedFd::from(reader);
    let reader = tokio::net::unix::pipe::Receiver::from_owned_fd(reader)?;
    let s3_snapshot_prefix = PathBuf::from(s3_snapshot_prefix);
    let url =
        s3_bucket_url.parse().map_err(BucketError::ParseError).map_err(Error::S3BucketError)?;
    let bucket = Bucket::new(url, UrlStyle::Path, s3_bucket_name, s3_bucket_region)
        .map_err(Error::S3BucketError)?;
    let credential = match s3_token {
        Some(token) => Credentials::new_with_token(s3_access_key, s3_secret_key, token),
        None => Credentials::new(s3_access_key, s3_secret_key),
    };

    // Note for the future (rust 1.91+): use with_added_extension, it's prettier
    let object_path = s3_snapshot_prefix.join(format!("{db_name}.snapshot"));
    // Note: It doesn't work on Windows and if a port to this platform is needed,
    //       use the slash-path crate or similar to get the correct path separator.
    let object = object_path.display().to_string();

    let action = bucket.create_multipart_upload(Some(&credential), &object);
    let url = action.sign(s3_signature_duration);

    let client = Client::builder().build().unwrap();
    let resp = client.post(url).send().await.map_err(Error::S3HttpError)?;
    let status = resp.status();

    let body = match resp.error_for_status_ref() {
        Ok(_) => resp
            .text()
            .await
            .map_err(http_client::reqwest::Error::from)
            .map_err(Error::S3HttpError)?,
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
    let mut in_flight =
        VecDeque::<(JoinHandle<http_client::reqwest::Result<Response>>, Bytes)>::with_capacity(
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
                    match client.put(url).prepare(|inner| inner.body(body)).send().await {
                        Ok(resp) if resp.status().is_client_error() => resp
                            .error_for_status()
                            .map_err(http_client::reqwest::Error::from)
                            .map_err(backoff::Error::Permanent),
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
            match client.post(url).prepare(|inner| inner.body(body)).send().await {
                Ok(resp) if resp.status().is_client_error() => {
                    Err(backoff::Error::Permanent(Error::S3Error {
                        status: resp.status(),
                        body: resp.text().await.unwrap_or_default(),
                    }))
                }
                Ok(resp) => Ok(resp),
                Err(e) => Err(backoff::Error::transient(Error::S3HttpError(e))),
            }
        }
    })
    .await?;

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
    join_handle: tokio::task::JoinHandle<
        Result<http_client::reqwest::Response, http_client::reqwest::Error>,
    >,
) -> Result<http_client::reqwest::Response> {
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
    headers: &http_client::reqwest::header::HeaderMap,
) -> Result<()> {
    use http_client::reqwest::header::ETAG;

    let etag = headers.get(ETAG).ok_or_else(|| Error::S3XmlError("Missing ETag header".into()))?;
    let etag = etag.to_str().map_err(|e| Error::S3XmlError(Box::new(e)))?;
    etags.push(bump.alloc_str(etag));

    Ok(())
}
