use std::collections::BTreeMap;
use std::io::{self, Write as _};
use std::ops::ControlFlow;
use std::sync::atomic;
use std::time::Duration;

use backoff::ExponentialBackoff;
use byte_unit::Byte;
use flate2::write::GzEncoder;
use flate2::Compression;
use meilisearch_types::error::Code;
use meilisearch_types::index_uid_pattern::IndexUidPattern;
use meilisearch_types::milli::constants::RESERVED_VECTORS_FIELD_NAME;
use meilisearch_types::milli::index::EmbeddingsWithMetadata;
use meilisearch_types::milli::progress::{Progress, VariableNameStep};
use meilisearch_types::milli::update::{request_threads, Setting};
use meilisearch_types::milli::vector::parsed_vectors::{ExplicitVectors, VectorOrArrayOfVectors};
use meilisearch_types::milli::{self, obkv_to_json, Filter, InternalError};
use meilisearch_types::settings::{self, SecretPolicy};
use meilisearch_types::tasks::network::headers::SetHeader as _;
use meilisearch_types::tasks::network::{headers, ImportData, ImportMetadata, Origin};
use meilisearch_types::tasks::{DetailsExportIndexSettings, ExportIndexSettings};
use roaring::RoaringBitmap;
use serde::Deserialize;
use serde_json::json;

use super::MustStopProcessing;
use crate::processing::AtomicDocumentStep;
use crate::{Error, IndexScheduler, Result};

type Response = http_client::ureq::http::Response<http_client::ureq::Body>;

impl IndexScheduler {
    pub(super) fn process_export(
        &self,
        base_url: &str,
        api_key: Option<&str>,
        payload_size: Option<&Byte>,
        indexes: &BTreeMap<IndexUidPattern, ExportIndexSettings>,
        progress: Progress,
    ) -> Result<BTreeMap<IndexUidPattern, DetailsExportIndexSettings>> {
        #[cfg(test)]
        self.maybe_fail(crate::test_utils::FailureLocation::ProcessExport)?;

        let indexes: Vec<_> = self
            .index_names()?
            .into_iter()
            .flat_map(|uid| {
                indexes
                    .iter()
                    .find(|(pattern, _)| pattern.matches_str(&uid))
                    .map(|(pattern, settings)| (pattern, uid, settings))
            })
            .collect();

        let mut output = BTreeMap::new();

        let config = http_client::ureq::config::Config::builder()
            .prepare(|config| {
                config.timeout_global(Some(Duration::from_secs(5))).http_status_as_error(false)
            })
            .build();

        let agent =
            http_client::ureq::Agent::new_with_config(config, self.scheduler.ip_policy.clone());

        let must_stop_processing = self.scheduler.must_stop_processing.clone();
        for (i, (_pattern, uid, export_settings)) in indexes.iter().enumerate() {
            let err = |err| Error::from_milli(err, Some(uid.to_string()));
            if must_stop_processing.get() {
                return Err(Error::AbortedTask);
            }

            progress.update_progress(VariableNameStep::<ExportIndex>::new(
                format!("Exporting index `{uid}`"),
                i as u32,
                indexes.len() as u32,
            ));

            let ExportIndexSettings { name, filter, override_settings } = export_settings;

            // Resolve target index name: apply $name substitution or use original
            let target_uid = resolve_target_index_name(name.as_deref(), uid);

            let index = self.index(uid)?;
            let index_rtxn = index.read_txn()?;
            let filter = filter.as_ref().map(Filter::from_json).transpose().map_err(err)?.flatten();
            let filter_universe =
                filter.map(|f| f.evaluate(&index_rtxn, &index)).transpose().map_err(err)?;
            let whole_universe =
                index.documents_ids(&index_rtxn).map_err(milli::Error::from).map_err(err)?;
            let universe = filter_universe.unwrap_or(whole_universe);
            let target = TargetInstance { remote_name: None, base_url, api_key };
            let ctx = ExportContext {
                index: &index,
                index_rtxn: &index_rtxn,
                universe: &universe,
                progress: &progress,
                agent: &agent,
                must_stop_processing: &must_stop_processing,
            };
            let options = ExportOptions {
                index_uid: &target_uid,
                payload_size,
                override_settings: *override_settings,
                export_mode: ExportMode::ExportRoute,
            };
            let total_documents = self.export_one_index(target, options, ctx)?;

            output.insert(
                IndexUidPattern::new_unchecked(uid.clone()),
                DetailsExportIndexSettings {
                    settings: (*export_settings).clone(),
                    matched_documents: Some(total_documents),
                },
            );
        }

        Ok(output)
    }

    pub(super) fn export_one_index(
        &self,
        target: TargetInstance<'_>,
        options: ExportOptions<'_>,
        ctx: ExportContext<'_>,
    ) -> Result<u64, Error> {
        let err = |err| Error::from_milli(err, Some(options.index_uid.to_string()));
        let total_index_documents = ctx.universe.len();
        let task_network = options.task_network(total_index_documents);

        let bearer = target.api_key.map(|api_key| format!("Bearer {api_key}"));
        let url = format!(
            "{base_url}/indexes/{index_uid}",
            base_url = target.base_url,
            index_uid = options.index_uid
        );
        let response = retry(ctx.must_stop_processing, || {
            let mut request = ctx.agent.get(&url);
            if let Some(bearer) = &bearer {
                request = request.header("Authorization", bearer);
            }

            request.call()
        });
        let index_exists = match response {
            Ok(response) => response.status() == 200,
            Err(Error::FromRemoteWhenExporting { code, .. })
                if code == Code::IndexNotFound.name() =>
            {
                false
            }
            Err(e) => return Err(e),
        };
        let primary_key =
            ctx.index.primary_key(ctx.index_rtxn).map_err(milli::Error::from).map_err(err)?;
        if !index_exists {
            let url = format!("{base_url}/indexes", base_url = target.base_url);
            let _ = handle_response(
                target.remote_name,
                retry(ctx.must_stop_processing, || {
                    let mut request = ctx.agent.post(&url);

                    if let Some((import_data, origin, metadata)) = &task_network {
                        request = set_network_ureq_headers(request, import_data, origin, metadata);
                    }

                    if let Some(bearer) = bearer.as_ref() {
                        request = request.header("Authorization", bearer);
                    }
                    let index_param =
                        json!({ "uid": options.index_uid, "primaryKey": primary_key });

                    request.send_json(&index_param)
                }),
            )?;
        }
        if index_exists && options.override_settings {
            let _ = handle_response(
                target.remote_name,
                retry(ctx.must_stop_processing, || {
                    let mut request = ctx.agent.patch(&url);
                    if let Some((import_data, origin, metadata)) = &task_network {
                        request = set_network_ureq_headers(request, import_data, origin, metadata);
                    }
                    if let Some(bearer) = &bearer {
                        request = request.header("Authorization", bearer);
                    }
                    let index_param = json!({ "primaryKey": primary_key });
                    request.send_json(&index_param)
                }),
            )?;
        }
        if !index_exists || options.override_settings {
            let mut settings =
                settings::settings(ctx.index, ctx.index_rtxn, SecretPolicy::RevealSecrets)
                    .map_err(err)?;
            // Remove the experimental chat setting if not enabled
            if self.features().check_chat_completions("exporting chat settings").is_err() {
                settings.chat = Setting::NotSet;
            }
            // Retry logic for sending settings
            let url = format!(
                "{base_url}/indexes/{index_uid}/settings",
                base_url = target.base_url,
                index_uid = options.index_uid
            );

            let _ = handle_response(
                target.remote_name,
                retry(ctx.must_stop_processing, || {
                    let mut request = ctx.agent.patch(&url);

                    if let Some((import_data, origin, metadata)) = &task_network {
                        request = set_network_ureq_headers(request, import_data, origin, metadata);
                    }

                    if let Some(bearer) = bearer.as_ref() {
                        request = request.header("Authorization", bearer);
                    }
                    request.send_json(settings.clone())
                }),
            )?;
        }

        let fields_ids_map = ctx.index.fields_ids_map(ctx.index_rtxn)?;
        let all_fields: Vec<_> = fields_ids_map.iter().map(|(id, _)| id).collect();
        let total_documents = ctx.universe.len() as u32;
        let (step, progress_step) = AtomicDocumentStep::new(total_documents);
        ctx.progress.update_progress(progress_step);

        let limit = options
            .payload_size
            .map(|ps| ps.as_u64() as usize)
            .unwrap_or(self.export_default_payload_size_bytes.as_u64() as usize);
        let documents_url = format!(
            "{base_url}/indexes/{index_uid}/documents",
            base_url = target.base_url,
            index_uid = options.index_uid
        );

        // no document to send, but we must still send a task when performing network balancing
        if ctx.universe.is_empty() {
            if let Some((import_data, network_change_origin, metadata)) = task_network {
                let mut compressed_buffer = Vec::new();
                // ignore control flow, we're returning anyway
                let _ = send_buffer(
                    b" ", // needs something otherwise meili complains about missing payload
                    &mut compressed_buffer,
                    ctx.must_stop_processing,
                    ctx.agent,
                    &documents_url,
                    target.remote_name,
                    bearer.as_deref(),
                    Some(&(import_data, network_change_origin.clone(), metadata)),
                    &err,
                )?;
            }
            return Ok(0);
        }

        let results = request_threads()
            .broadcast(|broadcast| {
                let mut task_network = options.task_network(total_index_documents);

                let index_rtxn = ctx.index.read_txn().map_err(milli::Error::from).map_err(err)?;

                let mut buffer = Vec::new();
                let mut tmp_buffer = Vec::new();
                let mut compressed_buffer = Vec::new();
                for (i, docid) in ctx.universe.iter().enumerate() {
                    if i % broadcast.num_threads() != broadcast.index() {
                        continue;
                    }
                    if let Some((import_data, _, metadata)) = &mut task_network {
                        import_data.document_count += 1;
                        metadata.task_key = Some(docid);
                    }

                    let document = ctx.index.document(&index_rtxn, docid).map_err(err)?;

                    let mut document =
                        obkv_to_json(&all_fields, &fields_ids_map, document).map_err(err)?;

                    // TODO definitely factorize this code
                    'inject_vectors: {
                        let embeddings = ctx.index.embeddings(&index_rtxn, docid).map_err(err)?;

                        if embeddings.is_empty() {
                            break 'inject_vectors;
                        }

                        let vectors = document
                            .entry(RESERVED_VECTORS_FIELD_NAME)
                            .or_insert(serde_json::Value::Object(Default::default()));

                        let serde_json::Value::Object(vectors) = vectors else {
                            return Err(err(milli::Error::UserError(
                                milli::UserError::InvalidVectorsMapType {
                                    document_id: {
                                        if let Ok(Some(Ok(index))) = ctx
                                            .index
                                            .external_id_of(&index_rtxn, std::iter::once(docid))
                                            .map(|it| it.into_iter().next())
                                        {
                                            index
                                        } else {
                                            format!("internal docid={docid}")
                                        }
                                    },
                                    value: vectors.clone(),
                                },
                            )));
                        };

                        for (
                            embedder_name,
                            EmbeddingsWithMetadata { embeddings, regenerate, has_fragments },
                        ) in embeddings
                        {
                            let embeddings = ExplicitVectors {
                                embeddings: Some(VectorOrArrayOfVectors::from_array_of_vectors(
                                    embeddings,
                                )),
                                regenerate: regenerate &&
                                // Meilisearch does not handle well dumps with fragments, because as the fragments
                                // are marked as user-provided,
                                // all embeddings would be regenerated on any settings change or document update.
                                // To prevent this, we mark embeddings has non regenerate in this case.
                                !has_fragments,
                            };
                            vectors
                                .insert(embedder_name, serde_json::to_value(embeddings).unwrap());
                        }
                    }

                    tmp_buffer.clear();
                    serde_json::to_writer(&mut tmp_buffer, &document)
                        .map_err(milli::InternalError::from)
                        .map_err(milli::Error::from)
                        .map_err(err)?;

                    // Make sure we put at least one document in the buffer even
                    // though we might go above the buffer limit before sending
                    if !buffer.is_empty() && buffer.len() + tmp_buffer.len() > limit {
                        let control_flow = send_buffer(
                            &buffer,
                            &mut compressed_buffer,
                            ctx.must_stop_processing,
                            ctx.agent,
                            &documents_url,
                            target.remote_name,
                            bearer.as_deref(),
                            task_network.as_ref(),
                            &err,
                        )?;
                        buffer.clear();
                        compressed_buffer.clear();
                        if let Some((import_data, _, metadata)) = &mut task_network {
                            import_data.document_count = 0;
                            metadata.task_key = None;
                        }
                        if control_flow.is_break() {
                            return Ok(());
                        }
                    }
                    buffer.extend_from_slice(&tmp_buffer);

                    if i > 0 && i % 100 == 0 {
                        step.fetch_add(100, atomic::Ordering::Relaxed);
                    }
                }

                // send the last buffered documents if any
                if !buffer.is_empty() {
                    // ignore control flow here
                    let _ = send_buffer(
                        &buffer,
                        &mut compressed_buffer,
                        ctx.must_stop_processing,
                        ctx.agent,
                        &documents_url,
                        target.remote_name,
                        bearer.as_deref(),
                        task_network.as_ref(),
                        &err,
                    )?;
                }

                Ok(())
            })
            .map_err(|e| err(milli::Error::InternalError(InternalError::PanicInThreadPool(e))))?;
        for result in results {
            result?;
        }
        step.store(total_documents, atomic::Ordering::Relaxed);
        Ok(total_documents as u64)
    }

    #[cfg(feature = "enterprise")] // only used in enterprise edition for now
    pub(super) fn export_no_index(
        &self,
        target: TargetInstance<'_>,
        export_old_remote_name: &str,
        network_change_origin: &Origin,
        agent: &http_client::ureq::Agent,
        must_stop_processing: &MustStopProcessing,
    ) -> Result<(), Error> {
        let bearer = target.api_key.map(|api_key| format!("Bearer {api_key}"));
        let url = format!("{base_url}/network", base_url = target.base_url,);

        {
            let _ = handle_response(
                target.remote_name,
                retry(must_stop_processing, || {
                    let request = agent.patch(&url);
                    let mut request = set_network_ureq_headers(
                        request,
                        &ImportData {
                            remote_name: export_old_remote_name.to_string(),
                            index_name: None,
                            document_count: 0,
                        },
                        network_change_origin,
                        &ImportMetadata {
                            index_count: 0,
                            task_key: None,
                            total_index_documents: 0,
                        },
                    );
                    request = request.header("Content-Type", "application/json");
                    if let Some(bearer) = &bearer {
                        request = request.header("Authorization", bearer);
                    }
                    request.send_json(
                        // empty payload that will be disregarded
                        serde_json::Value::Object(Default::default()),
                    )
                }),
            )?;
        }

        Ok(())
    }
}

fn set_network_ureq_headers<P>(
    request: http_client::ureq::RequestBuilder<P>,
    import_data: &ImportData,
    origin: &Origin,
    metadata: &ImportMetadata,
) -> http_client::ureq::RequestBuilder<P> {
    let request = RequestWrapper(request);

    let ImportMetadata { index_count, task_key, total_index_documents } = metadata;
    let Origin { remote_name: origin_remote, task_uid, network_version } = origin;
    let ImportData { remote_name: import_remote, index_name, document_count } = import_data;

    let request = request
        .set_origin_remote(origin_remote)
        .set_origin_task_uid(*task_uid)
        .set_origin_network_version(*network_version)
        .set_import_remote(import_remote)
        .set_import_docs(*document_count)
        .set_import_index_count(*index_count)
        .set_import_index_docs(*total_index_documents);

    let request = if let Some(index_name) = index_name.as_deref() {
        request.set_import_index(index_name)
    } else {
        request
    };
    let RequestWrapper(request) = if let Some(task_key) = task_key {
        request.set_import_task_key(*task_key)
    } else {
        request
    };

    request
}

struct RequestWrapper<P>(http_client::ureq::RequestBuilder<P>);
impl<P> headers::SetHeader for RequestWrapper<P> {
    fn set_header(self, name: &str, value: &str) -> Self {
        Self(self.0.header(name, value))
    }
}

#[allow(clippy::too_many_arguments)]
fn send_buffer<'a>(
    buffer: &'a [u8],
    mut compressed_buffer: &'a mut Vec<u8>,
    must_stop_processing: &MustStopProcessing,
    agent: &http_client::ureq::Agent,
    documents_url: &'a str,
    remote_name: Option<&str>,
    bearer: Option<&'a str>,
    task_network: Option<&(ImportData, Origin, ImportMetadata)>,
    err: &'a impl Fn(milli::Error) -> crate::Error,
) -> Result<ControlFlow<(), ()>> {
    // We compress the documents before sending them
    let mut encoder: GzEncoder<&mut &mut Vec<u8>> =
        GzEncoder::new(&mut compressed_buffer, Compression::default());
    encoder.write_all(buffer).map_err(milli::Error::from).map_err(err)?;
    encoder.finish().map_err(milli::Error::from).map_err(err)?;

    let res = retry(must_stop_processing, || {
        let mut request = agent.post(documents_url);
        request = request.header("Content-Type", "application/x-ndjson");
        request = request.header("Content-Encoding", "gzip");
        if let Some(bearer) = bearer {
            request = request.header("Authorization", bearer);
        }
        if let Some((import_data, origin, metadata)) = task_network {
            request = set_network_ureq_headers(request, import_data, origin, metadata);
        }
        request.send(compressed_buffer.as_slice())
    });

    handle_response(remote_name, res)
}

fn handle_response(remote_name: Option<&str>, res: Result<Response>) -> Result<ControlFlow<()>> {
    let remote_name = remote_name.unwrap_or("unnamed");
    match res {
        Ok(_response) => Ok(ControlFlow::Continue(())),
        Err(Error::FromRemoteWhenExporting { code, .. })
            if code == Code::ImportTaskAlreadyReceived.name() =>
        {
            Ok(ControlFlow::Continue(()))
        }
        Err(Error::FromRemoteWhenExporting { code, message, .. })
            if code == Code::ImportTaskUnknownRemote.name() =>
        {
            tracing::warn!("remote `{remote_name}` answered with: {message}");
            Ok(ControlFlow::Break(()))
        }
        // note: there has already been many attempts to get this due to exponential backoff
        Err(Error::FromRemoteWhenExporting { code, message, .. })
            if code == Code::ImportTaskWithoutNetworkTask.name() =>
        {
            tracing::warn!("remote `{remote_name}` answered with: {message}");
            Ok(ControlFlow::Break(()))
        }
        Err(e) => {
            tracing::warn!("error while exporting: {e}");
            Err(e)
        }
    }
}

fn retry<F>(must_stop_processing: &MustStopProcessing, send_request: F) -> Result<Response>
where
    F: Fn() -> Result<Response, http_client::ureq::Error>,
{
    match backoff::retry(
        ExponentialBackoff::default(),
        || -> Result<Response, backoff::Error<ResponseError>> {
            if must_stop_processing.get() {
                return Err(backoff::Error::Permanent(ResponseError::AbortedTask));
            }
            match send_request() {
                Ok(response) => {
                    if response.status().is_success() {
                        Ok(response)
                    } else {
                        Err(into_backoff_error(response))
                    }
                }
                Err(err) => Err(backoff::Error::Transient {
                    err: ResponseError::Transport(err),
                    retry_after: None,
                }),
            }
        },
    ) {
        Ok(response) => Ok(response),
        Err(backoff::Error::Permanent(e)) => Err(response_error_into_error(e)),
        Err(backoff::Error::Transient { err, retry_after: _ }) => {
            Err(response_error_into_error(err))
        }
    }
}

enum ResponseError {
    AbortedTask,
    FailedResponse(Response),
    Transport(http_client::ureq::Error),
}

fn into_backoff_error(failed_response: Response) -> backoff::Error<ResponseError> {
    match failed_response.status().as_u16() {
        // Those code status must trigger an automatic retry
        // <https://www.restapitutorial.com/advanced/responses/retries>
        408 | 429 | 500 | 502 | 503 | 504 => backoff::Error::Transient {
            err: ResponseError::FailedResponse(failed_response),
            retry_after: None,
        },
        _ => backoff::Error::Permanent(ResponseError::FailedResponse(failed_response)),
    }
}

/// Converts a `ureq::Error` into an `Error`.
fn response_error_into_error(error: ResponseError) -> Error {
    #[derive(Deserialize)]
    struct MeiliError {
        message: String,
        code: String,
        r#type: String,
        link: String,
    }

    match error {
        ResponseError::AbortedTask => Error::AbortedTask,
        ResponseError::FailedResponse(mut response) => match response.body_mut().read_json() {
            Ok(MeiliError { message, code, r#type, link }) => {
                Error::FromRemoteWhenExporting { message, code, r#type, link }
            }
            Err(e) => io::Error::other(e.into_io()).into(),
        },
        ResponseError::Transport(error) => io::Error::other(error.into_io()).into(),
    }
}

/// Resolves the target index name based on the optional name template.
///
/// If `name` is None, returns the original index uid.
/// If `name` contains `$name`, replaces it with the original index uid.
/// Otherwise, returns the name as-is (static target name).
///
/// This works together with wildcard patterns (e.g. `super-*`). When a pattern
/// matches an index, the matched index uid is passed as `original_uid`, so
/// `$name` correctly expands to the full matched name.
///
/// # Examples
///
/// - Pattern `super-*` matches `super-toto`, name template `mega-$name`
///   → target index is `mega-super-toto`
/// - Pattern `super-*` matches `super-boby`, name is `everyone`
///   → target index is `everyone` (static, no substitution)
/// - Pattern `super-*` matches `super-toto`, name is `None`
///   → target index is `super-toto` (original name preserved)
fn resolve_target_index_name(name: Option<&str>, original_uid: &str) -> String {
    match name {
        None => original_uid.to_string(),
        Some(template) => template.replace("$name", original_uid),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_resolve_target_index_name_none() {
        // No name template — use original uid
        assert_eq!(resolve_target_index_name(None, "movies"), "movies");
    }

    #[test]
    fn test_resolve_target_index_name_static() {
        // Static name — no $name variable, use as-is
        assert_eq!(resolve_target_index_name(Some("everyone"), "super-toto"), "everyone");
    }

    #[test]
    fn test_resolve_target_index_name_with_substitution() {
        // $name is replaced with the original uid
        assert_eq!(
            resolve_target_index_name(Some("mega-$name"), "super-toto"),
            "mega-super-toto"
        );
    }

    #[test]
    fn test_resolve_target_index_name_multiple_substitutions() {
        // Multiple $name occurrences are all replaced
        assert_eq!(
            resolve_target_index_name(Some("$name-backup-$name"), "movies"),
            "movies-backup-movies"
        );
    }

    #[test]
    fn test_resolve_target_index_name_only_variable() {
        // Template is just $name — equivalent to None
        assert_eq!(resolve_target_index_name(Some("$name"), "movies"), "movies");
    }

    #[test]
    fn test_resolve_target_index_name_wildcard_matched_index() {
        // Simulates wildcard pattern `prod-*` matching `prod-movies`
        // with name template `staging-$name`
        assert_eq!(
            resolve_target_index_name(Some("staging-$name"), "prod-movies"),
            "staging-prod-movies"
        );
    }
}

// export_one_index arguments
pub(super) struct TargetInstance<'a> {
    pub(super) remote_name: Option<&'a str>,
    pub(super) base_url: &'a str,
    pub(super) api_key: Option<&'a str>,
}

pub(super) struct ExportOptions<'a> {
    pub(super) index_uid: &'a str,
    pub(super) payload_size: Option<&'a Byte>,
    pub(super) override_settings: bool,
    pub(super) export_mode: ExportMode<'a>,
}

impl ExportOptions<'_> {
    fn task_network(
        &self,
        total_index_documents: u64,
    ) -> Option<(ImportData, Origin, ImportMetadata)> {
        if let ExportMode::NetworkBalancing {
            index_count,
            export_old_remote_name,
            network_change_origin,
        } = self.export_mode
        {
            Some((
                ImportData {
                    remote_name: export_old_remote_name.to_string(),
                    index_name: Some(self.index_uid.to_string()),
                    document_count: 0,
                },
                network_change_origin.clone(),
                ImportMetadata { index_count, task_key: None, total_index_documents },
            ))
        } else {
            None
        }
    }
}

pub(super) struct ExportContext<'a> {
    pub(super) index: &'a meilisearch_types::milli::Index,
    pub(super) index_rtxn: &'a milli::heed::RoTxn<'a>,
    pub(super) universe: &'a RoaringBitmap,
    pub(super) progress: &'a Progress,
    pub(super) agent: &'a http_client::ureq::Agent,
    pub(super) must_stop_processing: &'a MustStopProcessing,
}

pub(super) enum ExportMode<'a> {
    ExportRoute,
    #[cfg_attr(not(feature = "enterprise"), allow(dead_code))]
    NetworkBalancing {
        index_count: u64,

        export_old_remote_name: &'a str,
        network_change_origin: &'a Origin,
    },
}

// progress related
enum ExportIndex {}
