use std::collections::BTreeMap;
use std::io::{self, Write as _};
use std::sync::atomic;
use std::time::Duration;

use backoff::ExponentialBackoff;
use byte_unit::Byte;
use flate2::write::GzEncoder;
use flate2::Compression;
use meilisearch_types::index_uid_pattern::IndexUidPattern;
use meilisearch_types::milli::constants::RESERVED_VECTORS_FIELD_NAME;
use meilisearch_types::milli::index::EmbeddingsWithMetadata;
use meilisearch_types::milli::progress::{Progress, VariableNameStep};
use meilisearch_types::milli::update::{request_threads, Setting};
use meilisearch_types::milli::vector::parsed_vectors::{ExplicitVectors, VectorOrArrayOfVectors};
use meilisearch_types::milli::{self, obkv_to_json, Filter, InternalError};
use meilisearch_types::settings::{self, SecretPolicy};
use meilisearch_types::tasks::{DetailsExportIndexSettings, ExportIndexSettings};
use serde::Deserialize;
use ureq::{json, Response};

use super::MustStopProcessing;
use crate::processing::AtomicDocumentStep;
use crate::{Error, IndexScheduler, Result};

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
        let agent = ureq::AgentBuilder::new().timeout(Duration::from_secs(5)).build();
        let must_stop_processing = self.scheduler.must_stop_processing.clone();
        for (i, (_pattern, uid, export_settings)) in indexes.iter().enumerate() {
            if must_stop_processing.get() {
                return Err(Error::AbortedTask);
            }

            progress.update_progress(VariableNameStep::<ExportIndex>::new(
                format!("Exporting index `{uid}`"),
                i as u32,
                indexes.len() as u32,
            ));

            let ExportIndexSettings { filter, override_settings } = export_settings;
            let index = self.index(uid)?;
            let index_rtxn = index.read_txn()?;
            let bearer = api_key.map(|api_key| format!("Bearer {api_key}"));

            // First, check if the index already exists
            let url = format!("{base_url}/indexes/{uid}");
            let response = retry(&must_stop_processing, || {
                let mut request = agent.get(&url);
                if let Some(bearer) = &bearer {
                    request = request.set("Authorization", bearer);
                }

                request.send_bytes(Default::default()).map_err(into_backoff_error)
            });
            let index_exists = match response {
                Ok(response) => response.status() == 200,
                Err(Error::FromRemoteWhenExporting { code, .. }) if code == "index_not_found" => {
                    false
                }
                Err(e) => return Err(e),
            };

            let primary_key = index
                .primary_key(&index_rtxn)
                .map_err(|e| Error::from_milli(e.into(), Some(uid.to_string())))?;

            // Create the index
            if !index_exists {
                let url = format!("{base_url}/indexes");
                retry(&must_stop_processing, || {
                    let mut request = agent.post(&url);
                    if let Some(bearer) = &bearer {
                        request = request.set("Authorization", bearer);
                    }
                    let index_param = json!({ "uid": uid, "primaryKey": primary_key });
                    request.send_json(&index_param).map_err(into_backoff_error)
                })?;
            }

            // Patch the index primary key
            if index_exists && *override_settings {
                let url = format!("{base_url}/indexes/{uid}");
                retry(&must_stop_processing, || {
                    let mut request = agent.patch(&url);
                    if let Some(bearer) = &bearer {
                        request = request.set("Authorization", bearer);
                    }
                    let index_param = json!({ "primaryKey": primary_key });
                    request.send_json(&index_param).map_err(into_backoff_error)
                })?;
            }

            // Send the index settings
            if !index_exists || *override_settings {
                let mut settings =
                    settings::settings(&index, &index_rtxn, SecretPolicy::RevealSecrets)
                        .map_err(|e| Error::from_milli(e, Some(uid.to_string())))?;
                // Remove the experimental chat setting if not enabled
                if self.features().check_chat_completions("exporting chat settings").is_err() {
                    settings.chat = Setting::NotSet;
                }
                // Retry logic for sending settings
                let url = format!("{base_url}/indexes/{uid}/settings");
                retry(&must_stop_processing, || {
                    let mut request = agent.patch(&url);
                    if let Some(bearer) = bearer.as_ref() {
                        request = request.set("Authorization", bearer);
                    }
                    request.send_json(settings.clone()).map_err(into_backoff_error)
                })?;
            }

            let filter = filter
                .as_ref()
                .map(Filter::from_json)
                .transpose()
                .map_err(|e| Error::from_milli(e, Some(uid.to_string())))?
                .flatten();

            let filter_universe = filter
                .map(|f| f.evaluate(&index_rtxn, &index))
                .transpose()
                .map_err(|e| Error::from_milli(e, Some(uid.to_string())))?;
            let whole_universe = index
                .documents_ids(&index_rtxn)
                .map_err(|e| Error::from_milli(e.into(), Some(uid.to_string())))?;
            let universe = filter_universe.unwrap_or(whole_universe);

            let fields_ids_map = index.fields_ids_map(&index_rtxn)?;
            let all_fields: Vec<_> = fields_ids_map.iter().map(|(id, _)| id).collect();

            // We don't need to keep this one alive as we will
            // spawn many threads to process the documents
            drop(index_rtxn);

            let total_documents = universe.len() as u32;
            let (step, progress_step) = AtomicDocumentStep::new(total_documents);
            progress.update_progress(progress_step);

            output.insert(
                IndexUidPattern::new_unchecked(uid.clone()),
                DetailsExportIndexSettings {
                    settings: (*export_settings).clone(),
                    matched_documents: Some(total_documents as u64),
                },
            );

            let limit = payload_size.map(|ps| ps.as_u64() as usize).unwrap_or(20 * 1024 * 1024); // defaults to 20 MiB
            let documents_url = format!("{base_url}/indexes/{uid}/documents");

            let results = request_threads()
                .broadcast(|ctx| {
                    let index_rtxn = index
                        .read_txn()
                        .map_err(|e| Error::from_milli(e.into(), Some(uid.to_string())))?;

                    let mut buffer = Vec::new();
                    let mut tmp_buffer = Vec::new();
                    let mut compressed_buffer = Vec::new();
                    for (i, docid) in universe.iter().enumerate() {
                        if i % ctx.num_threads() != ctx.index() {
                            continue;
                        }

                        let document = index
                            .document(&index_rtxn, docid)
                            .map_err(|e| Error::from_milli(e, Some(uid.to_string())))?;

                        let mut document = obkv_to_json(&all_fields, &fields_ids_map, document)
                            .map_err(|e| Error::from_milli(e, Some(uid.to_string())))?;

                        // TODO definitely factorize this code
                        'inject_vectors: {
                            let embeddings = index
                                .embeddings(&index_rtxn, docid)
                                .map_err(|e| Error::from_milli(e, Some(uid.to_string())))?;

                            if embeddings.is_empty() {
                                break 'inject_vectors;
                            }

                            let vectors = document
                                .entry(RESERVED_VECTORS_FIELD_NAME)
                                .or_insert(serde_json::Value::Object(Default::default()));

                            let serde_json::Value::Object(vectors) = vectors else {
                                return Err(Error::from_milli(
                                    milli::Error::UserError(
                                        milli::UserError::InvalidVectorsMapType {
                                            document_id: {
                                                if let Ok(Some(Ok(index))) = index
                                                    .external_id_of(
                                                        &index_rtxn,
                                                        std::iter::once(docid),
                                                    )
                                                    .map(|it| it.into_iter().next())
                                                {
                                                    index
                                                } else {
                                                    format!("internal docid={docid}")
                                                }
                                            },
                                            value: vectors.clone(),
                                        },
                                    ),
                                    Some(uid.to_string()),
                                ));
                            };

                            for (
                                embedder_name,
                                EmbeddingsWithMetadata { embeddings, regenerate, has_fragments },
                            ) in embeddings
                            {
                                let embeddings = ExplicitVectors {
                                    embeddings: Some(
                                        VectorOrArrayOfVectors::from_array_of_vectors(embeddings),
                                    ),
                                    regenerate: regenerate &&
                                    // Meilisearch does not handle well dumps with fragments, because as the fragments
                                    // are marked as user-provided,
                                    // all embeddings would be regenerated on any settings change or document update.
                                    // To prevent this, we mark embeddings has non regenerate in this case.
                                    !has_fragments,
                                };
                                vectors.insert(
                                    embedder_name,
                                    serde_json::to_value(embeddings).unwrap(),
                                );
                            }
                        }

                        tmp_buffer.clear();
                        serde_json::to_writer(&mut tmp_buffer, &document)
                            .map_err(milli::InternalError::from)
                            .map_err(|e| Error::from_milli(e.into(), Some(uid.to_string())))?;

                        // Make sure we put at least one document in the buffer even
                        // though we might go above the buffer limit before sending
                        if !buffer.is_empty() && buffer.len() + tmp_buffer.len() > limit {
                            // We compress the documents before sending them
                            let mut encoder =
                                GzEncoder::new(&mut compressed_buffer, Compression::default());
                            encoder
                                .write_all(&buffer)
                                .map_err(|e| Error::from_milli(e.into(), Some(uid.clone())))?;
                            encoder
                                .finish()
                                .map_err(|e| Error::from_milli(e.into(), Some(uid.clone())))?;

                            retry(&must_stop_processing, || {
                                let mut request = agent.post(&documents_url);
                                request = request.set("Content-Type", "application/x-ndjson");
                                request = request.set("Content-Encoding", "gzip");
                                if let Some(bearer) = &bearer {
                                    request = request.set("Authorization", bearer);
                                }
                                request.send_bytes(&compressed_buffer).map_err(into_backoff_error)
                            })?;
                            buffer.clear();
                            compressed_buffer.clear();
                        }
                        buffer.extend_from_slice(&tmp_buffer);

                        if i > 0 && i % 100 == 0 {
                            step.fetch_add(100, atomic::Ordering::Relaxed);
                        }
                    }

                    retry(&must_stop_processing, || {
                        let mut request = agent.post(&documents_url);
                        request = request.set("Content-Type", "application/x-ndjson");
                        if let Some(bearer) = &bearer {
                            request = request.set("Authorization", bearer);
                        }
                        request.send_bytes(&buffer).map_err(into_backoff_error)
                    })?;

                    Ok(())
                })
                .map_err(|e| {
                    Error::from_milli(
                        milli::Error::InternalError(InternalError::PanicInThreadPool(e)),
                        Some(uid.to_string()),
                    )
                })?;
            for result in results {
                result?;
            }

            step.store(total_documents, atomic::Ordering::Relaxed);
        }

        Ok(output)
    }
}

fn retry<F>(must_stop_processing: &MustStopProcessing, send_request: F) -> Result<ureq::Response>
where
    F: Fn() -> Result<ureq::Response, backoff::Error<ureq::Error>>,
{
    match backoff::retry(ExponentialBackoff::default(), || {
        if must_stop_processing.get() {
            return Err(backoff::Error::Permanent(ureq::Error::Status(
                u16::MAX,
                // 444: Connection Closed Without Response
                Response::new(444, "Abort", "Aborted task").unwrap(),
            )));
        }
        send_request()
    }) {
        Ok(response) => Ok(response),
        Err(backoff::Error::Permanent(e)) => Err(ureq_error_into_error(e)),
        Err(backoff::Error::Transient { err, retry_after: _ }) => Err(ureq_error_into_error(err)),
    }
}

fn into_backoff_error(err: ureq::Error) -> backoff::Error<ureq::Error> {
    match err {
        // Those code status must trigger an automatic retry
        // <https://www.restapitutorial.com/advanced/responses/retries>
        ureq::Error::Status(408 | 429 | 500 | 502 | 503 | 504, _) => {
            backoff::Error::Transient { err, retry_after: None }
        }
        ureq::Error::Status(_, _) => backoff::Error::Permanent(err),
        ureq::Error::Transport(_) => backoff::Error::Transient { err, retry_after: None },
    }
}

/// Converts a `ureq::Error` into an `Error`.
fn ureq_error_into_error(error: ureq::Error) -> Error {
    #[derive(Deserialize)]
    struct MeiliError {
        message: String,
        code: String,
        r#type: String,
        link: String,
    }

    match error {
        // This is a workaround to handle task abortion - the error propagation path
        // makes it difficult to cleanly surface the abortion at this level.
        ureq::Error::Status(u16::MAX, _) => Error::AbortedTask,
        ureq::Error::Status(_, response) => match response.into_json() {
            Ok(MeiliError { message, code, r#type, link }) => {
                Error::FromRemoteWhenExporting { message, code, r#type, link }
            }
            Err(e) => e.into(),
        },
        ureq::Error::Transport(transport) => io::Error::other(transport).into(),
    }
}

enum ExportIndex {}
