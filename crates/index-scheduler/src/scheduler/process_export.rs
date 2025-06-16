use std::collections::BTreeMap;
use std::io;
use std::sync::atomic;
use std::time::Duration;

use backoff::ExponentialBackoff;
use meilisearch_types::index_uid_pattern::IndexUidPattern;
use meilisearch_types::milli::constants::RESERVED_VECTORS_FIELD_NAME;
use meilisearch_types::milli::progress::{Progress, VariableNameStep};
use meilisearch_types::milli::update::Setting;
use meilisearch_types::milli::vector::parsed_vectors::{ExplicitVectors, VectorOrArrayOfVectors};
use meilisearch_types::milli::{obkv_to_json, Filter};
use meilisearch_types::settings::{self, SecretPolicy};
use meilisearch_types::tasks::ExportIndexSettings;
use serde::Deserialize;
use ureq::{json, Agent};

use super::MustStopProcessing;
use crate::processing::AtomicDocumentStep;
use crate::{Error, IndexScheduler, Result};

impl IndexScheduler {
    pub(super) fn process_export(
        &self,
        base_url: &str,
        indexes: &BTreeMap<IndexUidPattern, ExportIndexSettings>,
        api_key: Option<&str>,
        progress: Progress,
    ) -> Result<()> {
        #[cfg(test)]
        self.maybe_fail(crate::test_utils::FailureLocation::ProcessExport)?;

        let indexes: Vec<_> = self
            .index_names()?
            .into_iter()
            .flat_map(|uid| {
                indexes
                    .iter()
                    .find(|(pattern, _)| pattern.matches_str(&uid))
                    .map(|(_pattern, settings)| (uid, settings))
            })
            .collect();

        let agent: Agent = ureq::AgentBuilder::new().timeout(Duration::from_secs(5)).build();
        let must_stop_processing = self.scheduler.must_stop_processing.clone();
        for (i, (uid, settings)) in indexes.iter().enumerate() {
            if must_stop_processing.get() {
                return Err(Error::AbortedTask);
            }

            progress.update_progress(VariableNameStep::<ExportIndex>::new(
                format!("Exporting index `{uid}`"),
                i as u32,
                indexes.len() as u32,
            ));

            let ExportIndexSettings { filter } = settings;
            let index = self.index(uid)?;
            let index_rtxn = index.read_txn()?;

            // Send the primary key
            let primary_key = index
                .primary_key(&index_rtxn)
                .map_err(|e| Error::from_milli(e.into(), Some(uid.to_string())))?;

            let url = format!("{base_url}/indexes");
            retry(&must_stop_processing, || {
                let mut request = agent.post(&url);
                if let Some(api_key) = api_key {
                    request = request.set("Authorization", &format!("Bearer {api_key}"));
                }
                let index_param = json!({ "uid": uid, "primaryKey": primary_key });
                request.send_json(&index_param).map_err(into_backoff_error)
            })?;

            // Send the index settings
            let mut settings = settings::settings(&index, &index_rtxn, SecretPolicy::RevealSecrets)
                .map_err(|e| Error::from_milli(e, Some(uid.to_string())))?;
            // Remove the experimental chat setting if not enabled
            if self.features().check_chat_completions("exporting chat settings").is_err() {
                settings.chat = Setting::NotSet;
            }
            // Retry logic for sending settings
            let url = format!("{base_url}/indexes/{uid}/settings");
            retry(&must_stop_processing, || {
                let mut request = agent.patch(&url);
                if let Some(api_key) = api_key {
                    request = request.set("Authorization", &format!("Bearer {api_key}"));
                }
                request.send_json(settings.clone()).map_err(into_backoff_error)
            })?;

            // TODO support JSON Value objects
            let filter = filter
                .as_deref()
                .map(Filter::from_str)
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
            let embedding_configs = index
                .embedding_configs(&index_rtxn)
                .map_err(|e| Error::from_milli(e, Some(uid.to_string())))?;

            let total_documents = universe.len() as u32;
            let (step, progress_step) = AtomicDocumentStep::new(total_documents);
            progress.update_progress(progress_step);

            let limit = 50 * 1024 * 1024; // 50 MiB
            let documents_url = format!("{base_url}/indexes/{uid}/documents");

            let mut buffer = Vec::new();
            let mut tmp_buffer = Vec::new();
            for (i, docid) in universe.into_iter().enumerate() {
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
                            meilisearch_types::milli::Error::UserError(
                                meilisearch_types::milli::UserError::InvalidVectorsMapType {
                                    document_id: {
                                        if let Ok(Some(Ok(index))) = index
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
                            ),
                            Some(uid.to_string()),
                        ));
                    };

                    for (embedder_name, embeddings) in embeddings {
                        let user_provided = embedding_configs
                            .iter()
                            .find(|conf| conf.name == embedder_name)
                            .is_some_and(|conf| conf.user_provided.contains(docid));

                        let embeddings = ExplicitVectors {
                            embeddings: Some(VectorOrArrayOfVectors::from_array_of_vectors(
                                embeddings,
                            )),
                            regenerate: !user_provided,
                        };
                        vectors.insert(embedder_name, serde_json::to_value(embeddings).unwrap());
                    }
                }

                tmp_buffer.clear();
                serde_json::to_writer(&mut tmp_buffer, &document)
                    .map_err(meilisearch_types::milli::InternalError::from)
                    .map_err(|e| Error::from_milli(e.into(), Some(uid.to_string())))?;

                if buffer.len() + tmp_buffer.len() > limit {
                    retry(&must_stop_processing, || {
                        let mut request = agent.post(&documents_url);
                        request = request.set("Content-Type", "application/x-ndjson");
                        if let Some(api_key) = api_key {
                            request = request.set("Authorization", &(format!("Bearer {api_key}")));
                        }
                        request.send_bytes(&buffer).map_err(into_backoff_error)
                    })?;
                    buffer.clear();
                }
                buffer.extend_from_slice(&tmp_buffer);

                if i % 100 == 0 {
                    step.fetch_add(100, atomic::Ordering::Relaxed);
                }
            }

            retry(&must_stop_processing, || {
                let mut request = agent.post(&documents_url);
                request = request.set("Content-Type", "application/x-ndjson");
                if let Some(api_key) = api_key {
                    request = request.set("Authorization", &(format!("Bearer {api_key}")));
                }
                request.send_bytes(&buffer).map_err(into_backoff_error)
            })?;
            step.store(total_documents, atomic::Ordering::Relaxed);
        }

        Ok(())
    }
}

fn retry<F>(must_stop_processing: &MustStopProcessing, send_request: F) -> Result<ureq::Response>
where
    F: Fn() -> Result<ureq::Response, backoff::Error<ureq::Error>>,
{
    if must_stop_processing.get() {
        return Err(Error::AbortedTask);
    }

    match backoff::retry(ExponentialBackoff::default(), send_request) {
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
        ureq::Error::Status(_, response) => match response.into_json() {
            Ok(MeiliError { message, code, r#type, link }) => {
                Error::FromRemoteWhenExporting { message, code, r#type, link }
            }
            Err(e) => e.into(),
        },
        ureq::Error::Transport(transport) => io::Error::new(io::ErrorKind::Other, transport).into(),
    }
}

enum ExportIndex {}
