use std::collections::BTreeMap;
use std::time::Duration;

use meilisearch_types::index_uid_pattern::IndexUidPattern;
use meilisearch_types::milli::progress::{Progress, VariableNameStep};
use meilisearch_types::milli::{obkv_to_json, Filter};
use meilisearch_types::settings::{self, SecretPolicy};
use meilisearch_types::tasks::ExportIndexSettings;
use ureq::{json, Agent};

use crate::{Error, IndexScheduler, Result};

impl IndexScheduler {
    pub(super) fn process_export(
        &self,
        url: &str,
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

        for (i, (uid, settings)) in indexes.iter().enumerate() {
            let must_stop_processing = self.scheduler.must_stop_processing.clone();
            if must_stop_processing.get() {
                return Err(Error::AbortedTask);
            }

            progress.update_progress(VariableNameStep::<ExportIndex>::new(
                format!("Exporting index `{uid}`"),
                i as u32,
                indexes.len() as u32,
            ));

            let ExportIndexSettings { skip_embeddings, filter } = settings;
            let index = self.index(uid)?;
            let index_rtxn = index.read_txn()?;

            // Send the primary key
            let primary_key = index.primary_key(&index_rtxn).unwrap();
            // TODO implement retry logic
            let mut request = agent.post(&format!("{url}/indexes"));
            if let Some(api_key) = api_key {
                request = request.set("Authorization", &format!("Bearer {api_key}"));
            }
            request.send_json(&json!({ "uid": uid, "primaryKey": primary_key })).unwrap();

            // Send the index settings
            let settings = settings::settings(&index, &index_rtxn, SecretPolicy::RevealSecrets)
                .map_err(|e| Error::from_milli(e, Some(uid.to_string())))?;
            // TODO implement retry logic
            //      improve error reporting (get error message)
            let mut request = agent.patch(&format!("{url}/indexes/{uid}/settings"));
            if let Some(api_key) = api_key {
                request = request.set("Authorization", &format!("Bearer {api_key}"));
            }
            request.send_json(settings).unwrap();

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
                .map_err(|e| Error::from_milli(e.into(), Some(uid.to_string())))?;

            let limit = 50 * 1024 * 1024; // 50 MiB
            let mut buffer = Vec::new();
            let mut tmp_buffer = Vec::new();
            for docid in universe {
                let document = index
                    .document(&index_rtxn, docid)
                    .map_err(|e| Error::from_milli(e, Some(uid.to_string())))?;

                let value = obkv_to_json(&all_fields, &fields_ids_map, document)
                    .map_err(|e| Error::from_milli(e, Some(uid.to_string())))?;

                tmp_buffer.clear();
                serde_json::to_writer(&mut tmp_buffer, &value)
                    .map_err(meilisearch_types::milli::InternalError::from)
                    .map_err(|e| Error::from_milli(e.into(), Some(uid.to_string())))?;

                if buffer.len() + tmp_buffer.len() > limit {
                    // TODO implement retry logic
                    post_serialized_documents(&agent, url, uid, api_key, &buffer).unwrap();
                    buffer.clear();
                }
                buffer.extend_from_slice(&tmp_buffer);
            }

            post_serialized_documents(&agent, url, uid, api_key, &buffer).unwrap();
        }

        Ok(())
    }
}

fn post_serialized_documents(
    agent: &Agent,
    url: &str,
    uid: &str,
    api_key: Option<&str>,
    buffer: &[u8],
) -> Result<ureq::Response, ureq::Error> {
    let mut request = agent.post(&format!("{url}/indexes/{uid}/documents"));
    request = request.set("Content-Type", "application/x-ndjson");
    if let Some(api_key) = api_key {
        request = request.set("Authorization", &format!("Bearer {api_key}"));
    }
    request.send_bytes(buffer)
}

enum ExportIndex {}
