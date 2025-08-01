use std::collections::BTreeMap;
use std::fs::File;
use std::io::BufWriter;
use std::sync::atomic::Ordering;

use dump::IndexMetadata;
use meilisearch_types::milli::constants::RESERVED_VECTORS_FIELD_NAME;
use meilisearch_types::milli::progress::{Progress, VariableNameStep};
use meilisearch_types::milli::vector::parsed_vectors::{ExplicitVectors, VectorOrArrayOfVectors};
use meilisearch_types::milli::{self};
use meilisearch_types::tasks::{Details, KindWithContent, Status, Task};
use time::macros::format_description;
use time::OffsetDateTime;

use crate::processing::{
    AtomicBatchStep, AtomicDocumentStep, AtomicTaskStep, DumpCreationProgress,
};
use crate::{Error, IndexScheduler, Result};

impl IndexScheduler {
    pub(super) fn process_dump_creation(
        &self,
        progress: Progress,
        mut task: Task,
    ) -> Result<Vec<Task>> {
        progress.update_progress(DumpCreationProgress::StartTheDumpCreation);
        let started_at = OffsetDateTime::now_utc();
        let (keys, instance_uid) =
            if let KindWithContent::DumpCreation { keys, instance_uid } = &task.kind {
                (keys, instance_uid)
            } else {
                unreachable!();
            };
        let dump = dump::DumpWriter::new(*instance_uid)?;

        // 1. dump the keys
        progress.update_progress(DumpCreationProgress::DumpTheApiKeys);
        let mut dump_keys = dump.create_keys()?;
        for key in keys {
            dump_keys.push_key(key)?;
        }
        dump_keys.flush()?;

        let rtxn = self.env.read_txn()?;

        // 2. dump the chat completion settings
        // TODO should I skip the export if the chat completion has been disabled?
        progress.update_progress(DumpCreationProgress::DumpTheChatCompletionSettings);
        let mut dump_chat_completion_settings = dump.create_chat_completions_settings()?;
        for result in self.chat_settings.iter(&rtxn)? {
            let (name, chat_settings) = result?;
            dump_chat_completion_settings.push_settings(name, &chat_settings)?;
        }

        // 3. dump the tasks
        progress.update_progress(DumpCreationProgress::DumpTheTasks);
        let mut dump_tasks = dump.create_tasks_queue()?;

        let (atomic, update_task_progress) =
            AtomicTaskStep::new(self.queue.tasks.all_tasks.len(&rtxn)? as u32);
        progress.update_progress(update_task_progress);

        for ret in self.queue.tasks.all_tasks.iter(&rtxn)? {
            if self.scheduler.must_stop_processing.get() {
                return Err(Error::AbortedTask);
            }

            let (_, mut t) = ret?;
            let status = t.status;
            let content_file = t.content_uuid();

            // In the case we're dumping ourselves we want to be marked as finished
            // to not loop over ourselves indefinitely.
            if t.uid == task.uid {
                let finished_at = OffsetDateTime::now_utc();

                // We're going to fake the date because we don't know if everything is going to go well.
                // But we need to dump the task as finished and successful.
                // If something fail everything will be set appropriately in the end.
                t.status = Status::Succeeded;
                t.started_at = Some(started_at);
                t.finished_at = Some(finished_at);
            }

            // Patch the task to remove the batch uid, because as of v1.12.5 batches are not persisted.
            // This prevent from referencing *future* batches not actually associated with the task.
            //
            // See <https://github.com/meilisearch/meilisearch/issues/5247> for details.
            t.batch_uid = None;

            let mut dump_content_file = dump_tasks.push_task(&t.into())?;

            // 3.1. Dump the `content_file` associated with the task if there is one and the task is not finished yet.
            if let Some(content_file) = content_file {
                if self.scheduler.must_stop_processing.get() {
                    return Err(Error::AbortedTask);
                }
                if status == Status::Enqueued {
                    let content_file = self.queue.file_store.get_update(content_file)?;

                    for document in
                        serde_json::de::Deserializer::from_reader(content_file).into_iter()
                    {
                        let document = document.map_err(|e| {
                            Error::from_milli(milli::InternalError::SerdeJson(e).into(), None)
                        })?;
                        dump_content_file.push_document(&document)?;
                    }

                    dump_content_file.flush()?;
                }
            }
            atomic.fetch_add(1, Ordering::Relaxed);
        }
        dump_tasks.flush()?;

        // 4. dump the batches
        progress.update_progress(DumpCreationProgress::DumpTheBatches);
        let mut dump_batches = dump.create_batches_queue()?;

        let (atomic_batch_progress, update_batch_progress) =
            AtomicBatchStep::new(self.queue.batches.all_batches.len(&rtxn)? as u32);
        progress.update_progress(update_batch_progress);

        for ret in self.queue.batches.all_batches.iter(&rtxn)? {
            if self.scheduler.must_stop_processing.get() {
                return Err(Error::AbortedTask);
            }

            let (_, mut b) = ret?;
            // In the case we're dumping ourselves we want to be marked as finished
            // to not loop over ourselves indefinitely.
            if b.uid == task.uid {
                let finished_at = OffsetDateTime::now_utc();

                // We're going to fake the date because we don't know if everything is going to go well.
                // But we need to dump the task as finished and successful.
                // If something fail everything will be set appropriately in the end.
                let mut statuses = BTreeMap::new();
                statuses.insert(Status::Succeeded, b.stats.total_nb_tasks);
                b.stats.status = statuses;
                b.finished_at = Some(finished_at);
            }

            dump_batches.push_batch(&b)?;
            atomic_batch_progress.fetch_add(1, Ordering::Relaxed);
        }
        dump_batches.flush()?;

        // 5. Dump the indexes
        progress.update_progress(DumpCreationProgress::DumpTheIndexes);
        let nb_indexes = self.index_mapper.index_mapping.len(&rtxn)? as u32;
        let mut count = 0;
        let () = self.index_mapper.try_for_each_index(&rtxn, |uid, index| -> Result<()> {
            progress.update_progress(VariableNameStep::<DumpCreationProgress>::new(
                uid.to_string(),
                count,
                nb_indexes,
            ));
            count += 1;

            let rtxn = index.read_txn()?;
            let metadata = IndexMetadata {
                uid: uid.to_owned(),
                primary_key: index.primary_key(&rtxn)?.map(String::from),
                created_at: index
                    .created_at(&rtxn)
                    .map_err(|e| Error::from_milli(e, Some(uid.to_string())))?,
                updated_at: index
                    .updated_at(&rtxn)
                    .map_err(|e| Error::from_milli(e, Some(uid.to_string())))?,
            };
            let mut index_dumper = dump.create_index(uid, &metadata)?;

            let fields_ids_map = index.fields_ids_map(&rtxn)?;
            let all_fields: Vec<_> = fields_ids_map.iter().map(|(id, _)| id).collect();

            let nb_documents = index
                .number_of_documents(&rtxn)
                .map_err(|e| Error::from_milli(e, Some(uid.to_string())))?
                as u32;
            let (atomic, update_document_progress) = AtomicDocumentStep::new(nb_documents);
            progress.update_progress(update_document_progress);
            let documents = index
                .all_documents(&rtxn)
                .map_err(|e| Error::from_milli(e, Some(uid.to_string())))?;
            // 5.1. Dump the documents
            for ret in documents {
                if self.scheduler.must_stop_processing.get() {
                    return Err(Error::AbortedTask);
                }

                let (id, doc) = ret.map_err(|e| Error::from_milli(e, Some(uid.to_string())))?;

                let mut document = milli::obkv_to_json(&all_fields, &fields_ids_map, doc)
                    .map_err(|e| Error::from_milli(e, Some(uid.to_string())))?;

                'inject_vectors: {
                    let embeddings = index
                        .embeddings(&rtxn, id)
                        .map_err(|e| Error::from_milli(e, Some(uid.to_string())))?;

                    if embeddings.is_empty() {
                        break 'inject_vectors;
                    }

                    let vectors = document
                        .entry(RESERVED_VECTORS_FIELD_NAME.to_owned())
                        .or_insert(serde_json::Value::Object(Default::default()));

                    let serde_json::Value::Object(vectors) = vectors else {
                        let user_err =
                            milli::Error::UserError(milli::UserError::InvalidVectorsMapType {
                                document_id: {
                                    if let Ok(Some(Ok(index))) = index
                                        .external_id_of(&rtxn, std::iter::once(id))
                                        .map(|it| it.into_iter().next())
                                    {
                                        index
                                    } else {
                                        format!("internal docid={id}")
                                    }
                                },
                                value: vectors.clone(),
                            });

                        return Err(Error::from_milli(user_err, Some(uid.to_string())));
                    };

                    for (embedder_name, (embeddings, regenerate)) in embeddings {
                        let embeddings = ExplicitVectors {
                            embeddings: Some(VectorOrArrayOfVectors::from_array_of_vectors(
                                embeddings,
                            )),
                            regenerate,
                        };
                        vectors.insert(embedder_name, serde_json::to_value(embeddings).unwrap());
                    }
                }

                index_dumper.push_document(&document)?;
                atomic.fetch_add(1, Ordering::Relaxed);
            }

            // 5.2. Dump the settings
            let settings = meilisearch_types::settings::settings(
                index,
                &rtxn,
                meilisearch_types::settings::SecretPolicy::RevealSecrets,
            )
            .map_err(|e| Error::from_milli(e, Some(uid.to_string())))?;
            index_dumper.settings(&settings)?;
            Ok(())
        })?;

        // 6. Dump experimental feature settings
        progress.update_progress(DumpCreationProgress::DumpTheExperimentalFeatures);
        let features = self.features().runtime_features();
        dump.create_experimental_features(features)?;
        let network = self.network();
        dump.create_network(network)?;

        let dump_uid = started_at.format(format_description!(
                    "[year repr:full][month repr:numerical][day padding:zero]-[hour padding:zero][minute padding:zero][second padding:zero][subsecond digits:3]"
                )).unwrap();

        if self.scheduler.must_stop_processing.get() {
            return Err(Error::AbortedTask);
        }
        progress.update_progress(DumpCreationProgress::CompressTheDump);
        let path = self.scheduler.dumps_path.join(format!("{}.dump", dump_uid));
        let file = File::create(path)?;
        dump.persist_to(BufWriter::new(file))?;

        // if we reached this step we can tell the scheduler we succeeded to dump ourselves.
        task.status = Status::Succeeded;
        task.details = Some(Details::Dump { dump_uid: Some(dump_uid) });
        Ok(vec![task])
    }
}
