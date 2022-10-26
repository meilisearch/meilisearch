use std::convert::TryInto;
use std::str::FromStr;

use time::OffsetDateTime;
use uuid::Uuid;

use super::v3_to_v4::CompatV3ToV4;
use crate::reader::{v2, v3, Document};
use crate::Result;

pub struct CompatV2ToV3 {
    pub from: v2::V2Reader,
}

impl CompatV2ToV3 {
    pub fn new(v2: v2::V2Reader) -> CompatV2ToV3 {
        CompatV2ToV3 { from: v2 }
    }

    pub fn index_uuid(&self) -> Vec<v3::meta::IndexUuid> {
        self.from
            .index_uuid()
            .into_iter()
            .map(|index| v3::meta::IndexUuid { uid: index.uid, uuid: index.uuid })
            .collect()
    }

    pub fn to_v4(self) -> CompatV3ToV4 {
        CompatV3ToV4::Compat(self)
    }

    pub fn version(&self) -> crate::Version {
        self.from.version()
    }

    pub fn date(&self) -> Option<time::OffsetDateTime> {
        self.from.date()
    }

    pub fn instance_uid(&self) -> Result<Option<uuid::Uuid>> {
        Ok(None)
    }

    pub fn indexes(&self) -> Result<impl Iterator<Item = Result<CompatIndexV2ToV3>> + '_> {
        Ok(self.from.indexes()?.map(|index_reader| -> Result<_> {
            let compat = CompatIndexV2ToV3::new(index_reader?);
            Ok(compat)
        }))
    }

    pub fn tasks(
        &mut self,
    ) -> Box<
        dyn Iterator<Item = Result<(v3::Task, Option<Box<dyn Iterator<Item = Result<Document>>>>)>>
            + '_,
    > {
        let _indexes = self.from.index_uuid.clone();

        Box::new(
            self.from
                .tasks()
                .map(move |task| {
                    task.map(|(task, content_file)| {
                        let task = v3::Task { uuid: task.uuid, update: task.update.into() };

                        Some((
                            task,
                            content_file.map(|content_file| {
                                Box::new(content_file) as Box<dyn Iterator<Item = Result<Document>>>
                            }),
                        ))
                    })
                })
                .filter_map(|res| res.transpose()),
        )
    }
}

pub struct CompatIndexV2ToV3 {
    from: v2::V2IndexReader,
}

impl CompatIndexV2ToV3 {
    pub fn new(v2: v2::V2IndexReader) -> CompatIndexV2ToV3 {
        CompatIndexV2ToV3 { from: v2 }
    }

    pub fn metadata(&self) -> &crate::IndexMetadata {
        self.from.metadata()
    }

    pub fn documents(&mut self) -> Result<Box<dyn Iterator<Item = Result<Document>> + '_>> {
        self.from
            .documents()
            .map(|iter| Box::new(iter) as Box<dyn Iterator<Item = Result<Document>> + '_>)
    }

    pub fn settings(&mut self) -> Result<v3::Settings<v3::Checked>> {
        Ok(v3::Settings::<v3::Unchecked>::from(self.from.settings()?).check())
    }
}

impl From<v2::updates::UpdateStatus> for v3::updates::UpdateStatus {
    fn from(update: v2::updates::UpdateStatus) -> Self {
        match update {
            v2::updates::UpdateStatus::Processing(processing) => {
                match (processing.from.meta.clone(), processing.from.content).try_into() {
                    Ok(meta) => v3::updates::UpdateStatus::Processing(v3::updates::Processing {
                        from: v3::updates::Enqueued {
                            update_id: processing.from.update_id,
                            meta,
                            enqueued_at: processing.from.enqueued_at,
                        },
                        started_processing_at: processing.started_processing_at,
                    }),
                    Err(e) => {
                        log::warn!("Error with task {}: {}", processing.from.update_id, e);
                        log::warn!("Task will be marked as `Failed`.");
                        v3::updates::UpdateStatus::Failed(v3::updates::Failed {
                            from: v3::updates::Processing {
                                from: v3::updates::Enqueued {
                                    update_id: processing.from.update_id,
                                    meta: update_from_unchecked_update_meta(processing.from.meta),
                                    enqueued_at: processing.from.enqueued_at,
                                },
                                started_processing_at: processing.started_processing_at,
                            },
                            msg: e.to_string(),
                            code: v3::Code::MalformedDump,
                            failed_at: OffsetDateTime::now_utc(),
                        })
                    }
                }
            }
            v2::updates::UpdateStatus::Enqueued(enqueued) => {
                match (enqueued.meta.clone(), enqueued.content).try_into() {
                    Ok(meta) => v3::updates::UpdateStatus::Enqueued(v3::updates::Enqueued {
                        update_id: enqueued.update_id,
                        meta,
                        enqueued_at: enqueued.enqueued_at,
                    }),
                    Err(e) => {
                        log::warn!("Error with task {}: {}", enqueued.update_id, e);
                        log::warn!("Task will be marked as `Failed`.");
                        v3::updates::UpdateStatus::Failed(v3::updates::Failed {
                            from: v3::updates::Processing {
                                from: v3::updates::Enqueued {
                                    update_id: enqueued.update_id,
                                    meta: update_from_unchecked_update_meta(enqueued.meta),
                                    enqueued_at: enqueued.enqueued_at,
                                },
                                started_processing_at: OffsetDateTime::now_utc(),
                            },
                            msg: e.to_string(),
                            code: v3::Code::MalformedDump,
                            failed_at: OffsetDateTime::now_utc(),
                        })
                    }
                }
            }
            v2::updates::UpdateStatus::Processed(processed) => {
                v3::updates::UpdateStatus::Processed(v3::updates::Processed {
                    success: processed.success.into(),
                    processed_at: processed.processed_at,
                    from: v3::updates::Processing {
                        from: v3::updates::Enqueued {
                            update_id: processed.from.from.update_id,
                            // since we're never going to read the content_file again it's ok to generate a fake one.
                            meta: update_from_unchecked_update_meta(processed.from.from.meta),
                            enqueued_at: processed.from.from.enqueued_at,
                        },
                        started_processing_at: processed.from.started_processing_at,
                    },
                })
            }
            v2::updates::UpdateStatus::Aborted(aborted) => {
                v3::updates::UpdateStatus::Aborted(v3::updates::Aborted {
                    from: v3::updates::Enqueued {
                        update_id: aborted.from.update_id,
                        // since we're never going to read the content_file again it's ok to generate a fake one.
                        meta: update_from_unchecked_update_meta(aborted.from.meta),
                        enqueued_at: aborted.from.enqueued_at,
                    },
                    aborted_at: aborted.aborted_at,
                })
            }
            v2::updates::UpdateStatus::Failed(failed) => {
                v3::updates::UpdateStatus::Failed(v3::updates::Failed {
                    from: v3::updates::Processing {
                        from: v3::updates::Enqueued {
                            update_id: failed.from.from.update_id,
                            // since we're never going to read the content_file again it's ok to generate a fake one.
                            meta: update_from_unchecked_update_meta(failed.from.from.meta),
                            enqueued_at: failed.from.from.enqueued_at,
                        },
                        started_processing_at: failed.from.started_processing_at,
                    },
                    msg: failed.error.message,
                    code: failed.error.error_code.into(),
                    failed_at: failed.failed_at,
                })
            }
        }
    }
}

impl TryFrom<(v2::updates::UpdateMeta, Option<Uuid>)> for v3::updates::Update {
    type Error = crate::Error;

    fn try_from((update, uuid): (v2::updates::UpdateMeta, Option<Uuid>)) -> Result<Self> {
        Ok(match update {
            v2::updates::UpdateMeta::DocumentsAddition { method, format: _, primary_key }
                if uuid.is_some() =>
            {
                v3::updates::Update::DocumentAddition {
                    primary_key,
                    method: match method {
                        v2::updates::IndexDocumentsMethod::ReplaceDocuments => {
                            v3::updates::IndexDocumentsMethod::ReplaceDocuments
                        }
                        v2::updates::IndexDocumentsMethod::UpdateDocuments => {
                            v3::updates::IndexDocumentsMethod::UpdateDocuments
                        }
                    },
                    content_uuid: uuid.unwrap(),
                }
            }
            v2::updates::UpdateMeta::DocumentsAddition { .. } => {
                return Err(crate::Error::MalformedTask)
            }
            v2::updates::UpdateMeta::ClearDocuments => v3::updates::Update::ClearDocuments,
            v2::updates::UpdateMeta::DeleteDocuments { ids } => {
                v3::updates::Update::DeleteDocuments(ids)
            }
            v2::updates::UpdateMeta::Settings(settings) => {
                v3::updates::Update::Settings(settings.into())
            }
        })
    }
}

pub fn update_from_unchecked_update_meta(update: v2::updates::UpdateMeta) -> v3::updates::Update {
    match update {
        v2::updates::UpdateMeta::DocumentsAddition { method, format: _, primary_key } => {
            v3::updates::Update::DocumentAddition {
                primary_key,
                method: match method {
                    v2::updates::IndexDocumentsMethod::ReplaceDocuments => {
                        v3::updates::IndexDocumentsMethod::ReplaceDocuments
                    }
                    v2::updates::IndexDocumentsMethod::UpdateDocuments => {
                        v3::updates::IndexDocumentsMethod::UpdateDocuments
                    }
                },
                // we use this special uuid so we can recognize it if one day there is a bug related to this field.
                content_uuid: Uuid::from_str("00112233-4455-6677-8899-aabbccddeeff").unwrap(),
            }
        }
        v2::updates::UpdateMeta::ClearDocuments => v3::updates::Update::ClearDocuments,
        v2::updates::UpdateMeta::DeleteDocuments { ids } => {
            v3::updates::Update::DeleteDocuments(ids)
        }
        v2::updates::UpdateMeta::Settings(settings) => {
            v3::updates::Update::Settings(settings.into())
        }
    }
}

impl From<v2::updates::UpdateResult> for v3::updates::UpdateResult {
    fn from(result: v2::updates::UpdateResult) -> Self {
        match result {
            v2::updates::UpdateResult::DocumentsAddition(addition) => {
                v3::updates::UpdateResult::DocumentsAddition(v3::updates::DocumentAdditionResult {
                    nb_documents: addition.nb_documents,
                })
            }
            v2::updates::UpdateResult::DocumentDeletion { deleted } => {
                v3::updates::UpdateResult::DocumentDeletion { deleted }
            }
            v2::updates::UpdateResult::Other => v3::updates::UpdateResult::Other,
        }
    }
}

impl From<String> for v3::Code {
    fn from(code: String) -> Self {
        match code.as_ref() {
            "create_index" => v3::Code::CreateIndex,
            "index_already_exists" => v3::Code::IndexAlreadyExists,
            "index_not_found" => v3::Code::IndexNotFound,
            "invalid_index_uid" => v3::Code::InvalidIndexUid,
            "invalid_state" => v3::Code::InvalidState,
            "missing_primary_key" => v3::Code::MissingPrimaryKey,
            "primary_key_already_present" => v3::Code::PrimaryKeyAlreadyPresent,
            "max_fields_limit_exceeded" => v3::Code::MaxFieldsLimitExceeded,
            "missing_document_id" => v3::Code::MissingDocumentId,
            "invalid_document_id" => v3::Code::InvalidDocumentId,
            "filter" => v3::Code::Filter,
            "sort" => v3::Code::Sort,
            "bad_parameter" => v3::Code::BadParameter,
            "bad_request" => v3::Code::BadRequest,
            "database_size_limit_reached" => v3::Code::DatabaseSizeLimitReached,
            "document_not_found" => v3::Code::DocumentNotFound,
            "internal" => v3::Code::Internal,
            "invalid_geo_field" => v3::Code::InvalidGeoField,
            "invalid_ranking_rule" => v3::Code::InvalidRankingRule,
            "invalid_store" => v3::Code::InvalidStore,
            "invalid_token" => v3::Code::InvalidToken,
            "missing_authorization_header" => v3::Code::MissingAuthorizationHeader,
            "no_space_left_on_device" => v3::Code::NoSpaceLeftOnDevice,
            "dump_not_found" => v3::Code::DumpNotFound,
            "task_not_found" => v3::Code::TaskNotFound,
            "payload_too_large" => v3::Code::PayloadTooLarge,
            "retrieve_document" => v3::Code::RetrieveDocument,
            "search_documents" => v3::Code::SearchDocuments,
            "unsupported_media_type" => v3::Code::UnsupportedMediaType,
            "dump_already_in_progress" => v3::Code::DumpAlreadyInProgress,
            "dump_process_failed" => v3::Code::DumpProcessFailed,
            "invalid_content_type" => v3::Code::InvalidContentType,
            "missing_content_type" => v3::Code::MissingContentType,
            "malformed_payload" => v3::Code::MalformedPayload,
            "missing_payload" => v3::Code::MissingPayload,
            other => {
                log::warn!("Unknown error code {}", other);
                v3::Code::UnretrievableErrorCode
            }
        }
    }
}

fn option_to_setting<T>(opt: Option<Option<T>>) -> v3::Setting<T> {
    match opt {
        Some(Some(t)) => v3::Setting::Set(t),
        None => v3::Setting::NotSet,
        Some(None) => v3::Setting::Reset,
    }
}

impl<T> From<v2::Settings<T>> for v3::Settings<v3::Unchecked> {
    fn from(settings: v2::Settings<T>) -> Self {
        v3::Settings {
            displayed_attributes: option_to_setting(settings.displayed_attributes),
            searchable_attributes: option_to_setting(settings.searchable_attributes),
            filterable_attributes: option_to_setting(settings.filterable_attributes)
                .map(|f| f.into_iter().collect()),
            sortable_attributes: v3::Setting::NotSet,
            ranking_rules: option_to_setting(settings.ranking_rules).map(|criteria| {
                criteria.into_iter().map(|criterion| patch_ranking_rules(&criterion)).collect()
            }),
            stop_words: option_to_setting(settings.stop_words),
            synonyms: option_to_setting(settings.synonyms),
            distinct_attribute: option_to_setting(settings.distinct_attribute),
            _kind: std::marker::PhantomData,
        }
    }
}

fn patch_ranking_rules(ranking_rule: &str) -> String {
    match v2::settings::Criterion::from_str(ranking_rule) {
        Ok(v2::settings::Criterion::Words) => String::from("words"),
        Ok(v2::settings::Criterion::Typo) => String::from("typo"),
        Ok(v2::settings::Criterion::Proximity) => String::from("proximity"),
        Ok(v2::settings::Criterion::Attribute) => String::from("attribute"),
        Ok(v2::settings::Criterion::Exactness) => String::from("exactness"),
        Ok(v2::settings::Criterion::Asc(name)) => format!("{name}:asc"),
        Ok(v2::settings::Criterion::Desc(name)) => format!("{name}:desc"),
        // we want to forward the error to the current version of meilisearch
        Err(_) => ranking_rule.to_string(),
    }
}

#[cfg(test)]
pub(crate) mod test {
    use std::fs::File;
    use std::io::BufReader;

    use flate2::bufread::GzDecoder;
    use tempfile::TempDir;

    use super::*;

    #[test]
    fn compat_v2_v3() {
        let dump = File::open("tests/assets/v2.dump").unwrap();
        let dir = TempDir::new().unwrap();
        let mut dump = BufReader::new(dump);
        let gz = GzDecoder::new(&mut dump);
        let mut archive = tar::Archive::new(gz);
        archive.unpack(dir.path()).unwrap();

        let mut dump = v2::V2Reader::open(dir).unwrap().to_v3();

        // top level infos
        insta::assert_display_snapshot!(dump.date().unwrap(), @"2022-10-09 20:27:59.904096267 +00:00:00");

        // tasks
        let tasks = dump.tasks().collect::<Result<Vec<_>>>().unwrap();
        let (tasks, mut update_files): (Vec<_>, Vec<_>) = tasks.into_iter().unzip();
        meili_snap::snapshot_hash!(meili_snap::json_string!(tasks), @"9507711db47c7171c79bc6d57d0bed79");
        assert_eq!(update_files.len(), 9);
        assert!(update_files[0].is_some()); // the enqueued document addition
        assert!(update_files[1..].iter().all(|u| u.is_none())); // everything already processed

        let update_file = update_files.remove(0).unwrap().collect::<Result<Vec<_>>>().unwrap();
        meili_snap::snapshot_hash!(meili_snap::json_string!(update_file), @"7b8889539b669c7b9ddba448bafa385d");

        // indexes
        let mut indexes = dump.indexes().unwrap().collect::<Result<Vec<_>>>().unwrap();
        // the index are not ordered in any way by default
        indexes.sort_by_key(|index| index.metadata().uid.to_string());

        let mut products = indexes.pop().unwrap();
        let mut movies2 = indexes.pop().unwrap();
        let mut movies = indexes.pop().unwrap();
        let mut spells = indexes.pop().unwrap();
        assert!(indexes.is_empty());

        // products
        insta::assert_json_snapshot!(products.metadata(), { ".createdAt" => "[now]", ".updatedAt" => "[now]" }, @r###"
        {
          "uid": "products",
          "primaryKey": "sku",
          "createdAt": "[now]",
          "updatedAt": "[now]"
        }
        "###);

        meili_snap::snapshot_hash!(format!("{:#?}", products.settings()), @"f43338ecceeddd1ce13ffd55438b2347");
        let documents = products.documents().unwrap().collect::<Result<Vec<_>>>().unwrap();
        assert_eq!(documents.len(), 10);
        meili_snap::snapshot_hash!(format!("{:#?}", documents), @"548284a84de510f71e88e6cdea495cf5");

        // movies
        insta::assert_json_snapshot!(movies.metadata(), { ".createdAt" => "[now]", ".updatedAt" => "[now]" }, @r###"
        {
          "uid": "movies",
          "primaryKey": "id",
          "createdAt": "[now]",
          "updatedAt": "[now]"
        }
        "###);

        meili_snap::snapshot_hash!(format!("{:#?}", movies.settings()), @"ae7c5ade2243a553152dab2f354e9095");
        let documents = movies.documents().unwrap().collect::<Result<Vec<_>>>().unwrap();
        assert_eq!(documents.len(), 110);
        meili_snap::snapshot_hash!(format!("{:#?}", documents), @"d153b5a81d8b3cdcbe1dec270b574022");

        // movies2
        insta::assert_json_snapshot!(movies2.metadata(), { ".createdAt" => "[now]", ".updatedAt" => "[now]" }, @r###"
        {
          "uid": "movies_2",
          "primaryKey": null,
          "createdAt": "[now]",
          "updatedAt": "[now]"
        }
        "###);

        meili_snap::snapshot_hash!(format!("{:#?}", movies2.settings()), @"1be82b894556d23953af557b6a328a58");
        let documents = movies2.documents().unwrap().collect::<Result<Vec<_>>>().unwrap();
        assert_eq!(documents.len(), 0);
        meili_snap::snapshot_hash!(format!("{:#?}", documents), @"d751713988987e9331980363e24189ce");

        // spells
        insta::assert_json_snapshot!(spells.metadata(), { ".createdAt" => "[now]", ".updatedAt" => "[now]" }, @r###"
        {
          "uid": "dnd_spells",
          "primaryKey": "index",
          "createdAt": "[now]",
          "updatedAt": "[now]"
        }
        "###);

        meili_snap::snapshot_hash!(format!("{:#?}", spells.settings()), @"1be82b894556d23953af557b6a328a58");
        let documents = spells.documents().unwrap().collect::<Result<Vec<_>>>().unwrap();
        assert_eq!(documents.len(), 10);
        meili_snap::snapshot_hash!(format!("{:#?}", documents), @"235016433dd04262c7f2da01d1e808ce");
    }
}
