use std::str::FromStr;

use time::OffsetDateTime;
use uuid::Uuid;

use super::v1_to_v2::{CompatIndexV1ToV2, CompatV1ToV2};
use super::v3_to_v4::CompatV3ToV4;
use crate::reader::{v2, v3, Document};
use crate::Result;

pub enum CompatV2ToV3 {
    V2(v2::V2Reader),
    Compat(CompatV1ToV2),
}

impl CompatV2ToV3 {
    pub fn new(v2: v2::V2Reader) -> Self {
        Self::V2(v2)
    }

    pub fn index_uuid(&self) -> Vec<v3::meta::IndexUuid> {
        let v2_uuids = match self {
            Self::V2(from) => from.index_uuid(),
            Self::Compat(compat) => compat.index_uuid(),
        };
        v2_uuids
            .into_iter()
            .map(|index| v3::meta::IndexUuid { uid: index.uid, uuid: index.uuid })
            .collect()
    }

    pub fn to_v4(self) -> CompatV3ToV4 {
        CompatV3ToV4::Compat(self)
    }

    pub fn version(&self) -> crate::Version {
        match self {
            Self::V2(from) => from.version(),
            Self::Compat(compat) => compat.version(),
        }
    }

    pub fn date(&self) -> Option<time::OffsetDateTime> {
        match self {
            Self::V2(from) => from.date(),
            Self::Compat(compat) => compat.date(),
        }
    }

    pub fn instance_uid(&self) -> Result<Option<uuid::Uuid>> {
        Ok(None)
    }

    pub fn indexes(&self) -> Result<impl Iterator<Item = Result<CompatIndexV2ToV3>> + '_> {
        Ok(match self {
            Self::V2(from) => Box::new(from.indexes()?.map(|index_reader| -> Result<_> {
                let compat = CompatIndexV2ToV3::new(index_reader?);
                Ok(compat)
            }))
                as Box<dyn Iterator<Item = Result<CompatIndexV2ToV3>> + '_>,
            Self::Compat(compat) => Box::new(compat.indexes()?.map(|index_reader| {
                let compat = CompatIndexV2ToV3::Compat(Box::new(index_reader?));
                Ok(compat)
            }))
                as Box<dyn Iterator<Item = Result<CompatIndexV2ToV3>> + '_>,
        })
    }

    pub fn tasks(
        &mut self,
    ) -> Box<
        dyn Iterator<Item = Result<(v3::Task, Option<Box<dyn Iterator<Item = Result<Document>>>>)>>
            + '_,
    > {
        let tasks = match self {
            Self::V2(from) => from.tasks(),
            Self::Compat(compat) => compat.tasks(),
        };

        Box::new(
            tasks
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

pub enum CompatIndexV2ToV3 {
    V2(v2::V2IndexReader),
    Compat(Box<CompatIndexV1ToV2>),
}

impl CompatIndexV2ToV3 {
    pub fn new(v2: v2::V2IndexReader) -> Self {
        Self::V2(v2)
    }

    pub fn metadata(&self) -> &crate::IndexMetadata {
        match self {
            Self::V2(from) => from.metadata(),
            Self::Compat(compat) => compat.metadata(),
        }
    }

    pub fn documents(&mut self) -> Result<Box<dyn Iterator<Item = Result<Document>> + '_>> {
        match self {
            Self::V2(from) => from
                .documents()
                .map(|iter| Box::new(iter) as Box<dyn Iterator<Item = Result<Document>> + '_>),
            Self::Compat(compat) => compat.documents(),
        }
    }

    pub fn settings(&mut self) -> Result<v3::Settings<v3::Checked>> {
        let settings = match self {
            Self::V2(from) => from.settings()?,
            Self::Compat(compat) => compat.settings()?,
        };
        Ok(v3::Settings::<v3::Unchecked>::from(settings).check())
    }
}

impl From<v2::updates::UpdateStatus> for v3::updates::UpdateStatus {
    fn from(update: v2::updates::UpdateStatus) -> Self {
        match update {
            v2::updates::UpdateStatus::Processing(processing) => {
                match (processing.from.meta.clone(), processing.from.content).try_into() {
                    Ok(meta) => Self::Processing(v3::updates::Processing {
                        from: v3::updates::Enqueued {
                            update_id: processing.from.update_id,
                            meta,
                            enqueued_at: processing.from.enqueued_at,
                        },
                        started_processing_at: processing.started_processing_at,
                    }),
                    Err(e) => {
                        tracing::warn!("Error with task {}: {}", processing.from.update_id, e);
                        tracing::warn!("Task will be marked as `Failed`.");
                        Self::Failed(v3::updates::Failed {
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
                    Ok(meta) => Self::Enqueued(v3::updates::Enqueued {
                        update_id: enqueued.update_id,
                        meta,
                        enqueued_at: enqueued.enqueued_at,
                    }),
                    Err(e) => {
                        tracing::warn!("Error with task {}: {}", enqueued.update_id, e);
                        tracing::warn!("Task will be marked as `Failed`.");
                        Self::Failed(v3::updates::Failed {
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
                Self::Processed(v3::updates::Processed {
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
                Self::Aborted(v3::updates::Aborted {
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
                Self::Failed(v3::updates::Failed {
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
                Self::DocumentAddition {
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
            v2::updates::UpdateMeta::ClearDocuments => Self::ClearDocuments,
            v2::updates::UpdateMeta::DeleteDocuments { ids } => Self::DeleteDocuments(ids),
            v2::updates::UpdateMeta::Settings(settings) => Self::Settings(settings.into()),
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
                Self::DocumentsAddition(v3::updates::DocumentAdditionResult {
                    nb_documents: addition.nb_documents,
                })
            }
            v2::updates::UpdateResult::DocumentDeletion { deleted } => {
                Self::DocumentDeletion { deleted }
            }
            v2::updates::UpdateResult::Other => Self::Other,
        }
    }
}

impl From<String> for v3::Code {
    fn from(code: String) -> Self {
        match code.as_ref() {
            "create_index" => Self::CreateIndex,
            "index_already_exists" => Self::IndexAlreadyExists,
            "index_not_found" => Self::IndexNotFound,
            "invalid_index_uid" => Self::InvalidIndexUid,
            "invalid_state" => Self::InvalidState,
            "missing_primary_key" => Self::MissingPrimaryKey,
            "primary_key_already_present" => Self::PrimaryKeyAlreadyPresent,
            "max_fields_limit_exceeded" => Self::MaxFieldsLimitExceeded,
            "missing_document_id" => Self::MissingDocumentId,
            "invalid_document_id" => Self::InvalidDocumentId,
            "filter" => Self::Filter,
            "sort" => Self::Sort,
            "bad_parameter" => Self::BadParameter,
            "bad_request" => Self::BadRequest,
            "database_size_limit_reached" => Self::DatabaseSizeLimitReached,
            "document_not_found" => Self::DocumentNotFound,
            "internal" => Self::Internal,
            "invalid_geo_field" => Self::InvalidGeoField,
            "invalid_ranking_rule" => Self::InvalidRankingRule,
            "invalid_store" => Self::InvalidStore,
            "invalid_token" => Self::InvalidToken,
            "missing_authorization_header" => Self::MissingAuthorizationHeader,
            "no_space_left_on_device" => Self::NoSpaceLeftOnDevice,
            "dump_not_found" => Self::DumpNotFound,
            "task_not_found" => Self::TaskNotFound,
            "payload_too_large" => Self::PayloadTooLarge,
            "retrieve_document" => Self::RetrieveDocument,
            "search_documents" => Self::SearchDocuments,
            "unsupported_media_type" => Self::UnsupportedMediaType,
            "dump_already_in_progress" => Self::DumpAlreadyInProgress,
            "dump_process_failed" => Self::DumpProcessFailed,
            "invalid_content_type" => Self::InvalidContentType,
            "missing_content_type" => Self::MissingContentType,
            "malformed_payload" => Self::MalformedPayload,
            "missing_payload" => Self::MissingPayload,
            other => {
                tracing::warn!("Unknown error code {}", other);
                Self::UnretrievableErrorCode
            }
        }
    }
}

impl<A> From<v2::Setting<A>> for v3::Setting<A> {
    fn from(setting: v2::Setting<A>) -> Self {
        match setting {
            v2::settings::Setting::Set(a) => Self::Set(a),
            v2::settings::Setting::Reset => Self::Reset,
            v2::settings::Setting::NotSet => Self::NotSet,
        }
    }
}

impl<T> From<v2::Settings<T>> for v3::Settings<v3::Unchecked> {
    fn from(settings: v2::Settings<T>) -> Self {
        Self {
            displayed_attributes: settings.displayed_attributes.into(),
            searchable_attributes: settings.searchable_attributes.into(),
            filterable_attributes: settings.filterable_attributes.into(),
            sortable_attributes: settings.sortable_attributes.into(),
            ranking_rules: v3::Setting::from(settings.ranking_rules).map(|criteria| {
                criteria.into_iter().map(|criterion| patch_ranking_rules(&criterion)).collect()
            }),
            stop_words: settings.stop_words.into(),
            synonyms: settings.synonyms.into(),
            distinct_attribute: settings.distinct_attribute.into(),
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
        Ok(v2::settings::Criterion::Sort) => String::from("sort"),
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
    use meili_snap::insta;
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
        insta::assert_snapshot!(dump.date().unwrap(), @"2022-10-09 20:27:59.904096267 +00:00:00");

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

        insta::assert_json_snapshot!(products.settings().unwrap());
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

        insta::assert_json_snapshot!(movies.settings().unwrap());
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

        insta::assert_json_snapshot!(movies2.settings().unwrap());
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

        insta::assert_json_snapshot!(spells.settings().unwrap());
        let documents = spells.documents().unwrap().collect::<Result<Vec<_>>>().unwrap();
        assert_eq!(documents.len(), 10);
        meili_snap::snapshot_hash!(format!("{:#?}", documents), @"235016433dd04262c7f2da01d1e808ce");
    }
}
