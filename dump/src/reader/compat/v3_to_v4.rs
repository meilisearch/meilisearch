use super::v2_to_v3::{CompatIndexV2ToV3, CompatV2ToV3};
use super::v4_to_v5::CompatV4ToV5;
use crate::reader::{v3, v4, UpdateFile};
use crate::Result;

pub enum CompatV3ToV4 {
    V3(v3::V3Reader),
    Compat(CompatV2ToV3),
}

impl CompatV3ToV4 {
    pub fn new(v3: v3::V3Reader) -> CompatV3ToV4 {
        CompatV3ToV4::V3(v3)
    }

    pub fn to_v5(self) -> CompatV4ToV5 {
        CompatV4ToV5::Compat(self)
    }

    pub fn version(&self) -> crate::Version {
        match self {
            CompatV3ToV4::V3(v3) => v3.version(),
            CompatV3ToV4::Compat(compat) => compat.version(),
        }
    }

    pub fn date(&self) -> Option<time::OffsetDateTime> {
        match self {
            CompatV3ToV4::V3(v3) => v3.date(),
            CompatV3ToV4::Compat(compat) => compat.date(),
        }
    }

    pub fn instance_uid(&self) -> Result<Option<uuid::Uuid>> {
        Ok(None)
    }

    pub fn indexes(&self) -> Result<impl Iterator<Item = Result<CompatIndexV3ToV4>> + '_> {
        Ok(match self {
            CompatV3ToV4::V3(v3) => {
                Box::new(v3.indexes()?.map(|index| index.map(CompatIndexV3ToV4::from)))
                    as Box<dyn Iterator<Item = Result<CompatIndexV3ToV4>> + '_>
            }

            CompatV3ToV4::Compat(compat) => {
                Box::new(compat.indexes()?.map(|index| index.map(CompatIndexV3ToV4::from)))
                    as Box<dyn Iterator<Item = Result<CompatIndexV3ToV4>> + '_>
            }
        })
    }

    pub fn tasks(
        &mut self,
    ) -> Box<dyn Iterator<Item = Result<(v4::Task, Option<Box<UpdateFile>>)>> + '_> {
        let indexes = match self {
            CompatV3ToV4::V3(v3) => v3.index_uuid(),
            CompatV3ToV4::Compat(compat) => compat.index_uuid(),
        };
        let tasks = match self {
            CompatV3ToV4::V3(v3) => v3.tasks(),
            CompatV3ToV4::Compat(compat) => compat.tasks(),
        };

        Box::new(
            tasks
                // we need to override the old task ids that were generated
                // by index in favor of a global unique incremental ID.
                .enumerate()
                .map(move |(task_id, task)| {
                    task.map(|(task, content_file)| {
                        let index_uid = indexes
                            .iter()
                            .find(|index| index.uuid == task.uuid)
                            .map(|index| index.uid.clone());

                        let index_uid = match index_uid {
                            Some(uid) => uid,
                            None => {
                                log::warn!(
                                    "Error while importing the update {}.",
                                    task.update.id()
                                );
                                log::warn!(
                                    "The index associated to the uuid `{}` could not be retrieved.",
                                    task.uuid.to_string()
                                );
                                if task.update.is_finished() {
                                    // we're fucking with his history but not his data, that's ok-ish.
                                    log::warn!("The index-uuid will be set as `unknown`.");
                                    String::from("unknown")
                                } else {
                                    log::warn!("The task will be ignored.");
                                    return None;
                                }
                            }
                        };

                        let task = v4::Task {
                            id: task_id as u32,
                            index_uid: v4::meta::IndexUid(index_uid),
                            content: match task.update.meta() {
                                v3::Kind::DeleteDocuments(documents) => {
                                    v4::tasks::TaskContent::DocumentDeletion(
                                        v4::tasks::DocumentDeletion::Ids(documents.clone()),
                                    )
                                }
                                v3::Kind::DocumentAddition {
                                    primary_key,
                                    method,
                                    content_uuid,
                                } => v4::tasks::TaskContent::DocumentAddition {
                                    merge_strategy: match method {
                                        v3::updates::IndexDocumentsMethod::ReplaceDocuments => {
                                            v4::tasks::IndexDocumentsMethod::ReplaceDocuments
                                        }
                                        v3::updates::IndexDocumentsMethod::UpdateDocuments => {
                                            v4::tasks::IndexDocumentsMethod::UpdateDocuments
                                        }
                                    },
                                    primary_key: primary_key.clone(),
                                    documents_count: 0, // we don't have this info
                                    allow_index_creation: true, // there was no API-key in the v3
                                    content_uuid: *content_uuid,
                                },
                                v3::Kind::Settings(settings) => {
                                    v4::tasks::TaskContent::SettingsUpdate {
                                        settings: v4::Settings::from(settings.clone()),
                                        is_deletion: false, // that didn't exist at this time
                                        allow_index_creation: true, // there was no API-key in the v3
                                    }
                                }
                                v3::Kind::ClearDocuments => {
                                    v4::tasks::TaskContent::DocumentDeletion(
                                        v4::tasks::DocumentDeletion::Clear,
                                    )
                                }
                            },
                            events: match task.update {
                                v3::Status::Processing(processing) => {
                                    vec![v4::tasks::TaskEvent::Created(processing.from.enqueued_at)]
                                }
                                v3::Status::Enqueued(enqueued) => {
                                    vec![v4::tasks::TaskEvent::Created(enqueued.enqueued_at)]
                                }
                                v3::Status::Processed(processed) => {
                                    vec![
                                        v4::tasks::TaskEvent::Created(
                                            processed.from.from.enqueued_at,
                                        ),
                                        v4::tasks::TaskEvent::Processing(
                                            processed.from.started_processing_at,
                                        ),
                                        v4::tasks::TaskEvent::Succeded {
                                            result: match processed.success {
                                                v3::updates::UpdateResult::DocumentsAddition(
                                                    document_addition,
                                                ) => v4::tasks::TaskResult::DocumentAddition {
                                                    indexed_documents: document_addition
                                                        .nb_documents
                                                        as u64,
                                                },
                                                v3::updates::UpdateResult::DocumentDeletion {
                                                    deleted,
                                                } => v4::tasks::TaskResult::DocumentDeletion {
                                                    deleted_documents: deleted,
                                                },
                                                v3::updates::UpdateResult::Other => {
                                                    v4::tasks::TaskResult::Other
                                                }
                                            },
                                            timestamp: processed.processed_at,
                                        },
                                    ]
                                }
                                v3::Status::Failed(failed) => vec![
                                    v4::tasks::TaskEvent::Created(failed.from.from.enqueued_at),
                                    v4::tasks::TaskEvent::Processing(
                                        failed.from.started_processing_at,
                                    ),
                                    v4::tasks::TaskEvent::Failed {
                                        error: v4::ResponseError::from_msg(
                                            failed.msg.to_string(),
                                            failed.code.into(),
                                        ),
                                        timestamp: failed.failed_at,
                                    },
                                ],
                                v3::Status::Aborted(aborted) => vec![
                            v4::tasks::TaskEvent::Created(aborted.from.enqueued_at),
                            v4::tasks::TaskEvent::Failed {
                                error: v4::ResponseError::from_msg(
                                    "Task was aborted in a previous version of meilisearch."
                                        .to_string(),
                                    v4::errors::Code::UnretrievableErrorCode,
                                ),
                                timestamp: aborted.aborted_at,
                            },
                        ],
                            },
                        };

                        Some((task, content_file))
                    })
                })
                .filter_map(|res| res.transpose()),
        )
    }

    pub fn keys(&mut self) -> Box<dyn Iterator<Item = Result<v4::Key>> + '_> {
        Box::new(std::iter::empty())
    }
}

pub enum CompatIndexV3ToV4 {
    V3(v3::V3IndexReader),
    Compat(CompatIndexV2ToV3),
}

impl From<v3::V3IndexReader> for CompatIndexV3ToV4 {
    fn from(index_reader: v3::V3IndexReader) -> Self {
        Self::V3(index_reader)
    }
}

impl From<CompatIndexV2ToV3> for CompatIndexV3ToV4 {
    fn from(index_reader: CompatIndexV2ToV3) -> Self {
        Self::Compat(index_reader)
    }
}

impl CompatIndexV3ToV4 {
    pub fn new(v3: v3::V3IndexReader) -> CompatIndexV3ToV4 {
        CompatIndexV3ToV4::V3(v3)
    }

    pub fn metadata(&self) -> &crate::IndexMetadata {
        match self {
            CompatIndexV3ToV4::V3(v3) => v3.metadata(),
            CompatIndexV3ToV4::Compat(compat) => compat.metadata(),
        }
    }

    pub fn documents(&mut self) -> Result<Box<dyn Iterator<Item = Result<v4::Document>> + '_>> {
        match self {
            CompatIndexV3ToV4::V3(v3) => v3
                .documents()
                .map(|iter| Box::new(iter) as Box<dyn Iterator<Item = Result<v4::Document>> + '_>),

            CompatIndexV3ToV4::Compat(compat) => compat
                .documents()
                .map(|iter| Box::new(iter) as Box<dyn Iterator<Item = Result<v4::Document>> + '_>),
        }
    }

    pub fn settings(&mut self) -> Result<v4::Settings<v4::Checked>> {
        Ok(match self {
            CompatIndexV3ToV4::V3(v3) => {
                v4::Settings::<v4::Unchecked>::from(v3.settings()?).check()
            }
            CompatIndexV3ToV4::Compat(compat) => {
                v4::Settings::<v4::Unchecked>::from(compat.settings()?).check()
            }
        })
    }
}

impl<T> From<v3::Setting<T>> for v4::Setting<T> {
    fn from(setting: v3::Setting<T>) -> Self {
        match setting {
            v3::Setting::Set(t) => v4::Setting::Set(t),
            v3::Setting::Reset => v4::Setting::Reset,
            v3::Setting::NotSet => v4::Setting::NotSet,
        }
    }
}

impl From<v3::Code> for v4::Code {
    fn from(code: v3::Code) -> Self {
        match code {
            v3::Code::CreateIndex => v4::Code::CreateIndex,
            v3::Code::IndexAlreadyExists => v4::Code::IndexAlreadyExists,
            v3::Code::IndexNotFound => v4::Code::IndexNotFound,
            v3::Code::InvalidIndexUid => v4::Code::InvalidIndexUid,
            v3::Code::InvalidState => v4::Code::InvalidState,
            v3::Code::MissingPrimaryKey => v4::Code::MissingPrimaryKey,
            v3::Code::PrimaryKeyAlreadyPresent => v4::Code::PrimaryKeyAlreadyPresent,
            v3::Code::MaxFieldsLimitExceeded => v4::Code::MaxFieldsLimitExceeded,
            v3::Code::MissingDocumentId => v4::Code::MissingDocumentId,
            v3::Code::InvalidDocumentId => v4::Code::InvalidDocumentId,
            v3::Code::Filter => v4::Code::Filter,
            v3::Code::Sort => v4::Code::Sort,
            v3::Code::BadParameter => v4::Code::BadParameter,
            v3::Code::BadRequest => v4::Code::BadRequest,
            v3::Code::DatabaseSizeLimitReached => v4::Code::DatabaseSizeLimitReached,
            v3::Code::DocumentNotFound => v4::Code::DocumentNotFound,
            v3::Code::Internal => v4::Code::Internal,
            v3::Code::InvalidGeoField => v4::Code::InvalidGeoField,
            v3::Code::InvalidRankingRule => v4::Code::InvalidRankingRule,
            v3::Code::InvalidStore => v4::Code::InvalidStore,
            v3::Code::InvalidToken => v4::Code::InvalidToken,
            v3::Code::MissingAuthorizationHeader => v4::Code::MissingAuthorizationHeader,
            v3::Code::NoSpaceLeftOnDevice => v4::Code::NoSpaceLeftOnDevice,
            v3::Code::DumpNotFound => v4::Code::DumpNotFound,
            v3::Code::TaskNotFound => v4::Code::TaskNotFound,
            v3::Code::PayloadTooLarge => v4::Code::PayloadTooLarge,
            v3::Code::RetrieveDocument => v4::Code::RetrieveDocument,
            v3::Code::SearchDocuments => v4::Code::SearchDocuments,
            v3::Code::UnsupportedMediaType => v4::Code::UnsupportedMediaType,
            v3::Code::DumpAlreadyInProgress => v4::Code::DumpAlreadyInProgress,
            v3::Code::DumpProcessFailed => v4::Code::DumpProcessFailed,
            v3::Code::InvalidContentType => v4::Code::InvalidContentType,
            v3::Code::MissingContentType => v4::Code::MissingContentType,
            v3::Code::MalformedPayload => v4::Code::MalformedPayload,
            v3::Code::MissingPayload => v4::Code::MissingPayload,
            v3::Code::UnretrievableErrorCode => v4::Code::UnretrievableErrorCode,
            v3::Code::MalformedDump => v4::Code::MalformedDump,
        }
    }
}

impl<T> From<v3::Settings<T>> for v4::Settings<v4::Unchecked> {
    fn from(settings: v3::Settings<T>) -> Self {
        v4::Settings {
            displayed_attributes: settings.displayed_attributes.into(),
            searchable_attributes: settings.searchable_attributes.into(),
            filterable_attributes: settings.filterable_attributes.into(),
            sortable_attributes: settings.sortable_attributes.into(),
            ranking_rules: settings.ranking_rules.into(),
            stop_words: settings.stop_words.into(),
            synonyms: settings.synonyms.into(),
            distinct_attribute: settings.distinct_attribute.into(),
            typo_tolerance: v4::Setting::NotSet,
            _kind: std::marker::PhantomData,
        }
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
    fn compat_v3_v4() {
        let dump = File::open("tests/assets/v3.dump").unwrap();
        let dir = TempDir::new().unwrap();
        let mut dump = BufReader::new(dump);
        let gz = GzDecoder::new(&mut dump);
        let mut archive = tar::Archive::new(gz);
        archive.unpack(dir.path()).unwrap();

        let mut dump = v3::V3Reader::open(dir).unwrap().to_v4();

        // top level infos
        insta::assert_display_snapshot!(dump.date().unwrap(), @"2022-10-07 11:39:03.709153554 +00:00:00");

        // tasks
        let tasks = dump.tasks().collect::<Result<Vec<_>>>().unwrap();
        let (tasks, mut update_files): (Vec<_>, Vec<_>) = tasks.into_iter().unzip();
        meili_snap::snapshot_hash!(meili_snap::json_string!(tasks), @"79bc053583a1a7172bbaaafb1edaeb78");
        assert_eq!(update_files.len(), 10);
        assert!(update_files[0].is_some()); // the enqueued document addition
        assert!(update_files[1..].iter().all(|u| u.is_none())); // everything already processed

        let update_file = update_files.remove(0).unwrap().collect::<Result<Vec<_>>>().unwrap();
        meili_snap::snapshot_hash!(meili_snap::json_string!(update_file), @"7b8889539b669c7b9ddba448bafa385d");

        // keys
        let keys = dump.keys().collect::<Result<Vec<_>>>().unwrap();
        meili_snap::snapshot_hash!(meili_snap::json_string!(keys, { "[].uid" => "[uuid]" }), @"d751713988987e9331980363e24189ce");

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

        meili_snap::snapshot_hash!(format!("{:#?}", products.settings()), @"ea46dd6b58c5e1d65c1c8159a32695ea");
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

        meili_snap::snapshot_hash!(format!("{:#?}", movies.settings()), @"687aaab250f01b55d57bc69aa313b581");
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

        meili_snap::snapshot_hash!(format!("{:#?}", movies2.settings()), @"cd9fedbd7e3492831a94da62c90013ea");
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

        meili_snap::snapshot_hash!(format!("{:#?}", spells.settings()), @"cd9fedbd7e3492831a94da62c90013ea");
        let documents = spells.documents().unwrap().collect::<Result<Vec<_>>>().unwrap();
        assert_eq!(documents.len(), 10);
        meili_snap::snapshot_hash!(format!("{:#?}", documents), @"235016433dd04262c7f2da01d1e808ce");
    }
}
