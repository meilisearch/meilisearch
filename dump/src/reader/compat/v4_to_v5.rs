use crate::reader::{v4, v5, DumpReader, IndexReader};
use crate::Result;

use super::v3_to_v4::{CompatIndexV3ToV4, CompatV3ToV4};
use super::v5_to_v6::CompatV5ToV6;

pub enum CompatV4ToV5 {
    V4(v4::V4Reader),
    Compat(CompatV3ToV4),
}

impl CompatV4ToV5 {
    pub fn new(v4: v4::V4Reader) -> CompatV4ToV5 {
        CompatV4ToV5::V4(v4)
    }

    pub fn to_v6(self) -> CompatV5ToV6 {
        CompatV5ToV6::Compat(self)
    }

    pub fn version(&self) -> crate::Version {
        match self {
            CompatV4ToV5::V4(v4) => v4.version(),
            CompatV4ToV5::Compat(compat) => compat.version(),
        }
    }

    pub fn date(&self) -> Option<time::OffsetDateTime> {
        match self {
            CompatV4ToV5::V4(v4) => v4.date(),
            CompatV4ToV5::Compat(compat) => compat.date(),
        }
    }

    pub fn instance_uid(&self) -> Result<Option<uuid::Uuid>> {
        match self {
            CompatV4ToV5::V4(v4) => v4.instance_uid(),
            CompatV4ToV5::Compat(compat) => compat.instance_uid(),
        }
    }

    pub fn indexes(&self) -> Result<Box<dyn Iterator<Item = Result<CompatIndexV4ToV5>> + '_>> {
        Ok(match self {
            CompatV4ToV5::V4(v4) => Box::new(
                v4.indexes()?
                    .map(|index| index.map(CompatIndexV4ToV5::from)),
            )
                as Box<dyn Iterator<Item = Result<CompatIndexV4ToV5>> + '_>,

            CompatV4ToV5::Compat(compat) => Box::new(
                compat
                    .indexes()?
                    .map(|index| index.map(CompatIndexV4ToV5::from)),
            )
                as Box<dyn Iterator<Item = Result<CompatIndexV4ToV5>> + '_>,
        })
    }

    pub fn tasks(
        &mut self,
    ) -> Box<dyn Iterator<Item = Result<(v5::Task, Option<v5::UpdateFile>)>> + '_> {
        let tasks = match self {
            CompatV4ToV5::V4(v4) => v4.tasks(),
            CompatV4ToV5::Compat(compat) => compat.tasks(),
        };
        Box::new(tasks.map(|task| {
            task.map(|(task, content_file)| {
                // let task_view: v4::tasks::TaskView = task.into();

                let task = v5::Task {
                    id: task.id,
                    content: match task.content {
                        v4::tasks::TaskContent::DocumentAddition {
                            content_uuid,
                            merge_strategy,
                            primary_key,
                            documents_count,
                            allow_index_creation,
                        } => v5::tasks::TaskContent::DocumentAddition {
                            index_uid: v5::meta::IndexUid(task.index_uid.0),
                            content_uuid,
                            merge_strategy: match merge_strategy {
                                v4::tasks::IndexDocumentsMethod::ReplaceDocuments => {
                                    v5::tasks::IndexDocumentsMethod::ReplaceDocuments
                                }
                                v4::tasks::IndexDocumentsMethod::UpdateDocuments => {
                                    v5::tasks::IndexDocumentsMethod::UpdateDocuments
                                }
                            },
                            primary_key,
                            documents_count,
                            allow_index_creation,
                        },
                        v4::tasks::TaskContent::DocumentDeletion(deletion) => {
                            v5::tasks::TaskContent::DocumentDeletion {
                                index_uid: v5::meta::IndexUid(task.index_uid.0),
                                deletion: match deletion {
                                    v4::tasks::DocumentDeletion::Clear => {
                                        v5::tasks::DocumentDeletion::Clear
                                    }
                                    v4::tasks::DocumentDeletion::Ids(ids) => {
                                        v5::tasks::DocumentDeletion::Ids(ids)
                                    }
                                },
                            }
                        }
                        v4::tasks::TaskContent::SettingsUpdate {
                            settings,
                            is_deletion,
                            allow_index_creation,
                        } => v5::tasks::TaskContent::SettingsUpdate {
                            index_uid: v5::meta::IndexUid(task.index_uid.0),
                            settings: settings.into(),
                            is_deletion,
                            allow_index_creation,
                        },
                        v4::tasks::TaskContent::IndexDeletion => {
                            v5::tasks::TaskContent::IndexDeletion {
                                index_uid: v5::meta::IndexUid(task.index_uid.0),
                            }
                        }
                        v4::tasks::TaskContent::IndexCreation { primary_key } => {
                            v5::tasks::TaskContent::IndexCreation {
                                index_uid: v5::meta::IndexUid(task.index_uid.0),
                                primary_key,
                            }
                        }
                        v4::tasks::TaskContent::IndexUpdate { primary_key } => {
                            v5::tasks::TaskContent::IndexUpdate {
                                index_uid: v5::meta::IndexUid(task.index_uid.0),
                                primary_key,
                            }
                        }
                    },
                    events: task
                        .events
                        .into_iter()
                        .map(|event| match event {
                            v4::tasks::TaskEvent::Created(date) => {
                                v5::tasks::TaskEvent::Created(date)
                            }
                            v4::tasks::TaskEvent::Batched {
                                timestamp,
                                batch_id,
                            } => v5::tasks::TaskEvent::Batched {
                                timestamp,
                                batch_id,
                            },
                            v4::tasks::TaskEvent::Processing(date) => {
                                v5::tasks::TaskEvent::Processing(date)
                            }
                            v4::tasks::TaskEvent::Succeded { result, timestamp } => {
                                v5::tasks::TaskEvent::Succeeded {
                                    result: match result {
                                        v4::tasks::TaskResult::DocumentAddition {
                                            indexed_documents,
                                        } => v5::tasks::TaskResult::DocumentAddition {
                                            indexed_documents,
                                        },
                                        v4::tasks::TaskResult::DocumentDeletion {
                                            deleted_documents,
                                        } => v5::tasks::TaskResult::DocumentDeletion {
                                            deleted_documents,
                                        },
                                        v4::tasks::TaskResult::ClearAll { deleted_documents } => {
                                            v5::tasks::TaskResult::ClearAll { deleted_documents }
                                        }
                                        v4::tasks::TaskResult::Other => {
                                            v5::tasks::TaskResult::Other
                                        }
                                    },
                                    timestamp,
                                }
                            }
                            v4::tasks::TaskEvent::Failed { error, timestamp } => {
                                v5::tasks::TaskEvent::Failed {
                                    error: v5::ResponseError::from(error),
                                    timestamp,
                                }
                            }
                        })
                        .collect(),
                };

                (task, content_file)
            })
        }))
    }

    pub fn keys(&mut self) -> Box<dyn Iterator<Item = Result<v5::Key>> + '_> {
        let keys = match self {
            CompatV4ToV5::V4(v4) => v4.keys(),
            CompatV4ToV5::Compat(compat) => compat.keys(),
        };
        Box::new(keys.map(|key| {
            key.map(|key| v5::Key {
                description: key.description,
                name: None,
                uid: v5::keys::KeyId::new_v4(),
                actions: key
                    .actions
                    .into_iter()
                    .filter_map(|action| action.into())
                    .collect(),
                indexes: key
                    .indexes
                    .into_iter()
                    .map(|index| match index.as_str() {
                        "*" => v5::StarOr::Star,
                        _ => v5::StarOr::Other(v5::meta::IndexUid(index)),
                    })
                    .collect(),
                expires_at: key.expires_at,
                created_at: key.created_at,
                updated_at: key.updated_at,
            })
        }))
    }
}

pub enum CompatIndexV4ToV5 {
    V4(v4::V4IndexReader),
    Compat(CompatIndexV3ToV4),
}

impl From<v4::V4IndexReader> for CompatIndexV4ToV5 {
    fn from(index_reader: v4::V4IndexReader) -> Self {
        Self::V4(index_reader)
    }
}

impl From<CompatIndexV3ToV4> for CompatIndexV4ToV5 {
    fn from(index_reader: CompatIndexV3ToV4) -> Self {
        Self::Compat(index_reader)
    }
}

impl CompatIndexV4ToV5 {
    pub fn metadata(&self) -> &crate::IndexMetadata {
        match self {
            CompatIndexV4ToV5::V4(v4) => v4.metadata(),
            CompatIndexV4ToV5::Compat(compat) => compat.metadata(),
        }
    }

    pub fn documents(&mut self) -> Result<Box<dyn Iterator<Item = Result<v5::Document>> + '_>> {
        match self {
            CompatIndexV4ToV5::V4(v4) => v4
                .documents()
                .map(|iter| Box::new(iter) as Box<dyn Iterator<Item = Result<v5::Document>> + '_>),
            CompatIndexV4ToV5::Compat(compat) => compat
                .documents()
                .map(|iter| Box::new(iter) as Box<dyn Iterator<Item = Result<v5::Document>> + '_>),
        }
    }

    pub fn settings(&mut self) -> Result<v5::Settings<v5::Checked>> {
        match self {
            CompatIndexV4ToV5::V4(v4) => Ok(v5::Settings::from(v4.settings()?).check()),
            CompatIndexV4ToV5::Compat(compat) => Ok(v5::Settings::from(compat.settings()?).check()),
        }
    }
}

impl<T> From<v4::Setting<T>> for v5::Setting<T> {
    fn from(setting: v4::Setting<T>) -> Self {
        match setting {
            v4::Setting::Set(t) => v5::Setting::Set(t),
            v4::Setting::Reset => v5::Setting::Reset,
            v4::Setting::NotSet => v5::Setting::NotSet,
        }
    }
}

impl From<v4::ResponseError> for v5::ResponseError {
    fn from(error: v4::ResponseError) -> Self {
        let code = match error.error_code.as_ref() {
            "CreateIndex" => v5::Code::CreateIndex,
            "IndexAlreadyExists" => v5::Code::IndexAlreadyExists,
            "IndexNotFound" => v5::Code::IndexNotFound,
            "InvalidIndexUid" => v5::Code::InvalidIndexUid,
            "InvalidMinWordLengthForTypo" => v5::Code::InvalidMinWordLengthForTypo,
            "InvalidState" => v5::Code::InvalidState,
            "MissingPrimaryKey" => v5::Code::MissingPrimaryKey,
            "PrimaryKeyAlreadyPresent" => v5::Code::PrimaryKeyAlreadyPresent,
            "MaxFieldsLimitExceeded" => v5::Code::MaxFieldsLimitExceeded,
            "MissingDocumentId" => v5::Code::MissingDocumentId,
            "InvalidDocumentId" => v5::Code::InvalidDocumentId,
            "Filter" => v5::Code::Filter,
            "Sort" => v5::Code::Sort,
            "BadParameter" => v5::Code::BadParameter,
            "BadRequest" => v5::Code::BadRequest,
            "DatabaseSizeLimitReached" => v5::Code::DatabaseSizeLimitReached,
            "DocumentNotFound" => v5::Code::DocumentNotFound,
            "Internal" => v5::Code::Internal,
            "InvalidGeoField" => v5::Code::InvalidGeoField,
            "InvalidRankingRule" => v5::Code::InvalidRankingRule,
            "InvalidStore" => v5::Code::InvalidStore,
            "InvalidToken" => v5::Code::InvalidToken,
            "MissingAuthorizationHeader" => v5::Code::MissingAuthorizationHeader,
            "NoSpaceLeftOnDevice" => v5::Code::NoSpaceLeftOnDevice,
            "DumpNotFound" => v5::Code::DumpNotFound,
            "TaskNotFound" => v5::Code::TaskNotFound,
            "PayloadTooLarge" => v5::Code::PayloadTooLarge,
            "RetrieveDocument" => v5::Code::RetrieveDocument,
            "SearchDocuments" => v5::Code::SearchDocuments,
            "UnsupportedMediaType" => v5::Code::UnsupportedMediaType,
            "DumpAlreadyInProgress" => v5::Code::DumpAlreadyInProgress,
            "DumpProcessFailed" => v5::Code::DumpProcessFailed,
            "InvalidContentType" => v5::Code::InvalidContentType,
            "MissingContentType" => v5::Code::MissingContentType,
            "MalformedPayload" => v5::Code::MalformedPayload,
            "MissingPayload" => v5::Code::MissingPayload,
            "ApiKeyNotFound" => v5::Code::ApiKeyNotFound,
            "MissingParameter" => v5::Code::MissingParameter,
            "InvalidApiKeyActions" => v5::Code::InvalidApiKeyActions,
            "InvalidApiKeyIndexes" => v5::Code::InvalidApiKeyIndexes,
            "InvalidApiKeyExpiresAt" => v5::Code::InvalidApiKeyExpiresAt,
            "InvalidApiKeyDescription" => v5::Code::InvalidApiKeyDescription,
            other => {
                log::warn!("Unknown error code {}", other);
                v5::Code::UnretrievableErrorCode
            }
        };
        v5::ResponseError::from_msg(error.message, code)
    }
}

impl<T> From<v4::Settings<T>> for v5::Settings<v5::Unchecked> {
    fn from(settings: v4::Settings<T>) -> Self {
        v5::Settings {
            displayed_attributes: settings.displayed_attributes.into(),
            searchable_attributes: settings.searchable_attributes.into(),
            filterable_attributes: settings.filterable_attributes.into(),
            sortable_attributes: settings.sortable_attributes.into(),
            ranking_rules: settings.ranking_rules.into(),
            stop_words: settings.stop_words.into(),
            synonyms: settings.synonyms.into(),
            distinct_attribute: settings.distinct_attribute.into(),
            typo_tolerance: match settings.typo_tolerance {
                v4::Setting::Set(typo) => v5::Setting::Set(v5::TypoTolerance {
                    enabled: typo.enabled.into(),
                    min_word_size_for_typos: match typo.min_word_size_for_typos {
                        v4::Setting::Set(t) => v5::Setting::Set(v5::MinWordSizeForTypos {
                            one_typo: t.one_typo.into(),
                            two_typos: t.two_typos.into(),
                        }),
                        v4::Setting::Reset => v5::Setting::Reset,
                        v4::Setting::NotSet => v5::Setting::NotSet,
                    },
                    disable_on_words: typo.disable_on_words.into(),
                    disable_on_attributes: typo.disable_on_attributes.into(),
                }),
                v4::Setting::Reset => v5::Setting::Reset,
                v4::Setting::NotSet => v5::Setting::NotSet,
            },
            faceting: v5::Setting::NotSet,
            pagination: v5::Setting::NotSet,
            _kind: std::marker::PhantomData,
        }
    }
}

impl From<v4::Action> for Option<v5::Action> {
    fn from(key: v4::Action) -> Self {
        match key {
            v4::Action::All => Some(v5::Action::All),
            v4::Action::Search => Some(v5::Action::Search),
            v4::Action::DocumentsAdd => Some(v5::Action::DocumentsAdd),
            v4::Action::DocumentsGet => Some(v5::Action::DocumentsGet),
            v4::Action::DocumentsDelete => Some(v5::Action::DocumentsDelete),
            v4::Action::IndexesAdd => Some(v5::Action::IndexesAdd),
            v4::Action::IndexesGet => Some(v5::Action::IndexesGet),
            v4::Action::IndexesUpdate => Some(v5::Action::IndexesUpdate),
            v4::Action::IndexesDelete => Some(v5::Action::IndexesDelete),
            v4::Action::TasksGet => Some(v5::Action::TasksGet),
            v4::Action::SettingsGet => Some(v5::Action::SettingsGet),
            v4::Action::SettingsUpdate => Some(v5::Action::SettingsUpdate),
            v4::Action::StatsGet => Some(v5::Action::StatsGet),
            v4::Action::DumpsCreate => Some(v5::Action::DumpsCreate),
            v4::Action::DumpsGet => None,
            v4::Action::Version => Some(v5::Action::Version),
        }
    }
}

#[cfg(test)]
pub(crate) mod test {
    use std::{fs::File, io::BufReader};

    use flate2::bufread::GzDecoder;
    use tempfile::TempDir;

    use super::*;

    #[test]
    fn compat_v4_v5() {
        let dump = File::open("tests/assets/v4.dump").unwrap();
        let dir = TempDir::new().unwrap();
        let mut dump = BufReader::new(dump);
        let gz = GzDecoder::new(&mut dump);
        let mut archive = tar::Archive::new(gz);
        archive.unpack(dir.path()).unwrap();

        let mut dump = v4::V4Reader::open(dir).unwrap().to_v5();

        // top level infos
        insta::assert_display_snapshot!(dump.date().unwrap(), @"2022-10-06 12:53:49.131989609 +00:00:00");
        insta::assert_display_snapshot!(dump.instance_uid().unwrap().unwrap(), @"9e15e977-f2ae-4761-943f-1eaf75fd736d");

        // tasks
        let tasks = dump.tasks().collect::<Result<Vec<_>>>().unwrap();
        let (tasks, update_files): (Vec<_>, Vec<_>) = tasks.into_iter().unzip();
        insta::assert_json_snapshot!(tasks);
        assert_eq!(update_files.len(), 10);
        assert!(update_files[0].is_some()); // the enqueued document addition
        assert!(update_files[1..].iter().all(|u| u.is_none())); // everything already processed

        // keys
        let keys = dump.keys().collect::<Result<Vec<_>>>().unwrap();
        insta::assert_json_snapshot!(keys, { "[].uid" => "[uuid]" });

        // indexes
        let mut indexes = dump.indexes().unwrap().collect::<Result<Vec<_>>>().unwrap();
        // the index are not ordered in any way by default
        indexes.sort_by_key(|index| index.metadata().uid.to_string());

        let mut products = indexes.pop().unwrap();
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

        insta::assert_debug_snapshot!(products.settings());
        let documents = products
            .documents()
            .unwrap()
            .collect::<Result<Vec<_>>>()
            .unwrap();
        assert_eq!(documents.len(), 10);
        insta::assert_json_snapshot!(documents);

        // movies
        insta::assert_json_snapshot!(movies.metadata(), { ".createdAt" => "[now]", ".updatedAt" => "[now]" }, @r###"
        {
          "uid": "movies",
          "primaryKey": "id",
          "createdAt": "[now]",
          "updatedAt": "[now]"
        }
        "###);

        insta::assert_debug_snapshot!(movies.settings());
        let documents = movies
            .documents()
            .unwrap()
            .collect::<Result<Vec<_>>>()
            .unwrap();
        assert_eq!(documents.len(), 110);
        insta::assert_debug_snapshot!(documents);

        // spells
        insta::assert_json_snapshot!(spells.metadata(), { ".createdAt" => "[now]", ".updatedAt" => "[now]" }, @r###"
        {
          "uid": "dnd_spells",
          "primaryKey": "index",
          "createdAt": "[now]",
          "updatedAt": "[now]"
        }
        "###);

        insta::assert_debug_snapshot!(spells.settings());
        let documents = spells
            .documents()
            .unwrap()
            .collect::<Result<Vec<_>>>()
            .unwrap();
        assert_eq!(documents.len(), 10);
        insta::assert_json_snapshot!(documents);
    }
}
