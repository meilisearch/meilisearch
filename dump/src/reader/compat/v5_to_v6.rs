use crate::reader::{v5, v6, Document, UpdateFile};
use crate::Result;

use super::v4_to_v5::{CompatIndexV4ToV5, CompatV4ToV5};

pub enum CompatV5ToV6 {
    V5(v5::V5Reader),
    Compat(CompatV4ToV5),
}

impl CompatV5ToV6 {
    pub fn new_v5(v5: v5::V5Reader) -> CompatV5ToV6 {
        CompatV5ToV6::V5(v5)
    }

    pub fn version(&self) -> crate::Version {
        match self {
            CompatV5ToV6::V5(v5) => v5.version(),
            CompatV5ToV6::Compat(compat) => compat.version(),
        }
    }

    pub fn date(&self) -> Option<time::OffsetDateTime> {
        match self {
            CompatV5ToV6::V5(v5) => v5.date(),
            CompatV5ToV6::Compat(compat) => compat.date(),
        }
    }

    pub fn instance_uid(&self) -> Result<Option<uuid::Uuid>> {
        match self {
            CompatV5ToV6::V5(v5) => v5.instance_uid(),
            CompatV5ToV6::Compat(compat) => compat.instance_uid(),
        }
    }

    pub fn indexes(&self) -> Result<Box<dyn Iterator<Item = Result<CompatIndexV5ToV6>> + '_>> {
        let indexes = match self {
            CompatV5ToV6::V5(v5) => Box::new(
                v5.indexes()?
                    .map(|index| index.map(CompatIndexV5ToV6::from)),
            )
                as Box<dyn Iterator<Item = Result<CompatIndexV5ToV6>> + '_>,

            CompatV5ToV6::Compat(compat) => Box::new(
                compat
                    .indexes()?
                    .map(|index| index.map(CompatIndexV5ToV6::from)),
            )
                as Box<dyn Iterator<Item = Result<CompatIndexV5ToV6>> + '_>,
        };
        Ok(indexes)
    }

    pub fn tasks(
        &mut self,
    ) -> Box<dyn Iterator<Item = Result<(v6::Task, Option<Box<UpdateFile>>)>> + '_> {
        let tasks = match self {
            CompatV5ToV6::V5(v5) => v5.tasks(),
            CompatV5ToV6::Compat(compat) => compat.tasks(),
        };
        Box::new(tasks.map(|task| {
            task.map(|(task, content_file)| {
                let task_view: v5::tasks::TaskView = task.clone().into();

                let task = v6::Task {
                    uid: task_view.uid,
                    index_uid: task_view.index_uid,
                    status: match task_view.status {
                        v5::Status::Enqueued => v6::Status::Enqueued,
                        v5::Status::Processing => v6::Status::Enqueued,
                        v5::Status::Succeeded => v6::Status::Succeeded,
                        v5::Status::Failed => v6::Status::Failed,
                    },
                    kind: match task.content.clone() {
                        v5::tasks::TaskContent::IndexCreation { primary_key, .. } => {
                            v6::Kind::IndexCreation { primary_key }
                        }
                        v5::tasks::TaskContent::IndexUpdate { primary_key, .. } => {
                            v6::Kind::IndexUpdate { primary_key }
                        }
                        v5::tasks::TaskContent::IndexDeletion { .. } => v6::Kind::IndexDeletion,
                        v5::tasks::TaskContent::DocumentAddition {
                            merge_strategy,
                            allow_index_creation,
                            primary_key,
                            documents_count,
                            ..
                        } => v6::Kind::DocumentImport {
                            primary_key,
                            documents_count: documents_count as u64,
                            method: match merge_strategy {
                                v5::tasks::IndexDocumentsMethod::ReplaceDocuments => {
                                    v6::milli::update::IndexDocumentsMethod::ReplaceDocuments
                                }
                                v5::tasks::IndexDocumentsMethod::UpdateDocuments => {
                                    v6::milli::update::IndexDocumentsMethod::UpdateDocuments
                                }
                            },
                            allow_index_creation: allow_index_creation.clone(),
                        },
                        v5::tasks::TaskContent::DocumentDeletion { deletion, .. } => match deletion
                        {
                            v5::tasks::DocumentDeletion::Clear => v6::Kind::DocumentClear,
                            v5::tasks::DocumentDeletion::Ids(documents_ids) => {
                                v6::Kind::DocumentDeletion { documents_ids }
                            }
                        },
                        v5::tasks::TaskContent::SettingsUpdate {
                            allow_index_creation,
                            is_deletion,
                            settings,
                            ..
                        } => v6::Kind::Settings {
                            is_deletion,
                            allow_index_creation,
                            settings: settings.into(),
                        },
                        v5::tasks::TaskContent::Dump { .. } => v6::Kind::DumpExport,
                    },
                    details: task_view.details.map(|details| match details {
                        v5::Details::DocumentAddition {
                            received_documents,
                            indexed_documents,
                        } => v6::Details::DocumentAddition {
                            received_documents: received_documents as u64,
                            indexed_documents: indexed_documents.map(|i| i as u64),
                        },
                        v5::Details::Settings { settings } => v6::Details::Settings {
                            settings: settings.into(),
                        },
                        v5::Details::IndexInfo { primary_key } => {
                            v6::Details::IndexInfo { primary_key }
                        }
                        v5::Details::DocumentDeletion {
                            received_document_ids,
                            deleted_documents,
                        } => v6::Details::DocumentDeletion {
                            received_document_ids,
                            deleted_documents,
                        },
                        v5::Details::ClearAll { deleted_documents } => {
                            v6::Details::ClearAll { deleted_documents }
                        }
                        v5::Details::Dump { dump_uid } => v6::Details::Dump { dump_uid },
                    }),
                    error: task_view.error.map(|e| e.into()),
                    enqueued_at: task_view.enqueued_at,
                    started_at: task_view.started_at,
                    finished_at: task_view.finished_at,
                };

                (task, content_file)
            })
        }))
    }

    pub fn keys(&mut self) -> Box<dyn Iterator<Item = Result<v6::Key>> + '_> {
        let keys = match self {
            CompatV5ToV6::V5(v5) => v5.keys(),
            CompatV5ToV6::Compat(compat) => compat.keys(),
        };
        Box::new(keys.map(|key| {
            key.map(|key| v6::Key {
                description: key.description,
                name: key.name,
                uid: key.uid,
                actions: key
                    .actions
                    .into_iter()
                    .map(|action| action.into())
                    .collect(),
                indexes: key
                    .indexes
                    .into_iter()
                    .map(|index| match index {
                        v5::StarOr::Star => v6::StarOr::Star,
                        v5::StarOr::Other(uid) => {
                            v6::StarOr::Other(v6::IndexUid::new_unchecked(uid.as_str()))
                        }
                    })
                    .collect(),
                expires_at: key.expires_at,
                created_at: key.created_at,
                updated_at: key.updated_at,
            })
        }))
    }
}

pub enum CompatIndexV5ToV6 {
    V5(v5::V5IndexReader),
    Compat(CompatIndexV4ToV5),
}

impl From<v5::V5IndexReader> for CompatIndexV5ToV6 {
    fn from(index_reader: v5::V5IndexReader) -> Self {
        Self::V5(index_reader)
    }
}

impl From<CompatIndexV4ToV5> for CompatIndexV5ToV6 {
    fn from(index_reader: CompatIndexV4ToV5) -> Self {
        Self::Compat(index_reader)
    }
}

impl CompatIndexV5ToV6 {
    pub fn new_v5(v5: v5::V5IndexReader) -> CompatIndexV5ToV6 {
        CompatIndexV5ToV6::V5(v5)
    }

    pub fn metadata(&self) -> &crate::IndexMetadata {
        match self {
            CompatIndexV5ToV6::V5(v5) => v5.metadata(),
            CompatIndexV5ToV6::Compat(compat) => compat.metadata(),
        }
    }

    pub fn documents(&mut self) -> Result<Box<dyn Iterator<Item = Result<Document>> + '_>> {
        match self {
            CompatIndexV5ToV6::V5(v5) => v5
                .documents()
                .map(|iter| Box::new(iter) as Box<dyn Iterator<Item = Result<Document>> + '_>),
            CompatIndexV5ToV6::Compat(compat) => compat
                .documents()
                .map(|iter| Box::new(iter) as Box<dyn Iterator<Item = Result<Document>> + '_>),
        }
    }

    pub fn settings(&mut self) -> Result<v6::Settings<v6::Checked>> {
        match self {
            CompatIndexV5ToV6::V5(v5) => Ok(v6::Settings::from(v5.settings()?).check()),
            CompatIndexV5ToV6::Compat(compat) => Ok(v6::Settings::from(compat.settings()?).check()),
        }
    }
}

impl<T> From<v5::Setting<T>> for v6::Setting<T> {
    fn from(setting: v5::Setting<T>) -> Self {
        match setting {
            v5::Setting::Set(t) => v6::Setting::Set(t),
            v5::Setting::Reset => v6::Setting::Reset,
            v5::Setting::NotSet => v6::Setting::NotSet,
        }
    }
}

impl From<v5::ResponseError> for v6::ResponseError {
    fn from(error: v5::ResponseError) -> Self {
        let code = match error.error_code.as_ref() {
            "CreateIndex" => v6::Code::CreateIndex,
            "IndexAlreadyExists" => v6::Code::IndexAlreadyExists,
            "IndexNotFound" => v6::Code::IndexNotFound,
            "InvalidIndexUid" => v6::Code::InvalidIndexUid,
            "InvalidMinWordLengthForTypo" => v6::Code::InvalidMinWordLengthForTypo,
            "InvalidState" => v6::Code::InvalidState,
            "MissingPrimaryKey" => v6::Code::MissingPrimaryKey,
            "PrimaryKeyAlreadyPresent" => v6::Code::PrimaryKeyAlreadyPresent,
            "MaxFieldsLimitExceeded" => v6::Code::MaxFieldsLimitExceeded,
            "MissingDocumentId" => v6::Code::MissingDocumentId,
            "InvalidDocumentId" => v6::Code::InvalidDocumentId,
            "Filter" => v6::Code::Filter,
            "Sort" => v6::Code::Sort,
            "BadParameter" => v6::Code::BadParameter,
            "BadRequest" => v6::Code::BadRequest,
            "DatabaseSizeLimitReached" => v6::Code::DatabaseSizeLimitReached,
            "DocumentNotFound" => v6::Code::DocumentNotFound,
            "Internal" => v6::Code::Internal,
            "InvalidGeoField" => v6::Code::InvalidGeoField,
            "InvalidRankingRule" => v6::Code::InvalidRankingRule,
            "InvalidStore" => v6::Code::InvalidStore,
            "InvalidToken" => v6::Code::InvalidToken,
            "MissingAuthorizationHeader" => v6::Code::MissingAuthorizationHeader,
            "NoSpaceLeftOnDevice" => v6::Code::NoSpaceLeftOnDevice,
            "DumpNotFound" => v6::Code::DumpNotFound,
            "TaskNotFound" => v6::Code::TaskNotFound,
            "PayloadTooLarge" => v6::Code::PayloadTooLarge,
            "RetrieveDocument" => v6::Code::RetrieveDocument,
            "SearchDocuments" => v6::Code::SearchDocuments,
            "UnsupportedMediaType" => v6::Code::UnsupportedMediaType,
            "DumpAlreadyInProgress" => v6::Code::DumpAlreadyInProgress,
            "DumpProcessFailed" => v6::Code::DumpProcessFailed,
            "InvalidContentType" => v6::Code::InvalidContentType,
            "MissingContentType" => v6::Code::MissingContentType,
            "MalformedPayload" => v6::Code::MalformedPayload,
            "MissingPayload" => v6::Code::MissingPayload,
            "ApiKeyNotFound" => v6::Code::ApiKeyNotFound,
            "MissingParameter" => v6::Code::MissingParameter,
            "InvalidApiKeyActions" => v6::Code::InvalidApiKeyActions,
            "InvalidApiKeyIndexes" => v6::Code::InvalidApiKeyIndexes,
            "InvalidApiKeyExpiresAt" => v6::Code::InvalidApiKeyExpiresAt,
            "InvalidApiKeyDescription" => v6::Code::InvalidApiKeyDescription,
            "InvalidApiKeyName" => v6::Code::InvalidApiKeyName,
            "InvalidApiKeyUid" => v6::Code::InvalidApiKeyUid,
            "ImmutableField" => v6::Code::ImmutableField,
            "ApiKeyAlreadyExists" => v6::Code::ApiKeyAlreadyExists,
            other => {
                log::warn!("Unknown error code {}", other);
                v6::Code::UnretrievableErrorCode
            }
        };
        v6::ResponseError::from_msg(error.message, code)
    }
}

impl<T> From<v5::Settings<T>> for v6::Settings<v6::Unchecked> {
    fn from(settings: v5::Settings<T>) -> Self {
        v6::Settings {
            displayed_attributes: settings.displayed_attributes.into(),
            searchable_attributes: settings.searchable_attributes.into(),
            filterable_attributes: settings.filterable_attributes.into(),
            sortable_attributes: settings.sortable_attributes.into(),
            ranking_rules: settings.ranking_rules.into(),
            stop_words: settings.stop_words.into(),
            synonyms: settings.synonyms.into(),
            distinct_attribute: settings.distinct_attribute.into(),
            typo_tolerance: match settings.typo_tolerance {
                v5::Setting::Set(typo) => v6::Setting::Set(v6::TypoTolerance {
                    enabled: typo.enabled.into(),
                    min_word_size_for_typos: match typo.min_word_size_for_typos {
                        v5::Setting::Set(t) => v6::Setting::Set(v6::MinWordSizeForTypos {
                            one_typo: t.one_typo.into(),
                            two_typos: t.two_typos.into(),
                        }),
                        v5::Setting::Reset => v6::Setting::Reset,
                        v5::Setting::NotSet => v6::Setting::NotSet,
                    },
                    disable_on_words: typo.disable_on_words.into(),
                    disable_on_attributes: typo.disable_on_attributes.into(),
                }),
                v5::Setting::Reset => v6::Setting::Reset,
                v5::Setting::NotSet => v6::Setting::NotSet,
            },
            faceting: match settings.faceting {
                v5::Setting::Set(faceting) => v6::Setting::Set(v6::FacetingSettings {
                    max_values_per_facet: faceting.max_values_per_facet.into(),
                }),
                v5::Setting::Reset => v6::Setting::Reset,
                v5::Setting::NotSet => v6::Setting::NotSet,
            },
            pagination: match settings.pagination {
                v5::Setting::Set(pagination) => v6::Setting::Set(v6::PaginationSettings {
                    max_total_hits: pagination.max_total_hits.into(),
                }),
                v5::Setting::Reset => v6::Setting::Reset,
                v5::Setting::NotSet => v6::Setting::NotSet,
            },
            _kind: std::marker::PhantomData,
        }
    }
}

impl From<v5::Action> for v6::Action {
    fn from(key: v5::Action) -> Self {
        match key {
            v5::Action::All => v6::Action::All,
            v5::Action::Search => v6::Action::Search,
            v5::Action::DocumentsAll => v6::Action::DocumentsAll,
            v5::Action::DocumentsAdd => v6::Action::DocumentsAdd,
            v5::Action::DocumentsGet => v6::Action::DocumentsGet,
            v5::Action::DocumentsDelete => v6::Action::DocumentsDelete,
            v5::Action::IndexesAll => v6::Action::IndexesAll,
            v5::Action::IndexesAdd => v6::Action::IndexesAdd,
            v5::Action::IndexesGet => v6::Action::IndexesGet,
            v5::Action::IndexesUpdate => v6::Action::IndexesUpdate,
            v5::Action::IndexesDelete => v6::Action::IndexesDelete,
            v5::Action::TasksAll => v6::Action::TasksAll,
            v5::Action::TasksGet => v6::Action::TasksGet,
            v5::Action::SettingsAll => v6::Action::SettingsAll,
            v5::Action::SettingsGet => v6::Action::SettingsGet,
            v5::Action::SettingsUpdate => v6::Action::SettingsUpdate,
            v5::Action::StatsAll => v6::Action::StatsAll,
            v5::Action::StatsGet => v6::Action::StatsGet,
            v5::Action::MetricsAll => v6::Action::MetricsAll,
            v5::Action::MetricsGet => v6::Action::MetricsGet,
            v5::Action::DumpsAll => v6::Action::DumpsAll,
            v5::Action::DumpsCreate => v6::Action::DumpsCreate,
            v5::Action::Version => v6::Action::Version,
            v5::Action::KeysAdd => v6::Action::KeysAdd,
            v5::Action::KeysGet => v6::Action::KeysGet,
            v5::Action::KeysUpdate => v6::Action::KeysUpdate,
            v5::Action::KeysDelete => v6::Action::KeysDelete,
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
    fn compat_v5_v6() {
        let dump = File::open("tests/assets/v5.dump").unwrap();
        let dir = TempDir::new().unwrap();
        let mut dump = BufReader::new(dump);
        let gz = GzDecoder::new(&mut dump);
        let mut archive = tar::Archive::new(gz);
        archive.unpack(dir.path()).unwrap();

        let mut dump = v5::V5Reader::open(dir).unwrap().to_v6();

        // top level infos
        insta::assert_display_snapshot!(dump.date().unwrap(), @"2022-10-04 15:55:10.344982459 +00:00:00");
        insta::assert_display_snapshot!(dump.instance_uid().unwrap().unwrap(), @"9e15e977-f2ae-4761-943f-1eaf75fd736d");

        // tasks
        let tasks = dump.tasks().collect::<Result<Vec<_>>>().unwrap();
        let (tasks, update_files): (Vec<_>, Vec<_>) = tasks.into_iter().unzip();
        meili_snap::snapshot_hash!(meili_snap::json_string!(tasks), @"0fff3c32487e3d3058d51ed951c1057f");
        assert_eq!(update_files.len(), 22);
        assert!(update_files[0].is_none()); // the dump creation
        assert!(update_files[1].is_some()); // the enqueued document addition
        assert!(update_files[2..].iter().all(|u| u.is_none())); // everything already processed

        // keys
        let keys = dump.keys().collect::<Result<Vec<_>>>().unwrap();
        meili_snap::snapshot_hash!(meili_snap::json_string!(keys), @"c9d2b467fe2fca0b35580d8a999808fb");

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

        meili_snap::snapshot_hash!(format!("{:#?}", products.settings()), @"8e5cadabf74aebe1160bf51c3d489efe");
        let documents = products
            .documents()
            .unwrap()
            .collect::<Result<Vec<_>>>()
            .unwrap();
        assert_eq!(documents.len(), 10);
        meili_snap::snapshot_hash!(format!("{:#?}", documents), @"b01c8371aea4c7171af0d4d846a2bdca");

        // movies
        insta::assert_json_snapshot!(movies.metadata(), { ".createdAt" => "[now]", ".updatedAt" => "[now]" }, @r###"
        {
          "uid": "movies",
          "primaryKey": "id",
          "createdAt": "[now]",
          "updatedAt": "[now]"
        }
        "###);

        meili_snap::snapshot_hash!(format!("{:#?}", movies.settings()), @"4894ac1e74b9e1069ed5ee262b7a1aca");
        let documents = movies
            .documents()
            .unwrap()
            .collect::<Result<Vec<_>>>()
            .unwrap();
        assert_eq!(documents.len(), 200);
        meili_snap::snapshot_hash!(format!("{:#?}", documents), @"e962baafd2fbae4cdd14e876053b0c5a");

        // spells
        insta::assert_json_snapshot!(spells.metadata(), { ".createdAt" => "[now]", ".updatedAt" => "[now]" }, @r###"
        {
          "uid": "dnd_spells",
          "primaryKey": "index",
          "createdAt": "[now]",
          "updatedAt": "[now]"
        }
        "###);

        meili_snap::snapshot_hash!(format!("{:#?}", spells.settings()), @"054dbf08a79e08bb9becba6f5d090f13");
        let documents = spells
            .documents()
            .unwrap()
            .collect::<Result<Vec<_>>>()
            .unwrap();
        assert_eq!(documents.len(), 10);
        meili_snap::snapshot_hash!(format!("{:#?}", documents), @"235016433dd04262c7f2da01d1e808ce");
    }
}
