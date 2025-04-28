use std::str::FromStr;

use super::v4_to_v5::{CompatIndexV4ToV5, CompatV4ToV5};
use crate::reader::{v5, v6, Document, UpdateFile};
use crate::Result;

pub enum CompatV5ToV6 {
    V5(v5::V5Reader),
    Compat(CompatV4ToV5),
}

impl CompatV5ToV6 {
    pub fn new_v5(v5: v5::V5Reader) -> Self {
        Self::V5(v5)
    }

    pub fn version(&self) -> crate::Version {
        match self {
            Self::V5(v5) => v5.version(),
            Self::Compat(compat) => compat.version(),
        }
    }

    pub fn date(&self) -> Option<time::OffsetDateTime> {
        match self {
            Self::V5(v5) => v5.date(),
            Self::Compat(compat) => compat.date(),
        }
    }

    pub fn instance_uid(&self) -> Result<Option<uuid::Uuid>> {
        match self {
            Self::V5(v5) => v5.instance_uid(),
            Self::Compat(compat) => compat.instance_uid(),
        }
    }

    pub fn indexes(&self) -> Result<Box<dyn Iterator<Item = Result<CompatIndexV5ToV6>> + '_>> {
        let indexes = match self {
            Self::V5(v5) => Box::new(v5.indexes()?.map(|index| index.map(CompatIndexV5ToV6::from)))
                as Box<dyn Iterator<Item = Result<CompatIndexV5ToV6>> + '_>,

            Self::Compat(compat) => {
                Box::new(compat.indexes()?.map(|index| index.map(CompatIndexV5ToV6::from)))
                    as Box<dyn Iterator<Item = Result<CompatIndexV5ToV6>> + '_>
            }
        };
        Ok(indexes)
    }

    pub fn tasks(
        &mut self,
    ) -> Result<Box<dyn Iterator<Item = Result<(v6::Task, Option<Box<UpdateFile>>)>> + '_>> {
        let instance_uid = self.instance_uid().ok().flatten();
        let keys = self.keys()?.collect::<Result<Vec<_>>>()?;

        let tasks = match self {
            Self::V5(v5) => v5.tasks(),
            Self::Compat(compat) => compat.tasks(),
        };
        Ok(Box::new(tasks.map(move |task| {
            task.map(|(task, content_file)| {
                let mut task_view: v5::tasks::TaskView = task.clone().into();

                if task_view.status == v5::Status::Processing {
                    task_view.started_at = None;
                }

                let task = v6::Task {
                    uid: task_view.uid,
                    batch_uid: None,
                    index_uid: task_view.index_uid,
                    status: match task_view.status {
                        v5::Status::Enqueued => v6::Status::Enqueued,
                        v5::Status::Processing => v6::Status::Enqueued,
                        v5::Status::Succeeded => v6::Status::Succeeded,
                        v5::Status::Failed => v6::Status::Failed,
                    },
                    kind: match task.content {
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
                            allow_index_creation,
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
                            settings: Box::new(settings.into()),
                        },
                        v5::tasks::TaskContent::Dump { uid: _ } => {
                            // in v6 we compute the dump_uid from the started_at processing time
                            v6::Kind::DumpCreation { keys: keys.clone(), instance_uid }
                        }
                    },
                    canceled_by: None,
                    details: task_view.details.map(|details| match details {
                        v5::Details::DocumentAddition { received_documents, indexed_documents } => {
                            v6::Details::DocumentAdditionOrUpdate {
                                received_documents: received_documents as u64,
                                indexed_documents,
                            }
                        }
                        v5::Details::Settings { settings } => {
                            v6::Details::SettingsUpdate { settings: Box::new(settings.into()) }
                        }
                        v5::Details::IndexInfo { primary_key } => {
                            v6::Details::IndexInfo { primary_key }
                        }
                        v5::Details::DocumentDeletion {
                            received_document_ids,
                            deleted_documents,
                        } => v6::Details::DocumentDeletion {
                            provided_ids: received_document_ids,
                            deleted_documents,
                        },
                        v5::Details::ClearAll { deleted_documents } => {
                            v6::Details::ClearAll { deleted_documents }
                        }
                        v5::Details::Dump { dump_uid } => {
                            v6::Details::Dump { dump_uid: Some(dump_uid) }
                        }
                    }),
                    error: task_view.error.map(|e| e.into()),
                    enqueued_at: task_view.enqueued_at,
                    started_at: task_view.started_at,
                    finished_at: task_view.finished_at,
                };

                (task, content_file)
            })
        })))
    }

    pub fn keys(&mut self) -> Result<Box<dyn Iterator<Item = Result<v6::Key>> + '_>> {
        let keys = match self {
            Self::V5(v5) => v5.keys()?,
            Self::Compat(compat) => compat.keys(),
        };

        Ok(Box::new(keys.map(|key| {
            key.map(|key| v6::Key {
                description: key.description,
                name: key.name,
                uid: key.uid,
                actions: key.actions.into_iter().map(|action| action.into()).collect(),
                indexes: key
                    .indexes
                    .into_iter()
                    .map(|index| match index {
                        v5::StarOr::Star => v6::IndexUidPattern::all(),
                        v5::StarOr::Other(uid) => v6::IndexUidPattern::new_unchecked(uid.as_str()),
                    })
                    .collect(),
                expires_at: key.expires_at,
                created_at: key.created_at,
                updated_at: key.updated_at,
            })
        })))
    }

    pub fn features(&self) -> Result<Option<v6::RuntimeTogglableFeatures>> {
        Ok(None)
    }

    pub fn network(&self) -> Result<Option<&v6::Network>> {
        Ok(None)
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
    pub fn new_v5(v5: v5::V5IndexReader) -> Self {
        Self::V5(v5)
    }

    pub fn metadata(&self) -> &crate::IndexMetadata {
        match self {
            Self::V5(v5) => v5.metadata(),
            Self::Compat(compat) => compat.metadata(),
        }
    }

    pub fn documents(&mut self) -> Result<Box<dyn Iterator<Item = Result<Document>> + '_>> {
        match self {
            Self::V5(v5) => v5
                .documents()
                .map(|iter| Box::new(iter) as Box<dyn Iterator<Item = Result<Document>> + '_>),
            Self::Compat(compat) => compat
                .documents()
                .map(|iter| Box::new(iter) as Box<dyn Iterator<Item = Result<Document>> + '_>),
        }
    }

    pub fn settings(&mut self) -> Result<v6::Settings<v6::Checked>> {
        match self {
            Self::V5(v5) => Ok(v6::Settings::from(v5.settings()?).check()),
            Self::Compat(compat) => Ok(v6::Settings::from(compat.settings()?).check()),
        }
    }
}

impl<T> From<v5::Setting<T>> for v6::Setting<T> {
    fn from(setting: v5::Setting<T>) -> Self {
        match setting {
            v5::Setting::Set(t) => Self::Set(t),
            v5::Setting::Reset => Self::Reset,
            v5::Setting::NotSet => Self::NotSet,
        }
    }
}

impl From<v5::ResponseError> for v6::ResponseError {
    fn from(error: v5::ResponseError) -> Self {
        let code = match error.error_code.as_ref() {
            "index_creation_failed" => v6::Code::IndexCreationFailed,
            "index_already_exists" => v6::Code::IndexAlreadyExists,
            "index_not_found" => v6::Code::IndexNotFound,
            "invalid_index_uid" => v6::Code::InvalidIndexUid,
            "invalid_min_word_length_for_typo" => v6::Code::InvalidSettingsTypoTolerance,
            "invalid_state" => v6::Code::InvalidState,
            "primary_key_inference_failed" => v6::Code::IndexPrimaryKeyNoCandidateFound,
            "index_primary_key_already_exists" => v6::Code::IndexPrimaryKeyAlreadyExists,
            "max_fields_limit_exceeded" => v6::Code::MaxFieldsLimitExceeded,
            "missing_document_id" => v6::Code::MissingDocumentId,
            "invalid_document_id" => v6::Code::InvalidDocumentId,
            "invalid_filter" => v6::Code::InvalidSettingsFilterableAttributes,
            "invalid_sort" => v6::Code::InvalidSettingsSortableAttributes,
            "bad_parameter" => v6::Code::BadParameter,
            "bad_request" => v6::Code::BadRequest,
            "database_size_limit_reached" => v6::Code::DatabaseSizeLimitReached,
            "document_not_found" => v6::Code::DocumentNotFound,
            "internal" => v6::Code::Internal,
            "invalid_geo_field" => v6::Code::InvalidDocumentGeoField,
            "invalid_ranking_rule" => v6::Code::InvalidSettingsRankingRules,
            "invalid_store_file" => v6::Code::InvalidStoreFile,
            "invalid_api_key" => v6::Code::InvalidApiKey,
            "missing_authorization_header" => v6::Code::MissingAuthorizationHeader,
            "no_space_left_on_device" => v6::Code::NoSpaceLeftOnDevice,
            "dump_not_found" => v6::Code::DumpNotFound,
            "task_not_found" => v6::Code::TaskNotFound,
            "payload_too_large" => v6::Code::PayloadTooLarge,
            "unretrievable_document" => v6::Code::UnretrievableDocument,
            "unsupported_media_type" => v6::Code::UnsupportedMediaType,
            "dump_already_processing" => v6::Code::DumpAlreadyProcessing,
            "dump_process_failed" => v6::Code::DumpProcessFailed,
            "invalid_content_type" => v6::Code::InvalidContentType,
            "missing_content_type" => v6::Code::MissingContentType,
            "malformed_payload" => v6::Code::MalformedPayload,
            "missing_payload" => v6::Code::MissingPayload,
            "api_key_not_found" => v6::Code::ApiKeyNotFound,
            "missing_parameter" => v6::Code::BadRequest,
            "invalid_api_key_actions" => v6::Code::InvalidApiKeyActions,
            "invalid_api_key_indexes" => v6::Code::InvalidApiKeyIndexes,
            "invalid_api_key_expires_at" => v6::Code::InvalidApiKeyExpiresAt,
            "invalid_api_key_description" => v6::Code::InvalidApiKeyDescription,
            "invalid_api_key_name" => v6::Code::InvalidApiKeyName,
            "invalid_api_key_uid" => v6::Code::InvalidApiKeyUid,
            "immutable_field" => v6::Code::BadRequest,
            "api_key_already_exists" => v6::Code::ApiKeyAlreadyExists,
            other => {
                tracing::warn!("Unknown error code {}", other);
                v6::Code::UnretrievableErrorCode
            }
        };
        Self::from_msg(error.message, code)
    }
}

impl<T> From<v5::Settings<T>> for v6::Settings<v6::Unchecked> {
    fn from(settings: v5::Settings<T>) -> Self {
        Self {
            displayed_attributes: v6::Setting::from(settings.displayed_attributes).into(),
            searchable_attributes: v6::Setting::from(settings.searchable_attributes).into(),
            filterable_attributes: match settings.filterable_attributes {
                v5::settings::Setting::Set(filterable_attributes) => v6::Setting::Set(
                    filterable_attributes
                        .into_iter()
                        .map(v6::FilterableAttributesRule::Field)
                        .collect(),
                ),
                v5::settings::Setting::Reset => v6::Setting::Reset,
                v5::settings::Setting::NotSet => v6::Setting::NotSet,
            },
            sortable_attributes: settings.sortable_attributes.into(),
            ranking_rules: {
                match settings.ranking_rules {
                    v5::settings::Setting::Set(ranking_rules) => {
                        let mut new_ranking_rules = vec![];
                        for rule in ranking_rules {
                            match v6::RankingRuleView::from_str(&rule) {
                                Ok(new_rule) => {
                                    new_ranking_rules.push(new_rule);
                                }
                                Err(_) => {
                                    tracing::warn!("Error while importing settings. The ranking rule `{rule}` does not exist anymore.")
                                }
                            }
                        }
                        v6::Setting::Set(new_ranking_rules)
                    }
                    v5::settings::Setting::Reset => v6::Setting::Reset,
                    v5::settings::Setting::NotSet => v6::Setting::NotSet,
                }
            },
            stop_words: settings.stop_words.into(),
            non_separator_tokens: v6::Setting::NotSet,
            separator_tokens: v6::Setting::NotSet,
            dictionary: v6::Setting::NotSet,
            synonyms: settings.synonyms.into(),
            distinct_attribute: settings.distinct_attribute.into(),
            proximity_precision: v6::Setting::NotSet,
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
                    sort_facet_values_by: v6::Setting::NotSet,
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
            embedders: v6::Setting::NotSet,
            localized_attributes: v6::Setting::NotSet,
            search_cutoff_ms: v6::Setting::NotSet,
            facet_search: v6::Setting::NotSet,
            prefix_search: v6::Setting::NotSet,
            _kind: std::marker::PhantomData,
        }
    }
}

impl From<v5::Action> for v6::Action {
    fn from(key: v5::Action) -> Self {
        match key {
            v5::Action::All => Self::All,
            v5::Action::Search => Self::Search,
            v5::Action::DocumentsAll => Self::DocumentsAll,
            v5::Action::DocumentsAdd => Self::DocumentsAdd,
            v5::Action::DocumentsGet => Self::DocumentsGet,
            v5::Action::DocumentsDelete => Self::DocumentsDelete,
            v5::Action::IndexesAll => Self::IndexesAll,
            v5::Action::IndexesAdd => Self::IndexesAdd,
            v5::Action::IndexesGet => Self::IndexesGet,
            v5::Action::IndexesUpdate => Self::IndexesUpdate,
            v5::Action::IndexesDelete => Self::IndexesDelete,
            v5::Action::TasksAll => Self::TasksAll,
            v5::Action::TasksGet => Self::TasksGet,
            v5::Action::SettingsAll => Self::SettingsAll,
            v5::Action::SettingsGet => Self::SettingsGet,
            v5::Action::SettingsUpdate => Self::SettingsUpdate,
            v5::Action::StatsAll => Self::StatsAll,
            v5::Action::StatsGet => Self::StatsGet,
            v5::Action::MetricsAll => Self::MetricsAll,
            v5::Action::MetricsGet => Self::MetricsGet,
            v5::Action::DumpsAll => Self::DumpsAll,
            v5::Action::DumpsCreate => Self::DumpsCreate,
            v5::Action::Version => Self::Version,
            v5::Action::KeysAdd => Self::KeysAdd,
            v5::Action::KeysGet => Self::KeysGet,
            v5::Action::KeysUpdate => Self::KeysUpdate,
            v5::Action::KeysDelete => Self::KeysDelete,
        }
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
    fn compat_v5_v6() {
        let dump = File::open("tests/assets/v5.dump").unwrap();
        let dir = TempDir::new().unwrap();
        let mut dump = BufReader::new(dump);
        let gz = GzDecoder::new(&mut dump);
        let mut archive = tar::Archive::new(gz);
        archive.unpack(dir.path()).unwrap();

        let mut dump = v5::V5Reader::open(dir).unwrap().to_v6();

        // top level infos
        insta::assert_snapshot!(dump.date().unwrap(), @"2022-10-04 15:55:10.344982459 +00:00:00");
        insta::assert_snapshot!(dump.instance_uid().unwrap().unwrap(), @"9e15e977-f2ae-4761-943f-1eaf75fd736d");

        // tasks
        let tasks = dump.tasks().unwrap().collect::<Result<Vec<_>>>().unwrap();
        let (tasks, update_files): (Vec<_>, Vec<_>) = tasks.into_iter().unzip();
        meili_snap::snapshot_hash!(meili_snap::json_string!(tasks), @"4b03e23e740b27bfb9d2a1faffe512e2");
        assert_eq!(update_files.len(), 22);
        assert!(update_files[0].is_none()); // the dump creation
        assert!(update_files[1].is_some()); // the enqueued document addition
        assert!(update_files[2..].iter().all(|u| u.is_none())); // everything already processed

        // keys
        let keys = dump.keys().unwrap().collect::<Result<Vec<_>>>().unwrap();
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

        insta::assert_json_snapshot!(products.settings().unwrap());
        let documents = products.documents().unwrap().collect::<Result<Vec<_>>>().unwrap();
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

        insta::assert_json_snapshot!(movies.settings().unwrap());
        let documents = movies.documents().unwrap().collect::<Result<Vec<_>>>().unwrap();
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

        insta::assert_json_snapshot!(spells.settings().unwrap());
        let documents = spells.documents().unwrap().collect::<Result<Vec<_>>>().unwrap();
        assert_eq!(documents.len(), 10);
        meili_snap::snapshot_hash!(format!("{:#?}", documents), @"235016433dd04262c7f2da01d1e808ce");
    }
}
