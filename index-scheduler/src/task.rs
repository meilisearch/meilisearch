use anyhow::Result;
use meilisearch_types::error::ResponseError;
use meilisearch_types::milli::update::IndexDocumentsMethod;
use meilisearch_types::settings::{Settings, Unchecked};

use meilisearch_types::tasks::{Details, Kind, Status, TaskView};
use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use time::OffsetDateTime;
use uuid::Uuid;

use crate::TaskId;

#[derive(Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Task {
    pub uid: TaskId,

    #[serde(with = "time::serde::rfc3339")]
    pub enqueued_at: OffsetDateTime,
    #[serde(with = "time::serde::rfc3339::option")]
    pub started_at: Option<OffsetDateTime>,
    #[serde(with = "time::serde::rfc3339::option")]
    pub finished_at: Option<OffsetDateTime>,

    pub error: Option<ResponseError>,
    pub details: Option<Details>,

    pub status: Status,
    pub kind: KindWithContent,
}

impl Task {
    /// Persist all the temp files associated with the task.
    pub fn persist(&self) -> Result<()> {
        self.kind.persist()
    }

    /// Delete all the files associated with the task.
    pub fn remove_data(&self) -> Result<()> {
        self.kind.remove_data()
    }

    /// Return the list of indexes updated by this tasks.
    pub fn indexes(&self) -> Option<Vec<&str>> {
        self.kind.indexes()
    }

    /// Convert a Task to a TaskView
    pub fn as_task_view(&self) -> TaskView {
        TaskView {
            uid: self.uid,
            index_uid: self
                .indexes()
                .and_then(|vec| vec.first().map(|i| i.to_string())),
            status: self.status,
            kind: self.kind.as_kind(),
            details: self.details.as_ref().map(Details::as_details_view),
            error: self.error.clone(),
            duration: self
                .started_at
                .zip(self.finished_at)
                .map(|(start, end)| end - start),
            enqueued_at: self.enqueued_at,
            started_at: self.started_at,
            finished_at: self.finished_at,
        }
    }
}

#[derive(Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum KindWithContent {
    DocumentImport {
        index_uid: String,
        primary_key: Option<String>,
        method: IndexDocumentsMethod,
        content_file: Uuid,
        documents_count: u64,
        allow_index_creation: bool,
    },
    DocumentDeletion {
        index_uid: String,
        documents_ids: Vec<String>,
    },
    DocumentClear {
        index_uid: String,
    },
    Settings {
        index_uid: String,
        new_settings: Settings<Unchecked>,
        is_deletion: bool,
        allow_index_creation: bool,
    },
    IndexDeletion {
        index_uid: String,
    },
    IndexCreation {
        index_uid: String,
        primary_key: Option<String>,
    },
    IndexUpdate {
        index_uid: String,
        primary_key: Option<String>,
    },
    IndexSwap {
        lhs: String,
        rhs: String,
    },
    CancelTask {
        tasks: Vec<TaskId>,
    },
    DeleteTasks {
        query: String,
        tasks: Vec<TaskId>,
    },
    DumpExport {
        output: PathBuf,
    },
    Snapshot,
}

impl KindWithContent {
    pub fn as_kind(&self) -> Kind {
        match self {
            KindWithContent::DocumentImport {
                method,
                allow_index_creation,
                ..
            } => Kind::DocumentImport {
                method: *method,
                allow_index_creation: *allow_index_creation,
            },
            KindWithContent::DocumentDeletion { .. } => Kind::DocumentDeletion,
            KindWithContent::DocumentClear { .. } => Kind::DocumentClear,
            KindWithContent::Settings {
                allow_index_creation,
                ..
            } => Kind::Settings {
                allow_index_creation: *allow_index_creation,
            },
            KindWithContent::IndexCreation { .. } => Kind::IndexCreation,
            KindWithContent::IndexDeletion { .. } => Kind::IndexDeletion,
            KindWithContent::IndexUpdate { .. } => Kind::IndexUpdate,
            KindWithContent::IndexSwap { .. } => Kind::IndexSwap,
            KindWithContent::CancelTask { .. } => Kind::CancelTask,
            KindWithContent::DeleteTasks { .. } => Kind::DeleteTasks,
            KindWithContent::DumpExport { .. } => Kind::DumpExport,
            KindWithContent::Snapshot => Kind::Snapshot,
        }
    }

    pub fn persist(&self) -> Result<()> {
        use KindWithContent::*;

        match self {
            DocumentImport { .. } => {
                // TODO: TAMO: persist the file
                // content_file.persist();
                Ok(())
            }
            DocumentDeletion { .. }
            | DocumentClear { .. }
            | Settings { .. }
            | IndexCreation { .. }
            | IndexDeletion { .. }
            | IndexUpdate { .. }
            | IndexSwap { .. }
            | CancelTask { .. }
            | DeleteTasks { .. }
            | DumpExport { .. }
            | Snapshot => Ok(()), // There is nothing to persist for all these tasks
        }
    }

    pub fn remove_data(&self) -> Result<()> {
        use KindWithContent::*;

        match self {
            DocumentImport { .. } => {
                // TODO: TAMO: delete the file
                // content_file.delete();
                Ok(())
            }
            IndexCreation { .. }
            | DocumentDeletion { .. }
            | DocumentClear { .. }
            | Settings { .. }
            | IndexDeletion { .. }
            | IndexUpdate { .. }
            | IndexSwap { .. }
            | CancelTask { .. }
            | DeleteTasks { .. }
            | DumpExport { .. }
            | Snapshot => Ok(()), // There is no data associated with all these tasks
        }
    }

    pub fn indexes(&self) -> Option<Vec<&str>> {
        use KindWithContent::*;

        match self {
            DumpExport { .. } | Snapshot | CancelTask { .. } | DeleteTasks { .. } => None,
            DocumentImport { index_uid, .. }
            | DocumentDeletion { index_uid, .. }
            | DocumentClear { index_uid }
            | Settings { index_uid, .. }
            | IndexCreation { index_uid, .. }
            | IndexUpdate { index_uid, .. }
            | IndexDeletion { index_uid } => Some(vec![index_uid]),
            IndexSwap { lhs, rhs } => Some(vec![lhs, rhs]),
        }
    }

    /// Returns the default `Details` that correspond to this `KindWithContent`,
    /// `None` if it cannot be generated.
    pub fn default_details(&self) -> Option<Details> {
        match self {
            KindWithContent::DocumentImport {
                documents_count, ..
            } => Some(Details::DocumentAddition {
                received_documents: *documents_count,
                indexed_documents: 0,
            }),
            KindWithContent::DocumentDeletion {
                index_uid: _,
                documents_ids,
            } => Some(Details::DocumentDeletion {
                received_document_ids: documents_ids.len(),
                deleted_documents: None,
            }),
            KindWithContent::DocumentClear { .. } => Some(Details::ClearAll {
                deleted_documents: None,
            }),
            KindWithContent::Settings { new_settings, .. } => Some(Details::Settings {
                settings: new_settings.clone(),
            }),
            KindWithContent::IndexDeletion { .. } => None,
            KindWithContent::IndexCreation { primary_key, .. }
            | KindWithContent::IndexUpdate { primary_key, .. } => Some(Details::IndexInfo {
                primary_key: primary_key.clone(),
            }),
            KindWithContent::IndexSwap { .. } => {
                todo!()
            }
            KindWithContent::CancelTask { .. } => {
                None // TODO: check correctness of this return value
            }
            KindWithContent::DeleteTasks { query, tasks } => Some(Details::DeleteTasks {
                matched_tasks: tasks.len(),
                deleted_tasks: None,
                original_query: query.clone(),
            }),
            KindWithContent::DumpExport { .. } => None,
            KindWithContent::Snapshot => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use meilisearch_types::heed::{types::SerdeJson, BytesDecode, BytesEncode};

    use crate::assert_smol_debug_snapshot;

    use super::Details;

    #[test]
    fn bad_deser() {
        let details = Details::DeleteTasks {
            matched_tasks: 1,
            deleted_tasks: None,
            original_query: "hello".to_owned(),
        };
        let serialised = SerdeJson::<Details>::bytes_encode(&details).unwrap();
        let deserialised = SerdeJson::<Details>::bytes_decode(&serialised).unwrap();
        assert_smol_debug_snapshot!(details, @r###"DeleteTasks { matched_tasks: 1, deleted_tasks: None, original_query: "hello" }"###);
        assert_smol_debug_snapshot!(deserialised, @r###"DeleteTasks { matched_tasks: 1, deleted_tasks: None, original_query: "hello" }"###);
    }
}
