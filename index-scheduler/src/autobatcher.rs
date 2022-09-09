use crate::{task::Kind, TaskId};

pub enum BatchKind {
    ClearAll {
        ids: Vec<TaskId>,
    },
    DocumentAddition {
        addition_ids: Vec<TaskId>,
    },
    DocumentDeletion {
        deletion_ids: Vec<TaskId>,
    },
    ClearAllAndSettings {
        other: Vec<TaskId>,
        settings_ids: Vec<TaskId>,
    },
    SettingsAndDocumentAddition {
        settings_ids: Vec<TaskId>,
        addition_ids: Vec<TaskId>,
    },
    Settings {
        settings_ids: Vec<TaskId>,
    },
    DeleteIndex {
        ids: Vec<TaskId>,
    },
    CreateIndex {
        id: TaskId,
    },
    SwapIndex {
        id: TaskId,
    },
    RenameIndex {
        id: TaskId,
    },
}

impl BatchKind {
    /// return true if you must stop right there.
    pub fn new(task_id: TaskId, kind: Kind) -> (Self, bool) {
        match kind {
            Kind::CreateIndex => (BatchKind::CreateIndex { id: task_id }, true),
            Kind::DeleteIndex => (BatchKind::DeleteIndex { ids: vec![task_id] }, true),
            Kind::RenameIndex => (BatchKind::RenameIndex { id: task_id }, true),
            Kind::SwapIndex => (BatchKind::SwapIndex { id: task_id }, true),
            Kind::ClearAllDocuments => (BatchKind::ClearAll { ids: vec![task_id] }, false),
            Kind::DocumentAddition => (
                BatchKind::DocumentAddition {
                    addition_ids: vec![task_id],
                },
                false,
            ),
            Kind::DocumentDeletion => (
                BatchKind::DocumentDeletion {
                    deletion_ids: vec![task_id],
                },
                false,
            ),
            Kind::Settings => (
                BatchKind::Settings {
                    settings_ids: vec![task_id],
                },
                false,
            ),

            Kind::DumpExport | Kind::Snapshot | Kind::CancelTask => unreachable!(),
        }
    }

    /// Return true if you must stop.
    fn accumulate(&mut self, id: TaskId, kind: Kind) -> bool {
        match (self, kind) {
            // must handle the deleteIndex
            (_, Kind::CreateIndex | Kind::RenameIndex | Kind::SwapIndex) => true,

            (BatchKind::ClearAll { ids }, Kind::ClearAllDocuments | Kind::DocumentDeletion) => {
                ids.push(id);
                false
            }
            (BatchKind::ClearAll { .. }, Kind::DocumentAddition | Kind::Settings) => true,
            (BatchKind::DocumentAddition { addition_ids }, Kind::ClearAllDocuments) => {
                addition_ids.push(id);
                *self = BatchKind::ClearAll {
                    ids: addition_ids.clone(),
                };
                false
            }

            (BatchKind::DocumentAddition { addition_ids }, Kind::DocumentAddition) => {
                addition_ids.push(id);
                false
            }
            (BatchKind::DocumentAddition { .. }, Kind::DocumentDeletion) => true,
            (BatchKind::DocumentAddition { addition_ids }, Kind::Settings) => {
                *self = BatchKind::SettingsAndDocumentAddition {
                    settings_ids: vec![id],
                    addition_ids: addition_ids.clone(),
                };
                false
            }

            (BatchKind::DocumentDeletion { deletion_ids }, Kind::ClearAllDocuments) => {
                deletion_ids.push(id);
                *self = BatchKind::ClearAll {
                    ids: deletion_ids.clone(),
                };
                false
            }
            (BatchKind::DocumentDeletion { .. }, Kind::DocumentAddition) => true,
            (BatchKind::DocumentDeletion { deletion_ids }, Kind::DocumentDeletion) => {
                deletion_ids.push(id);
                false
            }
            (BatchKind::DocumentDeletion { .. }, Kind::Settings) => true,

            (BatchKind::Settings { settings_ids }, Kind::ClearAllDocuments) => {
                *self = BatchKind::ClearAllAndSettings {
                    settings_ids: settings_ids.clone(),
                    other: vec![id],
                };
                false
            }
            (BatchKind::Settings { .. }, Kind::DocumentAddition) => true,
            (BatchKind::Settings { .. }, Kind::DocumentDeletion) => true,
            (BatchKind::Settings { settings_ids }, Kind::Settings) => {
                settings_ids.push(id);
                false
            }

            (
                BatchKind::ClearAllAndSettings {
                    other,
                    settings_ids,
                },
                Kind::ClearAllDocuments,
            ) => {
                other.push(id);
                false
            }
            (BatchKind::ClearAllAndSettings { .. }, Kind::DocumentAddition) => true,
            (
                BatchKind::ClearAllAndSettings {
                    other,
                    settings_ids,
                },
                Kind::DocumentDeletion,
            ) => {
                other.push(id);
                false
            }
            (
                BatchKind::ClearAllAndSettings {
                    settings_ids,
                    other,
                },
                Kind::Settings,
            ) => {
                settings_ids.push(id);
                false
            }
            (
                BatchKind::SettingsAndDocumentAddition {
                    settings_ids,
                    addition_ids,
                },
                Kind::ClearAllDocuments,
            ) => {
                addition_ids.push(id);
                *self = BatchKind::ClearAllAndSettings {
                    settings_ids: settings_ids.clone(),
                    other: addition_ids.clone(),
                };
                false
            }
            (
                BatchKind::SettingsAndDocumentAddition {
                    settings_ids,
                    addition_ids,
                },
                Kind::DocumentAddition,
            ) => {
                addition_ids.push(id);
                false
            }
            (
                BatchKind::SettingsAndDocumentAddition {
                    settings_ids,
                    addition_ids,
                },
                Kind::DocumentDeletion,
            ) => true,
            (
                BatchKind::SettingsAndDocumentAddition {
                    settings_ids,
                    addition_ids,
                },
                Kind::Settings,
            ) => {
                settings_ids.push(id);
                false
            }
            (_, Kind::CancelTask | Kind::DumpExport | Kind::Snapshot) => unreachable!(),
            (
                BatchKind::CreateIndex { .. }
                | BatchKind::DeleteIndex { .. }
                | BatchKind::SwapIndex { .. }
                | BatchKind::RenameIndex { .. },
                _,
            ) => {
                unreachable!()
            }
        }
    }
}

pub fn autobatch(enqueued: Vec<(TaskId, Kind)>) -> Option<BatchKind> {
    let mut enqueued = enqueued.into_iter();
    let (id, kind) = enqueued.next()?;
    let (mut acc, is_finished) = BatchKind::new(id, kind);
    if is_finished {
        return Some(acc);
    }

    for (id, kind) in enqueued {
        if acc.accumulate(id, kind) {
            break;
        }
    }

    Some(acc)
}
