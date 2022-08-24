use enum_iterator::IntoEnumIterator;
use serde::{Deserialize, Serialize};
use std::hash::Hash;

#[derive(IntoEnumIterator, Copy, Clone, Serialize, Deserialize, Debug, Eq, PartialEq, Hash)]
#[repr(u8)]
pub enum Action {
    #[serde(rename = "*")]
    All = 0,
    #[serde(rename = "search")]
    Search,
    #[serde(rename = "documents.*")]
    DocumentsAll,
    #[serde(rename = "documents.add")]
    DocumentsAdd,
    #[serde(rename = "documents.get")]
    DocumentsGet,
    #[serde(rename = "documents.delete")]
    DocumentsDelete,
    #[serde(rename = "indexes.*")]
    IndexesAll,
    #[serde(rename = "indexes.create")]
    IndexesAdd,
    #[serde(rename = "indexes.get")]
    IndexesGet,
    #[serde(rename = "indexes.update")]
    IndexesUpdate,
    #[serde(rename = "indexes.delete")]
    IndexesDelete,
    #[serde(rename = "tasks.*")]
    TasksAll,
    #[serde(rename = "tasks.get")]
    TasksGet,
    #[serde(rename = "settings.*")]
    SettingsAll,
    #[serde(rename = "settings.get")]
    SettingsGet,
    #[serde(rename = "settings.update")]
    SettingsUpdate,
    #[serde(rename = "stats.*")]
    StatsAll,
    #[serde(rename = "stats.get")]
    StatsGet,
    #[serde(rename = "metrics.*")]
    MetricsAll,
    #[serde(rename = "metrics.get")]
    MetricsGet,
    #[serde(rename = "dumps.*")]
    DumpsAll,
    #[serde(rename = "dumps.create")]
    DumpsCreate,
    #[serde(rename = "version")]
    Version,
    #[serde(rename = "keys.create")]
    KeysAdd,
    #[serde(rename = "keys.get")]
    KeysGet,
    #[serde(rename = "keys.update")]
    KeysUpdate,
    #[serde(rename = "keys.delete")]
    KeysDelete,
}

impl Action {
    pub const fn from_repr(repr: u8) -> Option<Self> {
        use actions::*;
        match repr {
            ALL => Some(Self::All),
            SEARCH => Some(Self::Search),
            DOCUMENTS_ALL => Some(Self::DocumentsAll),
            DOCUMENTS_ADD => Some(Self::DocumentsAdd),
            DOCUMENTS_GET => Some(Self::DocumentsGet),
            DOCUMENTS_DELETE => Some(Self::DocumentsDelete),
            INDEXES_ALL => Some(Self::IndexesAll),
            INDEXES_CREATE => Some(Self::IndexesAdd),
            INDEXES_GET => Some(Self::IndexesGet),
            INDEXES_UPDATE => Some(Self::IndexesUpdate),
            INDEXES_DELETE => Some(Self::IndexesDelete),
            TASKS_ALL => Some(Self::TasksAll),
            TASKS_GET => Some(Self::TasksGet),
            SETTINGS_ALL => Some(Self::SettingsAll),
            SETTINGS_GET => Some(Self::SettingsGet),
            SETTINGS_UPDATE => Some(Self::SettingsUpdate),
            STATS_ALL => Some(Self::StatsAll),
            STATS_GET => Some(Self::StatsGet),
            METRICS_ALL => Some(Self::MetricsAll),
            METRICS_GET => Some(Self::MetricsGet),
            DUMPS_ALL => Some(Self::DumpsAll),
            DUMPS_CREATE => Some(Self::DumpsCreate),
            VERSION => Some(Self::Version),
            KEYS_CREATE => Some(Self::KeysAdd),
            KEYS_GET => Some(Self::KeysGet),
            KEYS_UPDATE => Some(Self::KeysUpdate),
            KEYS_DELETE => Some(Self::KeysDelete),
            _otherwise => None,
        }
    }

    pub const fn repr(&self) -> u8 {
        *self as u8
    }
}

pub mod actions {
    use super::Action::*;

    pub(crate) const ALL: u8 = All.repr();
    pub const SEARCH: u8 = Search.repr();
    pub const DOCUMENTS_ALL: u8 = DocumentsAll.repr();
    pub const DOCUMENTS_ADD: u8 = DocumentsAdd.repr();
    pub const DOCUMENTS_GET: u8 = DocumentsGet.repr();
    pub const DOCUMENTS_DELETE: u8 = DocumentsDelete.repr();
    pub const INDEXES_ALL: u8 = IndexesAll.repr();
    pub const INDEXES_CREATE: u8 = IndexesAdd.repr();
    pub const INDEXES_GET: u8 = IndexesGet.repr();
    pub const INDEXES_UPDATE: u8 = IndexesUpdate.repr();
    pub const INDEXES_DELETE: u8 = IndexesDelete.repr();
    pub const TASKS_ALL: u8 = TasksAll.repr();
    pub const TASKS_GET: u8 = TasksGet.repr();
    pub const SETTINGS_ALL: u8 = SettingsAll.repr();
    pub const SETTINGS_GET: u8 = SettingsGet.repr();
    pub const SETTINGS_UPDATE: u8 = SettingsUpdate.repr();
    pub const STATS_ALL: u8 = StatsAll.repr();
    pub const STATS_GET: u8 = StatsGet.repr();
    pub const METRICS_ALL: u8 = MetricsAll.repr();
    pub const METRICS_GET: u8 = MetricsGet.repr();
    pub const DUMPS_ALL: u8 = DumpsAll.repr();
    pub const DUMPS_CREATE: u8 = DumpsCreate.repr();
    pub const VERSION: u8 = Version.repr();
    pub const KEYS_CREATE: u8 = KeysAdd.repr();
    pub const KEYS_GET: u8 = KeysGet.repr();
    pub const KEYS_UPDATE: u8 = KeysUpdate.repr();
    pub const KEYS_DELETE: u8 = KeysDelete.repr();
}
