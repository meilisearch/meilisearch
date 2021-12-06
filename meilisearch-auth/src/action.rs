use enum_iterator::IntoEnumIterator;
use serde::{Deserialize, Serialize};

#[derive(IntoEnumIterator, Copy, Clone, Serialize, Deserialize, Debug, Eq, PartialEq)]
#[repr(u8)]
pub enum Action {
    #[serde(rename = "*")]
    All = 0,
    #[serde(rename = "search")]
    Search = actions::SEARCH,
    #[serde(rename = "documents.add")]
    DocumentsAdd = actions::DOCUMENTS_ADD,
    #[serde(rename = "documents.get")]
    DocumentsGet = actions::DOCUMENTS_GET,
    #[serde(rename = "documents.delete")]
    DocumentsDelete = actions::DOCUMENTS_DELETE,
    #[serde(rename = "indexes.add")]
    IndexesAdd = actions::INDEXES_ADD,
    #[serde(rename = "indexes.get")]
    IndexesGet = actions::INDEXES_GET,
    #[serde(rename = "indexes.update")]
    IndexesUpdate = actions::INDEXES_UPDATE,
    #[serde(rename = "indexes.delete")]
    IndexesDelete = actions::INDEXES_DELETE,
    #[serde(rename = "tasks.get")]
    TasksGet = actions::TASKS_GET,
    #[serde(rename = "settings.get")]
    SettingsGet = actions::SETTINGS_GET,
    #[serde(rename = "settings.update")]
    SettingsUpdate = actions::SETTINGS_UPDATE,
    #[serde(rename = "stats.get")]
    StatsGet = actions::STATS_GET,
    #[serde(rename = "dumps.create")]
    DumpsCreate = actions::DUMPS_CREATE,
    #[serde(rename = "dumps.get")]
    DumpsGet = actions::DUMPS_GET,
    #[serde(rename = "version")]
    Version = actions::VERSION,
}

impl Action {
    pub fn from_repr(repr: u8) -> Option<Self> {
        use actions::*;
        match repr {
            0 => Some(Self::All),
            SEARCH => Some(Self::Search),
            DOCUMENTS_ADD => Some(Self::DocumentsAdd),
            DOCUMENTS_GET => Some(Self::DocumentsGet),
            DOCUMENTS_DELETE => Some(Self::DocumentsDelete),
            INDEXES_ADD => Some(Self::IndexesAdd),
            INDEXES_GET => Some(Self::IndexesGet),
            INDEXES_UPDATE => Some(Self::IndexesUpdate),
            INDEXES_DELETE => Some(Self::IndexesDelete),
            TASKS_GET => Some(Self::TasksGet),
            SETTINGS_GET => Some(Self::SettingsGet),
            SETTINGS_UPDATE => Some(Self::SettingsUpdate),
            STATS_GET => Some(Self::StatsGet),
            DUMPS_CREATE => Some(Self::DumpsCreate),
            DUMPS_GET => Some(Self::DumpsGet),
            VERSION => Some(Self::Version),
            _otherwise => None,
        }
    }

    pub fn repr(&self) -> u8 {
        use actions::*;
        match self {
            Self::All => 0,
            Self::Search => SEARCH,
            Self::DocumentsAdd => DOCUMENTS_ADD,
            Self::DocumentsGet => DOCUMENTS_GET,
            Self::DocumentsDelete => DOCUMENTS_DELETE,
            Self::IndexesAdd => INDEXES_ADD,
            Self::IndexesGet => INDEXES_GET,
            Self::IndexesUpdate => INDEXES_UPDATE,
            Self::IndexesDelete => INDEXES_DELETE,
            Self::TasksGet => TASKS_GET,
            Self::SettingsGet => SETTINGS_GET,
            Self::SettingsUpdate => SETTINGS_UPDATE,
            Self::StatsGet => STATS_GET,
            Self::DumpsCreate => DUMPS_CREATE,
            Self::DumpsGet => DUMPS_GET,
            Self::Version => VERSION,
        }
    }
}

pub mod actions {
    pub const SEARCH: u8 = 1;
    pub const DOCUMENTS_ADD: u8 = 2;
    pub const DOCUMENTS_GET: u8 = 3;
    pub const DOCUMENTS_DELETE: u8 = 4;
    pub const INDEXES_ADD: u8 = 5;
    pub const INDEXES_GET: u8 = 6;
    pub const INDEXES_UPDATE: u8 = 7;
    pub const INDEXES_DELETE: u8 = 8;
    pub const TASKS_GET: u8 = 9;
    pub const SETTINGS_GET: u8 = 10;
    pub const SETTINGS_UPDATE: u8 = 11;
    pub const STATS_GET: u8 = 12;
    pub const DUMPS_CREATE: u8 = 13;
    pub const DUMPS_GET: u8 = 14;
    pub const VERSION: u8 = 15;
}
