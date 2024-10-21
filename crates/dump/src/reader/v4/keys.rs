use serde::Deserialize;
use time::OffsetDateTime;

pub const KEY_ID_LENGTH: usize = 8;
pub type KeyId = [u8; KEY_ID_LENGTH];

#[derive(Debug, Deserialize)]
#[cfg_attr(test, derive(serde::Serialize))]
pub struct Key {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    pub id: KeyId,
    pub actions: Vec<Action>,
    pub indexes: Vec<String>,
    #[serde(with = "time::serde::rfc3339::option")]
    pub expires_at: Option<OffsetDateTime>,
    #[serde(with = "time::serde::rfc3339")]
    pub created_at: OffsetDateTime,
    #[serde(with = "time::serde::rfc3339")]
    pub updated_at: OffsetDateTime,
}

#[derive(Copy, Clone, Deserialize, Debug, Eq, PartialEq)]
#[cfg_attr(test, derive(serde::Serialize))]
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
    #[serde(rename = "indexes.create")]
    IndexesAdd = actions::INDEXES_CREATE,
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

pub mod actions {
    pub const SEARCH: u8 = 1;
    pub const DOCUMENTS_ADD: u8 = 2;
    pub const DOCUMENTS_GET: u8 = 3;
    pub const DOCUMENTS_DELETE: u8 = 4;
    pub const INDEXES_CREATE: u8 = 5;
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
