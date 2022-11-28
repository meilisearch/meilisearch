use serde::Deserialize;
use time::OffsetDateTime;
use uuid::Uuid;

use super::meta::{IndexUid, StarOr};

pub type KeyId = Uuid;

#[derive(Debug, Deserialize)]
#[cfg_attr(test, derive(serde::Serialize))]
pub struct Key {
    pub description: Option<String>,
    pub name: Option<String>,
    pub uid: KeyId,
    pub actions: Vec<Action>,
    pub indexes: Vec<StarOr<IndexUid>>,
    #[serde(with = "time::serde::rfc3339::option")]
    pub expires_at: Option<OffsetDateTime>,
    #[serde(with = "time::serde::rfc3339")]
    pub created_at: OffsetDateTime,
    #[serde(with = "time::serde::rfc3339")]
    pub updated_at: OffsetDateTime,
}

#[derive(Copy, Clone, Deserialize, Debug, Eq, PartialEq, Hash)]
#[cfg_attr(test, derive(serde::Serialize))]
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
