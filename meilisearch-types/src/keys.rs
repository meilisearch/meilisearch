use std::hash::Hash;

use enum_iterator::Sequence;
use serde::{Deserialize, Serialize};
use serde_json::{from_value, Value};
use time::format_description::well_known::Rfc3339;
use time::macros::{format_description, time};
use time::{Date, OffsetDateTime, PrimitiveDateTime};
use uuid::Uuid;

use crate::error::{Code, ErrorCode};
use crate::index_uid::IndexUid;
use crate::star_or::StarOr;

type Result<T> = std::result::Result<T, Error>;

pub type KeyId = Uuid;

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
pub struct Key {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
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

impl Key {
    pub fn create_from_value(value: Value) -> Result<Self> {
        let name = match value.get("name") {
            None | Some(Value::Null) => None,
            Some(des) => from_value(des.clone())
                .map(Some)
                .map_err(|_| Error::InvalidApiKeyName(des.clone()))?,
        };

        let description = match value.get("description") {
            None | Some(Value::Null) => None,
            Some(des) => from_value(des.clone())
                .map(Some)
                .map_err(|_| Error::InvalidApiKeyDescription(des.clone()))?,
        };

        let uid = value.get("uid").map_or_else(
            || Ok(Uuid::new_v4()),
            |uid| from_value(uid.clone()).map_err(|_| Error::InvalidApiKeyUid(uid.clone())),
        )?;

        let actions = value
            .get("actions")
            .map(|act| {
                from_value(act.clone()).map_err(|_| Error::InvalidApiKeyActions(act.clone()))
            })
            .ok_or(Error::MissingParameter("actions"))??;

        let indexes = value
            .get("indexes")
            .map(|ind| {
                from_value(ind.clone()).map_err(|_| Error::InvalidApiKeyIndexes(ind.clone()))
            })
            .ok_or(Error::MissingParameter("indexes"))??;

        let expires_at = value
            .get("expiresAt")
            .map(parse_expiration_date)
            .ok_or(Error::MissingParameter("expiresAt"))??;

        let created_at = OffsetDateTime::now_utc();
        let updated_at = created_at;

        Ok(Self { name, description, uid, actions, indexes, expires_at, created_at, updated_at })
    }

    pub fn update_from_value(&mut self, value: Value) -> Result<()> {
        if let Some(des) = value.get("description") {
            let des =
                from_value(des.clone()).map_err(|_| Error::InvalidApiKeyDescription(des.clone()));
            self.description = des?;
        }

        if let Some(des) = value.get("name") {
            let des = from_value(des.clone()).map_err(|_| Error::InvalidApiKeyName(des.clone()));
            self.name = des?;
        }

        if value.get("uid").is_some() {
            return Err(Error::ImmutableField("uid".to_string()));
        }

        if value.get("actions").is_some() {
            return Err(Error::ImmutableField("actions".to_string()));
        }

        if value.get("indexes").is_some() {
            return Err(Error::ImmutableField("indexes".to_string()));
        }

        if value.get("expiresAt").is_some() {
            return Err(Error::ImmutableField("expiresAt".to_string()));
        }

        if value.get("createdAt").is_some() {
            return Err(Error::ImmutableField("createdAt".to_string()));
        }

        if value.get("updatedAt").is_some() {
            return Err(Error::ImmutableField("updatedAt".to_string()));
        }

        self.updated_at = OffsetDateTime::now_utc();

        Ok(())
    }

    pub fn default_admin() -> Self {
        let now = OffsetDateTime::now_utc();
        let uid = Uuid::new_v4();
        Self {
            name: Some("Default Admin API Key".to_string()),
            description: Some("Use it for anything that is not a search operation. Caution! Do not expose it on a public frontend".to_string()),
            uid,
            actions: vec![Action::All],
            indexes: vec![StarOr::Star],
            expires_at: None,
            created_at: now,
            updated_at: now,
        }
    }

    pub fn default_search() -> Self {
        let now = OffsetDateTime::now_utc();
        let uid = Uuid::new_v4();
        Self {
            name: Some("Default Search API Key".to_string()),
            description: Some("Use it to search from the frontend".to_string()),
            uid,
            actions: vec![Action::Search],
            indexes: vec![StarOr::Star],
            expires_at: None,
            created_at: now,
            updated_at: now,
        }
    }
}

fn parse_expiration_date(value: &Value) -> Result<Option<OffsetDateTime>> {
    match value {
        Value::String(string) => OffsetDateTime::parse(string, &Rfc3339)
            .or_else(|_| {
                PrimitiveDateTime::parse(
                    string,
                    format_description!(
                        "[year repr:full base:calendar]-[month repr:numerical]-[day]T[hour]:[minute]:[second]"
                    ),
                ).map(|datetime| datetime.assume_utc())
            })
            .or_else(|_| {
                PrimitiveDateTime::parse(
                    string,
                    format_description!(
                        "[year repr:full base:calendar]-[month repr:numerical]-[day] [hour]:[minute]:[second]"
                    ),
                ).map(|datetime| datetime.assume_utc())
            })
            .or_else(|_| {
                    Date::parse(string, format_description!(
                        "[year repr:full base:calendar]-[month repr:numerical]-[day]"
                    )).map(|date| PrimitiveDateTime::new(date, time!(00:00)).assume_utc())
            })
            .map_err(|_| Error::InvalidApiKeyExpiresAt(value.clone()))
            // check if the key is already expired.
            .and_then(|d| {
                if d > OffsetDateTime::now_utc() {
                    Ok(d)
                } else {
                    Err(Error::InvalidApiKeyExpiresAt(value.clone()))
                }
            })
            .map(Option::Some),
        Value::Null => Ok(None),
        _otherwise => Err(Error::InvalidApiKeyExpiresAt(value.clone())),
    }
}

#[derive(Copy, Clone, Serialize, Deserialize, Debug, Eq, PartialEq, Hash, Sequence)]
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
    #[serde(rename = "indexes.swap")]
    IndexesSwap,
    #[serde(rename = "tasks.*")]
    TasksAll,
    #[serde(rename = "tasks.cancel")]
    TasksCancel,
    #[serde(rename = "tasks.delete")]
    TasksDelete,
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
            INDEXES_SWAP => Some(Self::IndexesSwap),
            TASKS_ALL => Some(Self::TasksAll),
            TASKS_CANCEL => Some(Self::TasksCancel),
            TASKS_DELETE => Some(Self::TasksDelete),
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
    pub const INDEXES_SWAP: u8 = IndexesSwap.repr();
    pub const TASKS_ALL: u8 = TasksAll.repr();
    pub const TASKS_CANCEL: u8 = TasksCancel.repr();
    pub const TASKS_DELETE: u8 = TasksDelete.repr();
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

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("`{0}` field is mandatory.")]
    MissingParameter(&'static str),
    #[error("`actions` field value `{0}` is invalid. It should be an array of string representing action names.")]
    InvalidApiKeyActions(Value),
    #[error(
        "`{0}` is not a valid index uid. It should be an array of string representing index names."
    )]
    InvalidApiKeyIndexes(Value),
    #[error("`expiresAt` field value `{0}` is invalid. It should follow the RFC 3339 format to represents a date or datetime in the future or specified as a null value. e.g. 'YYYY-MM-DD' or 'YYYY-MM-DD HH:MM:SS'.")]
    InvalidApiKeyExpiresAt(Value),
    #[error("`description` field value `{0}` is invalid. It should be a string or specified as a null value.")]
    InvalidApiKeyDescription(Value),
    #[error(
        "`name` field value `{0}` is invalid. It should be a string or specified as a null value."
    )]
    InvalidApiKeyName(Value),
    #[error("`uid` field value `{0}` is invalid. It should be a valid UUID v4 string or omitted.")]
    InvalidApiKeyUid(Value),
    #[error("The `{0}` field cannot be modified for the given resource.")]
    ImmutableField(String),
}

impl ErrorCode for Error {
    fn error_code(&self) -> Code {
        match self {
            Self::MissingParameter(_) => Code::MissingParameter,
            Self::InvalidApiKeyActions(_) => Code::InvalidApiKeyActions,
            Self::InvalidApiKeyIndexes(_) => Code::InvalidApiKeyIndexes,
            Self::InvalidApiKeyExpiresAt(_) => Code::InvalidApiKeyExpiresAt,
            Self::InvalidApiKeyDescription(_) => Code::InvalidApiKeyDescription,
            Self::InvalidApiKeyName(_) => Code::InvalidApiKeyName,
            Self::InvalidApiKeyUid(_) => Code::InvalidApiKeyUid,
            Self::ImmutableField(_) => Code::ImmutableField,
        }
    }
}
