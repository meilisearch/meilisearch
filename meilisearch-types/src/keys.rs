use std::convert::Infallible;
use std::fmt::Display;
use std::hash::Hash;

use deserr::{DeserializeError, DeserializeFromValue, MergeWithError, ValuePointerRef};
use enum_iterator::Sequence;
use serde::{Deserialize, Serialize};
use time::format_description::well_known::Rfc3339;
use time::macros::{format_description, time};
use time::{Date, OffsetDateTime, PrimitiveDateTime};
use uuid::Uuid;

use crate::error::deserr_codes::*;
use crate::error::{unwrap_any, Code, DeserrError, ErrorCode, TakeErrorMessage};
use crate::index_uid::{IndexUid, IndexUidFormatError};
use crate::star_or::StarOr;

pub type KeyId = Uuid;

impl<C: Default + ErrorCode> MergeWithError<IndexUidFormatError> for DeserrError<C> {
    fn merge(
        _self_: Option<Self>,
        other: IndexUidFormatError,
        merge_location: deserr::ValuePointerRef,
    ) -> std::result::Result<Self, Self> {
        DeserrError::error::<Infallible>(
            None,
            deserr::ErrorKind::Unexpected { msg: other.to_string() },
            merge_location,
        )
    }
}

fn parse_uuid_from_str(s: &str) -> Result<Uuid, TakeErrorMessage<uuid::Error>> {
    Uuid::parse_str(s).map_err(TakeErrorMessage)
}

#[derive(Debug, DeserializeFromValue)]
#[deserr(error = DeserrError, rename_all = camelCase, deny_unknown_fields)]
pub struct CreateApiKey {
    #[deserr(error = DeserrError<InvalidApiKeyDescription>)]
    pub description: Option<String>,
    #[deserr(error = DeserrError<InvalidApiKeyName>)]
    pub name: Option<String>,
    #[deserr(default = Uuid::new_v4(), error = DeserrError<InvalidApiKeyUid>, from(&String) = parse_uuid_from_str -> TakeErrorMessage<uuid::Error>)]
    pub uid: KeyId,
    #[deserr(error = DeserrError<InvalidApiKeyActions>)]
    pub actions: Vec<Action>,
    #[deserr(error = DeserrError<InvalidApiKeyIndexes>)]
    pub indexes: Vec<StarOr<IndexUid>>,
    #[deserr(error = DeserrError<InvalidApiKeyExpiresAt>, default = None, from(&String) = parse_expiration_date -> TakeErrorMessage<ParseOffsetDateTimeError>)]
    pub expires_at: Option<OffsetDateTime>,
}
impl CreateApiKey {
    pub fn to_key(self) -> Key {
        let CreateApiKey { description, name, uid, actions, indexes, expires_at } = self;
        let now = OffsetDateTime::now_utc();
        Key {
            description,
            name,
            uid,
            actions,
            indexes,
            expires_at,
            created_at: now,
            updated_at: now,
        }
    }
}

fn deny_immutable_fields_api_key(
    field: &str,
    accepted: &[&str],
    location: ValuePointerRef,
) -> DeserrError {
    let mut error = unwrap_any(DeserrError::<BadRequest>::error::<Infallible>(
        None,
        deserr::ErrorKind::UnknownKey { key: field, accepted },
        location,
    ));

    error.code = match field {
        "uid" => Code::ImmutableApiKeyUid,
        "actions" => Code::ImmutableApiKeyActions,
        "indexes" => Code::ImmutableApiKeyIndexes,
        "expiresAt" => Code::ImmutableApiKeyExpiresAt,
        "createdAt" => Code::ImmutableApiKeyCreatedAt,
        "updatedAt" => Code::ImmutableApiKeyUpdatedAt,
        _ => Code::BadRequest,
    };
    error
}

#[derive(Debug, DeserializeFromValue)]
#[deserr(error = DeserrError, rename_all = camelCase, deny_unknown_fields = deny_immutable_fields_api_key)]
pub struct PatchApiKey {
    #[deserr(error = DeserrError<InvalidApiKeyDescription>)]
    pub description: Option<String>,
    #[deserr(error = DeserrError<InvalidApiKeyName>)]
    pub name: Option<String>,
}

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

#[derive(Debug)]
pub struct ParseOffsetDateTimeError(String);
impl Display for ParseOffsetDateTimeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(f, "`{original}` is not a valid date. It should follow the RFC 3339 format to represents a date or datetime in the future or specified as a null value. e.g. 'YYYY-MM-DD' or 'YYYY-MM-DD HH:MM:SS'.", original = self.0)
    }
}
impl std::error::Error for ParseOffsetDateTimeError {}

fn parse_expiration_date(
    string: &str,
) -> std::result::Result<Option<OffsetDateTime>, TakeErrorMessage<ParseOffsetDateTimeError>> {
    let datetime = if let Ok(datetime) = OffsetDateTime::parse(string, &Rfc3339) {
        datetime
    } else if let Ok(primitive_datetime) = PrimitiveDateTime::parse(
        string,
        format_description!(
            "[year repr:full base:calendar]-[month repr:numerical]-[day]T[hour]:[minute]:[second]"
        ),
    ) {
        primitive_datetime.assume_utc()
    } else if let Ok(primitive_datetime) = PrimitiveDateTime::parse(
        string,
        format_description!(
            "[year repr:full base:calendar]-[month repr:numerical]-[day] [hour]:[minute]:[second]"
        ),
    ) {
        primitive_datetime.assume_utc()
    } else if let Ok(date) = Date::parse(
        string,
        format_description!("[year repr:full base:calendar]-[month repr:numerical]-[day]"),
    ) {
        PrimitiveDateTime::new(date, time!(00:00)).assume_utc()
    } else {
        return Err(TakeErrorMessage(ParseOffsetDateTimeError(string.to_owned())));
    };
    if datetime > OffsetDateTime::now_utc() {
        Ok(Some(datetime))
    } else {
        Err(TakeErrorMessage(ParseOffsetDateTimeError(string.to_owned())))
    }
}

#[derive(
    Copy, Clone, Serialize, Deserialize, Debug, Eq, PartialEq, Hash, Sequence, DeserializeFromValue,
)]
#[repr(u8)]
pub enum Action {
    #[serde(rename = "*")]
    #[deserr(rename = "*")]
    All = 0,
    #[serde(rename = "search")]
    #[deserr(rename = "search")]
    Search,
    #[serde(rename = "documents.*")]
    #[deserr(rename = "documents.*")]
    DocumentsAll,
    #[serde(rename = "documents.add")]
    #[deserr(rename = "documents.add")]
    DocumentsAdd,
    #[serde(rename = "documents.get")]
    #[deserr(rename = "documents.get")]
    DocumentsGet,
    #[serde(rename = "documents.delete")]
    #[deserr(rename = "documents.delete")]
    DocumentsDelete,
    #[serde(rename = "indexes.*")]
    #[deserr(rename = "indexes.*")]
    IndexesAll,
    #[serde(rename = "indexes.create")]
    #[deserr(rename = "indexes.create")]
    IndexesAdd,
    #[serde(rename = "indexes.get")]
    #[deserr(rename = "indexes.get")]
    IndexesGet,
    #[serde(rename = "indexes.update")]
    #[deserr(rename = "indexes.update")]
    IndexesUpdate,
    #[serde(rename = "indexes.delete")]
    #[deserr(rename = "indexes.delete")]
    IndexesDelete,
    #[serde(rename = "indexes.swap")]
    #[deserr(rename = "indexes.swap")]
    IndexesSwap,
    #[serde(rename = "tasks.*")]
    #[deserr(rename = "tasks.*")]
    TasksAll,
    #[serde(rename = "tasks.cancel")]
    #[deserr(rename = "tasks.cancel")]
    TasksCancel,
    #[serde(rename = "tasks.delete")]
    #[deserr(rename = "tasks.delete")]
    TasksDelete,
    #[serde(rename = "tasks.get")]
    #[deserr(rename = "tasks.get")]
    TasksGet,
    #[serde(rename = "settings.*")]
    #[deserr(rename = "settings.*")]
    SettingsAll,
    #[serde(rename = "settings.get")]
    #[deserr(rename = "settings.get")]
    SettingsGet,
    #[serde(rename = "settings.update")]
    #[deserr(rename = "settings.update")]
    SettingsUpdate,
    #[serde(rename = "stats.*")]
    #[deserr(rename = "stats.*")]
    StatsAll,
    #[serde(rename = "stats.get")]
    #[deserr(rename = "stats.get")]
    StatsGet,
    #[serde(rename = "metrics.*")]
    #[deserr(rename = "metrics.*")]
    MetricsAll,
    #[serde(rename = "metrics.get")]
    #[deserr(rename = "metrics.get")]
    MetricsGet,
    #[serde(rename = "dumps.*")]
    #[deserr(rename = "dumps.*")]
    DumpsAll,
    #[serde(rename = "dumps.create")]
    #[deserr(rename = "dumps.create")]
    DumpsCreate,
    #[serde(rename = "version")]
    #[deserr(rename = "version")]
    Version,
    #[serde(rename = "keys.create")]
    #[deserr(rename = "keys.create")]
    KeysAdd,
    #[serde(rename = "keys.get")]
    #[deserr(rename = "keys.get")]
    KeysGet,
    #[serde(rename = "keys.update")]
    #[deserr(rename = "keys.update")]
    KeysUpdate,
    #[serde(rename = "keys.delete")]
    #[deserr(rename = "keys.delete")]
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
