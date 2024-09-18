use std::convert::Infallible;
use std::hash::Hash;
use std::str::FromStr;

use bitflags::bitflags;
use deserr::{take_cf_content, DeserializeError, Deserr, MergeWithError, ValuePointerRef};
use enum_iterator::Sequence;
use milli::update::Setting;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use time::format_description::well_known::Rfc3339;
use time::macros::{format_description, time};
use time::{Date, OffsetDateTime, PrimitiveDateTime};
use uuid::Uuid;

use crate::deserr::{immutable_field_error, DeserrError, DeserrJsonError};
use crate::error::deserr_codes::*;
use crate::error::{Code, ErrorCode, ParseOffsetDateTimeError};
use crate::index_uid_pattern::{IndexUidPattern, IndexUidPatternFormatError};

pub type KeyId = Uuid;

impl<C: Default + ErrorCode> MergeWithError<IndexUidPatternFormatError> for DeserrJsonError<C> {
    fn merge(
        _self_: Option<Self>,
        other: IndexUidPatternFormatError,
        merge_location: deserr::ValuePointerRef,
    ) -> std::ops::ControlFlow<Self, Self> {
        DeserrError::error::<Infallible>(
            None,
            deserr::ErrorKind::Unexpected { msg: other.to_string() },
            merge_location,
        )
    }
}

#[derive(Debug, Deserr)]
#[deserr(error = DeserrJsonError, rename_all = camelCase, deny_unknown_fields)]
pub struct CreateApiKey {
    #[deserr(default, error = DeserrJsonError<InvalidApiKeyDescription>)]
    pub description: Option<String>,
    #[deserr(default, error = DeserrJsonError<InvalidApiKeyName>)]
    pub name: Option<String>,
    #[deserr(default = Uuid::new_v4(), error = DeserrJsonError<InvalidApiKeyUid>, try_from(&String) = Uuid::from_str -> uuid::Error)]
    pub uid: KeyId,
    #[deserr(error = DeserrJsonError<InvalidApiKeyActions>, missing_field_error = DeserrJsonError::missing_api_key_actions)]
    pub actions: Vec<Action>,
    #[deserr(error = DeserrJsonError<InvalidApiKeyIndexes>, missing_field_error = DeserrJsonError::missing_api_key_indexes)]
    pub indexes: Vec<IndexUidPattern>,
    #[deserr(error = DeserrJsonError<InvalidApiKeyExpiresAt>, try_from(Option<String>) = parse_expiration_date -> ParseOffsetDateTimeError, missing_field_error = DeserrJsonError::missing_api_key_expires_at)]
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
) -> DeserrJsonError {
    match field {
        "uid" => immutable_field_error(field, accepted, Code::ImmutableApiKeyUid),
        "actions" => immutable_field_error(field, accepted, Code::ImmutableApiKeyActions),
        "indexes" => immutable_field_error(field, accepted, Code::ImmutableApiKeyIndexes),
        "expiresAt" => immutable_field_error(field, accepted, Code::ImmutableApiKeyExpiresAt),
        "createdAt" => immutable_field_error(field, accepted, Code::ImmutableApiKeyCreatedAt),
        "updatedAt" => immutable_field_error(field, accepted, Code::ImmutableApiKeyUpdatedAt),
        _ => deserr::take_cf_content(DeserrJsonError::<BadRequest>::error::<Infallible>(
            None,
            deserr::ErrorKind::UnknownKey { key: field, accepted },
            location,
        )),
    }
}

#[derive(Debug, Deserr)]
#[deserr(error = DeserrJsonError, rename_all = camelCase, deny_unknown_fields = deny_immutable_fields_api_key)]
pub struct PatchApiKey {
    #[deserr(default, error = DeserrJsonError<InvalidApiKeyDescription>)]
    pub description: Setting<String>,
    #[deserr(default, error = DeserrJsonError<InvalidApiKeyName>)]
    pub name: Setting<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
pub struct Key {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    pub uid: KeyId,
    pub actions: Vec<Action>,
    pub indexes: Vec<IndexUidPattern>,
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
            indexes: vec![IndexUidPattern::all()],
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
            indexes: vec![IndexUidPattern::all()],
            expires_at: None,
            created_at: now,
            updated_at: now,
        }
    }
}

fn parse_expiration_date(
    string: Option<String>,
) -> std::result::Result<Option<OffsetDateTime>, ParseOffsetDateTimeError> {
    let Some(string) = string else { return Ok(None) };
    let datetime = if let Ok(datetime) = OffsetDateTime::parse(&string, &Rfc3339) {
        datetime
    } else if let Ok(primitive_datetime) = PrimitiveDateTime::parse(
        &string,
        format_description!(
            "[year repr:full base:calendar]-[month repr:numerical]-[day]T[hour]:[minute]:[second]"
        ),
    ) {
        primitive_datetime.assume_utc()
    } else if let Ok(primitive_datetime) = PrimitiveDateTime::parse(
        &string,
        format_description!(
            "[year repr:full base:calendar]-[month repr:numerical]-[day] [hour]:[minute]:[second]"
        ),
    ) {
        primitive_datetime.assume_utc()
    } else if let Ok(date) = Date::parse(
        &string,
        format_description!("[year repr:full base:calendar]-[month repr:numerical]-[day]"),
    ) {
        PrimitiveDateTime::new(date, time!(00:00)).assume_utc()
    } else {
        return Err(ParseOffsetDateTimeError(string));
    };
    if datetime > OffsetDateTime::now_utc() {
        Ok(Some(datetime))
    } else {
        Err(ParseOffsetDateTimeError(string))
    }
}

bitflags! {
    #[derive(Copy, Clone, Debug, Eq, PartialEq, Hash, PartialOrd, Ord)]
    #[repr(transparent)]
    pub struct Action: u8 {
    const All = 0;
    const Search = 1;
    const DocumentsAll = 2;
    const DocumentsAdd = 3;
    const DocumentsGet = 4;
    const DocumentsDelete = 5;
    const IndexesAll = 6;
    const IndexesAdd = 7;
    const IndexesGet = 8;
    const IndexesUpdate = 9;
    const IndexesDelete = 10;
    const IndexesSwap = 11;
    const TasksAll = 12;
    const TasksCancel = 13;
    const TasksDelete = 14;
    const TasksGet = 15;
    const SettingsAll = 16;
    const SettingsGet = 17;
    const SettingsUpdate = 18;
    const StatsAll = 19;
    const StatsGet = 20;
    const MetricsAll = 21;
    const MetricsGet = 22;
    const DumpsAll = 23;
    const DumpsCreate = 24;
    const SnapshotsAll = 25;
    const SnapshotsCreate = 26;
    const Version = 27;
    const KeysAdd = 28;
    const KeysGet = 29;
    const KeysUpdate = 30;
    const KeysDelete = 31;
    const ExperimentalFeaturesGet = 32;
    const ExperimentalFeaturesUpdate = 33;
    }
}

impl Action {
    const SERDE_MAP_ARR: [(&'static str, Self); 34] = [
        ("*", Self::All),
        ("search", Self::Search),
        ("documents.*", Self::DocumentsAll),
        ("documents.add", Self::DocumentsAdd),
        ("documents.get", Self::DocumentsGet),
        ("documents.delete", Self::DocumentsDelete),
        ("indexes.*", Self::IndexesAll),
        ("indexes.create", Self::IndexesAdd),
        ("indexes.get", Self::IndexesGet),
        ("indexes.update", Self::IndexesUpdate),
        ("indexes.delete", Self::IndexesDelete),
        ("indexes.swap", Self::IndexesSwap),
        ("tasks.*", Self::TasksAll),
        ("tasks.cancel", Self::TasksCancel),
        ("tasks.delete", Self::TasksDelete),
        ("tasks.get", Self::TasksGet),
        ("settings.*", Self::SettingsAll),
        ("settings.get", Self::SettingsGet),
        ("settings.update", Self::SettingsUpdate),
        ("stats.*", Self::StatsAll),
        ("stats.get", Self::StatsGet),
        ("metrics.*", Self::MetricsAll),
        ("metrics.get", Self::MetricsGet),
        ("dumps.*", Self::DumpsAll),
        ("dumps.create", Self::DumpsCreate),
        ("snapshots.*", Self::SnapshotsAll),
        ("snapshots.create", Self::SnapshotsCreate),
        ("version", Self::Version),
        ("keys.create", Self::KeysAdd),
        ("keys.get", Self::KeysGet),
        ("keys.update", Self::KeysUpdate),
        ("keys.delete", Self::KeysDelete),
        ("experimental.get", Self::ExperimentalFeaturesGet),
        ("experimental.update", Self::ExperimentalFeaturesUpdate),
    ];

    fn get_action(v: &str) -> Option<Action> {
        Self::SERDE_MAP_ARR
            .iter()
            .find(|(serde_name, _)| &v == serde_name)
            .map(|(_, action)| *action)
    }

    fn get_action_serde_name(v: &Action) -> &'static str {
        Self::SERDE_MAP_ARR
            .iter()
            .find(|(_, action)| v == action)
            .map(|(serde_name, _)| serde_name)
            // actions should always have matching serialized values
            .unwrap()
    }

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
            SNAPSHOTS_CREATE => Some(Self::SnapshotsCreate),
            VERSION => Some(Self::Version),
            KEYS_CREATE => Some(Self::KeysAdd),
            KEYS_GET => Some(Self::KeysGet),
            KEYS_UPDATE => Some(Self::KeysUpdate),
            KEYS_DELETE => Some(Self::KeysDelete),
            EXPERIMENTAL_FEATURES_GET => Some(Self::ExperimentalFeaturesGet),
            EXPERIMENTAL_FEATURES_UPDATE => Some(Self::ExperimentalFeaturesUpdate),
            _otherwise => None,
        }
    }

    pub const fn repr(&self) -> u8 {
        self.bits()
    }
}

impl<E: DeserializeError> Deserr<E> for Action {
    fn deserialize_from_value<V: deserr::IntoValue>(
        value: deserr::Value<V>,
        location: deserr::ValuePointerRef<'_>,
    ) -> Result<Self, E> {
        match value {
            deserr::Value::String(s) => match Self::get_action(&s) {
                Some(action) => Ok(action),
                None => Err(deserr::take_cf_content(E::error::<std::convert::Infallible>(
                    None,
                    deserr::ErrorKind::UnknownValue {
                        value: &s,
                        accepted: &Self::SERDE_MAP_ARR.map(|(ser_action, _)| ser_action),
                    },
                    location,
                ))),
            },
            _ => Err(take_cf_content(E::error(
                None,
                deserr::ErrorKind::IncorrectValueKind {
                    actual: value,
                    accepted: &[deserr::ValueKind::String],
                },
                location,
            ))),
        }
    }
}

impl Serialize for Action {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(Self::get_action_serde_name(self))
    }
}

impl<'de> Deserialize<'de> for Action {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct Visitor;
        impl<'de> serde::de::Visitor<'de> for Visitor {
            type Value = Action;

            fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
                write!(formatter, "the name of a valid action (string)")
            }

            fn visit_str<E>(self, s: &str) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                match Self::Value::get_action(s) {
                    Some(action) => Ok(action),
                    None => Err(E::invalid_value(serde::de::Unexpected::Str(s), &"a valid action")),
                }
            }
        }

        deserializer.deserialize_str(Visitor)
    }
}

impl Sequence for Action {
    const CARDINALITY: usize = Self::SERDE_MAP_ARR.len();

    fn next(&self) -> Option<Self> {
        let next_index = self.bits() as usize + 1;
        if next_index == Self::CARDINALITY {
            None
        } else {
            Some(Self::SERDE_MAP_ARR[next_index].1)
        }
    }

    fn previous(&self) -> Option<Self> {
        let current_index = self.bits() as usize;
        if current_index == 0 {
            None
        } else {
            Some(Self::SERDE_MAP_ARR[current_index - 1].1)
        }
    }

    fn first() -> Option<Self> {
        Some(Self::SERDE_MAP_ARR[0].1)
    }

    fn last() -> Option<Self> {
        Some(Self::SERDE_MAP_ARR[Self::CARDINALITY - 1].1)
    }
}

pub mod actions {
    use super::Action as A;

    pub(crate) const ALL: u8 = A::All.repr();
    pub const SEARCH: u8 = A::Search.repr();
    pub const DOCUMENTS_ALL: u8 = A::DocumentsAll.repr();
    pub const DOCUMENTS_ADD: u8 = A::DocumentsAdd.repr();
    pub const DOCUMENTS_GET: u8 = A::DocumentsGet.repr();
    pub const DOCUMENTS_DELETE: u8 = A::DocumentsDelete.repr();
    pub const INDEXES_ALL: u8 = A::IndexesAll.repr();
    pub const INDEXES_CREATE: u8 = A::IndexesAdd.repr();
    pub const INDEXES_GET: u8 = A::IndexesGet.repr();
    pub const INDEXES_UPDATE: u8 = A::IndexesUpdate.repr();
    pub const INDEXES_DELETE: u8 = A::IndexesDelete.repr();
    pub const INDEXES_SWAP: u8 = A::IndexesSwap.repr();
    pub const TASKS_ALL: u8 = A::TasksAll.repr();
    pub const TASKS_CANCEL: u8 = A::TasksCancel.repr();
    pub const TASKS_DELETE: u8 = A::TasksDelete.repr();
    pub const TASKS_GET: u8 = A::TasksGet.repr();
    pub const SETTINGS_ALL: u8 = A::SettingsAll.repr();
    pub const SETTINGS_GET: u8 = A::SettingsGet.repr();
    pub const SETTINGS_UPDATE: u8 = A::SettingsUpdate.repr();
    pub const STATS_ALL: u8 = A::StatsAll.repr();
    pub const STATS_GET: u8 = A::StatsGet.repr();
    pub const METRICS_ALL: u8 = A::MetricsAll.repr();
    pub const METRICS_GET: u8 = A::MetricsGet.repr();
    pub const DUMPS_ALL: u8 = A::DumpsAll.repr();
    pub const DUMPS_CREATE: u8 = A::DumpsCreate.repr();
    pub const SNAPSHOTS_CREATE: u8 = A::SnapshotsCreate.repr();
    pub const VERSION: u8 = A::Version.repr();
    pub const KEYS_CREATE: u8 = A::KeysAdd.repr();
    pub const KEYS_GET: u8 = A::KeysGet.repr();
    pub const KEYS_UPDATE: u8 = A::KeysUpdate.repr();
    pub const KEYS_DELETE: u8 = A::KeysDelete.repr();
    pub const EXPERIMENTAL_FEATURES_GET: u8 = A::ExperimentalFeaturesGet.repr();
    pub const EXPERIMENTAL_FEATURES_UPDATE: u8 = A::ExperimentalFeaturesUpdate.repr();
}
