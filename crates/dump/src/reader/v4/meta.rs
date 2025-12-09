use std::fmt::{self, Display, Formatter};
use std::str::FromStr;

use serde::Deserialize;
use uuid::Uuid;

use super::settings::{Settings, Unchecked};

#[derive(Deserialize, Debug)]
#[cfg_attr(test, derive(serde::Serialize))]
pub struct IndexUuid {
    pub uid: String,
    pub index_meta: IndexMeta,
}

#[derive(Deserialize, Debug)]
#[cfg_attr(test, derive(serde::Serialize))]
pub struct IndexMeta {
    pub uuid: Uuid,
    pub creation_task_id: usize,
}

// There is one in each indexes under `meta.json`.
#[derive(Deserialize)]
#[cfg_attr(test, derive(serde::Serialize))]
pub struct DumpMeta {
    pub settings: Settings<Unchecked>,
    pub primary_key: Option<String>,
}

#[derive(Deserialize, Debug, Clone, PartialEq, Eq)]
#[cfg_attr(test, derive(serde::Serialize))]
pub struct IndexUid(pub String);

impl TryFrom<String> for IndexUid {
    type Error = IndexUidFormatError;

    fn try_from(uid: String) -> Result<Self, Self::Error> {
        if !uid.chars().all(|x| x.is_ascii_alphanumeric() || x == '-' || x == '_')
            || uid.is_empty()
            || uid.len() > 400
        {
            Err(IndexUidFormatError { invalid_uid: uid })
        } else {
            Ok(IndexUid(uid))
        }
    }
}

impl FromStr for IndexUid {
    type Err = IndexUidFormatError;

    fn from_str(uid: &str) -> Result<IndexUid, IndexUidFormatError> {
        uid.to_string().try_into()
    }
}

impl From<IndexUid> for String {
    fn from(uid: IndexUid) -> Self {
        uid.into_inner()
    }
}

#[derive(Debug)]
pub struct IndexUidFormatError {
    pub invalid_uid: String,
}

impl Display for IndexUidFormatError {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "invalid index uid `{}`, the uid must be an integer \
            or a string containing only alphanumeric characters \
            a-z A-Z 0-9, hyphens - and underscores _, \
            and can not be more than 400 bytes.",
            self.invalid_uid,
        )
    }
}

impl std::error::Error for IndexUidFormatError {}
