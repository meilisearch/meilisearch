use std::fmt::{self, Display, Formatter};
use std::marker::PhantomData;
use std::str::FromStr;

use serde::de::Visitor;
use serde::{Deserialize, Deserializer};
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

/// A type that tries to match either a star (*) or
/// any other thing that implements `FromStr`.
#[derive(Debug)]
#[cfg_attr(test, derive(serde::Serialize))]
pub enum StarOr<T> {
    Star,
    Other(T),
}

impl<'de, T, E> Deserialize<'de> for StarOr<T>
where
    T: FromStr<Err = E>,
    E: Display,
{
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        /// Serde can't differentiate between `StarOr::Star` and `StarOr::Other` without a tag.
        /// Simply using `#[serde(untagged)]` + `#[serde(rename="*")]` will lead to attempting to
        /// deserialize everything as a `StarOr::Other`, including "*".
        /// [`#[serde(other)]`](https://serde.rs/variant-attrs.html#other) might have helped but is
        /// not supported on untagged enums.
        struct StarOrVisitor<T>(PhantomData<T>);

        impl<'de, T, FE> Visitor<'de> for StarOrVisitor<T>
        where
            T: FromStr<Err = FE>,
            FE: Display,
        {
            type Value = StarOr<T>;

            fn expecting(&self, formatter: &mut Formatter) -> std::fmt::Result {
                formatter.write_str("a string")
            }

            fn visit_str<SE>(self, v: &str) -> Result<Self::Value, SE>
            where
                SE: serde::de::Error,
            {
                match v {
                    "*" => Ok(StarOr::Star),
                    v => {
                        let other = FromStr::from_str(v).map_err(|e: T::Err| {
                            SE::custom(format!("Invalid `other` value: {}", e))
                        })?;
                        Ok(StarOr::Other(other))
                    }
                }
            }
        }

        deserializer.deserialize_str(StarOrVisitor(PhantomData))
    }
}
