use std::error::Error;
use std::fmt;
use std::str::FromStr;

use deserr::DeserializeFromValue;

use crate::error::{Code, ErrorCode};

/// An index uid is composed of only ascii alphanumeric characters, - and _, between 1 and 400
/// bytes long
#[derive(Debug, Clone, PartialEq, Eq, DeserializeFromValue)]
#[deserr(from(String) = IndexUid::try_from -> IndexUidFormatError)]
pub struct IndexUid(String);

impl IndexUid {
    pub fn new_unchecked(s: impl AsRef<str>) -> Self {
        Self(s.as_ref().to_string())
    }

    pub fn into_inner(self) -> String {
        self.0
    }

    /// Return a reference over the inner str.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for IndexUid {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(&self.0, f)
    }
}

impl std::ops::Deref for IndexUid {
    type Target = str;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

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

impl fmt::Display for IndexUidFormatError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "`{}` is not a valid index uid. Index uid can be an \
            integer or a string containing only alphanumeric \
            characters, hyphens (-) and underscores (_).",
            self.invalid_uid,
        )
    }
}

impl Error for IndexUidFormatError {}

impl ErrorCode for IndexUidFormatError {
    fn error_code(&self) -> Code {
        Code::InvalidIndexUid
    }
}
