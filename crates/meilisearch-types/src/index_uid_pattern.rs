use std::borrow::Borrow;
use std::error::Error;
use std::fmt;
use std::ops::Deref;
use std::str::FromStr;

use deserr::Deserr;
use serde::{Deserialize, Serialize};

use crate::error::{Code, ErrorCode};
use crate::index_uid::{IndexUid, IndexUidFormatError};

/// An index uid pattern is composed of only ascii alphanumeric characters, - and _, between 1 and 400
/// bytes long and optionally ending with a *.
#[derive(Serialize, Deserialize, Deserr, Debug, Clone, PartialEq, Eq, Hash)]
#[deserr(try_from(&String) = FromStr::from_str -> IndexUidPatternFormatError)]
pub struct IndexUidPattern(String);

impl IndexUidPattern {
    pub fn new_unchecked(s: impl AsRef<str>) -> Self {
        Self(s.as_ref().to_string())
    }

    /// Matches any index name.
    pub fn all() -> Self {
        IndexUidPattern::from_str("*").unwrap()
    }

    /// Returns `true` if it matches any index.
    pub fn matches_all(&self) -> bool {
        self.0 == "*"
    }

    /// Returns `true` if the pattern matches a specific index name.
    pub fn is_exact(&self) -> bool {
        !self.0.ends_with('*')
    }

    /// Returns wether this index uid matches this index uid pattern.
    pub fn matches(&self, uid: &IndexUid) -> bool {
        self.matches_str(uid.as_str())
    }

    /// Returns wether this string matches this index uid pattern.
    pub fn matches_str(&self, uid: &str) -> bool {
        match self.0.strip_suffix('*') {
            Some(prefix) => uid.starts_with(prefix),
            None => self.0 == uid,
        }
    }
}

impl Deref for IndexUidPattern {
    type Target = str;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl Borrow<str> for IndexUidPattern {
    fn borrow(&self) -> &str {
        &self.0
    }
}

impl TryFrom<String> for IndexUidPattern {
    type Error = IndexUidPatternFormatError;

    fn try_from(uid: String) -> Result<Self, Self::Error> {
        let result = match uid.strip_suffix('*') {
            Some("") => Ok(IndexUidPattern(uid)),
            Some(prefix) => IndexUid::from_str(prefix).map(|_| IndexUidPattern(uid)),
            None => IndexUid::try_from(uid).map(IndexUid::into_inner).map(IndexUidPattern),
        };

        match result {
            Ok(index_uid_pattern) => Ok(index_uid_pattern),
            Err(IndexUidFormatError { invalid_uid }) => {
                Err(IndexUidPatternFormatError { invalid_uid })
            }
        }
    }
}

impl FromStr for IndexUidPattern {
    type Err = IndexUidPatternFormatError;

    fn from_str(uid: &str) -> Result<IndexUidPattern, IndexUidPatternFormatError> {
        uid.to_string().try_into()
    }
}

impl From<IndexUidPattern> for String {
    fn from(IndexUidPattern(uid): IndexUidPattern) -> Self {
        uid
    }
}

#[derive(Debug)]
pub struct IndexUidPatternFormatError {
    pub invalid_uid: String,
}

impl fmt::Display for IndexUidPatternFormatError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "`{}` is not a valid index uid pattern. Index uid patterns \
            can be an integer or a string containing only alphanumeric \
            characters, hyphens (-), underscores (_), and \
            optionally end with a star (*).",
            self.invalid_uid,
        )
    }
}

impl Error for IndexUidPatternFormatError {}

impl ErrorCode for IndexUidPatternFormatError {
    fn error_code(&self) -> Code {
        Code::InvalidIndexUid
    }
}
