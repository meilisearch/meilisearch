use std::error::Error;
use std::fmt;
use std::str::FromStr;

use serde::{Deserialize, Serialize};

use crate::error::{Code, ErrorCode};
use crate::index_uid::{IndexUid, IndexUidFormatError};

/// An index uid pattern is composed of only ascii alphanumeric characters, - and _, between 1 and 400
/// bytes long and optionally ending with a *.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct IndexUidPattern(
    #[cfg_attr(feature = "test-traits", proptest(regex("[a-zA-Z0-9_-]{1,400}\\*?")))] String,
);

impl IndexUidPattern {
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

impl std::ops::Deref for IndexUidPattern {
    type Target = str;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl TryFrom<String> for IndexUidPattern {
    type Error = IndexUidPatternFormatError;

    fn try_from(uid: String) -> Result<Self, Self::Error> {
        let result = match uid.strip_suffix('*') {
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
        // TODO should I return a new error code?
        Code::InvalidIndexUid
    }
}
