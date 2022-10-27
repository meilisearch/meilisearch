use serde::{Deserialize, Serialize};
use std::cmp::Eq;
use std::error::Error;
use std::fmt;
use std::ops::Deref;
use std::str::FromStr;
const PATTERN_IDENTIFIER: char = '*';

#[derive(Debug, Clone, Eq, Hash)]
pub enum IndexType {
    Name(IndexUid),
    Pattern(IndexPattern),
}

impl PartialEq for IndexType {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Name(left), _) => other == left.as_str(),
            (_, Self::Name(right)) => self == right.as_str(),
            (Self::Pattern(left), Self::Pattern(right)) => left.deref() == right.deref(),
        }
    }
}

impl Deref for IndexType {
    type Target = str;
    fn deref(&self) -> &Self::Target {
        match self {
            Self::Name(x) => x.deref(),
            Self::Pattern(x) => x.deref(),
        }
    }
}

impl From<IndexType> for String {
    fn from(x: IndexType) -> Self {
        match x {
            IndexType::Name(y) => y.into_inner(),
            IndexType::Pattern(y) => y.original_pattern,
        }
    }
}

impl PartialEq<str> for IndexType {
    fn eq(&self, other: &str) -> bool {
        match (self, other) {
            (Self::Name(x), y) => x.0 == y,
            (Self::Pattern(x), y) => y.starts_with(&x.prefix),
        }
    }
}

#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub struct IndexPattern {
    prefix: String,
    original_pattern: String,
}

impl IndexPattern {
    fn from_pattern(pattern: String, original_pattern: String) -> Self {
        Self {
            prefix: pattern,
            original_pattern,
        }
    }
}

impl Deref for IndexPattern {
    type Target = str;
    fn deref(&self) -> &Self::Target {
        &self.original_pattern
    }
}

#[derive(Debug)]
pub struct IndexPatternError(String);

impl fmt::Display for IndexPatternError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Pattern should end with {}. Received => {}",
            PATTERN_IDENTIFIER, self.0
        )
    }
}

impl Error for IndexPatternError {}

#[derive(Debug)]
pub enum IndexTypeError {
    Name(IndexUidFormatError),
    Pattern(IndexPatternError),
}

impl Error for IndexTypeError {}

impl fmt::Display for IndexTypeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Name(x) => x.fmt(f),
            Self::Pattern(x) => x.fmt(f),
        }
    }
}

impl From<IndexUidFormatError> for IndexTypeError {
    fn from(x: IndexUidFormatError) -> Self {
        Self::Name(x)
    }
}

impl From<IndexPatternError> for IndexTypeError {
    fn from(x: IndexPatternError) -> Self {
        Self::Pattern(x)
    }
}

impl TryFrom<String> for IndexType {
    type Error = IndexTypeError;
    fn try_from(value: String) -> Result<Self, Self::Error> {
        if let Some(x) = value.strip_suffix(PATTERN_IDENTIFIER) {
            Ok(Self::Pattern(IndexPattern::from_pattern(
                x.to_owned(),
                value,
            )))
        } else {
            Ok(Self::Name(IndexUid::try_from(value)?))
        }
    }
}

impl FromStr for IndexType {
    type Err = IndexTypeError;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        s.to_owned().try_into()
    }
}
/// An index uid is composed of only ascii alphanumeric characters, - and _, between 1 and 400
/// bytes long
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "test-traits", derive(proptest_derive::Arbitrary))]
pub struct IndexUid(
    #[cfg_attr(feature = "test-traits", proptest(regex("[a-zA-Z0-9_-]{1,400}")))] String,
);

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

impl std::ops::Deref for IndexUid {
    type Target = str;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl TryFrom<String> for IndexUid {
    type Error = IndexTypeError;

    fn try_from(uid: String) -> Result<Self, Self::Error> {
        if !uid
            .chars()
            .all(|x| x.is_ascii_alphanumeric() || x == '-' || x == '_')
            || uid.is_empty()
            || uid.len() > 400
        {
            Err(IndexTypeError::Name(IndexUidFormatError {
                invalid_uid: uid,
            }))
        } else {
            Ok(IndexUid(uid))
        }
    }
}

impl FromStr for IndexUid {
    type Err = IndexTypeError;

    fn from_str(uid: &str) -> Result<IndexUid, IndexTypeError> {
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
            "invalid index uid `{}`, the uid must be an integer \
            or a string containing only alphanumeric characters \
            a-z A-Z 0-9, hyphens - and underscores _.",
            self.invalid_uid,
        )
    }
}

impl Error for IndexUidFormatError {}
