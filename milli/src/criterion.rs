use std::fmt;
use std::str::FromStr;

use serde::{Deserialize, Serialize};

use crate::error::{Error, UserError};

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
pub enum Criterion {
    /// Sorted by decreasing number of matched query terms.
    /// Query words at the front of an attribute is considered better than if it was at the back.
    Words,
    /// Sorted by increasing number of typos.
    Typo,
    /// Dynamically sort at query time the documents. None, one or multiple Asc/Desc sortable
    /// attributes can be used in place of this criterion at query time.
    Sort,
    /// Sorted by increasing distance between matched query terms.
    Proximity,
    /// Documents with quey words contained in more important
    /// attributes are considered better.
    Attribute,
    /// Sorted by the similarity of the matched words with the query words.
    Exactness,
    /// Sorted by the increasing value of the field specified.
    Asc(String),
    /// Sorted by the decreasing value of the field specified.
    Desc(String),
}

impl Criterion {
    /// Returns the field name parameter of this criterion.
    pub fn field_name(&self) -> Option<&str> {
        match self {
            Criterion::Asc(name) | Criterion::Desc(name) => Some(name),
            _otherwise => None,
        }
    }
}

impl FromStr for Criterion {
    type Err = Error;

    fn from_str(text: &str) -> Result<Criterion, Self::Err> {
        match text {
            "words" => Ok(Criterion::Words),
            "typo" => Ok(Criterion::Typo),
            "sort" => Ok(Criterion::Sort),
            "proximity" => Ok(Criterion::Proximity),
            "attribute" => Ok(Criterion::Attribute),
            "exactness" => Ok(Criterion::Exactness),
            text => match AscDesc::from_str(text) {
                Ok(AscDesc::Asc(field)) => Ok(Criterion::Asc(field)),
                Ok(AscDesc::Desc(field)) => Ok(Criterion::Desc(field)),
                Err(error) => Err(error.into()),
            },
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
pub enum AscDesc {
    Asc(String),
    Desc(String),
}

impl AscDesc {
    pub fn field(&self) -> &str {
        match self {
            AscDesc::Asc(field) => field,
            AscDesc::Desc(field) => field,
        }
    }
}

impl FromStr for AscDesc {
    type Err = UserError;

    fn from_str(text: &str) -> Result<AscDesc, Self::Err> {
        match text.rsplit_once(':') {
            Some((field_name, "asc")) => Ok(AscDesc::Asc(field_name.to_string())),
            Some((field_name, "desc")) => Ok(AscDesc::Desc(field_name.to_string())),
            _ => Err(UserError::InvalidCriterionName { name: text.to_string() }),
        }
    }
}

pub fn default_criteria() -> Vec<Criterion> {
    vec![
        Criterion::Words,
        Criterion::Typo,
        Criterion::Sort,
        Criterion::Proximity,
        Criterion::Attribute,
        Criterion::Exactness,
    ]
}

impl fmt::Display for Criterion {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        use Criterion::*;

        match self {
            Words => f.write_str("words"),
            Typo => f.write_str("typo"),
            Sort => f.write_str("sort"),
            Proximity => f.write_str("proximity"),
            Attribute => f.write_str("attribute"),
            Exactness => f.write_str("exactness"),
            Asc(attr) => write!(f, "{}:asc", attr),
            Desc(attr) => write!(f, "{}:desc", attr),
        }
    }
}
