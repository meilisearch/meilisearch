use std::fmt;
use std::str::FromStr;

use serde::{Deserialize, Serialize};

use crate::error::{is_reserved_keyword, Error, UserError};

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
pub enum Criterion {
    /// Sorted by decreasing number of matched query terms.
    /// Query words at the front of an attribute is considered better than if it was at the back.
    Words,
    /// Sorted by increasing number of typos.
    Typo,
    /// Sorted by increasing distance between matched query terms.
    Proximity,
    /// Documents with quey words contained in more important
    /// attributes are considered better.
    Attribute,
    /// Dynamically sort at query time the documents. None, one or multiple Asc/Desc sortable
    /// attributes can be used in place of this criterion at query time.
    Sort,
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
            "proximity" => Ok(Criterion::Proximity),
            "attribute" => Ok(Criterion::Attribute),
            "sort" => Ok(Criterion::Sort),
            "exactness" => Ok(Criterion::Exactness),
            text => match AscDesc::from_str(text) {
                Ok(AscDesc::Asc(Member::Field(field))) if is_reserved_keyword(&field) => {
                    Err(UserError::InvalidReservedRankingRuleName { name: text.to_string() })?
                }
                Ok(AscDesc::Asc(Member::Field(field))) => Ok(Criterion::Asc(field)),
                Ok(AscDesc::Desc(Member::Field(field))) => Ok(Criterion::Desc(field)),
                Ok(AscDesc::Asc(Member::Geo(_))) | Ok(AscDesc::Desc(Member::Geo(_))) => {
                    Err(UserError::InvalidRankingRuleName { name: text.to_string() })?
                }
                Err(UserError::InvalidAscDescSyntax { name }) => {
                    Err(UserError::InvalidCriterionName { name }.into())
                }
                Err(error) => {
                    Err(UserError::InvalidCriterionName { name: error.to_string() }.into())
                }
            },
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
pub enum Member {
    Field(String),
    Geo([f64; 2]),
}

impl FromStr for Member {
    type Err = UserError;

    fn from_str(text: &str) -> Result<Member, Self::Err> {
        if text.starts_with("_geoPoint(") {
            let point =
                text.strip_prefix("_geoPoint(")
                    .and_then(|point| point.strip_suffix(")"))
                    .ok_or_else(|| UserError::InvalidRankingRuleName { name: text.to_string() })?;
            let point = point
                .split(',')
                .map(|el| el.trim().parse())
                .collect::<Result<Vec<f64>, _>>()
                .map_err(|_| UserError::InvalidRankingRuleName { name: text.to_string() })?;
            Ok(Member::Geo([point[0], point[1]]))
        } else {
            Ok(Member::Field(text.to_string()))
        }
    }
}

impl fmt::Display for Member {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Member::Field(name) => write!(f, "{}", name),
            Member::Geo([lat, lng]) => write!(f, "_geoPoint({}, {})", lat, lng),
        }
    }
}

impl Member {
    pub fn field(&self) -> Option<&str> {
        match self {
            Member::Field(field) => Some(field),
            Member::Geo(_) => None,
        }
    }

    pub fn geo_point(&self) -> Option<&[f64; 2]> {
        match self {
            Member::Geo(point) => Some(point),
            Member::Field(_) => None,
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
pub enum AscDesc {
    Asc(Member),
    Desc(Member),
}

impl AscDesc {
    pub fn member(&self) -> &Member {
        match self {
            AscDesc::Asc(member) => member,
            AscDesc::Desc(member) => member,
        }
    }

    pub fn field(&self) -> Option<&str> {
        self.member().field()
    }
}

impl FromStr for AscDesc {
    type Err = UserError;

    /// Since we don't know if this was deserialized for a criterion or a sort we just return a
    /// string and let the caller create his own error
    fn from_str(text: &str) -> Result<AscDesc, Self::Err> {
        match text.rsplit_once(':') {
            Some((left, "asc")) => Ok(AscDesc::Asc(left.parse()?)),
            Some((left, "desc")) => Ok(AscDesc::Desc(left.parse()?)),
            _ => Err(UserError::InvalidRankingRuleName { name: text.to_string() }),
        }
    }
}

pub fn default_criteria() -> Vec<Criterion> {
    vec![
        Criterion::Words,
        Criterion::Typo,
        Criterion::Proximity,
        Criterion::Attribute,
        Criterion::Sort,
        Criterion::Exactness,
    ]
}

impl fmt::Display for Criterion {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        use Criterion::*;

        match self {
            Words => f.write_str("words"),
            Typo => f.write_str("typo"),
            Proximity => f.write_str("proximity"),
            Attribute => f.write_str("attribute"),
            Sort => f.write_str("sort"),
            Exactness => f.write_str("exactness"),
            Asc(attr) => write!(f, "{}:asc", attr),
            Desc(attr) => write!(f, "{}:desc", attr),
        }
    }
}
