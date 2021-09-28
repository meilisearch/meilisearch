//! This module provides the `AscDesc` type and defines all the errors related to this type.

use std::fmt;
use std::str::FromStr;

use serde::{Deserialize, Serialize};

use crate::error::is_reserved_keyword;
use crate::{CriterionError, Error, UserError};

/// This error type is never supposed to be shown to the end user.
/// You must always cast it to a sort error or a criterion error.
#[derive(Debug)]
pub enum AscDescError {
    InvalidSyntax { name: String },
    ReservedKeyword { name: String },
}

impl fmt::Display for AscDescError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Self::InvalidSyntax { name } => {
                write!(f, "invalid asc/desc syntax for {}", name)
            }
            Self::ReservedKeyword { name } => {
                write!(
                    f,
                    "{} is a reserved keyword and thus can't be used as a asc/desc rule",
                    name
                )
            }
        }
    }
}

impl From<AscDescError> for CriterionError {
    fn from(error: AscDescError) -> Self {
        match error {
            AscDescError::InvalidSyntax { name } => CriterionError::InvalidName { name },
            AscDescError::ReservedKeyword { name } if name.starts_with("_geoPoint") => {
                CriterionError::ReservedNameForSort { name: "_geoPoint".to_string() }
            }
            AscDescError::ReservedKeyword { name } if name.starts_with("_geoRadius") => {
                CriterionError::ReservedNameForFilter { name: "_geoRadius".to_string() }
            }
            AscDescError::ReservedKeyword { name } => CriterionError::ReservedName { name },
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
pub enum Member {
    Field(String),
    Geo([f64; 2]),
}

impl FromStr for Member {
    type Err = AscDescError;

    fn from_str(text: &str) -> Result<Member, Self::Err> {
        match text.strip_prefix("_geoPoint(").and_then(|text| text.strip_suffix(")")) {
            Some(point) => {
                let (lat, long) = point
                    .split_once(',')
                    .ok_or_else(|| AscDescError::ReservedKeyword { name: text.to_string() })
                    .and_then(|(lat, long)| {
                        lat.trim()
                            .parse()
                            .and_then(|lat| long.trim().parse().map(|long| (lat, long)))
                            .map_err(|_| AscDescError::ReservedKeyword { name: text.to_string() })
                    })?;
                Ok(Member::Geo([lat, long]))
            }
            None => {
                if is_reserved_keyword(text) || text.starts_with("_geoRadius(") {
                    return Err(AscDescError::ReservedKeyword { name: text.to_string() })?;
                }
                Ok(Member::Field(text.to_string()))
            }
        }
    }
}

impl fmt::Display for Member {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Member::Field(name) => f.write_str(name),
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
    type Err = AscDescError;

    /// Since we don't know if this was deserialized for a criterion or a sort we just return a
    /// string and let the caller create his own error.
    fn from_str(text: &str) -> Result<AscDesc, Self::Err> {
        match text.rsplit_once(':') {
            Some((left, "asc")) => Ok(AscDesc::Asc(left.parse()?)),
            Some((left, "desc")) => Ok(AscDesc::Desc(left.parse()?)),
            _ => Err(AscDescError::InvalidSyntax { name: text.to_string() }),
        }
    }
}

#[derive(Debug)]
pub enum SortError {
    BadGeoPointUsage { name: String },
    InvalidName { name: String },
    ReservedName { name: String },
    ReservedNameForSettings { name: String },
    ReservedNameForFilter { name: String },
}

impl From<AscDescError> for SortError {
    fn from(error: AscDescError) -> Self {
        match error {
            AscDescError::InvalidSyntax { name } => SortError::InvalidName { name },
            AscDescError::ReservedKeyword { name } if name.starts_with("_geoPoint") => {
                SortError::BadGeoPointUsage { name }
            }
            AscDescError::ReservedKeyword { name } if &name == "_geo" => {
                SortError::ReservedNameForSettings { name: "_geoPoint".to_string() }
            }
            AscDescError::ReservedKeyword { name } if name.starts_with("_geoRadius") => {
                SortError::ReservedNameForFilter { name: "_geoRadius".to_string() }
            }
            AscDescError::ReservedKeyword { name } => SortError::ReservedName { name },
        }
    }
}

impl fmt::Display for SortError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Self::BadGeoPointUsage { name } => {
                write!(
                    f,
                    "invalid syntax for the `_geoPoint` parameter: `{}`. \
Usage: `_geoPoint(latitude, longitude):asc`",
                    name
                )
            }
            Self::InvalidName { name } => {
                write!(f, "invalid syntax for the sort parameter {}", name)
            }
            Self::ReservedName { name } => {
                write!(
                    f,
                    "{} is a reserved keyword and thus can't be used as a sort expression",
                    name
                )
            }
            Self::ReservedNameForSettings { name } => {
                write!(
                    f,
                    "{} is a reserved keyword and thus can't be used as a sort expression. \
{} can only be used in the settings",
                    name, name
                )
            }
            Self::ReservedNameForFilter { name } => {
                write!(
                    f,
                    "{} is a reserved keyword and thus can't be used as a sort expression. \
{} can only be used for filtering at search time",
                    name, name
                )
            }
        }
    }
}

impl From<SortError> for Error {
    fn from(error: SortError) -> Self {
        Self::UserError(UserError::SortError(error))
    }
}

#[cfg(test)]
mod tests {
    use big_s::S;
    use AscDesc::*;
    use AscDescError::*;
    use Member::*;

    use super::*;

    #[test]
    fn parse_asc_desc() {
        let valid_req = [
            ("truc:asc", Asc(Field(S("truc")))),
            ("bidule:desc", Desc(Field(S("bidule")))),
            ("a-b:desc", Desc(Field(S("a-b")))),
            ("a:b:desc", Desc(Field(S("a:b")))),
            ("a12:asc", Asc(Field(S("a12")))),
            ("42:asc", Asc(Field(S("42")))),
            ("_geoPoint(42, 59):asc", Asc(Geo([42., 59.]))),
            ("_geoPoint(42.459, 59):desc", Desc(Geo([42.459, 59.]))),
            ("_geoPoint(42, 59.895):desc", Desc(Geo([42., 59.895]))),
            ("_geoPoint(42, 59.895):desc", Desc(Geo([42., 59.895]))),
            ("_geoPoint(42.0002, 59.895):desc", Desc(Geo([42.0002, 59.895]))),
            ("_geoPoint(42., 59.):desc", Desc(Geo([42., 59.]))),
            ("truc(12, 13):desc", Desc(Field(S("truc(12, 13)")))),
        ];

        for (req, expected) in valid_req {
            let res = req.parse::<AscDesc>();
            assert!(
                res.is_ok(),
                "Failed to parse `{}`, was expecting `{:?}` but instead got `{:?}`",
                req,
                expected,
                res
            );
            assert_eq!(res.unwrap(), expected);
        }

        let invalid_req = [
            ("truc:machin", InvalidSyntax { name: S("truc:machin") }),
            ("truc:deesc", InvalidSyntax { name: S("truc:deesc") }),
            ("truc:asc:deesc", InvalidSyntax { name: S("truc:asc:deesc") }),
            ("42desc", InvalidSyntax { name: S("42desc") }),
            ("_geoPoint:asc", ReservedKeyword { name: S("_geoPoint") }),
            ("_geoDistance:asc", ReservedKeyword { name: S("_geoDistance") }),
            ("_geoPoint(42.12 , 59.598)", InvalidSyntax { name: S("_geoPoint(42.12 , 59.598)") }),
            (
                "_geoPoint(42.12 , 59.598):deesc",
                InvalidSyntax { name: S("_geoPoint(42.12 , 59.598):deesc") },
            ),
            (
                "_geoPoint(42.12 , 59.598):machin",
                InvalidSyntax { name: S("_geoPoint(42.12 , 59.598):machin") },
            ),
            (
                "_geoPoint(42.12 , 59.598):asc:aasc",
                InvalidSyntax { name: S("_geoPoint(42.12 , 59.598):asc:aasc") },
            ),
            (
                "_geoPoint(42,12 , 59,598):desc",
                ReservedKeyword { name: S("_geoPoint(42,12 , 59,598)") },
            ),
            ("_geoPoint(35, 85, 75):asc", ReservedKeyword { name: S("_geoPoint(35, 85, 75)") }),
            ("_geoPoint(18):asc", ReservedKeyword { name: S("_geoPoint(18)") }),
        ];

        for (req, expected_error) in invalid_req {
            let res = req.parse::<AscDesc>();
            assert!(
                res.is_err(),
                "Should no be able to parse `{}`, was expecting an error but instead got: `{:?}`",
                req,
                res,
            );
            let res = res.unwrap_err();
            assert_eq!(
                res.to_string(),
                expected_error.to_string(),
                "Bad error for input {}: got `{:?}` instead of `{:?}`",
                req,
                res,
                expected_error
            );
        }
    }
}
