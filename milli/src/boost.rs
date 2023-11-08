//! This module provides the `Boost` type and defines all the errors related to this type.

use std::str::FromStr;

use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::RankingRuleError;

/// This error type is never supposed to be shown to the end user.
/// You must always cast it to a sort error or a criterion error.
#[derive(Error, Debug)]
pub enum BoostError {
    #[error("Invalid syntax for the boost parameter: expected expression ending by `boost:`, found `{name}`.")]
    InvalidSyntax { name: String },
}

impl From<BoostError> for RankingRuleError {
    fn from(error: BoostError) -> Self {
        match error {
            BoostError::InvalidSyntax { name } => RankingRuleError::InvalidName { name },
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
pub struct Boost(pub String);

impl Boost {
    pub fn filter(&self) -> &str {
        &self.0
    }
}

impl FromStr for Boost {
    type Err = BoostError;

    fn from_str(text: &str) -> Result<Boost, Self::Err> {
        match text.split_once(':') {
            Some(("boost", right)) => Ok(Boost(right.to_string())), // TODO check filter validity
            _ => Err(BoostError::InvalidSyntax { name: text.to_string() }),
        }
    }
}

#[cfg(test)]
mod tests {
    use big_s::S;
    use BoostError::*;

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
            ("_geoPoint(90.000000000, 180):desc", Desc(Geo([90., 180.]))),
            ("_geoPoint(-90, -180.0000000000):asc", Asc(Geo([-90., -180.]))),
            ("_geoPoint(42.0002, 59.895):desc", Desc(Geo([42.0002, 59.895]))),
            ("_geoPoint(42., 59.):desc", Desc(Geo([42., 59.]))),
            ("truc(12, 13):desc", Desc(Field(S("truc(12, 13)")))),
        ];

        for (req, expected) in valid_req {
            let res = req.parse::<Boost>();
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
            ("_geoPoint(200, 200):asc", GeoError(BadGeoError::Lat(200.))),
            ("_geoPoint(90.000001, 0):asc", GeoError(BadGeoError::Lat(90.000001))),
            ("_geoPoint(0, -180.000001):desc", GeoError(BadGeoError::Lng(-180.000001))),
            ("_geoPoint(159.256, 130):asc", GeoError(BadGeoError::Lat(159.256))),
            ("_geoPoint(12, -2021):desc", GeoError(BadGeoError::Lng(-2021.))),
            ("_geo(12, -2021):asc", ReservedKeyword { name: S("_geo(12, -2021)") }),
            ("_geo(12, -2021):desc", ReservedKeyword { name: S("_geo(12, -2021)") }),
            ("_geoDistance(12, -2021):asc", ReservedKeyword { name: S("_geoDistance(12, -2021)") }),
            (
                "_geoDistance(12, -2021):desc",
                ReservedKeyword { name: S("_geoDistance(12, -2021)") },
            ),
        ];

        for (req, expected_error) in invalid_req {
            let res = req.parse::<Boost>();
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
