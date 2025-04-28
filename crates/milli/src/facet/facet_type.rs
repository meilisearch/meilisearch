use std::error::Error;
use std::fmt;
use std::str::FromStr;

use serde::{Deserialize, Serialize};

#[derive(Debug, Copy, Clone, PartialOrd, Ord, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum FacetType {
    String,
    Number,
}

impl fmt::Display for FacetType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::String => f.write_str("string"),
            Self::Number => f.write_str("number"),
        }
    }
}

impl FromStr for FacetType {
    type Err = InvalidFacetType;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if s.trim().eq_ignore_ascii_case("string") {
            Ok(Self::String)
        } else if s.trim().eq_ignore_ascii_case("number") {
            Ok(Self::Number)
        } else {
            Err(InvalidFacetType)
        }
    }
}

#[derive(Debug, Copy, Clone, PartialOrd, Ord, PartialEq, Eq, Hash)]
pub struct InvalidFacetType;

impl fmt::Display for InvalidFacetType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(r#"Invalid facet type, must be "string" or "number""#)
    }
}

impl Error for InvalidFacetType {}
