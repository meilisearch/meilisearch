use std::error::Error;
use std::fmt;
use std::str::FromStr;

use serde::{Serialize, Deserialize};

#[derive(Debug, Copy, Clone, PartialOrd, Ord, PartialEq, Eq, Hash)]
#[derive(Serialize, Deserialize)]
pub enum FacetType {
    String,
    Float,
    Integer,
}

impl fmt::Display for FacetType {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            FacetType::String => f.write_str("string"),
            FacetType::Float => f.write_str("float"),
            FacetType::Integer => f.write_str("integer"),
        }
    }
}

impl FromStr for FacetType {
    type Err = InvalidFacetType;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if s.eq_ignore_ascii_case("string") {
            Ok(FacetType::String)
        } else if s.eq_ignore_ascii_case("float") {
            Ok(FacetType::Float)
        } else if s.eq_ignore_ascii_case("integer") {
            Ok(FacetType::Integer)
        } else {
            Err(InvalidFacetType)
        }
    }
}

#[derive(Debug, Copy, Clone, PartialOrd, Ord, PartialEq, Eq, Hash)]
pub struct InvalidFacetType;

impl fmt::Display for InvalidFacetType {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str(r#"Invalid facet type, must be "string", "float" or "integer""#)
    }
}

impl Error for InvalidFacetType { }
