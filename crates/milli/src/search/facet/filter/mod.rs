use std::collections::BTreeSet;
use std::fmt::{Debug, Display};

pub use filter_parser::{Error as FPError, FilterCondition};

use super::facet_range_search;
use crate::error::{Error, UserError};

mod index_filter;
pub use self::index_filter::IndexFilter;
mod parser;
mod vector;

#[cfg(test)]
mod tests;

/// The maximum number of filters the filter AST can process.
const MAX_FILTER_DEPTH: usize = 2000;
/// magic field name to use filter on shards
pub const SHARD_FIELD: &str = "_shard";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Filter<'a> {
    pub condition: FilterCondition<'a>,
}

#[derive(Debug)]
pub enum BadGeoError {
    Lat(f64),
    Lng(f64),
    InvalidResolution(usize),
    BoundingBoxTopIsBelowBottom(f64, f64),
}

impl std::error::Error for BadGeoError {}

impl Display for BadGeoError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::BoundingBoxTopIsBelowBottom(top, bottom) => {
                write!(f, "The top latitude `{top}` is below the bottom latitude `{bottom}`.")
            }
            Self::InvalidResolution(resolution) => write!(
                f,
                "Invalid resolution `{resolution}`. Resolution must be between 3 and 1000."
            ),
            Self::Lat(lat) => write!(
                f,
                "Bad latitude `{}`. Latitude must be contained between -90 and 90 degrees.",
                lat
            ),
            Self::Lng(lng) => {
                let normalized = (lng + 180.0).rem_euclid(360.0) - 180.0;
                write!(
                    f,
                    "Bad longitude `{}`. Longitude must be contained between -180 and 180 degrees. Hint: try using `{normalized}` instead.",
                    lng
                )
            }
        }
    }
}

#[derive(Debug)]
enum FilterError<'a> {
    AttributeNotFilterable { attribute: &'a str, filterable_patterns: BTreeSet<&'a str> },
    ParseGeoError(BadGeoError),
    TooDeep,
}
impl std::error::Error for FilterError<'_> {}

impl From<BadGeoError> for FilterError<'_> {
    fn from(geo_error: BadGeoError) -> Self {
        FilterError::ParseGeoError(geo_error)
    }
}

impl Display for FilterError<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::AttributeNotFilterable { attribute, filterable_patterns } => {
                write!(f, "Attribute `{attribute}` is not filterable.")?;
                if filterable_patterns.is_empty() {
                    write!(f, " This index does not have configured filterable attributes.")
                } else {
                    write!(f, " Available filterable attribute patterns are: ")?;
                    let mut filterables_list =
                        filterable_patterns.iter().map(AsRef::as_ref).collect::<Vec<&str>>();
                    filterables_list.sort_unstable();
                    for (idx, filterable) in filterables_list.iter().enumerate() {
                        write!(f, "`{filterable}`")?;
                        if idx != filterables_list.len() - 1 {
                            write!(f, ", ")?;
                        }
                    }
                    write!(f, ".")
                }
            }
            Self::TooDeep => write!(
                f,
                "Too many filter conditions, can't process more than {} filters.",
                MAX_FILTER_DEPTH
            ),
            Self::ParseGeoError(error) => write!(f, "{}", error),
        }
    }
}

impl<'a> From<FPError<'a>> for Error {
    fn from(error: FPError<'a>) -> Self {
        Self::UserError(UserError::InvalidFilter(error.to_string()))
    }
}

impl<'a> From<Filter<'a>> for FilterCondition<'a> {
    fn from(f: Filter<'a>) -> Self {
        f.condition
    }
}

impl<'a> From<FilterCondition<'a>> for Filter<'a> {
    fn from(fc: FilterCondition<'a>) -> Self {
        Self { condition: fc }
    }
}
