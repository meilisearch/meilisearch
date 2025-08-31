use std::fmt;

use super::FilterCondition;

/// Filter structure that can be used to filter documents in search results.
/// This will be enhanced with sub-object filtering in issue #3642.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Filter<'a> {
    /// The filter condition
    pub condition: FilterCondition<'a>,
}

impl<'a> fmt::Display for Filter<'a> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.condition)
    }
}

impl<'a> Filter<'a> {
    /// Create a new filter with the given condition
    pub fn new(condition: FilterCondition<'a>) -> Self {
        Self { condition }
    }
}

impl<'a> Filter<'a> {
    /// Execute the filter and return matching document IDs
    pub fn execute(&self, index: &Index) -> Result<RoaringBitmap, Box<dyn std::error::Error>> {
        match self {
            Filter::Condition(condition) => condition.execute(index),
            Filter::SubObject(sub_object) => sub_object.execute(index),
        }
    }
}
use std::fmt;
use std::collections::BTreeSet;
use roaring::RoaringBitmap;

use crate::search::filter::FilterCondition;

/// Filter structure that can be used to filter documents in search results.
/// This is a simplified version for testing purposes.
/// The complete implementation for sub-object filtering will be done in issue #3642.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Filter<'a> {
    /// A simple filter condition
    Condition(FilterCondition<'a>),
    /// Multiple conditions combined with AND
    And(Vec<FilterCondition<'a>>),
    /// Multiple conditions combined with OR
    Or(Vec<FilterCondition<'a>>),
    // SubObject variant will be added in issue #3642
    // SubObject(SubObjectFilter<'a>),
}

impl<'a> fmt::Display for Filter<'a> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Filter::Condition(condition) => write!(f, "{}", condition),
            Filter::And(conditions) => {
                let conditions: Vec<_> = conditions.iter().map(|c| format!("{}", c)).collect();
                write!(f, "({})", conditions.join(" AND "))
            },
            Filter::Or(conditions) => {
                let conditions: Vec<_> = conditions.iter().map(|c| format!("{}", c)).collect();
                write!(f, "({})", conditions.join(" OR "))
            },
            // Filter::SubObject(sub_object) => write!(f, "{}", sub_object),
        }
    }
}
impl<'a> fmt::Display for Filter<'a> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Filter::Condition(condition) => write!(f, "{}", condition),
            Filter::SubObject(sub_object) => write!(f, "{}", sub_object),
        }
    }
}
