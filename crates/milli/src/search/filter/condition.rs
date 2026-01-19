use std::fmt;

/// Represents a filter condition.
/// This is a simplified version that will be expanded in issue #3642.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FilterCondition<'a> {
    /// Equality filter: field = value
    Equal(&'a str, &'a str),
}

impl<'a> fmt::Display for FilterCondition<'a> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            FilterCondition::Equal(field, value) => write!(f, "{} = {}", field, value),
        }
    }
}

impl<'a> fmt::Display for FilterCondition<'a> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            FilterCondition::Equal(field, value) => write!(f, "{} = {}", field, value),
            FilterCondition::GreaterThan(field, value) => write!(f, "{} > {}", field, value),
            FilterCondition::GreaterThanOrEqual(field, value) => write!(f, "{} >= {}", field, value),
            FilterCondition::LessThan(field, value) => write!(f, "{} < {}", field, value),
            FilterCondition::LessThanOrEqual(field, value) => write!(f, "{} <= {}", field, value),
        }
    }
}
