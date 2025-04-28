use deserr::Deserr;
use milli::OrderBy;
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

#[derive(Debug, Default, Copy, Clone, PartialEq, Eq, Serialize, Deserialize, Deserr, ToSchema)]
#[serde(rename_all = "camelCase")]
#[deserr(rename_all = camelCase)]
pub enum FacetValuesSort {
    /// Facet values are sorted in alphabetical order, ascending from A to Z.
    #[default]
    Alpha,
    /// Facet values are sorted by decreasing count.
    /// The count is the number of records containing this facet value in the results of the query.
    Count,
}

impl From<FacetValuesSort> for OrderBy {
    fn from(val: FacetValuesSort) -> Self {
        match val {
            FacetValuesSort::Alpha => Self::Lexicographic,
            FacetValuesSort::Count => Self::Count,
        }
    }
}

impl From<OrderBy> for FacetValuesSort {
    fn from(val: OrderBy) -> Self {
        match val {
            OrderBy::Lexicographic => Self::Alpha,
            OrderBy::Count => Self::Count,
        }
    }
}
