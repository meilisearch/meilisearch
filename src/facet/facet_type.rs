use std::cmp;
use serde::{Serialize, Deserialize};

#[derive(Debug, Copy, Clone, PartialOrd, Ord, PartialEq, Eq, Hash)]
#[derive(Serialize, Deserialize)]
pub enum FacetType {
    String,
    F64,
    I64,
    U64,
}

impl FacetType {
    pub fn merge(a: FacetType, b: FacetType) -> FacetType {
        cmp::min(a, b)
    }
}
