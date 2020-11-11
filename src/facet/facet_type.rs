use std::cmp;

#[derive(Debug, Copy, Clone, PartialOrd, Ord, PartialEq, Eq, Hash)]
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
