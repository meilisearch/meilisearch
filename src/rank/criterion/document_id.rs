use std::cmp::Ordering;
use std::ops::Deref;

use crate::rank::criterion::Criterion;
use crate::rank::RawDocument;

#[derive(Debug, Clone, Copy)]
pub struct DocumentId;

impl Criterion for DocumentId {
    fn evaluate(&self, lhs: &RawDocument, rhs: &RawDocument) -> Ordering {
        lhs.id.cmp(&rhs.id)
    }
}
