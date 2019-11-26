use crate::criterion::Criterion;
use crate::RawDocument;
use std::cmp::Ordering;

#[derive(Debug, Clone, Copy)]
pub struct DocumentId;

impl Criterion for DocumentId {
    fn evaluate(&self, lhs: &RawDocument, rhs: &RawDocument) -> Ordering {
        lhs.id.cmp(&rhs.id)
    }

    fn name(&self) -> &str {
        "DocumentId"
    }
}
