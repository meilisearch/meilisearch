use super::{Context, Criterion};
use crate::RawDocument;
use std::cmp::Ordering;

pub struct DocumentId;

impl Criterion for DocumentId {
    fn name(&self) -> &str {
        "stable document id"
    }

    fn evaluate(&self, _ctx: &Context, lhs: &RawDocument, rhs: &RawDocument) -> Ordering {
        let lhs = &lhs.id;
        let rhs = &rhs.id;

        lhs.cmp(rhs)
    }
}
