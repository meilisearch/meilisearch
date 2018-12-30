use sdset::multi::OpBuilder as SdOpBuilder;
use sdset::Set;

use crate::database::blob::NegativeBlob;
use crate::data::DocIds;
use crate::DocumentId;

pub struct OpBuilder<'a> {
    inner: SdOpBuilder<'a, DocumentId>,
}

/// Do a set operation on multiple negative blobs.
impl<'a> OpBuilder<'a> {
    pub fn new() -> Self {
        Self { inner: SdOpBuilder::new() }
    }

    pub fn with_capacity(cap: usize) -> Self {
        Self { inner: SdOpBuilder::with_capacity(cap) }
    }

    pub fn add(mut self, blob: &'a NegativeBlob) -> Self {
        self.push(blob);
        self
    }

    pub fn push(&mut self, blob: &'a NegativeBlob) {
        let set = Set::new_unchecked(blob.as_ref());
        self.inner.push(set);
    }

    pub fn union(self) -> Union<'a> {
        Union::new(self.inner.union())
    }

    pub fn intersection(self) -> Intersection<'a> {
        Intersection::new(self.inner.intersection())
    }

    pub fn difference(self) -> Difference<'a> {
        Difference::new(self.inner.difference())
    }

    pub fn symmetric_difference(self) -> SymmetricDifference<'a> {
        SymmetricDifference::new(self.inner.symmetric_difference())
    }
}

macro_rules! logical_operation {
    (struct $name:ident, $operation:ident) => {

pub struct $name<'a> {
    op: sdset::multi::$name<'a, DocumentId>,
}

impl<'a> $name<'a> {
    fn new(op: sdset::multi::$name<'a, DocumentId>) -> Self {
        $name { op }
    }

    pub fn into_negative_blob(self) -> NegativeBlob {
        let document_ids = sdset::SetOperation::into_set_buf(self.op);
        let doc_ids = DocIds::from_raw(document_ids.into_vec());
        NegativeBlob::from_raw(doc_ids)
    }
}

}}

logical_operation!(struct Union, union);
logical_operation!(struct Intersection, intersection);
logical_operation!(struct Difference, difference);
logical_operation!(struct SymmetricDifference, symmetric_difference);
