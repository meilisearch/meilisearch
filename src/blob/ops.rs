use std::collections::BTreeMap;

use fst::{map, Streamer, Automaton};
use fst::automaton::AlwaysMatch;
use sdset::multi::OpBuilder as SdOpBuilder;
use sdset::{SetOperation, Set};

use crate::blob::ops_indexed_value::{
    OpIndexedValueBuilder, UnionIndexedValue,
};
use crate::blob::Blob;
use crate::data::DocIndexes;
use crate::vec_read_only::VecReadOnly;
use crate::DocIndex;

pub struct OpBuilder<'m, A: Automaton> {
    // the operation on the maps is always an union.
    maps: OpIndexedValueBuilder<'m>,
    automatons: Vec<A>,
    indexes: Vec<&'m DocIndexes>,
}

impl<'m> OpBuilder<'m, AlwaysMatch> {
    pub fn new() -> Self {
        Self {
            maps: OpIndexedValueBuilder::new(),
            automatons: vec![AlwaysMatch],
            indexes: Vec::new(),
        }
    }
}

/// Do a set operation on multiple maps with the same automatons.
impl<'m, A: 'm + Automaton> OpBuilder<'m, A> {
    pub fn with_automatons(automatons: Vec<A>) -> Self {
        Self {
            maps: OpIndexedValueBuilder::new(),
            automatons: automatons,
            indexes: Vec::new(),
        }
    }

    pub fn add(mut self, blob: &'m Blob) -> Self
    where A: Clone
    {
        self.push(blob);
        self
    }

    pub fn push(&mut self, blob: &'m Blob)
    where A: Clone
    {
        match blob {
            Blob::Positive(blob) => {
                let mut op = map::OpBuilder::new();
                for automaton in self.automatons.iter().cloned() {
                    let stream = blob.as_map().search(automaton);
                    op.push(stream);
                }

                let stream = op.union();
                let indexes = blob.as_indexes();

                self.maps.push(stream);
                self.indexes.push(indexes);
            },
            Blob::Negative(blob) => {
                unimplemented!()
            },
        }
    }

    pub fn union(self) -> Union<'m> {
        Union::new(self.maps, self.indexes, self.automatons.len())
    }

    pub fn intersection(self) -> Intersection<'m> {
        Intersection::new(self.maps, self.indexes, self.automatons.len())
    }

    pub fn difference(self) -> Difference<'m> {
        Difference::new(self.maps, self.indexes, self.automatons.len())
    }

    pub fn symmetric_difference(self) -> SymmetricDifference<'m> {
        SymmetricDifference::new(self.maps, self.indexes, self.automatons.len())
    }
}

#[derive(Debug, Clone, PartialOrd, Ord, PartialEq, Eq, Hash)]
pub struct IndexedDocIndexes {
    pub index: usize,
    pub doc_indexes: VecReadOnly<DocIndex>,
}

struct SlotIndexedDocIndexes {
    index: usize,
    start: usize,
    len: usize,
}

macro_rules! logical_operation {
    (struct $name:ident, $operation:ident) => {

pub struct $name<'m> {
    maps: UnionIndexedValue<'m>,
    indexes: Vec<&'m DocIndexes>,
    number_automatons: usize,
    outs: Vec<IndexedDocIndexes>,
}

impl<'m> $name<'m> {
    fn new(maps: OpIndexedValueBuilder<'m>, indexes: Vec<&'m DocIndexes>, number_automatons: usize) -> Self {
        $name {
            maps: maps.union(),
            indexes: indexes,
            number_automatons: number_automatons,
            outs: Vec::new(),
        }
    }
}

impl<'m, 'a> fst::Streamer<'a> for $name<'m> {
    type Item = (&'a [u8], &'a [IndexedDocIndexes]);

    fn next(&'a mut self) -> Option<Self::Item> {
        match self.maps.next() {
            Some((input, ivalues)) => {
                self.outs.clear();

                let mut builders = vec![BTreeMap::new(); self.number_automatons];
                for iv in ivalues {
                    let builder = &mut builders[iv.aut_index];
                    builder.insert(iv.rdr_index, iv.value);
                }

                let mut doc_indexes = Vec::new();
                let mut doc_indexes_slots = Vec::with_capacity(builders.len());
                for (aut_index, values) in builders.into_iter().enumerate() {
                    let mut builder = SdOpBuilder::with_capacity(values.len());
                    for (rdr_index, value) in values {
                        let indexes = self.indexes[rdr_index].get(value).expect("could not find indexes");
                        let indexes = Set::new_unchecked(indexes);
                        builder.push(indexes);
                    }

                    let start = doc_indexes.len();
                    builder.$operation().extend_vec(&mut doc_indexes);
                    let len = doc_indexes.len() - start;
                    if len != 0 {
                        let slot = SlotIndexedDocIndexes {
                            index: aut_index,
                            start: start,
                            len: len,
                        };
                        doc_indexes_slots.push(slot);
                    }
                }

                let read_only = VecReadOnly::new(doc_indexes);
                self.outs.reserve(doc_indexes_slots.len());
                for slot in doc_indexes_slots {
                    let indexes = IndexedDocIndexes {
                        index: slot.index,
                        doc_indexes: read_only.range(slot.start, slot.len),
                    };
                    self.outs.push(indexes);
                }

                if self.outs.is_empty() { return None }
                Some((input, &self.outs))
            },
            None => None,
        }
    }
}
}}

logical_operation!(struct Union, union);
logical_operation!(struct Intersection, intersection);
logical_operation!(struct Difference, difference);
logical_operation!(struct SymmetricDifference, symmetric_difference);

#[cfg(test)]
mod tests {
    use super::*;
    use crate::blob::PositiveBlobBuilder;

    fn get_exact_key<'m, I, S>(stream: I, key: &[u8]) -> Option<VecReadOnly<DocIndex>>
    where
        I: for<'a> fst::IntoStreamer<'a, Into=S, Item=(&'a [u8], &'a [IndexedDocIndexes])>,
        S: 'm + for<'a> fst::Streamer<'a, Item=(&'a [u8], &'a [IndexedDocIndexes])>,
    {
        let mut stream = stream.into_stream();
        while let Some((string, indexes)) = stream.next() {
            if string == key {
                return Some(indexes[0].doc_indexes.clone())
            }
        }
        None
    }

    #[test]
    fn union_two_blobs() {
        let doc1 = DocIndex { document_id: 12, attribute: 1, attribute_index: 22 };
        let doc2 = DocIndex { document_id: 31, attribute: 0, attribute_index: 1 };

        let meta1 = {
            let mapw = Vec::new();
            let indexesw = Vec::new();
            let mut builder = PositiveBlobBuilder::new(mapw, indexesw);

            builder.insert("chameau", doc1);

            Blob::Positive(builder.build().unwrap())
        };

        let meta2 = {
            let mapw = Vec::new();
            let indexesw = Vec::new();
            let mut builder = PositiveBlobBuilder::new(mapw, indexesw);

            builder.insert("chameau", doc2);

            Blob::Positive(builder.build().unwrap())
        };

        let metas = OpBuilder::new().add(&meta1).add(&meta2).union();
        let value = get_exact_key(metas, b"chameau");

        assert_eq!(&*value.unwrap(), &[doc1, doc2][..]);
    }

    #[test]
    fn intersection_two_blobs() {
        let doc1 = DocIndex { document_id: 31, attribute: 0, attribute_index: 1 };
        let doc2 = DocIndex { document_id: 31, attribute: 0, attribute_index: 1 };

        let meta1 = {
            let mapw = Vec::new();
            let indexesw = Vec::new();
            let mut builder = PositiveBlobBuilder::new(mapw, indexesw);

            builder.insert("chameau", doc1);

            Blob::Positive(builder.build().unwrap())
        };

        let meta2 = {
            let mapw = Vec::new();
            let indexesw = Vec::new();
            let mut builder = PositiveBlobBuilder::new(mapw, indexesw);

            builder.insert("chameau", doc2);

            Blob::Positive(builder.build().unwrap())
        };

        let metas = OpBuilder::new().add(&meta1).add(&meta2).intersection();
        let value = get_exact_key(metas, b"chameau");

        assert_eq!(&*value.unwrap(), &[doc1][..]);
    }

    #[test]
    fn difference_two_blobs() {
        let doc1 = DocIndex { document_id: 12, attribute: 1, attribute_index: 22 };
        let doc2 = DocIndex { document_id: 31, attribute: 0, attribute_index: 1 };
        let doc3 = DocIndex { document_id: 31, attribute: 0, attribute_index: 1 };

        let meta1 = {
            let mapw = Vec::new();
            let indexesw = Vec::new();
            let mut builder = PositiveBlobBuilder::new(mapw, indexesw);

            builder.insert("chameau", doc1);
            builder.insert("chameau", doc2);

            Blob::Positive(builder.build().unwrap())
        };

        let meta2 = {
            let mapw = Vec::new();
            let indexesw = Vec::new();
            let mut builder = PositiveBlobBuilder::new(mapw, indexesw);

            builder.insert("chameau", doc3);

            Blob::Positive(builder.build().unwrap())
        };

        let metas = OpBuilder::new().add(&meta1).add(&meta2).difference();
        let value = get_exact_key(metas, b"chameau");

        assert_eq!(&*value.unwrap(), &[doc1][..]);
    }

    #[test]
    fn symmetric_difference_two_blobs() {
        let doc1 = DocIndex { document_id: 12, attribute: 1, attribute_index: 22 };
        let doc2 = DocIndex { document_id: 31, attribute: 0, attribute_index: 1 };
        let doc3 = DocIndex { document_id: 32, attribute: 0, attribute_index: 1 };
        let doc4 = DocIndex { document_id: 34, attribute: 12, attribute_index: 1 };

        let meta1 = {
            let mapw = Vec::new();
            let indexesw = Vec::new();
            let mut builder = PositiveBlobBuilder::new(mapw, indexesw);

            builder.insert("chameau", doc1);
            builder.insert("chameau", doc2);
            builder.insert("chameau", doc3);

            Blob::Positive(builder.build().unwrap())
        };

        let meta2 = {
            let mapw = Vec::new();
            let indexesw = Vec::new();
            let mut builder = PositiveBlobBuilder::new(mapw, indexesw);

            builder.insert("chameau", doc2);
            builder.insert("chameau", doc3);
            builder.insert("chameau", doc4);

            Blob::Positive(builder.build().unwrap())
        };

        let metas = OpBuilder::new().add(&meta1).add(&meta2).symmetric_difference();
        let value = get_exact_key(metas, b"chameau");

        assert_eq!(&*value.unwrap(), &[doc1, doc4][..]);
    }
}
