use crate::vec_read_only::VecReadOnly;
use std::collections::BinaryHeap;
use std::{mem, cmp};
use std::rc::Rc;

use fst::{Automaton, Streamer};
use fst::automaton::AlwaysMatch;
use sdset::{Set, SetBuf, SetOperation};
use sdset::duo::OpBuilder as SdOpBuilder;
use group_by::GroupBy;

use crate::blob::{Blob, Sign};
use crate::blob::ops::{OpBuilder, Union, IndexedDocIndexes};
use crate::DocIndex;

fn group_is_negative(blobs: &&[Blob]) -> bool {
    blobs[0].sign() == Sign::Negative
}

fn blob_same_sign(a: &Blob, b: &Blob) -> bool {
    a.sign() == b.sign()
}

fn sign_from_group_index(group: usize) -> Sign {
    if group % 2 == 0 {
        Sign::Positive
    } else {
        Sign::Negative
    }
}

pub struct Merge<'b> {
    heap: GroupHeap<'b>,
    outs: Vec<IndexedDocIndexes>,
    cur_slot: Option<Slot>,
}

impl<'b> Merge<'b> {
    pub fn always_match(blobs: &'b [Blob]) -> Self {
        Self::with_automatons(vec![AlwaysMatch], blobs)
    }
}

impl<'b> Merge<'b> {
    pub fn with_automatons<A>(automatons: Vec<A>, blobs: &'b [Blob]) -> Self
    where A: 'b + Automaton + Clone
    {
        let mut groups = Vec::new();
        // We can skip blobs that are negative: they didn't remove anything at the start
        for blobs in GroupBy::new(blobs, blob_same_sign).skip_while(group_is_negative) {
            let mut builder = OpBuilder::with_automatons(automatons.clone());
            for blob in blobs {
                builder.push(blob);
            }
            groups.push(builder.union());
        }

        let mut heap = GroupHeap::new(groups);
        heap.refill();

        Merge {
            heap: heap,
            outs: Vec::new(),
            cur_slot: None,
        }
    }
}

impl<'b, 'a> Streamer<'a> for Merge<'b> {
    type Item = (&'a [u8], &'a [IndexedDocIndexes]);

    fn next(&'a mut self) -> Option<Self::Item> {
        self.outs.clear();
        loop {
            if let Some(slot) = self.cur_slot.take() {
                self.heap.refill();
            }
            let slot = match self.heap.pop() {
                None => return None,
                Some(slot) => {
                    self.cur_slot = Some(slot);
                    self.cur_slot.as_ref().unwrap()
                }
            };

            let mut doc_indexes = Vec::new();
            let mut doc_indexes_slots = Vec::with_capacity(self.heap.num_groups());

            let len = match sign_from_group_index(slot.grp_index) {
                Sign::Positive => {
                    doc_indexes.extend_from_slice(&slot.output);
                    slot.output.len()
                },
                Sign::Negative => 0,
            };

            let mut slotidi = SlotIndexedDocIndexes {
                index: slot.aut_index,
                start: 0,
                len: len,
            };

            let mut buffer = Vec::new();
            while let Some(slot2) = self.heap.pop_if_equal(slot.input()) {
                if slotidi.index == slot2.aut_index {
                    buffer.clear();
                    buffer.extend(doc_indexes.drain(slotidi.start..));

                    let a = Set::new_unchecked(&buffer);
                    let b = Set::new_unchecked(&slot2.output);
                    match sign_from_group_index(slot2.grp_index) {
                        Sign::Positive => { SdOpBuilder::new(a, b).union().extend_vec(&mut doc_indexes) },
                        Sign::Negative => SdOpBuilder::new(a, b).difference().extend_vec(&mut doc_indexes),
                    }
                    slotidi.len = doc_indexes.len() - slotidi.start;

                } else {
                    if slotidi.len != 0 {
                        doc_indexes_slots.push(slotidi);
                    }
                    slotidi = SlotIndexedDocIndexes {
                        index: slot2.aut_index,
                        start: doc_indexes.len(),
                        len: slot2.output.len(),
                    };
                    buffer.extend_from_slice(&slot2.output);
                }
            }

            if slotidi.len != 0 {
                doc_indexes_slots.push(slotidi);
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

            if !self.outs.is_empty() {
                let slot = self.cur_slot.as_ref().unwrap(); // FIXME
                return Some((slot.input(), &self.outs))
            }
        }
    }
}

struct SlotIndexedDocIndexes {
    index: usize,
    start: usize,
    len: usize,
}

#[derive(Debug, Eq, PartialEq)]
struct Slot {
    grp_index: usize,
    aut_index: usize,
    input: Rc<Vec<u8>>,
    output: VecReadOnly<DocIndex>,
}

impl Slot {
    fn input(&self) -> &[u8] {
        &self.input
    }
}

impl PartialOrd for Slot {
    fn partial_cmp(&self, other: &Slot) -> Option<cmp::Ordering> {
        (&self.input, self.aut_index, self.grp_index, &self.output)
        .partial_cmp(&(&other.input, other.aut_index, other.grp_index, &other.output))
        .map(|ord| ord.reverse())
    }
}

impl Ord for Slot {
    fn cmp(&self, other: &Slot) -> cmp::Ordering {
        self.partial_cmp(other).unwrap()
    }
}

struct GroupHeap<'b> {
    groups: Vec<Union<'b>>,
    heap: BinaryHeap<Slot>,
}

impl<'b> GroupHeap<'b> {
    fn new(groups: Vec<Union<'b>>) -> GroupHeap<'b> {
        GroupHeap {
            groups: groups,
            heap: BinaryHeap::new(),
        }
    }

    fn num_groups(&self) -> usize {
        self.groups.len()
    }

    fn pop(&mut self) -> Option<Slot> {
        self.heap.pop()
    }

    fn peek_is_duplicate(&self, key: &[u8]) -> bool {
        self.heap.peek().map(|s| *s.input == key).unwrap_or(false)
    }

    fn pop_if_equal(&mut self, key: &[u8]) -> Option<Slot> {
        if self.peek_is_duplicate(key) { self.pop() } else { None }
    }

    fn refill(&mut self) {
        for (i, group) in self.groups.iter_mut().enumerate() {
            if let Some((input, doc_indexes)) = group.next() {
                let input = Rc::new(input.to_vec());
                for doc_index in doc_indexes {
                    let slot = Slot {
                        input: input.clone(),
                        grp_index: i,
                        aut_index: doc_index.index,
                        output: doc_index.doc_indexes.clone(),
                    };
                    self.heap.push(slot);
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::blob::{PositiveBlobBuilder, NegativeBlobBuilder};
    use crate::DocIndex;

    fn get_all<'m, I, S>(stream: I) -> Vec<(String, VecReadOnly<DocIndex>)>
    where
        I: for<'a> fst::IntoStreamer<'a, Into=S, Item=(&'a [u8], &'a [IndexedDocIndexes])>,
        S: 'm + for<'a> fst::Streamer<'a, Item=(&'a [u8], &'a [IndexedDocIndexes])>,
    {
        let mut result = Vec::new();

        let mut stream = stream.into_stream();
        while let Some((string, indexes)) = stream.next() {
            let string = String::from_utf8(string.to_owned()).unwrap();
            result.push((string, indexes[0].doc_indexes.clone()))
        }

        result
    }

    #[test]
    fn single_positive_blob() {
        let doc1 = DocIndex{ document_id: 0,  attribute: 0, attribute_index: 0 };
        let doc2 = DocIndex{ document_id: 12, attribute: 0, attribute_index: 2 };
        let doc3 = DocIndex{ document_id: 0,  attribute: 0, attribute_index: 1 };
        let doc4 = DocIndex{ document_id: 0,  attribute: 0, attribute_index: 2 };

        let a = {
            let mut builder = PositiveBlobBuilder::new(Vec::new(), Vec::new());

            builder.insert("hell",  doc1);
            builder.insert("hell",  doc2);
            builder.insert("hello", doc3);
            builder.insert("wor",   doc4);

            Blob::Positive(builder.build().unwrap())
        };

        let blobs = &[a];
        let merge = Merge::always_match(blobs);

        let value = get_all(merge);
        assert_eq!(value.len(), 3);

        assert_eq!(value[0].0, "hell");
        assert_eq!(&*value[0].1, &[doc1, doc2][..]);

        assert_eq!(value[1].0, "hello");
        assert_eq!(&*value[1].1, &[doc3][..]);

        assert_eq!(value[2].0, "wor");
        assert_eq!(&*value[2].1, &[doc4][..]);
    }

    #[test]
    fn single_negative_blob() {
        let a = {
            let mut builder = NegativeBlobBuilder::new(Vec::new());

            builder.insert(1);
            builder.insert(2);
            builder.insert(3);
            builder.insert(4);

            Blob::Negative(builder.build().unwrap())
        };

        let blobs = &[a];
        let merge = Merge::always_match(blobs);

        let value = get_all(merge);
        assert_eq!(value.len(), 0);
    }

    #[test]
    fn two_positive_blobs() {
        let doc1 = DocIndex{ document_id: 0,  attribute: 0, attribute_index: 0 };
        let doc2 = DocIndex{ document_id: 12, attribute: 0, attribute_index: 2 };
        let doc3 = DocIndex{ document_id: 0,  attribute: 0, attribute_index: 1 };
        let doc4 = DocIndex{ document_id: 0,  attribute: 0, attribute_index: 2 };

        let a = {
            let mut builder = PositiveBlobBuilder::new(Vec::new(), Vec::new());

            builder.insert("hell",  doc1);
            builder.insert("wor",   doc4);

            Blob::Positive(builder.build().unwrap())
        };

        let b = {
            let mut builder = PositiveBlobBuilder::new(Vec::new(), Vec::new());

            builder.insert("hell",  doc2);
            builder.insert("hello", doc3);

            Blob::Positive(builder.build().unwrap())
        };

        let blobs = &[a, b];
        let merge = Merge::always_match(blobs);

        let value = get_all(merge);
        assert_eq!(value.len(), 3);

        assert_eq!(value[0].0, "hell");
        assert_eq!(&*value[0].1, &[doc1, doc2][..]);

        assert_eq!(value[1].0, "hello");
        assert_eq!(&*value[1].1, &[doc3][..]);

        assert_eq!(value[2].0, "wor");
        assert_eq!(&*value[2].1, &[doc4][..]);
    }

    #[test]
    fn one_positive_one_negative_blobs() {
        let doc1 = DocIndex{ document_id: 0,  attribute: 0, attribute_index: 0 };
        let doc2 = DocIndex{ document_id: 12, attribute: 0, attribute_index: 2 };
        let doc3 = DocIndex{ document_id: 0,  attribute: 0, attribute_index: 1 };
        let doc4 = DocIndex{ document_id: 0,  attribute: 0, attribute_index: 2 };

        let a = {
            let mut builder = PositiveBlobBuilder::new(Vec::new(), Vec::new());

            builder.insert("hell",  doc1);
            builder.insert("hell",  doc2);
            builder.insert("hello", doc3);
            builder.insert("wor",   doc4);

            Blob::Positive(builder.build().unwrap())
        };

        let b = {
            let mut builder = NegativeBlobBuilder::new(Vec::new());

            builder.insert(2);
            builder.insert(3);

            Blob::Negative(builder.build().unwrap())
        };

        let blobs = &[a, b];
        let merge = Merge::always_match(blobs);

        let value = get_all(merge);
        assert_eq!(value.len(), 2);

        assert_eq!(value[0].0, "hell");
        assert_eq!(&*value[0].1, &[doc1][..]);

        assert_eq!(value[1].0, "wor");
        assert_eq!(&*value[1].1, &[doc4][..]);
    }

    #[test]
    fn alternate_positive_negative_blobs() {
        let doc1 = DocIndex{ document_id: 0,  attribute: 0, attribute_index: 0 };
        let doc2 = DocIndex{ document_id: 12, attribute: 0, attribute_index: 2 };
        let doc3 = DocIndex{ document_id: 0,  attribute: 0, attribute_index: 1 };
        let doc4 = DocIndex{ document_id: 0,  attribute: 0, attribute_index: 2 };

        let a = {
            let mut builder = PositiveBlobBuilder::new(Vec::new(), Vec::new());

            builder.insert("hell",  doc1);
            builder.insert("hell",  doc2);
            builder.insert("hello", doc3);

            Blob::Positive(builder.build().unwrap())
        };

        let b = {
            let mut builder = NegativeBlobBuilder::new(Vec::new());

            builder.insert(1);
            builder.insert(4);

            Blob::Negative(builder.build().unwrap())
        };

        let c = {
            let mut builder = PositiveBlobBuilder::new(Vec::new(), Vec::new());

            builder.insert("hell",  doc1);
            builder.insert("wor",   doc4);

            Blob::Positive(builder.build().unwrap())
        };

        let d = {
            let mut builder = NegativeBlobBuilder::new(Vec::new());

            builder.insert(1);

            Blob::Negative(builder.build().unwrap())
        };

        let blobs = &[a, b, c, d];
        let merge = Merge::always_match(blobs);

        let value = get_all(merge);
        assert_eq!(value.len(), 3);

        assert_eq!(value[0].0, "hell");
        assert_eq!(&*value[0].1, &[doc2][..]);

        assert_eq!(value[1].0, "hello");
        assert_eq!(&*value[1].1, &[doc3][..]);

        assert_eq!(value[2].0, "wor");
        assert_eq!(&*value[2].1, &[doc4][..]);
    }

    #[test]
    fn alternate_multiple_positive_negative_blobs() {
        let doc1 = DocIndex{ document_id: 0,  attribute: 0, attribute_index: 0 };
        let doc2 = DocIndex{ document_id: 12, attribute: 0, attribute_index: 2 };
        let doc3 = DocIndex{ document_id: 0,  attribute: 0, attribute_index: 1 };
        let doc4 = DocIndex{ document_id: 0,  attribute: 0, attribute_index: 2 };

        let a = {
            let mut builder = PositiveBlobBuilder::new(Vec::new(), Vec::new());

            builder.insert("hell",  doc1);
            builder.insert("hell",  doc2);
            builder.insert("hello", doc3);

            Blob::Positive(builder.build().unwrap())
        };

        let b = {
            let mut builder = PositiveBlobBuilder::new(Vec::new(), Vec::new());

            builder.insert("hell",  doc1);
            builder.insert("wor",   doc4);

            Blob::Positive(builder.build().unwrap())
        };

        let c = {
            let mut builder = NegativeBlobBuilder::new(Vec::new());

            builder.insert(1);
            builder.insert(4);

            Blob::Negative(builder.build().unwrap())
        };

        let d = {
            let mut builder = NegativeBlobBuilder::new(Vec::new());

            builder.insert(1);

            Blob::Negative(builder.build().unwrap())
        };

        let blobs = &[a, b, c, d];
        let merge = Merge::always_match(blobs);

        let value = get_all(merge);
        assert_eq!(value.len(), 2);

        assert_eq!(value[0].0, "hell");
        assert_eq!(&*value[0].1, &[doc2][..]);

        assert_eq!(value[1].0, "hello");
        assert_eq!(&*value[1].1, &[doc3][..]);
    }
}
