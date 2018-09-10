use std::rc::Rc;
use std::collections::{BinaryHeap, HashMap, BTreeMap};
use std::cmp;
use fst::{IntoStreamer, Streamer};
use sdset::multi::OpBuilder as SdOpBuilder;
use sdset::{SetOperation, Set};
use crate::metadata::ops::IndexedDocIndexes;
use crate::vec_read_only::VecReadOnly;
use crate::DocIndex;

type BoxedStream<'f> = Box<for<'a> Streamer<'a, Item=(&'a [u8], &'a [IndexedDocIndexes])> + 'f>;

pub struct OpBuilder<'f> {
    streams: Vec<BoxedStream<'f>>,
}

impl<'f> OpBuilder<'f> {
    pub fn new() -> Self {
        Self { streams: Vec::new() }
    }

    /// Push a stream of `IndexedDocIndexes`.
    ///
    /// # Warning
    ///
    /// You must ensure yourself that the automatons are
    /// all the same in the same order for each stream you push.
    pub fn push<I, S>(&mut self, stream: I)
    where
        I: for<'a> IntoStreamer<'a, Into=S, Item=(&'a [u8], &'a [IndexedDocIndexes])>,
        S: 'f + for<'a> Streamer<'a, Item=(&'a [u8], &'a [IndexedDocIndexes])>,
    {
        self.streams.push(Box::new(stream.into_stream()));
    }

    pub fn union(self) -> Union<'f> {
        Union {
            heap: StreamHeap::new(self.streams),
            outs: Vec::new(),
            cur_slot: None,
        }
    }

    pub fn intersection(self) -> Intersection<'f> {
        Intersection {
            heap: StreamHeap::new(self.streams),
            outs: Vec::new(),
            cur_slot: None,
        }
    }

    pub fn difference(self) -> Difference<'f> {
        Difference {
            heap: StreamHeap::new(self.streams),
            outs: Vec::new(),
            cur_slot: None,
        }
    }

    pub fn symmetric_difference(self) -> SymmetricDifference<'f> {
        SymmetricDifference {
            heap: StreamHeap::new(self.streams),
            outs: Vec::new(),
            cur_slot: None,
        }
    }
}

// FIXME reuse it from metadata::ops
struct SlotIndexedDocIndexes {
    aut_index: usize,
    start: usize,
    len: usize,
}

macro_rules! logical_operation {
    (struct $name:ident, $operation:ident) => {

pub struct $name<'f> {
    heap: StreamHeap<'f>,
    outs: Vec<IndexedDocIndexes>,
    cur_slot: Option<Slot>,
}

impl<'a, 'f> Streamer<'a> for $name<'f> {
    type Item = (&'a [u8], &'a [IndexedDocIndexes]);

    // The Metadata could be types as "key-values present" and "key-values possibly not present"
    // in other words Metadata that "needs" to have key-values and other that doesn't needs.
    //
    // We could probably allow the user to define in Metadata some Document
    // that needs to be deleted and only declare the DocumentId, and not every DocIndex of each words.
    fn next(&'a mut self) -> Option<Self::Item> {
        if let Some(slot) = self.cur_slot.take() {
            self.heap.refill(slot);
        }
        let slot = match self.heap.pop() {
            None => return None,
            Some(slot) => {
                self.cur_slot = Some(slot);
                self.cur_slot.as_mut().unwrap()
            }
        };

        self.outs.clear();

        // retrieve all the doc_indexes of all the streams,
        // store them in an HashMap which the key is
        // the aut_index (associated with the state that is ignored),
        // the doc_indexes must be stored in another BTreeMap which the key
        // is the rdr_index.
        //
        // This will permit us to do set operations on readers (using the rdr_index)
        // the BTreeMap will gives the rdr_index in order and the final result
        // will be aggregated in a Vec of IndexedDocIndexes which the aut_index and state
        // are the key of the first HashMap

        // TODO use the fnv Hasher!

        let mut builders = HashMap::new();
        let iv = slot.indexed_value();
        let builder = builders.entry(iv.index).or_insert_with(BTreeMap::new);
        builder.insert(slot.rdr_index, iv.doc_indexes);

        while let Some(mut slot) = self.heap.pop_if_equal(slot.input()) {
            let iv = slot.indexed_value();
            let builder = builders.entry(iv.index).or_insert_with(BTreeMap::new);
            builder.insert(slot.rdr_index, iv.doc_indexes);

            self.heap.refill(slot);
        }

        // now that we have accumulated all the doc_indexes like so:
        // HashMap<(aut_index, state*), BtreeMap<rdr_index, doc_indexes>>
        // we will be able to retrieve, for each aut_index, the doc_indexes
        // that are needed to do the set operation

        let mut doc_indexes = Vec::new();
        let mut doc_indexes_slots = Vec::with_capacity(builders.len());
        for (aut_index, values) in builders {

            let sets = values.iter().map(|(_, v)| Set::new_unchecked(v.as_slice())).collect();
            let builder = SdOpBuilder::from_vec(sets);

            let start = doc_indexes.len();
            builder.$operation().extend_vec(&mut doc_indexes);
            let len = doc_indexes.len() - start;
            if len == 0 { continue }

            let slot = SlotIndexedDocIndexes {
                aut_index: aut_index,
                start: start,
                len: len,
            };
            doc_indexes_slots.push(slot);
        }

        let read_only = VecReadOnly::new(doc_indexes);
        self.outs.reserve(doc_indexes_slots.len());
        for slot in doc_indexes_slots {
            let indexes = IndexedDocIndexes {
                index: slot.aut_index,
                doc_indexes: read_only.range(slot.start, slot.len),
            };
            self.outs.push(indexes);
        }

        if self.outs.is_empty() { return None }
        Some((slot.input(), &self.outs))
    }
}
}}

logical_operation!(struct Union, union);
logical_operation!(struct Intersection, intersection);
logical_operation!(struct Difference, difference);
logical_operation!(struct SymmetricDifference, symmetric_difference);

struct StreamHeap<'f> {
    rdrs: Vec<BoxedStream<'f>>,
    heap: BinaryHeap<Slot>,
}

impl<'f> StreamHeap<'f> {
    fn new(streams: Vec<BoxedStream<'f>>) -> StreamHeap<'f> {
        let mut heap = StreamHeap {
            rdrs: streams,
            heap: BinaryHeap::new(),
        };
        for i in 0..heap.rdrs.len() {
            heap.refill(Slot::new(i));
        }
        heap
    }

    fn pop(&mut self) -> Option<Slot> {
        self.heap.pop()
    }

    fn peek_is_duplicate(&self, key: &[u8]) -> bool {
        self.heap.peek().map(|s| s.input() == key).unwrap_or(false)
    }

    fn pop_if_equal(&mut self, key: &[u8]) -> Option<Slot> {
        if self.peek_is_duplicate(key) {
            self.pop()
        } else {
            None
        }
    }

    fn pop_if_le(&mut self, key: &[u8]) -> Option<Slot> {
        if self.heap.peek().map(|s| s.input() <= key).unwrap_or(false) {
            self.pop()
        } else {
            None
        }
    }

    fn num_slots(&self) -> usize {
        self.rdrs.len()
    }

    fn refill(&mut self, mut slot: Slot) {
        if let Some((input, outputs)) = self.rdrs[slot.rdr_index].next() {
            slot.set_input(input);
            for output in outputs {
                slot.set_aut_index(output.index);
                slot.set_output(output.doc_indexes.clone());
                self.heap.push(slot.clone());
            }
        }
    }
}

#[derive(Debug, Clone)]
struct Slot {
    rdr_index: usize,
    aut_index: usize,
    input: Rc<Vec<u8>>,
    output: Option<VecReadOnly<DocIndex>>,
}

impl PartialEq for Slot {
    fn eq(&self, other: &Self) -> bool {
        (&self.input, self.rdr_index, self.aut_index)
        .eq(&(&other.input, other.rdr_index, other.aut_index))
    }
}

impl Eq for Slot { }

impl PartialOrd for Slot {
    fn partial_cmp(&self, other: &Self) -> Option<cmp::Ordering> {
        (&self.input, self.rdr_index, self.aut_index)
        .partial_cmp(&(&other.input, other.rdr_index, other.aut_index))
        .map(|ord| ord.reverse())
    }
}

impl Ord for Slot {
    fn cmp(&self, other: &Self) -> cmp::Ordering {
        self.partial_cmp(other).unwrap()
    }
}

impl Slot {
    fn new(rdr_index: usize) -> Self {
        Slot {
            rdr_index: rdr_index,
            aut_index: 0,
            input: Rc::new(Vec::with_capacity(64)),
            output: None,
        }
    }

    fn indexed_value(&mut self) -> IndexedDocIndexes {
        IndexedDocIndexes {
            index: self.aut_index,
            doc_indexes: self.output.take().unwrap(),
        }
    }

    fn input(&self) -> &[u8] {
        &self.input
    }

    fn set_input(&mut self, input: &[u8]) {
        if *self.input != input {
            let inner = Rc::make_mut(&mut self.input);
            inner.clear();
            inner.extend(input);
        }
    }

    fn set_aut_index(&mut self, aut_index: usize) {
        self.aut_index = aut_index;
    }

    fn set_output(&mut self, output: VecReadOnly<DocIndex>) {
        self.output = Some(output);
    }
}

#[derive(Copy, Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct IndexedValueWithState {
    pub index: usize,
    pub value: u64,
}
