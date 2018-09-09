use std::collections::BinaryHeap;
use std::rc::Rc;
use std::cmp;
use fst::raw::{self, Output};
use fst::{self, IntoStreamer, Streamer};

type BoxedStream<'f> = Box<for<'a> Streamer<'a, Item=(&'a [u8], &'a [raw::IndexedValue])> + 'f>;

pub struct OpIndexedValueBuilder<'f> {
    streams: Vec<BoxedStream<'f>>,
}

impl<'f> OpIndexedValueBuilder<'f> {
    pub fn new() -> Self {
        Self { streams: Vec::new() }
    }

    pub fn push<I, S>(&mut self, stream: I)
    where
        I: for<'a> IntoStreamer<'a, Into=S, Item=(&'a [u8], &'a [raw::IndexedValue])>,
        S: 'f + for<'a> Streamer<'a, Item=(&'a [u8], &'a [raw::IndexedValue])>,
    {
        self.streams.push(Box::new(stream.into_stream()));
    }

    pub fn union(self) -> UnionIndexedValue<'f> {
        UnionIndexedValue {
            heap: StreamIndexedValueHeap::new(self.streams),
            outs: Vec::new(),
            cur_slot: None,
        }
    }
}

pub struct UnionIndexedValue<'f> {
    heap: StreamIndexedValueHeap<'f>,
    outs: Vec<IndexedValue>,
    cur_slot: Option<SlotIndexedValue>,
}

impl<'a, 'm> fst::Streamer<'a> for UnionIndexedValue<'m> {
    type Item = (&'a [u8], &'a [IndexedValue]);

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
        self.outs.push(slot.indexed_value());
        while let Some(mut slot2) = self.heap.pop_if_equal(slot.input()) {
            self.outs.push(slot2.indexed_value());
            self.heap.refill(slot2);
        }
        Some((slot.input(), &self.outs))
    }
}

struct StreamIndexedValueHeap<'f> {
    rdrs: Vec<BoxedStream<'f>>,
    heap: BinaryHeap<SlotIndexedValue>,
}

impl<'f> StreamIndexedValueHeap<'f> {
    fn new(streams: Vec<BoxedStream<'f>>) -> StreamIndexedValueHeap<'f> {
        let mut u = StreamIndexedValueHeap {
            rdrs: streams,
            heap: BinaryHeap::new(),
        };
        for i in 0..u.rdrs.len() {
            u.refill(SlotIndexedValue::new(i));
        }
        u
    }

    fn pop(&mut self) -> Option<SlotIndexedValue> {
        self.heap.pop()
    }

    fn peek_is_duplicate(&self, key: &[u8]) -> bool {
        self.heap.peek().map(|s| s.input() == key).unwrap_or(false)
    }

    fn pop_if_equal(&mut self, key: &[u8]) -> Option<SlotIndexedValue> {
        if self.peek_is_duplicate(key) {
            self.pop()
        } else {
            None
        }
    }

    fn pop_if_le(&mut self, key: &[u8]) -> Option<SlotIndexedValue> {
        if self.heap.peek().map(|s| s.input() <= key).unwrap_or(false) {
            self.pop()
        } else {
            None
        }
    }

    fn num_slots(&self) -> usize {
        self.rdrs.len()
    }

    fn refill(&mut self, mut slot: SlotIndexedValue) {
        if let Some((input, ivalues)) = self.rdrs[slot.rdr_index].next() {
            slot.set_input(input);
            for values in ivalues {
                slot.set_aut_index(values.index);
                slot.set_output(values.value);
                self.heap.push(slot.clone());
            }
        }
    }
}

#[derive(Debug, Clone)]
struct SlotIndexedValue {
    rdr_index: usize,
    aut_index: usize,
    input: Rc<Vec<u8>>,
    output: Output,
}

#[derive(Debug)]
pub struct IndexedValue {
    pub rdr_index: usize,
    pub aut_index: usize,
    pub value: u64,
}

impl PartialEq for SlotIndexedValue {
    fn eq(&self, other: &Self) -> bool {
        (&self.input, self.rdr_index, self.aut_index, self.output)
        .eq(&(&other.input, other.rdr_index, other.aut_index, other.output))
    }
}

impl Eq for SlotIndexedValue { }

impl PartialOrd for SlotIndexedValue {
    fn partial_cmp(&self, other: &Self) -> Option<cmp::Ordering> {
        (&self.input, self.rdr_index, self.aut_index, self.output)
        .partial_cmp(&(&other.input, other.rdr_index, other.aut_index, other.output))
        .map(|ord| ord.reverse())
    }
}

impl Ord for SlotIndexedValue {
    fn cmp(&self, other: &Self) -> cmp::Ordering {
        self.partial_cmp(other).unwrap()
    }
}

impl SlotIndexedValue {
    fn new(rdr_index: usize) -> SlotIndexedValue {
        SlotIndexedValue {
            rdr_index: rdr_index,
            aut_index: 0,
            input: Rc::new(Vec::with_capacity(64)),
            output: Output::zero(),
        }
    }

    fn indexed_value(&self) -> IndexedValue {
        IndexedValue {
            rdr_index: self.rdr_index,
            aut_index: self.aut_index,
            value: self.output.value(),
        }
    }

    fn input(&self) -> &[u8] {
        &self.input
    }

    fn set_aut_index(&mut self, aut_index: usize) {
        self.aut_index = aut_index;
    }

    fn set_input(&mut self, input: &[u8]) {
        if *self.input != input {
            let inner = Rc::make_mut(&mut self.input);
            inner.clear();
            inner.extend(input);
        }
    }

    fn set_output(&mut self, output: u64) {
        self.output = Output::new(output);
    }
}
