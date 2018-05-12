#[macro_use] extern crate serde_derive;
extern crate bincode;
extern crate fst;
extern crate serde;

mod map;

use fst::Automaton;

pub use self::map::{Map, MapBuilder, Values};
pub use self::map::{
    OpBuilder, IndexedValues,
    OpWithStateBuilder, IndexedValuesWithState,
};

pub struct StreamBuilder<'m, 'v, T: 'v, A> {
    inner: fst::map::StreamBuilder<'m, A>,
    values: &'v Values<T>,
}

impl<'m, 'v, T: 'v, A> StreamBuilder<'m, 'v, T, A> {
    pub fn with_state(self) -> StreamWithStateBuilder<'m, 'v, T, A> {
        StreamWithStateBuilder {
            inner: self.inner.with_state(),
            values: self.values,
        }
    }
}

impl<'m, 'v, 'a, T: 'v + 'a, A: Automaton> fst::IntoStreamer<'a> for StreamBuilder<'m, 'v, T, A> {
    type Item = <Self::Into as fst::Streamer<'a>>::Item;
    type Into = Stream<'m, 'v, T, A>;

    fn into_stream(self) -> Self::Into {
        Stream {
            inner: self.inner.into_stream(),
            values: self.values,
        }
    }
}

pub struct Stream<'m, 'v, T: 'v, A: Automaton = fst::automaton::AlwaysMatch> {
    inner: fst::map::Stream<'m, A>,
    values: &'v Values<T>,
}

impl<'m, 'v, 'a, T: 'v + 'a, A: Automaton> fst::Streamer<'a> for Stream<'m, 'v, T, A> {
    type Item = (&'a [u8], &'a [T]);

    fn next(&'a mut self) -> Option<Self::Item> {
        // Here we can't just `map` because of some borrow rules
        match self.inner.next() {
            Some((key, i)) => {
                let values = unsafe { self.values.get_unchecked(i as usize) };
                Some((key, values))
            },
            None => None,
        }
    }
}

pub struct StreamWithStateBuilder<'m, 'v, T: 'v, A> {
    inner: fst::map::StreamWithStateBuilder<'m, A>,
    values: &'v Values<T>,
}

impl<'m, 'v, 'a, T: 'v + 'a, A: 'a> fst::IntoStreamer<'a> for StreamWithStateBuilder<'m, 'v, T, A>
where
    A: Automaton,
    A::State: Clone,
{
    type Item = <Self::Into as fst::Streamer<'a>>::Item;
    type Into = StreamWithState<'m, 'v, T, A>;

    fn into_stream(self) -> Self::Into {
        StreamWithState {
            inner: self.inner.into_stream(),
            values: self.values,
        }
    }
}

pub struct StreamWithState<'m, 'v, T: 'v, A: Automaton = fst::automaton::AlwaysMatch> {
    inner: fst::map::StreamWithState<'m, A>,
    values: &'v Values<T>,
}

impl<'m, 'v, 'a, T: 'v + 'a, A: 'a> fst::Streamer<'a> for StreamWithState<'m, 'v, T, A>
where
    A: Automaton,
    A::State: Clone,
{
    type Item = (&'a [u8], &'a [T], A::State);

    fn next(&'a mut self) -> Option<Self::Item> {
        match self.inner.next() {
            Some((key, i, state)) => {
                let values = unsafe { self.values.get_unchecked(i as usize) };
                Some((key, values, state))
            },
            None => None,
        }
    }
}
