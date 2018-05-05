#[macro_use] extern crate serde_derive;
extern crate bincode;
extern crate fst;
extern crate serde;

mod fst_map;

use std::ops::Range;
use std::io::{Write, BufReader};
use std::fs::File;
use std::path::Path;
use std::str::from_utf8_unchecked;
use fst::Automaton;

pub use self::fst_map::{FstMap, FstMapBuilder};
use self::fst_map::Values;

pub struct StreamBuilder<'a, T: 'a, A: Automaton> {
    inner: fst::map::StreamBuilder<'a, A>,
    values: &'a Values<T>,
}

impl<'a, T: 'a, A: Automaton> fst::IntoStreamer<'a> for StreamBuilder<'a, T, A> {
    type Item = (&'a str, &'a [T]);

    type Into = Stream<'a, T, A>;

    fn into_stream(self) -> Self::Into {
        Stream {
            inner: self.inner.into_stream(),
            values: self.values,
        }
    }
}

pub struct Stream<'a, T: 'a, A: Automaton = fst::automaton::AlwaysMatch> {
    inner: fst::map::Stream<'a, A>,
    values: &'a Values<T>,
}

impl<'a, 'm, T: 'a, A: Automaton> fst::Streamer<'a> for Stream<'m, T, A> {
    type Item = (&'a str, &'a [T]);

    fn next(&'a mut self) -> Option<Self::Item> {
        // Here we can't just `map` because of some borrow rules
        match self.inner.next() {
            Some((key, i)) => {
                let key = unsafe { from_utf8_unchecked(key) };
                let values = unsafe { self.values.get_unchecked(i as usize) };
                Some((key, values))
            },
            None => None,
        }
    }
}
