#[macro_use] extern crate serde_derive;
extern crate bincode;
extern crate fst;
extern crate levenshtein_automata;
extern crate serde;

pub mod map;
mod levenshtein;

pub use self::map::{Map, MapBuilder, Values};
pub use self::map::{
    OpBuilder, IndexedValues,
    OpWithStateBuilder, IndexedValuesWithState,
};

pub use self::levenshtein::LevBuilder;
