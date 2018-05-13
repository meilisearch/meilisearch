#[macro_use] extern crate serde_derive;
extern crate bincode;
extern crate fst;
extern crate levenshtein_automata;
extern crate serde;

pub mod map;
pub mod capped_btree_map;
mod levenshtein;

pub use self::map::{Map, MapBuilder, Values};
pub use self::map::{
    OpBuilder, IndexedValues,
    OpWithStateBuilder, IndexedValuesWithState,
};
pub use self::capped_btree_map::{CappedBTreeMap, Insertion};
pub use self::levenshtein::LevBuilder;

#[derive(Debug, Serialize, Deserialize)]
pub struct Value {
    pub id: u64,
    pub attr_index: AttrIndex,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct AttrIndex {
    pub attribute: u8,
    pub index: u64,
}
