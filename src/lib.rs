extern crate fst;
extern crate serde;
extern crate serde_json;
#[macro_use] extern crate serde_derive;
extern crate smallvec;

use std::io::Write;

pub use fst::MapBuilder;
use smallvec::SmallVec;

type SmallVec16<T> = SmallVec<[T; 16]>;

#[derive(Debug, Serialize)]
struct Product<'a> {
    product_id: u64,
    title: &'a str,
    ft: &'a str,
}

#[derive(Debug)]
pub struct MultiMap {
    map: fst::Map,
    values: Box<[SmallVec16<u64>]>,
}

impl MultiMap {
    pub fn contains_key<K: AsRef<[u8]>>(&self, key: K) -> bool {
        self.map.contains_key(key)
    }

    pub fn get<K: AsRef<[u8]>>(&self, key: K) -> Option<&[u64]> {
        self.map.get(key).map(|i| &*self.values[i as usize])
    }
}

#[derive(Debug)]
pub struct MultiMapBuilder<'a> {
    map: Vec<(&'a str, u64)>,
    values: Vec<SmallVec16<u64>>,
}

impl<'a> MultiMapBuilder<'a> {
    pub fn new() -> MultiMapBuilder<'a> {
        MultiMapBuilder {
            map: Vec::new(),
            values: Vec::new(),
        }
    }

    pub fn insert(&mut self, key: &'a str, value: u64) {
        match self.map.binary_search_by_key(&key, |&(k, _)| k) {
            Ok(index) => {
                let (_, index) = self.map[index];
                let values = &mut self.values[index as usize];
                if let Err(index) = values.binary_search(&value) {
                    values.insert(index, value)
                }
            },
            Err(index) => {
                let values = {
                    let mut vec = SmallVec16::new();
                    vec.push(value);
                    vec
                };
                self.values.push(values);
                let values_index = (self.values.len() - 1) as u64;

                let value = (key, values_index);
                self.map.insert(index, value);
            },
        }
    }

    pub fn build_memory(self) -> fst::Result<MultiMap> {
        Ok(MultiMap {
            map: fst::Map::from_iter(self.map)?,
            values: self.values.into_boxed_slice(),
        })
    }
}
