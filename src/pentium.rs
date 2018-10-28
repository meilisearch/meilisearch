use std::error::Error;

use crate::automaton;
use crate::rank::Document;
use crate::index::Index;

pub struct Pentium {
    index: Index,
}

impl Pentium {
    pub fn from_index(index: Index) -> Result<Self, Box<Error>> {
        unimplemented!()
    }

    pub fn search(&self, query: &str) -> Vec<Document> {

        let mut automatons = Vec::new();
        for word in query.split_whitespace().map(str::to_lowercase) {
            let dfa = automaton::build_prefix_dfa(&word);
            automatons.push(dfa);
        }

        let stream = unimplemented!();

        unimplemented!()
    }
}
