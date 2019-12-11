#[cfg(test)]
#[macro_use]
extern crate assert_matches;

mod automaton;
pub mod criterion;
mod database;
mod distinct_map;
mod error;
mod levenshtein;
mod number;
mod query_builder;
mod ranked_map;
mod raw_document;
pub mod raw_indexer;
mod reordered_attrs;
pub mod serde;
pub mod store;
mod update;

// TODO replace
mod bucket_sort;

pub use self::database::{BoxUpdateFn, Database, MainT, UpdateT};
pub use self::error::{Error, MResult};
pub use self::number::{Number, ParseNumberError};
pub use self::ranked_map::RankedMap;
pub use self::raw_document::RawDocument;
pub use self::store::Index;
pub use self::update::{EnqueuedUpdateResult, ProcessedUpdateResult, UpdateStatus, UpdateType};
pub use meilisearch_types::{DocIndex, DocumentId, Highlight, AttrCount};

use compact_arena::SmallArena;
use crate::bucket_sort::{QueryWordAutomaton, PostingsListView};
use crate::levenshtein::prefix_damerau_levenshtein;

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Document {
    pub id: DocumentId,
    pub highlights: Vec<Highlight>,

    // #[cfg(test)]
    // pub matches: Vec<TmpMatch>,
}

impl Document {
    pub fn from_raw<'a, 'tag, 'txn>(
        raw_document: RawDocument<'a, 'tag>,
        automatons: &[QueryWordAutomaton],
        arena: &SmallArena<'tag, PostingsListView<'txn>>,
    ) -> Document
    {
        let highlights = raw_document.raw_matches.iter().flat_map(|sm| {
            let postings_list = &arena[sm.postings_list];
            let input = postings_list.input();
            let query = &automatons[sm.query_index as usize].query;
            postings_list.iter().map(move |m| {
                let covered_area = if query.len() > input.len() {
                    input.len()
                } else {
                    prefix_damerau_levenshtein(query.as_bytes(), input).1
                };

                Highlight {
                    attribute: m.attribute,
                    char_index: m.char_index,
                    char_length: covered_area as u16,
                }
            })
        }).collect();

        Document { id: raw_document.id, highlights }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::mem;

    #[test]
    fn docindex_mem_size() {
        assert_eq!(mem::size_of::<DocIndex>(), 16);
    }
}
