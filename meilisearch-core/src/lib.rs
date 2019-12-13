#[cfg(test)]
#[macro_use]
extern crate assert_matches;

mod automaton;
mod bucket_sort;
mod database;
mod distinct_map;
mod error;
mod levenshtein;
mod number;
mod query_builder;
mod ranked_map;
mod raw_document;
mod reordered_attrs;
mod update;
pub mod criterion;
pub mod raw_indexer;
pub mod serde;
pub mod store;

pub use self::database::{BoxUpdateFn, Database, MainT, UpdateT};
pub use self::error::{Error, MResult};
pub use self::number::{Number, ParseNumberError};
pub use self::ranked_map::RankedMap;
pub use self::raw_document::RawDocument;
pub use self::store::Index;
pub use self::update::{EnqueuedUpdateResult, ProcessedUpdateResult, UpdateStatus, UpdateType};
pub use meilisearch_types::{DocIndex, DocumentId, Highlight};

use compact_arena::SmallArena;
use crate::bucket_sort::{QueryWordAutomaton, PostingsListView};
use crate::levenshtein::prefix_damerau_levenshtein;

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct Document {
    pub id: DocumentId,
    pub highlights: Vec<Highlight>,

    #[cfg(test)]
    pub matches: Vec<crate::bucket_sort::SimpleMatch>,
}

impl Document {
    pub fn from_raw<'a, 'tag, 'txn>(
        raw_document: RawDocument<'a, 'tag>,
        automatons: &[QueryWordAutomaton],
        arena: &SmallArena<'tag, PostingsListView<'txn>>,
    ) -> Document
    {
        let highlights = raw_document.bare_matches.iter().flat_map(|sm| {
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

        #[cfg(not(test))]
        {
            Document { id: raw_document.id, highlights }
        }

        #[cfg(test)]
        {
            let matches = raw_document.processed_matches;
            Document { id: raw_document.id, highlights, matches }
        }
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
