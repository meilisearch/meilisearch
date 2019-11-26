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

pub use self::database::{BoxUpdateFn, Database};
pub use self::error::{Error, MResult};
pub use self::number::{Number, ParseNumberError};
pub use self::ranked_map::RankedMap;
pub use self::raw_document::RawDocument;
pub use self::store::Index;
pub use self::update::{EnqueuedUpdateResult, ProcessedUpdateResult, UpdateStatus, UpdateType};
pub use meilisearch_types::{DocIndex, DocumentId, Highlight};

#[doc(hidden)]
#[derive(Debug, Copy, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct TmpMatch {
    pub query_index: u32,
    pub distance: u8,
    pub attribute: u16,
    pub word_index: u16,
    pub is_exact: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Document {
    pub id: DocumentId,
    pub highlights: Vec<Highlight>,

    #[cfg(test)]
    pub matches: Vec<TmpMatch>,
}

impl Document {
    #[cfg(not(test))]
    fn from_raw(raw: RawDocument) -> Document {
        Document {
            id: raw.id,
            highlights: raw.highlights,
        }
    }

    #[cfg(test)]
    fn from_raw(raw: RawDocument) -> Document {
        let len = raw.query_index().len();
        let mut matches = Vec::with_capacity(len);

        let query_index = raw.query_index();
        let distance = raw.distance();
        let attribute = raw.attribute();
        let word_index = raw.word_index();
        let is_exact = raw.is_exact();

        for i in 0..len {
            let match_ = TmpMatch {
                query_index: query_index[i],
                distance: distance[i],
                attribute: attribute[i],
                word_index: word_index[i],
                is_exact: is_exact[i],
            };
            matches.push(match_);
        }

        Document {
            id: raw.id,
            matches,
            highlights: raw.highlights,
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
