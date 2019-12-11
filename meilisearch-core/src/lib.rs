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

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Document {
    pub id: DocumentId,
    pub highlights: Vec<Highlight>,

    // #[cfg(test)]
    // pub matches: Vec<TmpMatch>,
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
