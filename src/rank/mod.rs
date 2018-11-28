pub mod criterion;
mod ranked_stream;
mod distinct_map;

use crate::{Match, DocumentId};

pub use self::ranked_stream::{QueryBuilder, DistinctQueryBuilder};

#[inline]
fn match_query_index(a: &Match, b: &Match) -> bool {
    a.query_index == b.query_index
}

#[derive(Debug, Clone)]
pub struct Document {
    pub id: DocumentId,
    pub matches: Vec<Match>,
}

impl Document {
    pub fn new(doc: DocumentId, match_: Match) -> Self {
        unsafe { Self::from_sorted_matches(doc, vec![match_]) }
    }

    pub fn from_matches(doc: DocumentId, mut matches: Vec<Match>) -> Self {
        matches.sort_unstable();
        unsafe { Self::from_sorted_matches(doc, matches) }
    }

    pub unsafe fn from_sorted_matches(id: DocumentId, matches: Vec<Match>) -> Self {
        Self { id, matches }
    }
}
