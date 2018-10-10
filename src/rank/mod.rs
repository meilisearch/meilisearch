pub mod criterion;
mod ranked_stream;

use crate::{Match, DocumentId};

pub use self::ranked_stream::{RankedStream, Config};

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
        Self::from_sorted_matches(doc, vec![match_])
    }

    pub fn from_sorted_matches(id: DocumentId, matches: Vec<Match>) -> Self {
        Self { id, matches }
    }
}
