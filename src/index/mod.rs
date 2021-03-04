mod search;
mod updates;

use std::sync::Arc;
use std::ops::Deref;

pub use search::{SearchQuery, SearchResult, DEFAULT_SEARCH_LIMIT};
pub use updates::{Settings, Facets, UpdateResult};

#[derive(Clone)]
pub struct Index(pub Arc<milli::Index>);

impl Deref for Index {
    type Target = milli::Index;

    fn deref(&self) -> &Self::Target {
        self.0.as_ref()
    }
}
