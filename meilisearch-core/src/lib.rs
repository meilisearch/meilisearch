#![allow(clippy::type_complexity)]

#[cfg(test)]
#[macro_use]
extern crate assert_matches;
#[macro_use]
extern crate pest_derive;

mod automaton;
mod bucket_sort;
mod database;
mod distinct_map;
mod error;
mod filters;
mod levenshtein;
mod number;
mod query_builder;
mod query_tree;
mod query_words_mapper;
mod ranked_map;
mod raw_document;
mod reordered_attrs;
pub mod criterion;
pub mod facets;
pub mod raw_indexer;
pub mod serde;
pub mod settings;
pub mod store;
pub mod update;

pub use self::database::{BoxUpdateFn, Database, DatabaseOptions, MainT, UpdateT, MainWriter, MainReader, UpdateWriter, UpdateReader};
pub use self::error::{Error, HeedError, FstError, MResult, pest_error, FacetError};
pub use self::filters::Filter;
pub use self::number::{Number, ParseNumberError};
pub use self::ranked_map::RankedMap;
pub use self::raw_document::RawDocument;
pub use self::store::Index;
pub use self::update::{EnqueuedUpdateResult, ProcessedUpdateResult, UpdateStatus, UpdateType};
pub use meilisearch_types::{DocIndex, DocumentId, Highlight};
pub use meilisearch_schema::Schema;
pub use query_words_mapper::QueryWordsMapper;
pub use query_tree::MAX_QUERY_LEN;

use compact_arena::SmallArena;
use log::{error, trace};
use std::borrow::Cow;
use std::collections::HashMap;
use std::convert::TryFrom;

use crate::bucket_sort::PostingsListView;
use crate::levenshtein::prefix_damerau_levenshtein;
use crate::query_tree::{QueryId, QueryKind};
use crate::reordered_attrs::ReorderedAttrs;

type FstSetCow<'a> = fst::Set<Cow<'a, [u8]>>;
type FstMapCow<'a> = fst::Map<Cow<'a, [u8]>>;

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct Document {
    pub id: DocumentId,
    pub highlights: Vec<Highlight>,

    #[cfg(test)]
    pub matches: Vec<crate::bucket_sort::SimpleMatch>,
}

fn highlights_from_raw_document<'a, 'tag, 'txn>(
    raw_document: &RawDocument<'a, 'tag>,
    queries_kinds: &HashMap<QueryId, &QueryKind>,
    arena: &SmallArena<'tag, PostingsListView<'txn>>,
    searchable_attrs: Option<&ReorderedAttrs>,
    schema: &Schema,
) -> Vec<Highlight>
{
    let mut highlights = Vec::new();

    for bm in raw_document.bare_matches.iter() {
        let postings_list = &arena[bm.postings_list];
        let input = postings_list.input();
        let kind = &queries_kinds.get(&bm.query_index);

        for di in postings_list.iter() {
            let covered_area = match kind {
                Some(QueryKind::NonTolerant(query)) | Some(QueryKind::Tolerant(query)) => {
                    let len = if query.len() > input.len() {
                        input.len()
                    } else {
                        prefix_damerau_levenshtein(query.as_bytes(), input).1
                    };
                    u16::try_from(len).unwrap_or(u16::max_value())
                },
                _ => di.char_length,
            };

            let attribute = searchable_attrs
                .and_then(|sa| sa.reverse(di.attribute))
                .unwrap_or(di.attribute);

            let attribute = match schema.indexed_pos_to_field_id(attribute) {
                Some(field_id) => field_id.0,
                None => {
                    error!("Cannot convert indexed_pos {} to field_id", attribute);
                    trace!("Schema is compromized; {:?}", schema);
                    continue
                }
            };

            let highlight = Highlight {
                attribute,
                char_index: di.char_index,
                char_length: covered_area,
            };

            highlights.push(highlight);
        }
    }

    highlights
}

impl Document {
    #[cfg(not(test))]
    pub fn from_highlights(id: DocumentId, highlights: &[Highlight]) -> Document {
        Document { id, highlights: highlights.to_owned() }
    }

    #[cfg(test)]
    pub fn from_highlights(id: DocumentId, highlights: &[Highlight]) -> Document {
        Document { id, highlights: highlights.to_owned(), matches: Vec::new() }
    }

    #[cfg(not(test))]
    pub fn from_raw<'a, 'tag, 'txn>(
        raw_document: RawDocument<'a, 'tag>,
        queries_kinds: &HashMap<QueryId, &QueryKind>,
        arena: &SmallArena<'tag, PostingsListView<'txn>>,
        searchable_attrs: Option<&ReorderedAttrs>,
        schema: &Schema,
    ) -> Document
    {
        let highlights = highlights_from_raw_document(
            &raw_document,
            queries_kinds,
            arena,
            searchable_attrs,
            schema,
        );

        Document { id: raw_document.id, highlights }
    }

    #[cfg(test)]
    pub fn from_raw<'a, 'tag, 'txn>(
        raw_document: RawDocument<'a, 'tag>,
        queries_kinds: &HashMap<QueryId, &QueryKind>,
        arena: &SmallArena<'tag, PostingsListView<'txn>>,
        searchable_attrs: Option<&ReorderedAttrs>,
        schema: &Schema,
    ) -> Document
    {
        use crate::bucket_sort::SimpleMatch;

        let highlights = highlights_from_raw_document(
            &raw_document,
            queries_kinds,
            arena,
            searchable_attrs,
            schema,
        );

        let mut matches = Vec::new();
        for sm in raw_document.processed_matches {
            let attribute = searchable_attrs
                .and_then(|sa| sa.reverse(sm.attribute))
                .unwrap_or(sm.attribute);

            let attribute = match schema.indexed_pos_to_field_id(attribute) {
                Some(field_id) => field_id.0,
                None => {
                    error!("Cannot convert indexed_pos {} to field_id", attribute);
                    trace!("Schema is compromized; {:?}", schema);
                    continue
                }
            };

            matches.push(SimpleMatch { attribute, ..sm });
        }
        matches.sort_unstable();

        Document { id: raw_document.id, highlights, matches }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::mem;

    #[test]
    fn docindex_mem_size() {
        assert_eq!(mem::size_of::<DocIndex>(), 12);
    }
}
