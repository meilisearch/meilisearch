use std::cmp::{self, Ordering};
use std::collections::HashMap;
use std::ops::Range;

use compact_arena::SmallArena;
use sdset::SetBuf;
use slice_group_by::GroupBy;

use crate::automaton::QueryEnhancer;
use crate::bucket_sort::{SimpleMatch, PostingsListView, QueryWordAutomaton};
use crate::database::MainT;
use crate::query_tree::QueryId;
use crate::{store, RawDocument, MResult};

mod typo;
mod words;
mod proximity;
mod attribute;
mod words_position;
mod exact;
mod document_id;
mod sort_by_attr;

pub use self::typo::Typo;
pub use self::words::Words;
pub use self::proximity::Proximity;
pub use self::attribute::Attribute;
pub use self::words_position::WordsPosition;
pub use self::exact::Exact;
pub use self::document_id::DocumentId;
pub use self::sort_by_attr::SortByAttr;

pub trait Criterion {
    fn name(&self) -> &str;

    fn prepare<'h, 'p, 'tag, 'txn, 'q, 'r>(
        &self,
        _ctx: ContextMut<'h, 'p, 'tag, 'txn, 'q>,
        _documents: &mut [RawDocument<'r, 'tag>],
    ) -> MResult<()>
    {
        Ok(())
    }

    fn evaluate<'p, 'tag, 'txn, 'q, 'r>(
        &self,
        ctx: &Context<'p, 'tag, 'txn, 'q>,
        lhs: &RawDocument<'r, 'tag>,
        rhs: &RawDocument<'r, 'tag>,
    ) -> Ordering;

    #[inline]
    fn eq<'p, 'tag, 'txn, 'q, 'r>(
        &self,
        ctx: &Context<'p, 'tag, 'txn, 'q>,
        lhs: &RawDocument<'r, 'tag>,
        rhs: &RawDocument<'r, 'tag>,
    ) -> bool
    {
        self.evaluate(ctx, lhs, rhs) == Ordering::Equal
    }
}

pub struct ContextMut<'h, 'p, 'tag, 'txn, 'q> {
    pub reader: &'h heed::RoTxn<MainT>,
    pub postings_lists: &'p mut SmallArena<'tag, PostingsListView<'txn>>,
    pub query_mapping: &'q HashMap<QueryId, Range<usize>>,
    pub documents_fields_counts_store: store::DocumentsFieldsCounts,
}

pub struct Context<'p, 'tag, 'txn, 'q> {
    pub postings_lists: &'p SmallArena<'tag, PostingsListView<'txn>>,
    pub query_mapping: &'q HashMap<QueryId, Range<usize>>,
}

#[derive(Default)]
pub struct CriteriaBuilder<'a> {
    inner: Vec<Box<dyn Criterion + 'a>>,
}

impl<'a> CriteriaBuilder<'a> {
    pub fn new() -> CriteriaBuilder<'a> {
        CriteriaBuilder { inner: Vec::new() }
    }

    pub fn with_capacity(capacity: usize) -> CriteriaBuilder<'a> {
        CriteriaBuilder {
            inner: Vec::with_capacity(capacity),
        }
    }

    pub fn reserve(&mut self, additional: usize) {
        self.inner.reserve(additional)
    }

    pub fn add<C: 'a>(mut self, criterion: C) -> CriteriaBuilder<'a>
    where
        C: Criterion,
    {
        self.push(criterion);
        self
    }

    pub fn push<C: 'a>(&mut self, criterion: C)
    where
        C: Criterion,
    {
        self.inner.push(Box::new(criterion));
    }

    pub fn build(self) -> Criteria<'a> {
        Criteria { inner: self.inner }
    }
}

pub struct Criteria<'a> {
    inner: Vec<Box<dyn Criterion + 'a>>,
}

impl<'a> Default for Criteria<'a> {
    fn default() -> Self {
        CriteriaBuilder::with_capacity(7)
            .add(Typo)
            .add(Words)
            .add(Proximity)
            .add(Attribute)
            .add(WordsPosition)
            .add(Exact)
            .add(DocumentId)
            .build()
    }
}

impl<'a> AsRef<[Box<dyn Criterion + 'a>]> for Criteria<'a> {
    fn as_ref(&self) -> &[Box<dyn Criterion + 'a>] {
        &self.inner
    }
}

fn prepare_query_distances<'a, 'tag, 'txn>(
    documents: &mut [RawDocument<'a, 'tag>],
    query_mapping: &HashMap<QueryId, Range<usize>>,
    postings_lists: &SmallArena<'tag, PostingsListView<'txn>>,
) {
    for document in documents {
        if !document.processed_distances.is_empty() { continue }

        let mut processed = Vec::new();
        for m in document.bare_matches.iter() {
            if postings_lists[m.postings_list].is_empty() { continue }

            let range = query_mapping[&(m.query_index as usize)].clone();
            let new_len = cmp::max(range.end as usize, processed.len());
            processed.resize(new_len, None);

            for index in range {
                let index = index as usize;
                processed[index] = match processed[index] {
                    Some(distance) if distance > m.distance => Some(m.distance),
                    Some(distance) => Some(distance),
                    None => Some(m.distance),
                };
            }
        }

        document.processed_distances = processed;
    }
}

fn prepare_bare_matches<'a, 'tag, 'txn>(
    documents: &mut [RawDocument<'a, 'tag>],
    postings_lists: &mut SmallArena<'tag, PostingsListView<'txn>>,
    query_mapping: &HashMap<QueryId, Range<usize>>,
) {
    for document in documents {
        if !document.processed_matches.is_empty() { continue }

        let mut processed = Vec::new();
        for m in document.bare_matches.iter() {
            let postings_list = &postings_lists[m.postings_list];
            processed.reserve(postings_list.len());
            for di in postings_list.as_ref() {
                let simple_match = SimpleMatch {
                    query_index: m.query_index,
                    distance: m.distance,
                    attribute: di.attribute,
                    word_index: di.word_index,
                    is_exact: m.is_exact,
                };
                processed.push(simple_match);
            }
        }

        let processed = multiword_rewrite_matches(&mut processed, query_mapping);
        document.processed_matches = processed.into_vec();
    }
}

fn multiword_rewrite_matches(
    matches: &mut [SimpleMatch],
    query_mapping: &HashMap<QueryId, Range<usize>>,
) -> SetBuf<SimpleMatch>
{
    matches.sort_unstable_by_key(|m| (m.attribute, m.word_index));

    let mut padded_matches = Vec::with_capacity(matches.len());

    // let before_padding = Instant::now();
    // for each attribute of each document
    for same_document_attribute in matches.linear_group_by_key(|m| m.attribute) {
        // padding will only be applied
        // to word indices in the same attribute
        let mut padding = 0;
        let mut iter = same_document_attribute.linear_group_by_key(|m| m.word_index);

        // for each match at the same position
        // in this document attribute
        while let Some(same_word_index) = iter.next() {
            // find the biggest padding
            let mut biggest = 0;
            for match_ in same_word_index {
                let mut replacement = query_mapping[&(match_.query_index as usize)].clone();
                let replacement_len = replacement.len();
                let nexts = iter.remainder().linear_group_by_key(|m| m.word_index);

                if let Some(query_index) = replacement.next() {
                    let word_index = match_.word_index + padding as u16;
                    let match_ = SimpleMatch { query_index, word_index, ..*match_ };
                    padded_matches.push(match_);
                }

                let mut found = false;

                // look ahead and if there already is a match
                // corresponding to this padding word, abort the padding
                'padding: for (x, next_group) in nexts.enumerate() {
                    for (i, query_index) in replacement.clone().enumerate().skip(x) {
                        let word_index = match_.word_index + padding as u16 + (i + 1) as u16;
                        let padmatch = SimpleMatch { query_index, word_index, ..*match_ };

                        for nmatch_ in next_group {
                            let mut rep = query_mapping[&(nmatch_.query_index as usize)].clone();
                            let query_index = rep.next().unwrap();
                            if query_index == padmatch.query_index {
                                if !found {
                                    // if we find a corresponding padding for the
                                    // first time we must push preceding paddings
                                    for (i, query_index) in replacement.clone().enumerate().take(i) {
                                        let word_index = match_.word_index + padding as u16 + (i + 1) as u16;
                                        let match_ = SimpleMatch { query_index, word_index, ..*match_ };
                                        padded_matches.push(match_);
                                        biggest = biggest.max(i + 1);
                                    }
                                }

                                padded_matches.push(padmatch);
                                found = true;
                                continue 'padding;
                            }
                        }
                    }

                    // if we do not find a corresponding padding in the
                    // next groups so stop here and pad what was found
                    break;
                }

                if !found {
                    // if no padding was found in the following matches
                    // we must insert the entire padding
                    for (i, query_index) in replacement.enumerate() {
                        let word_index = match_.word_index + padding as u16 + (i + 1) as u16;
                        let match_ = SimpleMatch { query_index, word_index, ..*match_ };
                        padded_matches.push(match_);
                    }

                    biggest = biggest.max(replacement_len - 1);
                }
            }

            padding += biggest;
        }
    }

    // debug!("padding matches took {:.02?}", before_padding.elapsed());

    // With this check we can see that the loop above takes something
    // like 43% of the search time even when no rewrite is needed.
    // assert_eq!(before_matches, padded_matches);

    SetBuf::from_dirty(padded_matches)
}
