use std::{cmp, mem, vec, str, char};
use std::ops::{Deref, Range};
use std::error::Error;
use std::hash::Hash;
use std::rc::Rc;

use group_by::GroupByMut;
use hashbrown::HashMap;
use fst::Streamer;
use rocksdb::DB;

use crate::automaton::{self, DfaExt, AutomatonExt};
use crate::rank::distinct_map::{DistinctMap, BufferedDistinctMap};
use crate::rank::criterion::Criteria;
use crate::database::DatabaseView;
use crate::{Match, DocumentId};
use crate::rank::Document;

fn split_whitespace_automatons(query: &str) -> Vec<DfaExt> {
    let has_end_whitespace = query.chars().last().map_or(false, char::is_whitespace);
    let mut automatons = Vec::new();
    let mut words = query.split_whitespace().map(str::to_lowercase).peekable();

    while let Some(word) = words.next() {
        let has_following_word = words.peek().is_some();
        let lev = if has_following_word || has_end_whitespace {
            automaton::build_dfa(&word)
        } else {
            automaton::build_prefix_dfa(&word)
        };
        automatons.push(lev);
    }

    automatons
}

/// A structure that allows to query documents that match a query.
///
/// This builder is able to handle different criteria, useful to implement custom ranking.
pub struct QueryBuilder<'a, D>
where D: Deref<Target=DB>
{
    view: &'a DatabaseView<D>,
    criteria: Criteria<D>,
}

impl<'a, D> QueryBuilder<'a, D>
where D: Deref<Target=DB>
{
    /// Create a builder that use the default criteria.
    ///
    /// You can have more informations about [the default criteria][1] on the documentation of it.
    ///
    /// [1]: criterion/struct.Criteria.html
    pub fn new(view: &'a DatabaseView<D>) -> Result<Self, Box<Error>> {
        QueryBuilder::with_criteria(view, Criteria::default())
    }
}

impl<'a, D> QueryBuilder<'a, D>
where D: Deref<Target=DB>
{
    /// Create a builder that use the criteria that you specify.
    pub fn with_criteria(view: &'a DatabaseView<D>, criteria: Criteria<D>) -> Result<Self, Box<Error>> {
        Ok(QueryBuilder { view, criteria })
    }

    /// Change the criteria of this builder with the one specified.
    pub fn criteria(&mut self, criteria: Criteria<D>) -> &mut Self {
        self.criteria = criteria;
        self
    }

    /// Change this query builder to become a query distinct builder.
    ///
    /// The `F` parameter is a function that takes the document id, the database view and must
    /// return a key that will identify to which group the given document belongs.
    ///
    /// The `size` parameter specify the length of the groups. How many maximum number of documents
    /// of the same group must appear in the result.
    pub fn with_distinct<F>(self, function: F, size: usize) -> DistinctQueryBuilder<'a, D, F> {
        DistinctQueryBuilder {
            inner: self,
            function: function,
            size: size
        }
    }

    fn query_all(&self, query: &str) -> Vec<Document> {
        let automatons = split_whitespace_automatons(query);

        let mut stream = {
            let mut op_builder = fst::map::OpBuilder::new();
            for automaton in &automatons {
                let stream = self.view.blob().as_map().search(automaton);
                op_builder.push(stream);
            }
            op_builder.union()
        };

        let mut matches = HashMap::new();

        while let Some((input, indexed_values)) = stream.next() {
            for iv in indexed_values {
                let automaton = &automatons[iv.index];
                let distance = automaton.eval(input).to_u8();
                let is_exact = distance == 0 && input.len() == automaton.query_len();

                let doc_indexes = self.view.blob().as_indexes();
                let doc_indexes = &doc_indexes[iv.value as usize];

                for doc_index in doc_indexes {
                    let match_ = Match {
                        query_index: iv.index as u32,
                        distance: distance,
                        attribute: doc_index.attribute,
                        is_exact: is_exact,
                        word_area: doc_index.word_area,
                    };
                    matches.entry(doc_index.document_id).or_insert_with(Vec::new).push(match_);
                }
            }
        }

        matches.into_iter().map(|(id, matches)| Document::from_matches(id, matches)).collect()
    }
}

impl<'a, D> QueryBuilder<'a, D>
where D: Deref<Target=DB>,
{
    /// Query the database with a string and the range that is interresting to have.
    ///
    /// The range is useful for infinite scroll or page-based applications.
    pub fn query(&self, query: &str, range: Range<usize>) -> Vec<Document> {
        let mut documents = self.query_all(query);
        let mut groups = vec![documents.as_mut_slice()];
        let view = &self.view;

        'criteria: for criterion in self.criteria.as_ref() {
            let tmp_groups = mem::replace(&mut groups, Vec::new());
            let mut documents_seen = 0;

            for group in tmp_groups {
                // if this group does not overlap with the requested range,
                // push it without sorting and splitting it
                if documents_seen + group.len() < range.start {
                    documents_seen += group.len();
                    groups.push(group);
                    continue;
                }

                group.sort_unstable_by(|a, b| criterion.evaluate(a, b, view));

                for group in GroupByMut::new(group, |a, b| criterion.eq(a, b, view)) {
                    documents_seen += group.len();
                    groups.push(group);

                    // we have sort enough documents if the last document sorted is after
                    // the end of the requested range, we can continue to the next criterion
                    if documents_seen >= range.end { continue 'criteria }
                }
            }
        }

        // `drain` removes the documents efficiently using `ptr::copy`
        // TODO it could be more efficient to have a custom iterator
        let offset = cmp::min(documents.len(), range.start);
        documents.drain(0..offset);
        documents.truncate(range.len());
        documents
    }
}

/// A structure that allows to query documents that match a query and
/// that distinct documents of the same group.
///
/// This builder is created from a simple the `QueryBuilder` [with_distinct][1] method.
///
/// [1]: struct.QueryBuilder.html#method.with_distinct
pub struct DistinctQueryBuilder<'a, D, F>
where D: Deref<Target=DB>
{
    inner: QueryBuilder<'a, D>,
    function: F,
    size: usize,
}

impl<'a, D, F, K> DistinctQueryBuilder<'a, D, F>
where D: Deref<Target=DB>,
      F: Fn(DocumentId, &DatabaseView<D>) -> Option<K>,
      K: Hash + Eq,
{
    /// Query the database with a string and the range that is interresting to have.
    ///
    /// The range is useful for infinite scroll or page-based applications.
    pub fn query(&self, query: &str, range: Range<usize>) -> Vec<Document> {
        let mut documents = self.inner.query_all(query);
        let mut groups = vec![documents.as_mut_slice()];
        let mut key_cache = HashMap::new();
        let view = &self.inner.view;

        // these two variables informs on the current distinct map and
        // on the raw offset of the start of the group where the
        // range.start bound is located according to the distinct function
        let mut distinct_map = DistinctMap::new(self.size);
        let mut distinct_raw_offset = 0;

        'criteria: for criterion in self.inner.criteria.as_ref() {
            let tmp_groups = mem::replace(&mut groups, Vec::new());
            let mut buf_distinct = BufferedDistinctMap::new(&mut distinct_map);
            let mut documents_seen = 0;

            for group in tmp_groups {
                // if this group does not overlap with the requested range,
                // push it without sorting and splitting it
                if documents_seen + group.len() < distinct_raw_offset {
                    documents_seen += group.len();
                    groups.push(group);
                    continue;
                }

                group.sort_unstable_by(|a, b| criterion.evaluate(a, b, view));

                for group in GroupByMut::new(group, |a, b| criterion.eq(a, b, view)) {
                    // we must compute the real distinguished len of this sub-group
                    for document in group.iter() {
                        let entry = key_cache.entry(document.id);
                        let key = entry.or_insert_with(|| (self.function)(document.id, view).map(Rc::new));

                        match key.clone() {
                            Some(key) => buf_distinct.register(key),
                            None      => buf_distinct.register_without_key(),
                        };

                        // the requested range end is reached: stop computing distinct
                        if buf_distinct.len() >= range.end { break }
                    }

                    documents_seen += group.len();
                    groups.push(group);

                    // if this sub-group does not overlap with the requested range
                    // we must update the distinct map and its start index
                    if buf_distinct.len() < range.start {
                        buf_distinct.transfert_to_internal();
                        distinct_raw_offset = documents_seen;
                    }

                    // we have sort enough documents if the last document sorted is after
                    // the end of the requested range, we can continue to the next criterion
                    if buf_distinct.len() >= range.end { continue 'criteria }
                }
            }
        }

        let mut out_documents = Vec::with_capacity(range.len());
        let mut seen = BufferedDistinctMap::new(&mut distinct_map);

        for document in documents.into_iter().skip(distinct_raw_offset) {
            let key = key_cache.remove(&document.id).expect("BUG: cached key not found");

            let accepted = match key {
                Some(key) => seen.register(key),
                None      => seen.register_without_key(),
            };

            if accepted && seen.len() > range.start {
                out_documents.push(document);
                if out_documents.len() == range.len() { break }
            }
        }

        out_documents
    }
}
