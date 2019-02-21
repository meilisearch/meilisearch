use std::{cmp, mem, vec, str, char};
use std::ops::{Deref, Range};
use std::time::Instant;
use std::error::Error;
use std::hash::Hash;
use std::rc::Rc;

use rayon::slice::ParallelSliceMut;
use slice_group_by::GroupByMut;
use hashbrown::HashMap;
use fst::Streamer;
use rocksdb::DB;
use log::info;

use crate::automaton::{self, DfaExt, AutomatonExt};
use crate::rank::distinct_map::{DistinctMap, BufferedDistinctMap};
use crate::rank::criterion::Criteria;
use crate::database::DatabaseView;
use crate::{Match, DocumentId};
use crate::rank::{raw_documents_from_matches, RawDocument, Document};

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

pub type FilterFunc<D> = fn(DocumentId, &DatabaseView<D>) -> bool;

pub struct QueryBuilder<'a, 'b, D, FI>
where D: Deref<Target=DB>
{
    view: &'a DatabaseView<D>,
    criteria: Criteria<'b>,
    filter: Option<FI>,
}

impl<'a, 'b, D> QueryBuilder<'a, 'b, D, FilterFunc<D>>
where D: Deref<Target=DB>
{
    pub fn new(view: &'a DatabaseView<D>) -> Result<Self, Box<Error>> {
        QueryBuilder::with_criteria(view, Criteria::default())
    }

    pub fn with_criteria(view: &'a DatabaseView<D>, criteria: Criteria<'b>) -> Result<Self, Box<Error>> {
        Ok(QueryBuilder { view, criteria, filter: None })
    }
}

impl<'a, 'b, D, FI> QueryBuilder<'a, 'b, D, FI>
where D: Deref<Target=DB>,
{
    pub fn with_filter<F>(self, function: F) -> QueryBuilder<'a, 'b, D, F>
    where F: Fn(DocumentId, &DatabaseView<D>) -> bool,
    {
        QueryBuilder {
            view: self.view,
            criteria: self.criteria,
            filter: Some(function)
        }
    }

    pub fn with_distinct<F, K>(self, function: F, size: usize) -> DistinctQueryBuilder<'a, 'b, D, FI, F>
    where F: Fn(DocumentId, &DatabaseView<D>) -> Option<K>,
          K: Hash + Eq,
    {
        DistinctQueryBuilder {
            inner: self,
            function: function,
            size: size
        }
    }

    fn query_all(&self, query: &str) -> Vec<RawDocument> {
        let automatons = split_whitespace_automatons(query);

        let mut stream = {
            let mut op_builder = fst::map::OpBuilder::new();
            for automaton in &automatons {
                let stream = self.view.index().map.search(automaton);
                op_builder.push(stream);
            }
            op_builder.union()
        };

        let mut matches = Vec::new();

        while let Some((input, indexed_values)) = stream.next() {
            for iv in indexed_values {
                let automaton = &automatons[iv.index];
                let distance = automaton.eval(input).to_u8();
                let is_exact = distance == 0 && input.len() == automaton.query_len();

                let doc_indexes = &self.view.index().indexes;
                let doc_indexes = &doc_indexes[iv.value as usize];

                for doc_index in doc_indexes {
                    let match_ = Match {
                        query_index: iv.index as u32,
                        distance: distance,
                        attribute: doc_index.attribute,
                        word_index: doc_index.word_index,
                        is_exact: is_exact,
                        char_index: doc_index.char_index,
                        char_length: doc_index.char_length,
                    };
                    matches.push((doc_index.document_id, match_));
                }
            }
        }

        let total_matches = matches.len();
        let raw_documents = raw_documents_from_matches(matches);

        info!("{} total documents to classify", raw_documents.len());
        info!("{} total matches to classify", total_matches);

        raw_documents
    }
}

impl<'a, 'b, D, FI> QueryBuilder<'a, 'b, D, FI>
where D: Deref<Target=DB>,
      FI: Fn(DocumentId, &DatabaseView<D>) -> bool,
{
    pub fn query(self, query: &str, range: Range<usize>) -> Vec<Document> {
        // We delegate the filter work to the distinct query builder,
        // specifying a distinct rule that has no effect.
        if self.filter.is_some() {
            let builder = self.with_distinct(|_, _| None as Option<()>, 1);
            return builder.query(query, range);
        }

        let start = Instant::now();
        let mut documents = self.query_all(query);
        info!("query_all took {:.2?}", start.elapsed());

        let mut groups = vec![documents.as_mut_slice()];

        'criteria: for (ci, criterion) in self.criteria.as_ref().iter().enumerate() {
            let tmp_groups = mem::replace(&mut groups, Vec::new());
            let mut documents_seen = 0;

            for group in tmp_groups {
                info!("criterion {}, documents group of size {}", ci, group.len());

                // if this group does not overlap with the requested range,
                // push it without sorting and splitting it
                if documents_seen + group.len() < range.start {
                    documents_seen += group.len();
                    groups.push(group);
                    continue;
                }

                let start = Instant::now();
                group.par_sort_unstable_by(|a, b| criterion.evaluate(a, b));
                info!("criterion {} sort took {:.2?}", ci, start.elapsed());

                for group in group.binary_group_by_mut(|a, b| criterion.eq(a, b)) {
                    documents_seen += group.len();
                    groups.push(group);

                    // we have sort enough documents if the last document sorted is after
                    // the end of the requested range, we can continue to the next criterion
                    if documents_seen >= range.end { continue 'criteria }
                }
            }
        }

        let offset = cmp::min(documents.len(), range.start);
        let iter = documents.into_iter().skip(offset).take(range.len());
        iter.map(|d| Document::from_raw(&d)).collect()
    }
}

pub struct DistinctQueryBuilder<'a, 'b, D, FI, FD>
where D: Deref<Target=DB>
{
    inner: QueryBuilder<'a, 'b, D, FI>,
    function: FD,
    size: usize,
}

impl<'a, 'b, D, FI, FD> DistinctQueryBuilder<'a, 'b, D, FI, FD>
where D: Deref<Target=DB>,
{
    pub fn with_filter<F>(self, function: F) -> DistinctQueryBuilder<'a, 'b, D, F, FD>
    where F: Fn(DocumentId, &DatabaseView<D>) -> bool,
    {
        DistinctQueryBuilder {
            inner: self.inner.with_filter(function),
            function: self.function,
            size: self.size
        }
    }
}

impl<'a, 'b, D, FI, FD, K> DistinctQueryBuilder<'a, 'b, D, FI, FD>
where D: Deref<Target=DB>,
      FI: Fn(DocumentId, &DatabaseView<D>) -> bool,
      FD: Fn(DocumentId, &DatabaseView<D>) -> Option<K>,
      K: Hash + Eq,
{
    pub fn query(self, query: &str, range: Range<usize>) -> Vec<Document> {
        let start = Instant::now();
        let mut documents = self.inner.query_all(query);
        info!("query_all took {:.2?}", start.elapsed());

        let mut groups = vec![documents.as_mut_slice()];
        let mut key_cache = HashMap::new();
        let view = &self.inner.view;

        let mut filter_map = HashMap::new();
        // these two variables informs on the current distinct map and
        // on the raw offset of the start of the group where the
        // range.start bound is located according to the distinct function
        let mut distinct_map = DistinctMap::new(self.size);
        let mut distinct_raw_offset = 0;

        'criteria: for (ci, criterion) in self.inner.criteria.as_ref().iter().enumerate() {
            let tmp_groups = mem::replace(&mut groups, Vec::new());
            let mut buf_distinct = BufferedDistinctMap::new(&mut distinct_map);
            let mut documents_seen = 0;

            for group in tmp_groups {
                info!("criterion {}, documents group of size {}", ci, group.len());

                // if this group does not overlap with the requested range,
                // push it without sorting and splitting it
                if documents_seen + group.len() < distinct_raw_offset {
                    documents_seen += group.len();
                    groups.push(group);
                    continue;
                }

                let start = Instant::now();
                group.par_sort_unstable_by(|a, b| criterion.evaluate(a, b));
                info!("criterion {} sort took {:.2?}", ci, start.elapsed());

                for group in group.binary_group_by_mut(|a, b| criterion.eq(a, b)) {
                    // we must compute the real distinguished len of this sub-group
                    for document in group.iter() {
                        let filter_accepted = match &self.inner.filter {
                            Some(filter) => {
                                let entry = filter_map.entry(document.id);
                                *entry.or_insert_with(|| (filter)(document.id, view))
                            },
                            None => true,
                        };

                        if filter_accepted {
                            let entry = key_cache.entry(document.id);
                            let key = entry.or_insert_with(|| (self.function)(document.id, view).map(Rc::new));

                            match key.clone() {
                                Some(key) => buf_distinct.register(key),
                                None => buf_distinct.register_without_key(),
                            };
                        }

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
            let filter_accepted = match &self.inner.filter {
                Some(_) => filter_map.remove(&document.id).expect("BUG: filtered not found"),
                None => true,
            };

            if filter_accepted {
                let key = key_cache.remove(&document.id).expect("BUG: cached key not found");
                let distinct_accepted = match key {
                    Some(key) => seen.register(key),
                    None => seen.register_without_key(),
                };

                if distinct_accepted && seen.len() > range.start {
                    out_documents.push(Document::from_raw(&document));
                    if out_documents.len() == range.len() { break }
                }
            }
        }

        out_documents
    }
}
