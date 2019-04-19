use std::hash::Hash;
use std::ops::{Range, Deref};
use std::rc::Rc;
use std::time::Instant;
use std::{cmp, mem};

use rayon::slice::ParallelSliceMut;
use slice_group_by::GroupByMut;
use meilidb_tokenizer::{is_cjk, split_query_string};
use hashbrown::{HashMap, HashSet};
use fst::Streamer;
use log::info;

use crate::automaton::{self, DfaExt, AutomatonExt};
use crate::distinct_map::{DistinctMap, BufferedDistinctMap};
use crate::criterion::Criteria;
use crate::raw_documents_from_matches;
use crate::{Match, DocumentId, Index, Store, RawDocument, Document};

fn generate_automatons(query: &str) -> Vec<DfaExt> {
    let has_end_whitespace = query.chars().last().map_or(false, char::is_whitespace);
    let mut groups = split_query_string(query).map(str::to_lowercase).peekable();
    let mut automatons = Vec::new();

    while let Some(word) = groups.next() {
        let has_following_word = groups.peek().is_some();
        let lev = if has_following_word || has_end_whitespace || word.chars().all(is_cjk) {
            automaton::build_dfa(&word)
        } else {
            automaton::build_prefix_dfa(&word)
        };
        automatons.push(lev);
    }

    automatons
}

pub struct QueryBuilder<'c, I, FI = fn(DocumentId) -> bool> {
    index: I,
    criteria: Criteria<'c>,
    searchable_attrs: Option<HashSet<u16>>,
    filter: Option<FI>,
}

impl<'c, I> QueryBuilder<'c, I, fn(DocumentId) -> bool> {
    pub fn new(index: I) -> Self {
        QueryBuilder::with_criteria(index, Criteria::default())
    }

    pub fn with_criteria(index: I, criteria: Criteria<'c>) -> Self {
        QueryBuilder { index, criteria, searchable_attrs: None, filter: None }
    }
}

impl<'c, I, FI> QueryBuilder<'c, I, FI>
{
    pub fn with_filter<F>(self, function: F) -> QueryBuilder<'c, I, F>
    where F: Fn(DocumentId) -> bool,
    {
        QueryBuilder {
            index: self.index,
            criteria: self.criteria,
            searchable_attrs: self.searchable_attrs,
            filter: Some(function)
        }
    }

    pub fn with_distinct<F, K>(self, function: F, size: usize) -> DistinctQueryBuilder<'c, I, FI, F>
    where F: Fn(DocumentId) -> Option<K>,
          K: Hash + Eq,
    {
        DistinctQueryBuilder {
            inner: self,
            function: function,
            size: size
        }
    }

    pub fn add_searchable_attribute(&mut self, attribute: u16) {
        let attributes = self.searchable_attrs.get_or_insert_with(HashSet::new);
        attributes.insert(attribute);
    }
}

impl<'c, I, FI, S> QueryBuilder<'c, I, FI>
where I: Deref<Target=Index<S>>,
      S: Store,
{
    fn query_all(&self, query: &str) -> Vec<RawDocument> {
        let automatons = generate_automatons(query);
        let fst = self.index.set.as_fst();

        let mut stream = {
            let mut op_builder = fst::raw::OpBuilder::new();
            for automaton in &automatons {
                let stream = fst.search(automaton);
                op_builder.push(stream);
            }
            op_builder.r#union()
        };

        let mut matches = Vec::new();

        while let Some((input, indexed_values)) = stream.next() {
            for iv in indexed_values {
                let automaton = &automatons[iv.index];
                let distance = automaton.eval(input).to_u8();
                let is_exact = distance == 0 && input.len() == automaton.query_len();

                // let doc_indexes = &self.index.indexes;
                // let doc_indexes = &doc_indexes[iv.value as usize];

                let doc_indexes = self.index.store.get_indexes(input).unwrap().unwrap();

                for di in doc_indexes.as_slice() {
                    if self.searchable_attrs.as_ref().map_or(true, |r| r.contains(&di.attribute)) {
                        let match_ = Match {
                            query_index: iv.index as u32,
                            distance: distance,
                            attribute: di.attribute,
                            word_index: di.word_index,
                            is_exact: is_exact,
                            char_index: di.char_index,
                            char_length: di.char_length,
                        };
                        matches.push((di.document_id, match_));
                    }
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

impl<'c, I, FI, S> QueryBuilder<'c, I, FI>
where I: Deref<Target=Index<S>>,
      FI: Fn(DocumentId) -> bool,
      S: Store,
{
    pub fn query(self, query: &str, range: Range<usize>) -> Vec<Document> {
        // We delegate the filter work to the distinct query builder,
        // specifying a distinct rule that has no effect.
        if self.filter.is_some() {
            let builder = self.with_distinct(|_| None as Option<()>, 1);
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

pub struct DistinctQueryBuilder<'c, I, FI, FD> {
    inner: QueryBuilder<'c, I, FI>,
    function: FD,
    size: usize,
}

impl<'c, I, FI, FD> DistinctQueryBuilder<'c, I, FI, FD>
{
    pub fn with_filter<F>(self, function: F) -> DistinctQueryBuilder<'c, I, F, FD>
    where F: Fn(DocumentId) -> bool,
    {
        DistinctQueryBuilder {
            inner: self.inner.with_filter(function),
            function: self.function,
            size: self.size
        }
    }

    pub fn add_searchable_attribute(&mut self, attribute: u16) {
        self.inner.add_searchable_attribute(attribute);
    }
}

impl<'c, I, FI, FD, K, S> DistinctQueryBuilder<'c, I, FI, FD>
where I: Deref<Target=Index<S>>,
      FI: Fn(DocumentId) -> bool,
      FD: Fn(DocumentId) -> Option<K>,
      K: Hash + Eq,
      S: Store,
{
    pub fn query(self, query: &str, range: Range<usize>) -> Vec<Document> {
        let start = Instant::now();
        let mut documents = self.inner.query_all(query);
        info!("query_all took {:.2?}", start.elapsed());

        let mut groups = vec![documents.as_mut_slice()];
        let mut key_cache = HashMap::new();

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
                                *entry.or_insert_with(|| (filter)(document.id))
                            },
                            None => true,
                        };

                        if filter_accepted {
                            let entry = key_cache.entry(document.id);
                            let key = entry.or_insert_with(|| (self.function)(document.id).map(Rc::new));

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
