use std::ops::{Deref, Range, RangeBounds};
use std::collections::HashMap;
use std::{mem, vec, str};
use std::ops::Bound::*;
use std::error::Error;
use std::hash::Hash;
use std::rc::Rc;

use fnv::FnvHashMap;
use fst::Streamer;
use group_by::GroupByMut;
use ::rocksdb::rocksdb::{DB, Snapshot};

use crate::automaton::{self, DfaExt, AutomatonExt};
use crate::rank::criterion::{self, Criterion};
use crate::blob::{PositiveBlob, Merge};
use crate::blob::ops::Union;
use crate::{Match, DocumentId};
use crate::database::Retrieve;
use crate::rank::Document;
use crate::index::Index;

fn clamp_range<T: Copy + Ord>(range: Range<T>, big: Range<T>) -> Range<T> {
    Range {
        start: range.start.min(big.end).max(big.start),
        end: range.end.min(big.end).max(big.start),
    }
}

fn split_whitespace_automatons(query: &str) -> Vec<DfaExt> {
    let mut automatons = Vec::new();
    for query in query.split_whitespace().map(str::to_lowercase) {
        let lev = automaton::build_prefix_dfa(&query);
        automatons.push(lev);
    }
    automatons
}

pub struct QueryBuilder<T: Deref<Target=DB>, C> {
    snapshot: Snapshot<T>,
    blob: PositiveBlob,
    criteria: Vec<C>,
}

impl<T: Deref<Target=DB>> QueryBuilder<T, Box<dyn Criterion>> {
    pub fn new(snapshot: Snapshot<T>) -> Result<Self, Box<Error>> {
        QueryBuilder::with_criteria(snapshot, criterion::default())
    }
}

impl<T, C> QueryBuilder<T, C>
where T: Deref<Target=DB>,
{
    pub fn with_criteria(snapshot: Snapshot<T>, criteria: Vec<C>) -> Result<Self, Box<Error>> {
        let blob = snapshot.data_index()?;
        Ok(QueryBuilder { snapshot, blob, criteria })
    }

    pub fn criteria(&mut self, criteria: Vec<C>) -> &mut Self {
        self.criteria = criteria;
        self
    }

    pub fn with_distinct<F>(self, function: F, size: usize) -> DistinctQueryBuilder<T, F, C> {
        DistinctQueryBuilder {
            snapshot: self.snapshot,
            blob: self.blob,
            criteria: self.criteria,
            function: function,
            size: size
        }
    }

    fn query_all(&self, query: &str) -> Vec<Document> {
        let automatons = split_whitespace_automatons(query);
        let mut stream: Union = unimplemented!();
        let mut matches = FnvHashMap::default();

        while let Some((string, indexed_values)) = stream.next() {
            for iv in indexed_values {
                let automaton = &automatons[iv.index];
                let distance = automaton.eval(string).to_u8();
                let is_exact = distance == 0 && string.len() == automaton.query_len();

                for doc_index in iv.doc_indexes.as_slice() {
                    let match_ = Match {
                        query_index: iv.index as u32,
                        distance: distance,
                        attribute: doc_index.attribute,
                        attribute_index: doc_index.attribute_index,
                        is_exact: is_exact,
                    };
                    matches.entry(doc_index.document_id).or_insert_with(Vec::new).push(match_);
                }
            }
        }

        matches.into_iter().map(|(id, matches)| Document::from_matches(id, matches)).collect()
    }
}

impl<T, C> QueryBuilder<T, C>
where T: Deref<Target=DB>,
      C: Criterion,
{
    pub fn query(&self, query: &str, range: impl RangeBounds<usize>) -> Vec<Document> {
        let mut documents = self.query_all(query);
        let mut groups = vec![documents.as_mut_slice()];

        for criterion in self.criteria {
            let tmp_groups = mem::replace(&mut groups, Vec::new());

            for group in tmp_groups {
                group.sort_unstable_by(|a, b| criterion.evaluate(a, b));
                for group in GroupByMut::new(group, |a, b| criterion.eq(a, b)) {
                    groups.push(group);
                }
            }
        }

        // let range = clamp_range(range, 0..documents.len());
        let range: Range<usize> = unimplemented!();
        documents[range].to_vec()
    }
}

pub struct DistinctQueryBuilder<T: Deref<Target=DB>, F, C> {
    snapshot: Snapshot<T>,
    blob: PositiveBlob,
    criteria: Vec<C>,
    function: F,
    size: usize,
}

// pub struct Schema;
// pub struct DocDatabase;
// where F: Fn(&Schema, &DocDatabase) -> Option<K>,
//       K: Hash + Eq,

impl<T: Deref<Target=DB>, F, C> DistinctQueryBuilder<T, F, C>
where T: Deref<Target=DB>,
      C: Criterion,
{
    pub fn query(&self, query: &str, range: impl RangeBounds<usize>) -> Vec<Document> {
        // let mut documents = self.retrieve_all_documents();
        // let mut groups = vec![documents.as_mut_slice()];

        // for criterion in self.criteria {
        //     let tmp_groups = mem::replace(&mut groups, Vec::new());

        //     for group in tmp_groups {
        //         group.sort_unstable_by(|a, b| criterion.evaluate(a, b));
        //         for group in GroupByMut::new(group, |a, b| criterion.eq(a, b)) {
        //             groups.push(group);
        //         }
        //     }
        // }

        // let mut out_documents = Vec::with_capacity(range.len());
        // let (distinct, limit) = self.distinct;
        // let mut seen = DistinctMap::new(limit);

        // for document in documents {
        //     let accepted = match distinct(&document.id) {
        //         Some(key) => seen.digest(key),
        //         None => seen.accept_without_key(),
        //     };

        //     if accepted {
        //         if seen.len() == range.end { break }
        //         if seen.len() >= range.start {
        //             out_documents.push(document);
        //         }
        //     }
        // }

        // out_documents

        unimplemented!()
    }
}

pub struct DistinctMap<K> {
    inner: HashMap<K, usize>,
    limit: usize,
    len: usize,
}

impl<K: Hash + Eq> DistinctMap<K> {
    pub fn new(limit: usize) -> Self {
        DistinctMap {
            inner: HashMap::new(),
            limit: limit,
            len: 0,
        }
    }

    pub fn digest(&mut self, key: K) -> bool {
        let seen = self.inner.entry(key).or_insert(0);
        if *seen < self.limit {
            *seen += 1;
            self.len += 1;
            true
        } else {
            false
        }
    }

    pub fn accept_without_key(&mut self) -> bool {
        self.len += 1;
        true
    }

    pub fn len(&self) -> usize {
        self.len
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn easy_distinct_map() {
        let mut map = DistinctMap::new(2);
        for x in &[1, 1, 1, 2, 3, 4, 5, 6, 6, 6, 6, 6] {
            map.digest(x);
        }
        assert_eq!(map.len(), 8);

        let mut map = DistinctMap::new(2);
        assert_eq!(map.digest(1), true);
        assert_eq!(map.digest(1), true);
        assert_eq!(map.digest(1), false);
        assert_eq!(map.digest(1), false);

        assert_eq!(map.digest(2), true);
        assert_eq!(map.digest(3), true);
        assert_eq!(map.digest(2), true);
        assert_eq!(map.digest(2), false);

        assert_eq!(map.len(), 5);
    }
}
