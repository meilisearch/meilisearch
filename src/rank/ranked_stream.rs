use std::collections::HashMap;
use std::hash::Hash;
use std::ops::Range;
use std::rc::Rc;
use std::{mem, vec};

use fnv::FnvHashMap;
use fst::Streamer;
use group_by::GroupByMut;

use crate::automaton::{DfaExt, AutomatonExt};
use crate::index::Index;
use crate::blob::{Blob, Merge};
use crate::rank::criterion::Criterion;
use crate::rank::Document;
use crate::{Match, DocumentId};

fn clamp_range<T: Copy + Ord>(range: Range<T>, big: Range<T>) -> Range<T> {
    Range {
        start: range.start.min(big.end).max(big.start),
        end: range.end.min(big.end).max(big.start),
    }
}

pub struct Config<'a, C, F> {
    pub blobs: &'a [Blob],
    pub automatons: Vec<DfaExt>,
    pub criteria: Vec<C>,
    pub distinct: (F, usize),
}

pub struct RankedStream<'m, C, F> {
    stream: crate::blob::Merge<'m>,
    automatons: Vec<Rc<DfaExt>>,
    criteria: Vec<C>,
    distinct: (F, usize),
}

impl<'m, C, F> RankedStream<'m, C, F> {
    pub fn new(config: Config<'m, C, F>) -> Self {
        let automatons: Vec<_> = config.automatons.into_iter().map(Rc::new).collect();

        RankedStream {
            stream: Merge::with_automatons(automatons.clone(), config.blobs),
            automatons: automatons,
            criteria: config.criteria,
            distinct: config.distinct,
        }
    }
}

impl<'m, C, F> RankedStream<'m, C, F> {
    fn retrieve_all_documents(&mut self) -> Vec<Document> {
        let mut matches = FnvHashMap::default();

        while let Some((string, indexed_values)) = self.stream.next() {
            for iv in indexed_values {
                let automaton = &self.automatons[iv.index];
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

impl<'a, C, F> RankedStream<'a, C, F>
where C: Criterion
{
    // TODO don't sort to much documents, we can skip useless sorts
    pub fn retrieve_documents(mut self, range: Range<usize>) -> Vec<Document> {
        let mut documents = self.retrieve_all_documents();
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

        let range = clamp_range(range, 0..documents.len());
        documents[range].to_vec()
    }

    pub fn retrieve_distinct_documents<K>(mut self, range: Range<usize>) -> Vec<Document>
    where F: Fn(&DocumentId) -> Option<K>,
          K: Hash + Eq,
    {
        let mut documents = self.retrieve_all_documents();
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

        let mut out_documents = Vec::with_capacity(range.len());
        let (distinct, limit) = self.distinct;
        let mut seen = DistinctMap::new(limit);

        for document in documents {
            let accepted = match distinct(&document.id) {
                Some(key) => seen.digest(key),
                None => seen.accept_without_key(),
            };

            if accepted {
                if seen.len() == range.end { break }
                if seen.len() >= range.start {
                    out_documents.push(document);
                }
            }
        }

        out_documents
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
