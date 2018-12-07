use std::ops::{Deref, Range};
use std::error::Error;
use std::hash::Hash;
use std::{mem, vec, str};

use group_by::GroupByMut;
use hashbrown::HashMap;
use fst::Streamer;
use rocksdb::DB;

use crate::automaton::{self, DfaExt, AutomatonExt};
use crate::rank::criterion::{self, Criterion};
use crate::rank::distinct_map::DistinctMap;
use crate::database::DatabaseView;
use crate::{Match, DocumentId};
use crate::rank::Document;

fn split_whitespace_automatons(query: &str) -> Vec<DfaExt> {
    let mut automatons = Vec::new();
    for query in query.split_whitespace().map(str::to_lowercase) {
        let lev = automaton::build_prefix_dfa(&query);
        automatons.push(lev);
    }
    automatons
}

pub struct QueryBuilder<'a, D, C>
where D: Deref<Target=DB>
{
    view: &'a DatabaseView<D>,
    criteria: Vec<C>,
}

impl<'a, D> QueryBuilder<'a, D, Box<dyn Criterion<D>>>
where D: Deref<Target=DB>
{
    pub fn new(view: &'a DatabaseView<D>) -> Result<Self, Box<Error>> {
        QueryBuilder::with_criteria(view, criterion::default())
    }
}

impl<'a, D, C> QueryBuilder<'a, D, C>
where D: Deref<Target=DB>
{
    pub fn with_criteria(view: &'a DatabaseView<D>, criteria: Vec<C>) -> Result<Self, Box<Error>> {
        Ok(QueryBuilder { view, criteria })
    }

    pub fn criteria(&mut self, criteria: Vec<C>) -> &mut Self {
        self.criteria = criteria;
        self
    }

    pub fn with_distinct<F>(self, function: F, size: usize) -> DistinctQueryBuilder<'a, D, F, C> {
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

impl<'a, D, C> QueryBuilder<'a, D, C>
where D: Deref<Target=DB>,
      C: Criterion<D>
{
    pub fn query(&self, query: &str, limit: usize) -> Vec<Document> {
        let mut documents = self.query_all(query);
        let mut groups = vec![documents.as_mut_slice()];
        let view = &self.view;

        'group: for criterion in &self.criteria {
            let tmp_groups = mem::replace(&mut groups, Vec::new());
            let mut computed = 0;

            for group in tmp_groups {
                group.sort_unstable_by(|a, b| criterion.evaluate(a, b, view));
                for group in GroupByMut::new(group, |a, b| criterion.eq(a, b, view)) {
                    computed += group.len();
                    groups.push(group);
                    if computed >= limit { break 'group }
                }
            }
        }

        documents.truncate(limit);
        documents
    }
}

pub struct DistinctQueryBuilder<'a, D, F, C>
where D: Deref<Target=DB>
{
    inner: QueryBuilder<'a, D, C>,
    function: F,
    size: usize,
}

impl<'a, D, F, K, C> DistinctQueryBuilder<'a, D, F, C>
where D: Deref<Target=DB>,
      F: Fn(DocumentId, &DatabaseView<D>) -> Option<K>,
      K: Hash + Eq,
      C: Criterion<D>,
{
    pub fn query(&self, query: &str, range: Range<usize>) -> Vec<Document> {
        let mut documents = self.inner.query_all(query);
        let mut groups = vec![documents.as_mut_slice()];
        let view = &self.inner.view;

        for criterion in &self.inner.criteria {
            let tmp_groups = mem::replace(&mut groups, Vec::new());

            for group in tmp_groups {
                group.sort_unstable_by(|a, b| criterion.evaluate(a, b, view));
                for group in GroupByMut::new(group, |a, b| criterion.eq(a, b, view)) {
                    groups.push(group);
                }
            }
        }

        let mut out_documents = Vec::with_capacity(range.len());
        let mut seen = DistinctMap::new(self.size);

        for document in documents {
            let accepted = match (self.function)(document.id, &self.inner.view) {
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
