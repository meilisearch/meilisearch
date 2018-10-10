use std::cmp::Ordering;
use std::rc::Rc;
use std::{mem, vec};

use fnv::FnvHashMap;
use fst::Streamer;
use group_by::GroupByMut;

use crate::automaton::{DfaExt, AutomatonExt};
use crate::metadata::Metadata;
use crate::metadata::ops::{OpBuilder, Union};
use crate::rank::criterion::Criteria;
use crate::rank::Document;
use crate::{Match, DocumentId};

pub struct Config<'m, F> {
    pub criteria: Criteria<F>,
    pub metadata: &'m Metadata,
    pub automatons: Vec<DfaExt>,
    pub limit: usize,
}

pub struct RankedStream<'m, F>(RankedStreamInner<'m, F>);

impl<'m, F> RankedStream<'m, F> {
    pub fn new(config: Config<'m, F>) -> Self {
        let automatons: Vec<_> = config.automatons.into_iter().map(Rc::new).collect();
        let mut builder = OpBuilder::with_automatons(automatons.clone());
        builder.push(config.metadata);

        let inner = RankedStreamInner::Fed {
            inner: builder.union(),
            automatons: automatons,
            criteria: config.criteria,
            limit: config.limit,
            matches: FnvHashMap::default(),
        };

        RankedStream(inner)
    }
}

impl<'m, 'a, F> fst::Streamer<'a> for RankedStream<'m, F>
where F: Fn(&Document, &Document) -> Ordering + Copy,
{
    type Item = Document;

    fn next(&'a mut self) -> Option<Self::Item> {
        self.0.next()
    }
}

enum RankedStreamInner<'m, F> {
    Fed {
        inner: Union<'m>,
        automatons: Vec<Rc<DfaExt>>,
        criteria: Criteria<F>,
        limit: usize,
        matches: FnvHashMap<DocumentId, Vec<Match>>,
    },
    Pours {
        inner: vec::IntoIter<Document>,
    },
}

impl<'m, 'a, F> fst::Streamer<'a> for RankedStreamInner<'m, F>
where F: Fn(&Document, &Document) -> Ordering + Copy,
{
    type Item = Document;

    fn next(&'a mut self) -> Option<Self::Item> {
        loop {
            match self {
                RankedStreamInner::Fed { inner, automatons, criteria, limit, matches } => {
                    match inner.next() {
                        Some((string, indexed_values)) => {
                            for iv in indexed_values {
                                let automaton = &automatons[iv.index];
                                let distance = automaton.eval(string).to_u8();
                                let same_length = string.len() == automaton.query_len();

                                for di in iv.doc_indexes.as_slice() {
                                    let match_ = Match {
                                        query_index: iv.index as u32,
                                        distance: distance,
                                        attribute: di.attribute,
                                        attribute_index: di.attribute_index,
                                        is_exact: distance == 0 && same_length,
                                    };
                                    matches.entry(di.document)
                                           .or_insert_with(Vec::new)
                                           .push(match_);
                                }
                            }
                        },
                        None => {
                            let matches = mem::replace(matches, FnvHashMap::default());
                            let criteria = mem::replace(criteria, Criteria::new());
                            *self = RankedStreamInner::Pours {
                                inner: matches_into_iter(matches, criteria, *limit).into_iter()
                            };
                        },
                    }
                },
                RankedStreamInner::Pours { inner } => {
                    return inner.next()
                },
            }
        }
    }
}

fn matches_into_iter<F>(matches: FnvHashMap<DocumentId, Vec<Match>>,
                        criteria: Criteria<F>,
                        limit: usize) -> vec::IntoIter<Document>
where F: Fn(&Document, &Document) -> Ordering + Copy,
{
    let mut documents: Vec<_> = matches.into_iter().map(|(id, mut matches)| {
        matches.sort_unstable();
        Document::from_sorted_matches(id, matches)
    }).collect();

    let mut groups = vec![documents.as_mut_slice()];

    for sort in criteria {
        let temp = mem::replace(&mut groups, Vec::new());
        let mut computed = 0;

        'grp: for group in temp {
            group.sort_unstable_by(sort);
            for group in GroupByMut::new(group, |a, b| sort(a, b) == Ordering::Equal) {
                computed += group.len();
                groups.push(group);
                if computed >= limit { break 'grp }
            }
        }
    }

    documents.truncate(limit);
    documents.into_iter()
}
