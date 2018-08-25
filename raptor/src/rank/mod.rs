mod sum_of_typos;
mod number_of_words;
mod words_proximity;
mod sum_of_words_attribute;
mod sum_of_words_position;
mod exact;

use std::cmp::Ordering;
use std::{mem, vec};
use fst;
use fnv::FnvHashMap;
use levenshtein::Levenshtein;
use metadata::{DocIndexes, OpWithStateBuilder, UnionWithState};
use {Match, DocumentId};
use group_by::GroupByMut;

use self::sum_of_typos::sum_of_typos;
use self::number_of_words::number_of_words;
use self::words_proximity::words_proximity;
use self::sum_of_words_attribute::sum_of_words_attribute;
use self::sum_of_words_position::sum_of_words_position;
use self::exact::exact;

#[inline]
fn match_query_index(a: &Match, b: &Match) -> bool {
    a.query_index == b.query_index
}

#[derive(Debug, Clone)]
pub struct Document {
    pub document_id: DocumentId,
    pub matches: Vec<Match>,
}

impl Document {
    pub fn new(doc: DocumentId, match_: Match) -> Self {
        Self::from_sorted_matches(doc, vec![match_])
    }

    pub fn from_sorted_matches(doc: DocumentId, matches: Vec<Match>) -> Self {
        Self {
            document_id: doc,
            matches: matches,
        }
    }
}

fn matches_into_iter(matches: FnvHashMap<DocumentId, Vec<Match>>, limit: usize) -> vec::IntoIter<Document> {
    let mut documents: Vec<_> = matches.into_iter().map(|(id, mut matches)| {
        matches.sort_unstable();
        Document::from_sorted_matches(id, matches)
    }).collect();

    let sorts = &[
        sum_of_typos,
        number_of_words,
        words_proximity,
        sum_of_words_attribute,
        sum_of_words_position,
        exact,
    ];

    let mut groups = vec![documents.as_mut_slice()];

    for sort in sorts {
        let temp = mem::replace(&mut groups, Vec::new());
        let mut computed = 0;

        for group in temp {
            group.sort_unstable_by(sort);
            for group in GroupByMut::new(group, |a, b| sort(a, b) == Ordering::Equal) {
                computed += group.len();
                groups.push(group);
                if computed >= limit { break }
            }
        }
    }

    documents.truncate(limit);
    documents.into_iter()
}

pub struct RankedStream<'m, 'v>(RankedStreamInner<'m, 'v>);

impl<'m, 'v> RankedStream<'m, 'v> {
    pub fn new(map: &'m fst::Map, indexes: &'v DocIndexes, automatons: Vec<Levenshtein>, limit: usize) -> Self {
        let mut op = OpWithStateBuilder::new(indexes);

        for automaton in automatons.iter().map(|l| l.dfa.clone()) {
            let stream = map.search(automaton).with_state();
            op.push(stream);
        }

        let inner = RankedStreamInner::Fed {
            inner: op.union(),
            automatons: automatons,
            limit: limit,
            matches: FnvHashMap::default(),
        };

        RankedStream(inner)
    }
}

impl<'m, 'v, 'a> fst::Streamer<'a> for RankedStream<'m, 'v> {
    type Item = Document;

    fn next(&'a mut self) -> Option<Self::Item> {
        self.0.next()
    }
}

enum RankedStreamInner<'m, 'v> {
    Fed {
        inner: UnionWithState<'m, 'v, u32>,
        automatons: Vec<Levenshtein>,
        limit: usize,
        matches: FnvHashMap<DocumentId, Vec<Match>>,
    },
    Pours {
        inner: vec::IntoIter<Document>,
    },
}

impl<'m, 'v, 'a> fst::Streamer<'a> for RankedStreamInner<'m, 'v> {
    type Item = Document;

    fn next(&'a mut self) -> Option<Self::Item> {
        loop {
            match self {
                RankedStreamInner::Fed { inner, automatons, limit, matches } => {
                    match inner.next() {
                        Some((string, indexed_values)) => {
                            for iv in indexed_values {

                                let automaton = &automatons[iv.index];
                                let distance = automaton.dfa.distance(iv.state).to_u8();

                                for di in iv.values {
                                    let match_ = Match {
                                        query_index: iv.index as u32,
                                        distance: distance,
                                        attribute: di.attribute,
                                        attribute_index: di.attribute_index,
                                        is_exact: distance == 0 && string.len() == automaton.query_len,
                                    };
                                    matches.entry(di.document)
                                            .or_insert_with(Vec::new)
                                            .push(match_);
                                }
                            }
                        },
                        None => {
                            let matches = mem::replace(matches, FnvHashMap::default());
                            *self = RankedStreamInner::Pours {
                                inner: matches_into_iter(matches, *limit).into_iter()
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
