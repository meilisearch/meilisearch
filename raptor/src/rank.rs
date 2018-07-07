use std::cmp::{self, Ordering};
use std::collections::HashMap;
use std::{mem, vec, iter};
use DocIndexMap;
use fst;
use levenshtein::Levenshtein;
use map::{
    OpWithStateBuilder, UnionWithState,
    StreamWithStateBuilder,
    Values,
};
use {Match, DocIndex, DocumentId};
use group_by::{GroupBy, GroupByMut};

const MAX_DISTANCE: u32 = 8;

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

fn sum_of_typos(lhs: &Document, rhs: &Document) -> Ordering {
    let key = |matches: &[Match]| -> u8 {
        GroupBy::new(matches, match_query_index).map(|m| m[0].distance).sum()
    };

    key(&lhs.matches).cmp(&key(&rhs.matches))
}

fn number_of_words(lhs: &Document, rhs: &Document) -> Ordering {
    let key = |matches: &[Match]| -> usize {
        GroupBy::new(matches, match_query_index).count()
    };

    key(&lhs.matches).cmp(&key(&rhs.matches)).reverse()
}

fn index_proximity(lhs: u32, rhs: u32) -> u32 {
    if lhs < rhs {
        cmp::min(rhs - lhs, MAX_DISTANCE)
    } else {
        cmp::min(lhs - rhs, MAX_DISTANCE) + 1
    }
}

fn attribute_proximity(lhs: &Match, rhs: &Match) -> u32 {
    if lhs.attribute != rhs.attribute { return MAX_DISTANCE }
    index_proximity(lhs.attribute_index, rhs.attribute_index)
}

fn min_proximity(lhs: &[Match], rhs: &[Match]) -> u32 {
    let mut min_prox = u32::max_value();
    for a in lhs {
        for b in rhs {
            min_prox = cmp::min(min_prox, attribute_proximity(a, b));
        }
    }
    min_prox
}

fn matches_proximity(matches: &[Match]) -> u32 {
    let mut proximity = 0;
    let mut iter = GroupBy::new(matches, match_query_index);

    let mut last = iter.next();
    while let (Some(lhs), Some(rhs)) = (last, iter.next()) {
        proximity += min_proximity(lhs, rhs);
        last = Some(rhs);
    }

    proximity
}

fn words_proximity(lhs: &Document, rhs: &Document) -> Ordering {
    matches_proximity(&lhs.matches).cmp(&matches_proximity(&rhs.matches))
}

#[test]
fn easy_matches_proximity() {

    // "soup" "of the" "the day"
    //
    // { id: 0, attr: 0, attr_index: 0 }
    // { id: 1, attr: 1, attr_index: 0 }
    // { id: 2, attr: 1, attr_index: 1 }
    // { id: 2, attr: 2, attr_index: 0 }
    // { id: 3, attr: 3, attr_index: 1 }

    let matches = &[
        Match { query_index: 0, attribute: 0, attribute_index: 0, ..Match::zero() },
        Match { query_index: 1, attribute: 1, attribute_index: 0, ..Match::zero() },
        Match { query_index: 2, attribute: 1, attribute_index: 1, ..Match::zero() },
        Match { query_index: 2, attribute: 2, attribute_index: 0, ..Match::zero() },
        Match { query_index: 3, attribute: 3, attribute_index: 1, ..Match::zero() },
    ];

    //   soup -> of = 8
    // + of -> the  = 1
    // + the -> day = 8 (not 1)
    assert_eq!(matches_proximity(matches), 17);
}

#[test]
fn another_matches_proximity() {

    // "soup day" "soup of the day"
    //
    // { id: 0, attr: 0, attr_index: 0 }
    // { id: 0, attr: 1, attr_index: 0 }
    // { id: 1, attr: 1, attr_index: 1 }
    // { id: 2, attr: 1, attr_index: 2 }
    // { id: 3, attr: 0, attr_index: 1 }
    // { id: 3, attr: 1, attr_index: 3 }

    let matches = &[
        Match { query_index: 0, attribute: 0, attribute_index: 0, ..Match::zero() },
        Match { query_index: 0, attribute: 1, attribute_index: 0, ..Match::zero() },
        Match { query_index: 1, attribute: 1, attribute_index: 1, ..Match::zero() },
        Match { query_index: 2, attribute: 1, attribute_index: 2, ..Match::zero() },
        Match { query_index: 3, attribute: 0, attribute_index: 1, ..Match::zero() },
        Match { query_index: 3, attribute: 1, attribute_index: 3, ..Match::zero() },
    ];

    //   soup -> of = 1
    // + of -> the  = 1
    // + the -> day = 1
    assert_eq!(matches_proximity(matches), 3);
}

fn sum_of_words_attribute(lhs: &Document, rhs: &Document) -> Ordering {
    let key = |matches: &[Match]| -> u8 {
        GroupBy::new(matches, match_query_index).map(|m| m[0].attribute).sum()
    };

    key(&lhs.matches).cmp(&key(&rhs.matches))
}

fn sum_of_words_position(lhs: &Document, rhs: &Document) -> Ordering {
    let key = |matches: &[Match]| -> u32 {
        GroupBy::new(matches, match_query_index).map(|m| m[0].attribute_index).sum()
    };

    key(&lhs.matches).cmp(&key(&rhs.matches))
}

fn exact(lhs: &Document, rhs: &Document) -> Ordering {
    let contains_exact = |matches: &[Match]| matches.iter().any(|m| m.is_exact);
    let key = |matches: &[Match]| -> usize {
        GroupBy::new(matches, match_query_index).map(contains_exact).filter(Clone::clone).count()
    };

    key(&lhs.matches).cmp(&key(&rhs.matches))
}

pub struct Pool {
    documents: Vec<Document>,
    limit: usize,
}

impl Pool {
    pub fn new(query_size: usize, limit: usize) -> Self {
        Self {
            documents: Vec::new(),
            limit: limit,
        }
    }

    // TODO remove the matches HashMap, not proud of it
    pub fn extend(&mut self, matches: &mut HashMap<DocumentId, Vec<Match>>) {
        for doc in self.documents.iter_mut() {
            if let Some(matches) = matches.remove(&doc.document_id) {
                doc.matches.extend(matches);
                doc.matches.sort_unstable();
            }
        }

        for (id, mut matches) in matches.drain() {
            // note that matches are already sorted we do that by security
            // TODO remove this useless sort
            matches.sort_unstable();

            let document = Document::from_sorted_matches(id, matches);
            self.documents.push(document);
        }
    }
}

fn invert_sorts<F>(a: &Document, b: &Document, sorts: &[F]) -> bool
where F: Fn(&Document, &Document) -> Ordering,
{
    sorts.iter().rev().all(|sort| sort(a, b) == Ordering::Equal)
}

impl IntoIterator for Pool {
    type Item = Document;
    type IntoIter = vec::IntoIter<Self::Item>;

    fn into_iter(mut self) -> Self::IntoIter {
        let sorts = &[
            sum_of_typos,
            number_of_words,
            words_proximity,
            sum_of_words_attribute,
            sum_of_words_position,
            exact,
        ];

        for (i, sort) in sorts.iter().enumerate() {
            let mut computed = 0;
            for group in GroupByMut::new(&mut self.documents, |a, b| invert_sorts(a, b, &sorts[..i])) {
                // TODO prefer using `sort_unstable_by_key` to allow reusing the key computation
                //      `number of words` needs to be reversed, we can use the `cmp::Reverse` struct to do that
                group.sort_unstable_by(sort);
                computed += group.len();
                if computed >= self.limit { break }
            }
        }

        self.documents.truncate(self.limit);
        self.documents.into_iter()
    }
}

pub enum RankedStream<'m, 'v> {
    Fed {
        inner: UnionWithState<'m, 'v, DocIndex, u32>,
        automatons: Vec<Levenshtein>,
        pool: Pool,
    },
    Pours {
        inner: vec::IntoIter<Document>,
    },
}

impl<'m, 'v> RankedStream<'m, 'v> {
    pub fn new(map: &'m DocIndexMap, values: &'v Values<DocIndex>, automatons: Vec<Levenshtein>, limit: usize) -> Self {
        let mut op = OpWithStateBuilder::new(values);

        for automaton in automatons.iter().map(|l| l.dfa.clone()) {
            let stream = map.as_map().search(automaton).with_state();
            op.push(stream);
        }

        let pool = Pool::new(automatons.len(), limit);

        RankedStream::Fed {
            inner: op.union(),
            automatons: automatons,
            pool: pool,
        }
    }
}

impl<'m, 'v, 'a> fst::Streamer<'a> for RankedStream<'m, 'v> {
    type Item = Document;

    fn next(&'a mut self) -> Option<Self::Item> {
        let mut matches = HashMap::new();

        loop {
            // TODO remove that when NLL are here !
            let mut transfert_pool = None;

            match self {
                RankedStream::Fed { inner, automatons, pool } => {
                    match inner.next() {
                        Some((string, indexed_values)) => {
                            for iv in indexed_values {

                                // TODO extend documents matches by batch of query_index
                                //      that way it will be possible to discard matches that
                                //      have an invalid distance *before* adding them
                                //      to the matches of the documents and, that way, avoid a sort

                                let automaton = &automatons[iv.index];
                                let distance = automaton.dfa.distance(iv.state).to_u8();

                                // TODO remove the Pool system !
                                //      this is an internal Pool rule but
                                //      it is more efficient to test that here
                                // if pool.limitation.is_reached() && distance != 0 { continue }

                                for di in iv.values {
                                    let match_ = Match {
                                        query_index: iv.index as u32,
                                        distance: distance,
                                        attribute: di.attribute,
                                        attribute_index: di.attribute_index,
                                        is_exact: string.len() == automaton.query_len,
                                    };
                                    matches.entry(di.document)
                                            .and_modify(|ms: &mut Vec<_>| ms.push(match_))
                                            .or_insert_with(|| vec![match_]);
                                }
                                pool.extend(&mut matches);
                            }
                        },
                        None => {
                            // TODO remove this when NLL are here !
                            transfert_pool = Some(mem::replace(pool, Pool::new(1, 1)));
                        },
                    }
                },
                RankedStream::Pours { inner } => {
                    return inner.next()
                },
            }

            // transform the `RankedStream` into a `Pours`
            if let Some(pool) = transfert_pool {
                *self = RankedStream::Pours {
                    inner: pool.into_iter(),
                }
            }
        }
    }
}
