use std::cmp::{self, Ordering};
use std::collections::HashMap;
use std::{mem, vec, iter};
use DocIndexMap;
use fst;
use levenshtein_automata::DFA;
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
    document_id: DocumentId,
    matches: Vec<Match>,
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
    fn sum_of_typos(doc: &Document) -> u8 {
        GroupBy::new(&doc.matches, match_query_index).map(|m| m[0].distance).sum()
    }
    sum_of_typos(lhs).cmp(&sum_of_typos(rhs))
}

fn number_of_words(lhs: &Document, rhs: &Document) -> Ordering {
    fn number_of_words(doc: &Document) -> usize {
        GroupBy::new(&doc.matches, match_query_index).count()
    }
    number_of_words(lhs).cmp(&number_of_words(rhs)).reverse()
}

fn words_proximity(lhs: &Document, rhs: &Document) -> Ordering {
    fn word_proximity(doc: &Document) -> u32 {
        fn attribute_proximity(lhs: &Match, rhs: &Match) -> u32 {
            fn index_proximity(lhs: u32, rhs: u32) -> u32 {
                if lhs < rhs {
                    cmp::min(rhs - lhs, MAX_DISTANCE)
                } else {
                    cmp::min(lhs - rhs, MAX_DISTANCE) + 1
                }
            }

            if lhs.attribute != rhs.attribute { return MAX_DISTANCE }
            index_proximity(lhs.attribute_index, rhs.attribute_index)
        }

        let mut proximity = 0;
        let mut next_group_index = 0;
        for group in GroupBy::new(&doc.matches, match_query_index) {
            next_group_index += group.len();
            // FIXME distance is wrong if 2 different attributes matches
            // FIXME do that in a manner to avoid memory cache misses
            if let Some(first_next_group) = doc.matches.get(next_group_index) {
                proximity += attribute_proximity(first_next_group, &group[0]);
            }
        }
        proximity
    }
    word_proximity(lhs).cmp(&word_proximity(rhs))
}

fn sum_of_words_attribute(lhs: &Document, rhs: &Document) -> Ordering {
    fn sum_attribute(doc: &Document) -> u8 {
        GroupBy::new(&doc.matches, match_query_index).map(|m| m[0].attribute).sum()
    }
    sum_attribute(lhs).cmp(&sum_attribute(rhs))
}

fn sum_of_words_position(lhs: &Document, rhs: &Document) -> Ordering {
    fn sum_attribute_index(doc: &Document) -> u32 {
        GroupBy::new(&doc.matches, match_query_index).map(|m| m[0].attribute_index).sum()
    }
    sum_attribute_index(lhs).cmp(&sum_attribute_index(rhs))
}

fn exact(lhs: &Document, rhs: &Document) -> Ordering {
    unimplemented!()
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
        automatons: Vec<DFA>,
        pool: Pool,
    },
    Pours {
        inner: vec::IntoIter<Document>,
    },
}

impl<'m, 'v> RankedStream<'m, 'v> {
    pub fn new(map: &'m DocIndexMap, values: &'v Values<DocIndex>, automatons: Vec<DFA>, limit: usize) -> Self {
        let mut op = OpWithStateBuilder::new(values);

        for automaton in automatons.iter().cloned() {
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
    type Item = DocumentId;

    fn next(&'a mut self) -> Option<Self::Item> {
        let mut matches = HashMap::new();

        loop {
            // TODO remove that when NLL are here !
            let mut transfert_pool = None;

            match self {
                RankedStream::Fed { inner, automatons, pool } => {
                    match inner.next() {
                        Some((_string, indexed_values)) => {
                            for iv in indexed_values {

                                // TODO extend documents matches by batch of query_index
                                //      that way it will be possible to discard matches that
                                //      have an invalid distance *before* adding them
                                //      to the matches of the documents and, that way, avoid a sort

                                // let string = unsafe { str::from_utf8_unchecked(_string) };
                                // println!("for {:15} ", string);

                                let distance = automatons[iv.index].distance(iv.state).to_u8();

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
                                    };
                                    matches.entry(di.document)
                                            .and_modify(|ms: &mut Vec<_>| ms.push(match_))
                                            .or_insert_with(|| vec![match_]);
                                }
                                pool.extend(&mut matches);
                            }
                        },
                        None => {
                            transfert_pool = Some(mem::replace(pool, Pool::new(1, 1)));
                        },
                    }
                },
                RankedStream::Pours { inner } => {
                    return inner.next().map(|d| d.document_id)
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
