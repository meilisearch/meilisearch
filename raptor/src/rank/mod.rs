mod sum_of_typos;
mod number_of_words;
mod words_proximity;
mod sum_of_words_attribute;
mod sum_of_words_position;
mod exact;

use std::cmp::Ordering;
use std::rc::Rc;
use std::{mem, vec};
use fst::Streamer;
use fnv::FnvHashMap;
use group_by::GroupByMut;
use crate::automaton::{DfaExt, AutomatonExt};
use crate::metadata::Metadata;
use crate::metadata::ops::{OpBuilder, Union};
use crate::metadata::doc_indexes::DocIndexes;
use crate::{Match, DocumentId};

use self::{
    sum_of_typos::sum_of_typos,
    number_of_words::number_of_words,
    words_proximity::words_proximity,
    sum_of_words_attribute::sum_of_words_attribute,
    sum_of_words_position::sum_of_words_position,
    exact::exact,
};

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

pub struct RankedStream<'m>(RankedStreamInner<'m>);

impl<'m> RankedStream<'m> {
    pub fn new(metadata: &'m Metadata, automatons: Vec<DfaExt>, limit: usize) -> Self {
        let automatons: Vec<_> = automatons.into_iter().map(Rc::new).collect();
        let mut builder = OpBuilder::with_automatons(automatons.clone());
        builder.push(metadata);

        let inner = RankedStreamInner::Fed {
            inner: builder.union(),
            automatons: automatons,
            limit: limit,
            matches: FnvHashMap::default(),
        };

        RankedStream(inner)
    }
}

impl<'m, 'a> fst::Streamer<'a> for RankedStream<'m> {
    type Item = Document;

    fn next(&'a mut self) -> Option<Self::Item> {
        self.0.next()
    }
}

enum RankedStreamInner<'m> {
    Fed {
        inner: Union<'m>,
        automatons: Vec<Rc<DfaExt>>,
        limit: usize,
        matches: FnvHashMap<DocumentId, Vec<Match>>,
    },
    Pours {
        inner: vec::IntoIter<Document>,
    },
}

impl<'m, 'a> fst::Streamer<'a> for RankedStreamInner<'m> {
    type Item = Document;

    fn next(&'a mut self) -> Option<Self::Item> {
        loop {
            match self {
                RankedStreamInner::Fed { inner, automatons, limit, matches } => {
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
