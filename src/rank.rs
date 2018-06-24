use std::cmp::{self, Ordering};
use std::{mem, vec};
use std::collections::{HashSet, HashMap};
use DocIndexMap;
use fst;
use levenshtein_automata::DFA;
use map::{
    OpWithStateBuilder, UnionWithState,
    StreamWithStateBuilder,
    Values,
};
use {Match, DocIndex, DocumentId};
use group_by::GroupBy;

const MAX_DISTANCE: usize = 8;

#[derive(Debug, Eq, Clone)]
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

impl PartialEq for Document {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == Ordering::Equal
    }
}

impl PartialOrd for Document {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Document {
    fn cmp(&self, other: &Self) -> Ordering {
        let lhs = DocumentScore::new(&self.matches);
        let rhs = DocumentScore::new(&other.matches);
        lhs.cmp(&rhs)
    }
}

#[derive(Debug, Default, Eq, PartialEq, PartialOrd)]
struct DocumentScore {
    typo: usize,
    words: usize,
    proximity: usize,
    attribute: usize,
    words_position: usize,
}

impl Ord for DocumentScore {
    fn cmp(&self, other: &Self) -> Ordering {
        self.typo.cmp(&other.typo)
        .then(self.words.cmp(&other.words).reverse())
        .then(self.proximity.cmp(&other.proximity))
        .then(self.attribute.cmp(&other.attribute))
        .then(self.words_position.cmp(&other.words_position))
        // ~exact~ (see prefix option of the `DFA` builder)
    }
}

fn min_attribute(matches: &[Match]) -> usize {
    let mut attribute = usize::max_value();
    for match_ in matches {
        if match_.attribute == 0 { return 0 }
        attribute = cmp::min(match_.attribute as usize, attribute);
    }
    attribute
}

fn min_attribute_index(matches: &[Match]) -> usize {
    let mut attribute_index = usize::max_value();
    for match_ in matches {
        if match_.attribute_index == 0 { return 0 }
        attribute_index = cmp::min(match_.attribute_index as usize, attribute_index);
    }
    attribute_index
}

impl DocumentScore {
    fn new(matches: &[Match]) -> Self {
        let mut score = DocumentScore::default();

        let mut index = 0; // FIXME could be replaced by the `GroupBy::remaining` method
        for group in GroupBy::new(matches, |a, b| a.query_index == b.query_index) {
            index += group.len();

            score.typo = cmp::max(group[0].distance as usize, score.typo);
            score.words += 1;

            // FIXME distance is wrong if 2 different attributes matches
            if let Some(first_next_group) = (&matches[index..]).first() {
                score.proximity += attribute_proximity(first_next_group, &group[0]);
            }

            score.attribute += min_attribute(group);
            score.words_position += min_attribute_index(group);
        }

        score
    }
}

fn proximity(first: usize, second: usize) -> usize {
    if first < second {
        cmp::min(second - first, MAX_DISTANCE)
    } else {
        cmp::min(first - second, MAX_DISTANCE) + 1
    }
}

fn attribute_proximity(lhs: &Match, rhs: &Match) -> usize {
    if lhs.attribute != rhs.attribute {
        MAX_DISTANCE
    } else {
        let lhs_attr = lhs.attribute_index as usize;
        let rhs_attr = rhs.attribute_index as usize;
        proximity(lhs_attr, rhs_attr)
    }
}

pub struct Pool {
    returned_documents: HashSet<DocumentId>,
    documents: Vec<Document>,
    limitation: Limitation,
}

#[derive(Debug, Copy, Clone)]
enum Limitation {
    /// No limitation is specified.
    Unspecified { // FIXME rename that !
        /// The maximum number of results to return.
        limit: usize,
    },

    /// The limitation is specified but not reached.
    Specified {
        /// The maximum number of results to return.
        limit: usize,

        /// documents with a distance of zero which can be used
        /// in the step-by-step sort-and-return.
        ///
        /// this field must be equal to the limit to reach
        /// the limitation
        matching_documents: usize,
    },

    /// No more documents with a distance of zero
    /// can never be returned now.
    Reached {
        /// The number of remaining documents to return in order.
        remaining: usize,
    },
}

impl Limitation {
    fn reached(&self) -> Option<usize> {
        match self {
            Limitation::Reached { remaining } => Some(*remaining),
            _ => None,
        }
    }

    fn is_reached(&self) -> bool {
        self.reached().is_some()
    }
}

impl Pool {
    pub fn new(query_size: usize, limit: usize) -> Self {
        assert!(query_size > 0, "query size can not be less that one");
        assert!(limit > 0, "limit can not be less that one");

        let limitation = match query_size {
            1 => Limitation::Specified { limit, matching_documents: 0 },
            _ => Limitation::Unspecified { limit },
        };

        Self {
            returned_documents: HashSet::new(),
            documents: Vec::new(),
            limitation: limitation,
        }
    }

    pub fn extend(&mut self, mut matches: HashMap<DocumentId, Vec<Match>>) {
        for doc in self.documents.iter_mut() {
            if let Some(matches) = matches.remove(&doc.document_id) {
                doc.matches.extend(matches);
                doc.matches.sort_unstable();
            }
        }

        matches.retain(|id, _| !self.returned_documents.contains(id));
        self.documents.reserve(matches.len());

        let mut new_matches = 0;
        for (id, mut matches) in matches.into_iter() {
            matches.sort_unstable();
            if matches[0].distance == 0 { new_matches += 1 }

            if self.limitation.is_reached() {
                match matches.iter().position(|match_| match_.distance > 0) {
                    Some(pos) if pos == 0 => continue,
                    Some(pos) => matches.truncate(pos),
                    None => (),
                }
            }

            let document = Document::from_sorted_matches(id, matches);
            self.documents.push(document);
        }
        self.documents.sort_unstable();

        self.limitation = match self.limitation {
            Limitation::Specified { limit, matching_documents } if matching_documents + new_matches >= limit => {
                // this is the biggest valid match
                // used to find the next smallest invalid match
                let biggest_valid = Match { query_index: 0, distance: 0, ..Match::max() };

                // documents which does not have a match with a distance of 0 can be removed.
                // note that documents have a query size of 1.
                match self.documents.binary_search_by(|d| d.matches[0].cmp(&biggest_valid)) {
                    Ok(index) => self.documents.truncate(index + 1), // this will never happen :)
                    Err(index) => self.documents.truncate(index),
                }

                Limitation::Reached { remaining: limit }
            },
            Limitation::Specified { limit, matching_documents } => {
                Limitation::Specified {
                    limit: limit,
                    matching_documents: matching_documents + new_matches
                }
            },
            limitation => limitation,
        };
    }
}

impl IntoIterator for Pool {
    type Item = Document;
    type IntoIter = vec::IntoIter<Self::Item>;

    fn into_iter(mut self) -> Self::IntoIter {
        let limit = match self.limitation {
            Limitation::Unspecified { limit } => limit,
            Limitation::Specified { limit, .. } => limit,
            Limitation::Reached { remaining } => remaining,
        };

        self.documents.truncate(limit);
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
        loop {
            // TODO remove that when NLL are here !
            let mut transfert_pool = None;

            match self {
                RankedStream::Fed { inner, automatons, pool } => {
                    match inner.next() {
                        Some((_string, indexed_values)) => {
                            for iv in indexed_values {

                                let distance = automatons[iv.index].distance(iv.state).to_u8();

                                // TODO remove the Pool system !
                                //      this is an internal Pool rule but
                                //      it is more efficient to test that here
                                if pool.limitation.is_reached() && distance != 0 { continue }

                                let mut matches = HashMap::with_capacity(iv.values.len() / 2);
                                for di in iv.values {
                                    let match_ = Match {
                                        query_index: iv.index as u32,
                                        distance: distance,
                                        attribute: di.attribute,
                                        attribute_index: di.attribute_index,
                                    };
                                    matches.entry(di.document)
                                            .and_modify(|matches: &mut Vec<_>| matches.push(match_))
                                            .or_insert_with(|| vec![match_]);
                                }
                                pool.extend(matches);
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
