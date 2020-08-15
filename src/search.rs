use std::cell::RefCell;
use std::collections::{HashMap, HashSet};
use std::rc::Rc;

use astar_iter::AstarBagIter;
use fst::{IntoStreamer, Streamer};
use levenshtein_automata::DFA;
use levenshtein_automata::LevenshteinAutomatonBuilder as LevBuilder;
use log::debug;
use once_cell::sync::Lazy;
use roaring::RoaringBitmap;

use crate::node::{self, Node};
use crate::query_tokens::{QueryTokens, QueryToken};
use crate::{Index, DocumentId, Position, Attribute};

// Building these factories is not free.
static LEVDIST0: Lazy<LevBuilder> = Lazy::new(|| LevBuilder::new(0, true));
static LEVDIST1: Lazy<LevBuilder> = Lazy::new(|| LevBuilder::new(1, true));
static LEVDIST2: Lazy<LevBuilder> = Lazy::new(|| LevBuilder::new(2, true));

pub struct Search<'a> {
    query: Option<String>,
    offset: usize,
    limit: usize,
    rtxn: &'a heed::RoTxn,
    index: &'a Index,
}

impl<'a> Search<'a> {
    pub fn new(rtxn: &'a heed::RoTxn, index: &'a Index) -> Search<'a> {
        Search {
            query: None,
            offset: 0,
            limit: 20,
            rtxn,
            index,
        }
    }

    pub fn query(&mut self, query: impl Into<String>) -> &mut Search<'a> {
        self.query = Some(query.into());
        self
    }

    pub fn offset(&mut self, offset: usize) -> &mut Search<'a> {
        self.offset = offset;
        self
    }

    pub fn limit(&mut self, limit: usize) -> &mut Search<'a> {
        self.limit = limit;
        self
    }

    /// Extracts the query words from the query string and returns the DFAs accordingly.
    /// TODO introduce settings for the number of typos regarding the words lengths.
    fn generate_query_dfas(query: &str) -> Vec<(String, bool, DFA)> {
        let (lev0, lev1, lev2) = (&LEVDIST0, &LEVDIST1, &LEVDIST2);

        let words: Vec<_> = QueryTokens::new(query).collect();
        let ends_with_whitespace = query.chars().last().map_or(false, char::is_whitespace);
        let number_of_words = words.len();

        words.into_iter().enumerate().map(|(i, word)| {
            let (word, quoted) = match word {
                QueryToken::Free(word) => (word.to_lowercase(), word.len() <= 3),
                QueryToken::Quoted(word) => (word.to_lowercase(), true),
            };
            let is_last = i + 1 == number_of_words;
            let is_prefix = is_last && !ends_with_whitespace && !quoted;
            let lev = match word.len() {
                0..=4 => if quoted { lev0 } else { lev0 },
                5..=8 => if quoted { lev0 } else { lev1 },
                _     => if quoted { lev0 } else { lev2 },
            };

            let dfa = if is_prefix {
                lev.build_prefix_dfa(&word)
            } else {
                lev.build_dfa(&word)
            };

            (word, is_prefix, dfa)
        })
        .collect()
    }

    /// Fetch the words from the given FST related to the given DFAs along with the associated
    /// positions and the unions of those positions where the words found appears in the documents.
    fn fetch_words_positions(
        rtxn: &heed::RoTxn,
        index: &Index,
        fst: &fst::Set<&[u8]>,
        dfas: Vec<(String, bool, DFA)>,
    ) -> anyhow::Result<(Vec<Vec<(String, u8, RoaringBitmap)>>, Vec<RoaringBitmap>)>
    {
        // A Vec storing all the derived words from the original query words, associated
        // with the distance from the original word and the positions it appears at.
        // The index the derived words appears in the Vec corresponds to the original query
        // word position.
        let mut derived_words = Vec::<Vec::<(String, u8, RoaringBitmap)>>::with_capacity(dfas.len());
        // A Vec storing the unions of all of each of the derived words positions. The index
        // the union appears in the Vec corresponds to the original query word position.
        let mut union_positions = Vec::<RoaringBitmap>::with_capacity(dfas.len());

        for (_word, _is_prefix, dfa) in dfas {

            let mut acc_derived_words = Vec::new();
            let mut acc_union_positions = RoaringBitmap::new();
            let mut stream = fst.search_with_state(&dfa).into_stream();
            while let Some((word, state)) = stream.next() {

                let word = std::str::from_utf8(word)?;
                let positions = index.word_positions.get(rtxn, word)?.unwrap();
                let distance = dfa.distance(state);
                acc_union_positions.union_with(&positions);
                acc_derived_words.push((word.to_string(), distance.to_u8(), positions));
            }
            derived_words.push(acc_derived_words);
            union_positions.push(acc_union_positions);
        }

        Ok((derived_words, union_positions))
    }

    /// Returns the set of docids that contains all of the query words.
    fn compute_candidates(
        rtxn: &heed::RoTxn,
        index: &Index,
        derived_words: &[Vec<(String, u8, RoaringBitmap)>],
    ) -> anyhow::Result<RoaringBitmap>
    {
        // we do a union between all the docids of each of the derived words,
        // we got N unions (the number of original query words), we then intersect them.
        // TODO we must store the words documents ids to avoid these unions.
        let mut candidates = RoaringBitmap::new();
        let number_of_attributes = index.number_of_attributes(rtxn)?.map_or(0, |n| n as u32);

        for (i, derived_words) in derived_words.iter().enumerate() {

            let mut union_docids = RoaringBitmap::new();
            for (word, _distance, _positions) in derived_words {
                for attr in 0..number_of_attributes {

                    let mut key = word.clone().into_bytes();
                    key.extend_from_slice(&attr.to_be_bytes());
                    if let Some(docids) = index.word_attribute_docids.get(rtxn, &key)? {
                        union_docids.union_with(&docids);
                    }
                }
            }

            if i == 0 {
                candidates = union_docids;
            } else {
                candidates.intersect_with(&union_docids);
            }
        }

        Ok(candidates)
    }

    /// Returns the union of the same position for all the given words.
    fn union_word_position(
        rtxn: &heed::RoTxn,
        index: &Index,
        words: &[(String, u8, RoaringBitmap)],
        position: Position,
    ) -> anyhow::Result<RoaringBitmap>
    {
        let mut union_docids = RoaringBitmap::new();
        for (word, _distance, positions) in words {
            if positions.contains(position) {
                let mut key = word.clone().into_bytes();
                key.extend_from_slice(&position.to_be_bytes());
                if let Some(docids) = index.word_position_docids.get(rtxn, &key)? {
                    union_docids.union_with(&docids);
                }
            }
        }
        Ok(union_docids)
    }

    /// Returns the union of the same attribute for all the given words.
    fn union_word_attribute(
        rtxn: &heed::RoTxn,
        index: &Index,
        words: &[(String, u8, RoaringBitmap)],
        attribute: Attribute,
    ) -> anyhow::Result<RoaringBitmap>
    {
        let mut union_docids = RoaringBitmap::new();
        for (word, _distance, _positions) in words {
            let mut key = word.clone().into_bytes();
            key.extend_from_slice(&attribute.to_be_bytes());
            if let Some(docids) = index.word_attribute_docids.get(rtxn, &key)? {
                union_docids.union_with(&docids);
            }
        }
        Ok(union_docids)
    }

    pub fn execute(&self) -> anyhow::Result<SearchResult> {
        let rtxn = self.rtxn;
        let index = self.index;

        let fst = match index.fst(rtxn)? {
            Some(fst) => fst,
            None => return Ok(Default::default()),
        };

        // Construct the DFAs related to the query words.
        // TODO do a placeholder search when query string isn't present.
        let dfas = match &self.query {
            Some(q) => Self::generate_query_dfas(q),
            None => return Ok(Default::default()),
        };

        let (derived_words, union_positions) = Self::fetch_words_positions(rtxn, index, &fst, dfas)?;
        let candidates = Self::compute_candidates(rtxn, index, &derived_words)?;

        let union_cache = HashMap::new();
        let mut intersect_cache = HashMap::new();

        let mut attribute_union_cache = HashMap::new();
        let mut attribute_intersect_cache = HashMap::new();

        let candidates = Rc::new(RefCell::new(candidates));
        let union_cache = Rc::new(RefCell::new(union_cache));

        // Returns `true` if there is documents in common between the two words and positions given.
        // TODO move this closure to a better place.
        let candidates_cloned = candidates.clone();
        let union_cache_cloned = union_cache.clone();
        let mut contains_documents = |(lword, lpos), (rword, rpos)| {
            if lpos == rpos { return false }

            // TODO move this function to a better place.
            let (lattr, _) = node::extract_position(lpos);
            let (rattr, _) = node::extract_position(rpos);

            let candidates = &candidates_cloned.borrow();
            let mut union_cache = union_cache_cloned.borrow_mut();

            if lattr == rattr {
                // We retrieve or compute the intersection between the two given words and positions.
                *intersect_cache.entry(((lword, lpos), (rword, rpos))).or_insert_with(|| {
                    // We retrieve or compute the unions for the two words and positions.
                    union_cache.entry((lword, lpos)).or_insert_with(|| {
                        let words: &Vec<_> = &derived_words[lword];
                        Self::union_word_position(rtxn, index, words, lpos).unwrap()
                    });
                    union_cache.entry((rword, rpos)).or_insert_with(|| {
                        let words: &Vec<_> = &derived_words[rword];
                        Self::union_word_position(rtxn, index, words, rpos).unwrap()
                    });

                    // TODO is there a way to avoid this double gets?
                    let lunion_docids = union_cache.get(&(lword, lpos)).unwrap();
                    let runion_docids = union_cache.get(&(rword, rpos)).unwrap();

                    // We first check that the docids of these unions are part of the candidates.
                    if lunion_docids.is_disjoint(candidates) { return false }
                    if runion_docids.is_disjoint(candidates) { return false }

                    !lunion_docids.is_disjoint(&runion_docids)
                })
            } else {
                *attribute_intersect_cache.entry(((lword, lattr), (rword, rattr))).or_insert_with(|| {
                    // We retrieve or compute the unions for the two words and positions.
                    attribute_union_cache.entry((lword, lattr)).or_insert_with(|| {
                        let words: &Vec<_> = &derived_words[lword];
                        Self::union_word_attribute(rtxn, index, words, lattr).unwrap()
                    });
                    attribute_union_cache.entry((rword, rattr)).or_insert_with(|| {
                        let words: &Vec<_> = &derived_words[rword];
                        Self::union_word_attribute(rtxn, index, words, rattr).unwrap()
                    });

                    // TODO is there a way to avoid this double gets?
                    let lunion_docids = attribute_union_cache.get(&(lword, lattr)).unwrap();
                    let runion_docids = attribute_union_cache.get(&(rword, rattr)).unwrap();

                    // We first check that the docids of these unions are part of the candidates.
                    if lunion_docids.is_disjoint(candidates) { return false }
                    if runion_docids.is_disjoint(candidates) { return false }

                    !lunion_docids.is_disjoint(&runion_docids)
                })
            }
        };

        // We instantiate an astar bag Iterator that returns the best paths incrementally,
        // it means that it will first return the best paths then the next best paths...
        let astar_iter = AstarBagIter::new(
            Node::Uninit, // start
            |n| n.successors(&union_positions, &mut contains_documents), // successors
            |_| 0, // heuristic
            |n| n.is_complete(&union_positions), // success
        );

        let mut documents = Vec::new();
        for (paths, proximity) in astar_iter {

            let mut union_cache = union_cache.borrow_mut();
            let mut candidates = candidates.borrow_mut();

            let mut positions: Vec<Vec<_>> = paths.map(|p| p.iter().filter_map(Node::position).collect()).collect();
            positions.sort_unstable();

            debug!("Found {} positions with a proximity of {}", positions.len(), proximity);

            let mut same_proximity_union = RoaringBitmap::default();
            for positions in positions {
                // Precompute the potentially missing unions
                positions.iter().enumerate().for_each(|(word, pos)| {
                    union_cache.entry((word, *pos)).or_insert_with(|| {
                        let words = &derived_words[word];
                        Self::union_word_position(rtxn, index, words, *pos).unwrap()
                    });
                });

                // Retrieve the unions along with the popularity of it.
                let mut to_intersect: Vec<_> = positions.iter()
                    .enumerate()
                    .map(|(word, pos)| {
                        let docids = union_cache.get(&(word, *pos)).unwrap();
                        (docids.len(), docids)
                    })
                    .collect();

                // Sort the unions by popularity to help reduce
                // the number of documents as soon as possible.
                to_intersect.sort_unstable_by_key(|(l, _)| *l);

                let intersect_docids: Option<RoaringBitmap> = to_intersect.into_iter()
                    .fold(None, |acc, (_, union_docids)| {
                        match acc {
                            Some(mut left) => {
                                left.intersect_with(&union_docids);
                                Some(left)
                            },
                            None => Some(union_docids.clone()),
                        }
                    });

                if let Some(intersect_docids) = intersect_docids {
                    same_proximity_union.union_with(&intersect_docids);
                }

                // We found enough documents we can stop here
                if documents.iter().map(RoaringBitmap::len).sum::<u64>() + same_proximity_union.len() >= 20 {
                    break;
                }
            }

            // We achieve to find valid documents ids so we remove them from the candidates list.
            candidates.difference_with(&same_proximity_union);

            documents.push(same_proximity_union);

            // We remove the double occurences of documents.
            for i in 0..documents.len() {
                if let Some((docs, others)) = documents[..=i].split_last_mut() {
                    others.iter().for_each(|other| docs.difference_with(other));
                }
            }
            documents.retain(|rb| !rb.is_empty());

            // We found enough documents we can stop here.
            if documents.iter().map(RoaringBitmap::len).sum::<u64>() >= 20 {
                break;
            }
        }

        let found_words = derived_words.into_iter().flatten().map(|(w, _, _)| w).collect();
        let documents_ids = documents.iter().flatten().take(20).collect();

        Ok(SearchResult { found_words, documents_ids })
    }
}

#[derive(Default)]
pub struct SearchResult {
    pub found_words: HashSet<String>,
    // TODO those documents ids should be associated with their criteria scores.
    pub documents_ids: Vec<DocumentId>,
}
