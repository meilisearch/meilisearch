use std::collections::{HashMap, HashSet};
use std::collections::hash_map::Entry::{Occupied, Vacant};

use fst::{IntoStreamer, Streamer};
use levenshtein_automata::DFA;
use levenshtein_automata::LevenshteinAutomatonBuilder as LevBuilder;
use log::debug;
use once_cell::sync::Lazy;
use roaring::bitmap::RoaringBitmap;

use crate::query_tokens::{QueryTokens, QueryToken};
use crate::{Index, DocumentId};

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
        Search { query: None, offset: 0, limit: 20, rtxn, index }
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

    /// Fetch the words from the given FST related to the given DFAs along with
    /// the associated documents ids.
    fn fetch_words_docids(
        &self,
        fst: &fst::Set<&[u8]>,
        dfas: Vec<(String, bool, DFA)>,
    ) -> anyhow::Result<Vec<(HashMap<String, (u8, RoaringBitmap)>, RoaringBitmap)>>
    {
        // A Vec storing all the derived words from the original query words, associated
        // with the distance from the original word and the docids where the words appears.
        let mut derived_words = Vec::<(HashMap::<String, (u8, RoaringBitmap)>, RoaringBitmap)>::with_capacity(dfas.len());

        for (_word, _is_prefix, dfa) in dfas {

            let mut acc_derived_words = HashMap::new();
            let mut unions_docids = RoaringBitmap::new();
            let mut stream = fst.search_with_state(&dfa).into_stream();
            while let Some((word, state)) = stream.next() {

                let word = std::str::from_utf8(word)?;
                let docids = self.index.word_docids.get(self.rtxn, word)?.unwrap();
                let distance = dfa.distance(state);
                unions_docids.union_with(&docids);
                acc_derived_words.insert(word.to_string(), (distance.to_u8(), docids));
            }
            derived_words.push((acc_derived_words, unions_docids));
        }

        Ok(derived_words)
    }

    /// Returns the set of docids that contains all of the query words.
    fn compute_candidates(
        derived_words: &[(HashMap<String, (u8, RoaringBitmap)>, RoaringBitmap)],
    ) -> RoaringBitmap
    {
        // We sort the derived words by inverse popularity, this way intersections are faster.
        let mut derived_words: Vec<_> = derived_words.iter().collect();
        derived_words.sort_unstable_by_key(|(_, docids)| docids.len());

        // we do a union between all the docids of each of the derived words,
        // we got N unions (the number of original query words), we then intersect them.
        let mut candidates = RoaringBitmap::new();

        for (i, (_, union_docids)) in derived_words.iter().enumerate() {
            if i == 0 {
                candidates = union_docids.clone();
            } else {
                candidates.intersect_with(&union_docids);
            }
        }

        candidates
    }

    // TODO Move this elsewhere!
    fn mana_depth_first_search(
        &self,
        words: &[(HashMap<String, (u8, RoaringBitmap)>, RoaringBitmap)],
        candidates: &RoaringBitmap,
        union_cache: &mut HashMap<(usize, u8), RoaringBitmap>,
    ) -> anyhow::Result<Option<RoaringBitmap>>
    {
        fn words_pair_combinations<'h>(
            w1: &'h HashMap<String, (u8, RoaringBitmap)>,
            w2: &'h HashMap<String, (u8, RoaringBitmap)>,
        ) -> Vec<(&'h str, &'h str)>
        {
            let mut pairs = Vec::new();
            for (w1, (_typos, docids1)) in w1 {
                for (w2, (_typos, docids2)) in w2 {
                    if !docids1.is_disjoint(&docids2) {
                        pairs.push((w1.as_str(), w2.as_str()));
                    }
                }
            }
            pairs
        }

        fn mdfs(
            index: &Index,
            rtxn: &heed::RoTxn,
            mana: u32,
            words: &[(HashMap<String, (u8, RoaringBitmap)>, RoaringBitmap)],
            candidates: &RoaringBitmap,
            parent_docids: &RoaringBitmap,
            union_cache: &mut HashMap<(usize, u8), RoaringBitmap>,
        ) -> anyhow::Result<Option<RoaringBitmap>>
        {
            use std::cmp::{min, max};

            let (words1, words2) = (&words[0].0, &words[1].0);
            let pairs = words_pair_combinations(words1, words2);
            let tail = &words[1..];
            let nb_children = tail.len() as u32 - 1;

            // The minimum amount of mana that you must consume is at least 1 and the
            // amount of mana that your children can consume. Because the last child must
            // consume the remaining mana, it is mandatory that there not too much at the end.
            let min_proximity = max(1, mana.saturating_sub(nb_children * 8)) as u8;

            // The maximum amount of mana that you can use is 8 or the remaining amount of
            // mana minus your children, as you can't just consume all the mana,
            // your children must have at least 1 mana.
            let max_proximity = min(8, mana - nb_children) as u8;

            for proximity in min_proximity..=max_proximity {
                let mut docids = match union_cache.entry((words.len(), proximity)) {
                    Occupied(entry) => entry.get().clone(),
                    Vacant(entry) => {
                        let mut docids = RoaringBitmap::new();
                        if proximity == 8 {
                            docids = candidates.clone();
                        } else {
                            for (w1, w2) in pairs.iter().cloned() {
                                let key = (w1, w2, proximity);
                                if let Some(di) = index.word_pair_proximity_docids.get(rtxn, &key)? {
                                    docids.union_with(&di);
                                }
                            }
                        }
                        entry.insert(docids).clone()
                    }
                };

                docids.intersect_with(parent_docids);

                if !docids.is_empty() {
                    let mana = mana.checked_sub(proximity as u32).unwrap();
                    // We are the last pair, we return without recursing as we don't have any child.
                    if tail.len() < 2 { return Ok(Some(docids)) }
                    if let Some(di) = mdfs(index, rtxn, mana, tail, candidates, &docids, union_cache)? {
                        return Ok(Some(di))
                    }
                }
            }

            Ok(None)
        }

        // Compute the number of pairs (windows) we have for this list of words.
        // If there only is one word therefore the only possible documents are the candidates.
        let initial_mana = match words.len().checked_sub(1) {
            Some(nb_windows) if nb_windows != 0 => nb_windows as u32,
            _ => return Ok(Some(candidates.clone())),
        };

        // TODO We must keep track of where we are in terms of mana and that should either be
        //      handled by an Iterator or by the caller. Keeping track of the amount of mana
        //      is an optimization, it makes this mdfs to only be called with the next valid
        //      mana and not called with all of the previous mana values.
        for mana in initial_mana..=initial_mana * 8 {
            if let Some(answer) = mdfs(&self.index, &self.rtxn, mana, words, candidates, candidates, union_cache)? {
                return Ok(Some(answer));
            }
        }

        Ok(None)
    }

    pub fn execute(&self) -> anyhow::Result<SearchResult> {
        let limit = self.limit;

        let fst = match self.index.fst(self.rtxn)? {
            Some(fst) => fst,
            None => return Ok(Default::default()),
        };

        // Construct the DFAs related to the query words.
        // TODO do a placeholder search when query string isn't present.
        let dfas = match &self.query {
            Some(q) => Self::generate_query_dfas(q),
            None => return Ok(Default::default()),
        };

        if dfas.is_empty() {
            return Ok(Default::default());
        }

        let derived_words = self.fetch_words_docids(&fst, dfas)?;
        let mut candidates = Self::compute_candidates(&derived_words);

        debug!("candidates: {:?}", candidates);

        let mut documents = Vec::new();
        let mut union_cache = HashMap::new();

        // We execute the DFS until we find enough documents, we run it with the
        // candidates list and remove the found documents from this list at each iteration.
        while documents.iter().map(RoaringBitmap::len).sum::<u64>() < limit as u64 {
            let answer = self.mana_depth_first_search(&derived_words, &candidates, &mut union_cache)?;

            let answer = match answer {
                Some(answer) if !answer.is_empty() => answer,
                _ => break,
            };

            debug!("answer: {:?}", answer);

            // We remove the answered documents from the list of
            // candidates to be sure we don't search for them again.
            candidates.difference_with(&answer);
            documents.push(answer);
        }

        let found_words = derived_words.into_iter().flat_map(|(w, _)| w).map(|(w, _)| w).collect();
        let documents_ids = documents.into_iter().flatten().take(limit).collect();
        Ok(SearchResult { found_words, documents_ids })
    }
}

#[derive(Default)]
pub struct SearchResult {
    pub found_words: HashSet<String>,
    // TODO those documents ids should be associated with their criteria scores.
    pub documents_ids: Vec<DocumentId>,
}
