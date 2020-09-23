use std::collections::{HashMap, HashSet};

use fst::{IntoStreamer, Streamer};
use levenshtein_automata::DFA;
use levenshtein_automata::LevenshteinAutomatonBuilder as LevBuilder;
use log::debug;
use once_cell::sync::Lazy;
use roaring::bitmap::{IntoIter, RoaringBitmap};

use near_proximity::near_proximity;

use crate::proximity::path_proximity;
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
        rtxn: &heed::RoTxn,
        index: &Index,
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
                let docids = index.word_docids.get(rtxn, word)?.unwrap();
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

    fn fecth_keywords(
        rtxn: &heed::RoTxn,
        index: &Index,
        derived_words: &[(HashMap<String, (u8, RoaringBitmap)>, RoaringBitmap)],
        candidate: DocumentId,
    ) -> anyhow::Result<Vec<IntoIter>>
    {
        let mut keywords = Vec::with_capacity(derived_words.len());

        for (words, _) in derived_words {

            let mut union_positions = RoaringBitmap::new();
            for (word, (_distance, docids)) in words {

                if !docids.contains(candidate) { continue; }
                if let Some(positions) = index.docid_word_positions.get(rtxn, &(candidate, word))? {
                    union_positions.union_with(&positions);
                }
            }
            keywords.push(union_positions.into_iter());
        }

        Ok(keywords)
    }

    pub fn execute(&self) -> anyhow::Result<SearchResult> {
        let rtxn = self.rtxn;
        let index = self.index;
        let limit = self.limit;

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

        if dfas.is_empty() {
            return Ok(Default::default());
        }

        let derived_words = Self::fetch_words_docids(rtxn, index, &fst, dfas)?;
        let candidates = Self::compute_candidates(&derived_words);

        debug!("candidates: {:?}", candidates);

        let mut documents = Vec::new();

        // If there is only one query word, no need to compute the best proximities.
        if derived_words.len() == 1 || candidates.is_empty() {
            let found_words = derived_words.into_iter().flat_map(|(w, _)| w).map(|(w, _)| w).collect();
            let documents_ids = candidates.iter().take(limit).collect();
            return Ok(SearchResult { found_words, documents_ids });
        }

        fn words_pair_combinations<'a>(
            w1: &'a HashMap<String, (u8, RoaringBitmap)>,
            w2: &'a HashMap<String, (u8, RoaringBitmap)>,
        ) -> Vec<(&'a str, &'a str)>
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

        let mut answer = RoaringBitmap::new();
        for (i, words) in derived_words.windows(2).enumerate() {
            let pairs = words_pair_combinations(&words[0].0, &words[1].0);
            eprintln!("found pairs {:?}", pairs);

            let mut pairs_union = RoaringBitmap::new();
            for (w1, w2) in pairs {
                let key = (w1, w2, 1);
                if let Some(docids) = index.word_pair_proximity_docids.get(rtxn, &key)? {
                    pairs_union.union_with(&docids);
                }
            }

            if i == 0 {
                answer = pairs_union;
            } else {
                answer.intersect_with(&pairs_union);
            }
        }

        documents.push(answer);

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
