use std::borrow::Cow;
use std::collections::{HashMap, HashSet};
use std::fmt;
use std::ops::Bound::{self, Unbounded, Included, Excluded};

use anyhow::{bail, ensure, Context};
use fst::{IntoStreamer, Streamer};
use heed::types::DecodeIgnore;
use levenshtein_automata::DFA;
use levenshtein_automata::LevenshteinAutomatonBuilder as LevBuilder;
use log::debug;
use once_cell::sync::Lazy;
use roaring::bitmap::RoaringBitmap;

use crate::facet::FacetType;
use crate::heed_codec::facet::FacetLevelValueI64Codec;
use crate::mdfs::Mdfs;
use crate::query_tokens::{QueryTokens, QueryToken};
use crate::{Index, DocumentId};

// Building these factories is not free.
static LEVDIST0: Lazy<LevBuilder> = Lazy::new(|| LevBuilder::new(0, true));
static LEVDIST1: Lazy<LevBuilder> = Lazy::new(|| LevBuilder::new(1, true));
static LEVDIST2: Lazy<LevBuilder> = Lazy::new(|| LevBuilder::new(2, true));

// TODO support also floats
#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash)]
pub enum FacetOperator {
    GreaterThan(i64),
    GreaterThanOrEqual(i64),
    LowerThan(i64),
    LowerThanOrEqual(i64),
    Equal(i64),
    Between(i64, i64),
}

// TODO also support ANDs, ORs, NOTs.
#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash)]
pub enum FacetCondition {
    Operator(u8, FacetOperator),
}

impl FacetCondition {
    pub fn from_str(
        rtxn: &heed::RoTxn,
        index: &Index,
        string: &str,
    ) -> anyhow::Result<Option<FacetCondition>>
    {
        use FacetCondition::*;
        use FacetOperator::*;

        let fields_ids_map = index.fields_ids_map(rtxn)?;
        let faceted_fields = index.faceted_fields(rtxn)?;

        // TODO use a better parsing technic
        let mut iter = string.split_whitespace();

        let field_name = match iter.next() {
            Some(field_name) => field_name,
            None => return Ok(None),
        };

        let field_id = fields_ids_map.id(&field_name).with_context(|| format!("field {} not found", field_name))?;
        let field_type = faceted_fields.get(&field_id).with_context(|| format!("field {} is not faceted", field_name))?;

        ensure!(*field_type == FacetType::Integer, "Only conditions on integer facets");

        match iter.next() {
            Some(">") => {
                let param = iter.next().context("missing parameter")?;
                let value = param.parse().with_context(|| format!("invalid parameter ({:?})", param))?;
                Ok(Some(Operator(field_id, GreaterThan(value))))
            },
            Some(">=") => {
                let param = iter.next().context("missing parameter")?;
                let value = param.parse().with_context(|| format!("invalid parameter ({:?})", param))?;
                Ok(Some(Operator(field_id, GreaterThanOrEqual(value))))
            },
            Some("<") => {
                let param = iter.next().context("missing parameter")?;
                let value = param.parse().with_context(|| format!("invalid parameter ({:?})", param))?;
                Ok(Some(Operator(field_id, LowerThan(value))))
            },
            Some("<=") => {
                let param = iter.next().context("missing parameter")?;
                let value = param.parse().with_context(|| format!("invalid parameter ({:?})", param))?;
                Ok(Some(Operator(field_id, LowerThanOrEqual(value))))
            },
            Some("=") => {
                let param = iter.next().context("missing parameter")?;
                let value = param.parse().with_context(|| format!("invalid parameter ({:?})", param))?;
                Ok(Some(Operator(field_id, Equal(value))))
            },
            Some(otherwise) => {
                // BETWEEN or X TO Y (both inclusive)
                let left_param = otherwise.parse().with_context(|| format!("invalid first TO parameter ({:?})", otherwise))?;
                ensure!(iter.next().map_or(false, |s| s.eq_ignore_ascii_case("to")), "TO keyword missing or invalid");
                let next = iter.next().context("missing second TO parameter")?;
                let right_param = next.parse().with_context(|| format!("invalid second TO parameter ({:?})", next))?;
                Ok(Some(Operator(field_id, Between(left_param, right_param))))
            },
            None => bail!("missing facet filter first parameter"),
        }
    }
}

pub struct Search<'a> {
    query: Option<String>,
    facet_condition: Option<FacetCondition>,
    offset: usize,
    limit: usize,
    rtxn: &'a heed::RoTxn<'a>,
    index: &'a Index,
}

impl<'a> Search<'a> {
    pub fn new(rtxn: &'a heed::RoTxn, index: &'a Index) -> Search<'a> {
        Search { query: None, facet_condition: None, offset: 0, limit: 20, rtxn, index }
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

    pub fn facet_condition(&mut self, condition: FacetCondition) -> &mut Search<'a> {
        self.facet_condition = Some(condition);
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
        fst: &fst::Set<Cow<[u8]>>,
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

    /// Aggregates the documents ids that are part of the specified range automatically
    /// going deeper through the levels.
    fn explore_facet_levels(
        &self,
        field_id: u8,
        level: u8,
        left: Bound<i64>,
        right: Bound<i64>,
        output: &mut RoaringBitmap,
    ) -> anyhow::Result<()>
    {
        match (left, right) {
            // If the request is an exact value we must go directly to the deepest level.
            (Included(l), Included(r)) if l == r && level > 0 => {
                return self.explore_facet_levels(field_id, 0, left, right, output);
            },
            // lower TO upper when lower > upper must return no result
            (Included(l), Included(r)) if l > r => return Ok(()),
            (Included(l), Excluded(r)) if l >= r => return Ok(()),
            (Excluded(l), Excluded(r)) if l >= r => return Ok(()),
            (Excluded(l), Included(r)) if l >= r => return Ok(()),
            (_, _) => (),
        }

        let mut left_found = None;
        let mut right_found = None;

        // We must create a custom iterator to be able to iterate over the
        // requested range as the range iterator cannot express some conditions.
        let left_bound = match left {
            Included(left) => Included((field_id, level, left, i64::MIN)),
            Excluded(left) => Excluded((field_id, level, left, i64::MIN)),
            Unbounded => Unbounded,
        };
        let right_bound = Included((field_id, level, i64::MAX, i64::MAX));
        let db = self.index.facet_field_id_value_docids.remap_key_type::<FacetLevelValueI64Codec>();
        let iter = db
            .range(self.rtxn, &(left_bound, right_bound))?
            .take_while(|r| r.as_ref().map_or(true, |((.., r), _)| {
                match right {
                    Included(right) => *r <= right,
                    Excluded(right) => *r < right,
                    Unbounded => true,
                }
            }));

        debug!("Iterating between {:?} and {:?} (level {})", left, right, level);

        for (i, result) in iter.enumerate() {
            let ((_fid, _level, l, r), docids) = result?;
            debug!("{} to {} (level {}) found {} documents", l, r, _level, docids.len());
            output.union_with(&docids);
            // We save the leftest and rightest bounds we actually found at this level.
            if i == 0 { left_found = Some(l); }
            right_found = Some(r);
        }

        // Can we go deeper?
        let deeper_level = match level.checked_sub(1) {
            Some(level) => level,
            None => return Ok(()),
        };

        // We must refine the left and right bounds of this range by retrieving the
        // missing part in a deeper level.
        match left_found.zip(right_found) {
            Some((left_found, right_found)) => {
                // If the bound is satisfied we avoid calling this function again.
                if !matches!(left, Included(l) if l == left_found) {
                    let sub_right = Excluded(left_found);
                    debug!("calling left with {:?} to {:?} (level {})",  left, sub_right, deeper_level);
                    self.explore_facet_levels(field_id, deeper_level, left, sub_right, output)?;
                }
                if !matches!(right, Included(r) if r == right_found) {
                    let sub_left = Excluded(right_found);
                    debug!("calling right with {:?} to {:?} (level {})", sub_left, right, deeper_level);
                    self.explore_facet_levels(field_id, deeper_level, sub_left, right, output)?;
                }
            },
            None => {
                // If we found nothing at this level it means that we must find
                // the same bounds but at a deeper, more precise level.
                self.explore_facet_levels(field_id, deeper_level, left, right, output)?;
            },
        }

        Ok(())
    }

    pub fn execute(&self) -> anyhow::Result<SearchResult> {
        let limit = self.limit;
        let fst = self.index.words_fst(self.rtxn)?;

        // Construct the DFAs related to the query words.
        let derived_words = match self.query.as_deref().map(Self::generate_query_dfas) {
            Some(dfas) if !dfas.is_empty() => Some(self.fetch_words_docids(&fst, dfas)?),
            _otherwise => None,
        };

        // We create the original candidates with the facet conditions results.
        let facet_candidates = match self.facet_condition {
            Some(FacetCondition::Operator(fid, operator)) => {
                use FacetOperator::*;

                // Make sure we always bound the ranges with the field id and the level,
                // as the facets values are all in the same database and prefixed by the
                // field id and the level.
                let (left, right) = match operator {
                    GreaterThan(val)        => (Excluded(val),      Included(i64::MAX)),
                    GreaterThanOrEqual(val) => (Included(val),      Included(i64::MAX)),
                    LowerThan(val)          => (Included(i64::MIN), Excluded(val)),
                    LowerThanOrEqual(val)   => (Included(i64::MIN), Included(val)),
                    Equal(val)              => (Included(val),      Included(val)),
                    Between(left, right)    => (Included(left),     Included(right)),
                };

                let db = self.index
                    .facet_field_id_value_docids
                    .remap_key_type::<FacetLevelValueI64Codec>();

                // Ask for the biggest value that can exist for this specific field, if it exists
                // that's fine if it don't, the value just before will be returned instead.
                let biggest_level = db
                    .remap_data_type::<DecodeIgnore>()
                    .get_lower_than_or_equal_to(self.rtxn, &(fid, u8::MAX, i64::MAX, i64::MAX))?
                    .and_then(|((id, level, _, _), _)| if id == fid { Some(level) } else { None });

                match biggest_level {
                    Some(level) => {
                        let mut output = RoaringBitmap::new();
                        self.explore_facet_levels(fid, level, left, right, &mut output)?;
                        Some(output)
                    },
                    None => None,
                }
            },
            None => None,
        };

        debug!("facet candidates: {:?}", facet_candidates);

        let (candidates, derived_words) = match (facet_candidates, derived_words) {
            (Some(mut facet_candidates), Some(derived_words)) => {
                let words_candidates = Self::compute_candidates(&derived_words);
                facet_candidates.intersect_with(&words_candidates);
                (facet_candidates, derived_words)
            },
            (None, Some(derived_words)) => {
                (Self::compute_candidates(&derived_words), derived_words)
            },
            (Some(facet_candidates), None) => {
                // If the query is not set or results in no DFAs but
                // there is some facet conditions we return a placeholder.
                let documents_ids = facet_candidates.iter().take(limit).collect();
                return Ok(SearchResult { documents_ids, ..Default::default() })
            },
            (None, None) => {
                // If the query is not set or results in no DFAs we return a placeholder.
                let documents_ids = self.index.documents_ids(self.rtxn)?.iter().take(limit).collect();
                return Ok(SearchResult { documents_ids, ..Default::default() })
            },
        };

        debug!("candidates: {:?}", candidates);

        // The mana depth first search is a revised DFS that explore
        // solutions in the order of their proximities.
        let mut mdfs = Mdfs::new(self.index, self.rtxn, &derived_words, candidates);
        let mut documents = Vec::new();

        // We execute the Mdfs iterator until we find enough documents.
        while documents.iter().map(RoaringBitmap::len).sum::<u64>() < limit as u64 {
            match mdfs.next().transpose()? {
                Some((proximity, answer)) => {
                    debug!("answer with a proximity of {}: {:?}", proximity, answer);
                    documents.push(answer);
                },
                None => break,
            }
        }

        let found_words = derived_words.into_iter().flat_map(|(w, _)| w).map(|(w, _)| w).collect();
        let documents_ids = documents.into_iter().flatten().take(limit).collect();
        Ok(SearchResult { found_words, documents_ids })
    }
}

impl fmt::Debug for Search<'_> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("Search")
            .field("query", &self.query)
            .field("facet_condition", &self.facet_condition)
            .field("offset", &self.offset)
            .field("limit", &self.limit)
            .finish()
    }
}

#[derive(Default)]
pub struct SearchResult {
    pub found_words: HashSet<String>,
    // TODO those documents ids should be associated with their criteria scores.
    pub documents_ids: Vec<DocumentId>,
}
