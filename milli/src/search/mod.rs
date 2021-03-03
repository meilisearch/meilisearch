use std::borrow::Cow;
use std::collections::{HashMap, HashSet};
use std::fmt;
use std::time::Instant;

use anyhow::{bail, Context};
use fst::{IntoStreamer, Streamer, Set};
use levenshtein_automata::DFA;
use levenshtein_automata::LevenshteinAutomatonBuilder as LevBuilder;
use log::debug;
use meilisearch_tokenizer::{AnalyzerConfig, Analyzer};
use once_cell::sync::Lazy;
use ordered_float::OrderedFloat;
use roaring::bitmap::RoaringBitmap;

use crate::facet::FacetType;
use crate::heed_codec::facet::{FacetLevelValueF64Codec, FacetLevelValueI64Codec};
use crate::heed_codec::facet::{FieldDocIdFacetF64Codec, FieldDocIdFacetI64Codec};
use crate::mdfs::Mdfs;
use crate::query_tokens::{query_tokens, QueryToken};
use crate::{Index, FieldId, DocumentId, Criterion};

pub use self::facet::{FacetCondition, FacetDistribution, FacetNumberOperator, FacetStringOperator};
pub use self::facet::{FacetIter};

// Building these factories is not free.
static LEVDIST0: Lazy<LevBuilder> = Lazy::new(|| LevBuilder::new(0, true));
static LEVDIST1: Lazy<LevBuilder> = Lazy::new(|| LevBuilder::new(1, true));
static LEVDIST2: Lazy<LevBuilder> = Lazy::new(|| LevBuilder::new(2, true));

mod facet;
mod query_tree;

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

        let stop_words = Set::default();
        let analyzer = Analyzer::new(AnalyzerConfig::default_with_stopwords(&stop_words));
        let analyzed = analyzer.analyze(query);
        let tokens = analyzed.tokens();
        let words: Vec<_> = query_tokens(tokens).collect();

        let ends_with_whitespace = query.chars().last().map_or(false, char::is_whitespace);
        let number_of_words = words.len();

        words.into_iter().enumerate().map(|(i, word)| {
            let (word, quoted) = match word {
                QueryToken::Free(token) => (token.text().to_string(), token.text().len() <= 3),
                QueryToken::Quoted(token) => (token.text().to_string(), true),
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

    fn facet_ordered(
        &self,
        field_id: FieldId,
        facet_type: FacetType,
        ascending: bool,
        mut documents_ids: RoaringBitmap,
        limit: usize,
    ) -> anyhow::Result<Vec<DocumentId>>
    {
        let mut output: Vec<_> = match facet_type {
            FacetType::Float => {
                if documents_ids.len() <= 1000 {
                    let db = self.index.field_id_docid_facet_values.remap_key_type::<FieldDocIdFacetF64Codec>();
                    let mut docids_values = Vec::with_capacity(documents_ids.len() as usize);
                    for docid in documents_ids.iter() {
                        let left = (field_id, docid, f64::MIN);
                        let right = (field_id, docid, f64::MAX);
                        let mut iter = db.range(self.rtxn, &(left..=right))?;
                        let entry = if ascending { iter.next() } else { iter.last() };
                        if let Some(((_, _, value), ())) = entry.transpose()? {
                            docids_values.push((docid, OrderedFloat(value)));
                        }
                    }
                    docids_values.sort_unstable_by_key(|(_, value)| *value);
                    let iter = docids_values.into_iter().map(|(id, _)| id);
                    if ascending {
                        iter.take(limit).collect()
                    } else {
                        iter.rev().take(limit).collect()
                    }
                } else {
                    let facet_fn = if ascending {
                        FacetIter::<f64, FacetLevelValueF64Codec>::new_reducing
                    } else {
                        FacetIter::<f64, FacetLevelValueF64Codec>::new_reverse_reducing
                    };
                    let mut limit_tmp = limit;
                    let mut output = Vec::new();
                    for result in facet_fn(self.rtxn, self.index, field_id, documents_ids.clone())? {
                        let (_val, docids) = result?;
                        limit_tmp = limit_tmp.saturating_sub(docids.len() as usize);
                        output.push(docids);
                        if limit_tmp == 0 { break }
                    }
                    output.into_iter().flatten().take(limit).collect()
                }
            },
            FacetType::Integer => {
                if documents_ids.len() <= 1000 {
                    let db = self.index.field_id_docid_facet_values.remap_key_type::<FieldDocIdFacetI64Codec>();
                    let mut docids_values = Vec::with_capacity(documents_ids.len() as usize);
                    for docid in documents_ids.iter() {
                        let left = (field_id, docid, i64::MIN);
                        let right = (field_id, docid, i64::MAX);
                        let mut iter = db.range(self.rtxn, &(left..=right))?;
                        let entry = if ascending { iter.next() } else { iter.last() };
                        if let Some(((_, _, value), ())) = entry.transpose()? {
                            docids_values.push((docid, value));
                        }
                    }
                    docids_values.sort_unstable_by_key(|(_, value)| *value);
                    let iter = docids_values.into_iter().map(|(id, _)| id);
                    if ascending {
                        iter.take(limit).collect()
                    } else {
                        iter.rev().take(limit).collect()
                    }
                } else {
                    let facet_fn = if ascending {
                        FacetIter::<i64, FacetLevelValueI64Codec>::new_reducing
                    } else {
                        FacetIter::<i64, FacetLevelValueI64Codec>::new_reverse_reducing
                    };
                    let mut limit_tmp = limit;
                    let mut output = Vec::new();
                    for result in facet_fn(self.rtxn, self.index, field_id, documents_ids.clone())? {
                        let (_val, docids) = result?;
                        limit_tmp = limit_tmp.saturating_sub(docids.len() as usize);
                        output.push(docids);
                        if limit_tmp == 0 { break }
                    }
                    output.into_iter().flatten().take(limit).collect()
                }
            },
            FacetType::String => bail!("criteria facet type must be a number"),
        };

        // if there isn't enough documents to return we try to complete that list
        // with documents that are maybe not faceted under this field and therefore
        // not returned by the previous facet iteration.
        if output.len() < limit {
            output.iter().for_each(|n| { documents_ids.remove(*n); });
            let remaining = documents_ids.iter().take(limit - output.len());
            output.extend(remaining);
        }

        Ok(output)
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
        let before = Instant::now();
        let facet_candidates = match &self.facet_condition {
            Some(condition) => Some(condition.evaluate(self.rtxn, self.index)?),
            None => None,
        };

        debug!("facet candidates: {:?} took {:.02?}", facet_candidates, before.elapsed());

        let order_by_facet = {
            let criteria = self.index.criteria(self.rtxn)?;
            let result = criteria.into_iter().flat_map(|criterion| {
                match criterion {
                    Criterion::Asc(fid) => Some((fid, true)),
                    Criterion::Desc(fid) => Some((fid, false)),
                    _ => None
                }
            }).next();
            match result {
                Some((attr_name, is_ascending)) => {
                    let field_id_map = self.index.fields_ids_map(self.rtxn)?;
                    let fid = field_id_map.id(&attr_name).with_context(|| format!("unknown field: {:?}", attr_name))?;
                    let faceted_fields = self.index.faceted_fields_ids(self.rtxn)?;
                    let ftype = *faceted_fields.get(&fid)
                        .with_context(|| format!("{:?} not found in the faceted fields.", attr_name))
                        .expect("corrupted data: ");
                    Some((fid, ftype, is_ascending))
                },
                None => None,
            }
        };

        let before = Instant::now();
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
                let documents_ids = match order_by_facet {
                    Some((fid, ftype, is_ascending)) => {
                        self.facet_ordered(fid, ftype, is_ascending, facet_candidates.clone(), limit)?
                    },
                    None => facet_candidates.iter().take(limit).collect(),
                };
                return Ok(SearchResult {
                    documents_ids,
                    candidates: facet_candidates,
                    ..Default::default()
                })
            },
            (None, None) => {
                // If the query is not set or results in no DFAs we return a placeholder.
                let all_docids = self.index.documents_ids(self.rtxn)?;
                let documents_ids = match order_by_facet {
                    Some((fid, ftype, is_ascending)) => {
                        self.facet_ordered(fid, ftype, is_ascending, all_docids.clone(), limit)?
                    },
                    None => all_docids.iter().take(limit).collect(),
                };
                return Ok(SearchResult { documents_ids, candidates: all_docids,..Default::default() })
            },
        };

        debug!("candidates: {:?} took {:.02?}", candidates, before.elapsed());

        // The mana depth first search is a revised DFS that explore
        // solutions in the order of their proximities.
        let mut mdfs = Mdfs::new(self.index, self.rtxn, &derived_words, candidates.clone());
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
        let documents_ids = match order_by_facet {
            Some((fid, ftype, order)) => {
                let mut ordered_documents = Vec::new();
                for documents_ids in documents {
                    let docids = self.facet_ordered(fid, ftype, order, documents_ids, limit)?;
                    ordered_documents.push(docids);
                    if ordered_documents.iter().map(Vec::len).sum::<usize>() >= limit { break }
                }
                ordered_documents.into_iter().flatten().take(limit).collect()
            },
            None => documents.into_iter().flatten().take(limit).collect(),
        };

        Ok(SearchResult { found_words, candidates, documents_ids })
    }
}

impl fmt::Debug for Search<'_> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let Search { query, facet_condition, offset, limit, rtxn: _, index: _ } = self;
        f.debug_struct("Search")
            .field("query", query)
            .field("facet_condition", facet_condition)
            .field("offset", offset)
            .field("limit", limit)
            .finish()
    }
}

#[derive(Default)]
pub struct SearchResult {
    pub found_words: HashSet<String>,
    pub candidates: RoaringBitmap,
    // TODO those documents ids should be associated with their criteria scores.
    pub documents_ids: Vec<DocumentId>,
}

pub fn word_typos(word: &str, is_prefix: bool, max_typo: u8, fst: &fst::Set<Cow<[u8]>>) -> anyhow::Result<Vec<(String, u8)>> {
    let dfa = {
        let lev = match max_typo {
            0 => &LEVDIST0,
            1 => &LEVDIST1,
            _ => &LEVDIST2,
        };

        if is_prefix {
            lev.build_prefix_dfa(&word)
        } else {
            lev.build_dfa(&word)
        }
    };

    let mut derived_words = Vec::new();
    let mut stream = fst.search_with_state(&dfa).into_stream();

    while let Some((word, state)) = stream.next() {
        let word = std::str::from_utf8(word)?;
        let distance = dfa.distance(state);
        derived_words.push((word.to_string(), distance.to_u8()));
    }

    Ok(derived_words)
}
