use std::cmp::{Ordering, Reverse};
use std::collections::BinaryHeap;
use std::ops::ControlFlow;

use charabia::normalizer::NormalizerOption;
use charabia::Normalize;
use fst::automaton::{Automaton, Str};
use fst::{IntoStreamer, Streamer};
use roaring::RoaringBitmap;
use tracing::error;

use crate::error::UserError;
use crate::heed_codec::facet::{FacetGroupKey, FacetGroupValue};
use crate::search::build_dfa;
use crate::{DocumentId, FieldId, OrderBy, Result, Search};

/// The maximum number of values per facet returned by the facet search route.
const DEFAULT_MAX_NUMBER_OF_VALUES_PER_FACET: usize = 100;

pub struct SearchForFacetValues<'a> {
    query: Option<String>,
    facet: String,
    search_query: Search<'a>,
    max_values: usize,
    is_hybrid: bool,
}

impl<'a> SearchForFacetValues<'a> {
    pub fn new(
        facet: String,
        search_query: Search<'a>,
        is_hybrid: bool,
    ) -> SearchForFacetValues<'a> {
        SearchForFacetValues {
            query: None,
            facet,
            search_query,
            max_values: DEFAULT_MAX_NUMBER_OF_VALUES_PER_FACET,
            is_hybrid,
        }
    }

    pub fn query(&mut self, query: impl Into<String>) -> &mut Self {
        self.query = Some(query.into());
        self
    }

    pub fn max_values(&mut self, max: usize) -> &mut Self {
        self.max_values = max;
        self
    }

    fn one_original_value_of(
        &self,
        field_id: FieldId,
        facet_str: &str,
        any_docid: DocumentId,
    ) -> Result<Option<String>> {
        let index = self.search_query.index;
        let rtxn = self.search_query.rtxn;
        let key: (FieldId, _, &str) = (field_id, any_docid, facet_str);
        Ok(index.field_id_docid_facet_strings.get(rtxn, &key)?.map(|v| v.to_owned()))
    }

    pub fn execute(&self) -> Result<Vec<FacetValueHit>> {
        let index = self.search_query.index;
        let rtxn = self.search_query.rtxn;

        let filterable_fields = index.filterable_fields(rtxn)?;
        if !filterable_fields.contains(&self.facet) {
            let (valid_fields, hidden_fields) =
                index.remove_hidden_fields(rtxn, filterable_fields)?;

            return Err(UserError::InvalidFacetSearchFacetName {
                field: self.facet.clone(),
                valid_fields,
                hidden_fields,
            }
            .into());
        }

        let fields_ids_map = index.fields_ids_map(rtxn)?;
        let fid = match fields_ids_map.id(&self.facet) {
            Some(fid) => fid,
            // we return an empty list of results when the attribute has been
            // set as filterable but no document contains this field (yet).
            None => return Ok(Vec::new()),
        };

        let fst = match self.search_query.index.facet_id_string_fst.get(rtxn, &fid)? {
            Some(fst) => fst,
            None => return Ok(vec![]),
        };

        let search_candidates = self
            .search_query
            .execute_for_candidates(self.is_hybrid || self.search_query.vector.is_some())?;

        let sort_by = index.sort_facet_values_by(rtxn)?.get(&self.facet);

        match self.query.as_ref() {
            Some(query) => {
                let options = NormalizerOption { lossy: true, ..Default::default() };
                let query = query.normalize(&options);
                let query = query.as_ref();

                let authorize_typos = self.search_query.index.authorize_typos(rtxn)?;
                let field_authorizes_typos =
                    !self.search_query.index.exact_attributes_ids(rtxn)?.contains(&fid);

                if authorize_typos && field_authorizes_typos {
                    let exact_words_fst = self.search_query.index.exact_words(rtxn)?;
                    if exact_words_fst.map_or(false, |fst| fst.contains(query)) {
                        let mut results = vec![];
                        if fst.contains(query) {
                            self.fetch_original_facets_using_normalized(
                                fid,
                                query,
                                query,
                                &search_candidates,
                                &mut results,
                            )?;
                        }
                        Ok(results)
                    } else {
                        let one_typo = self.search_query.index.min_word_len_one_typo(rtxn)?;
                        let two_typos = self.search_query.index.min_word_len_two_typos(rtxn)?;

                        let is_prefix = true;
                        let automaton = if query.len() < one_typo as usize {
                            build_dfa(query, 0, is_prefix)
                        } else if query.len() < two_typos as usize {
                            build_dfa(query, 1, is_prefix)
                        } else {
                            build_dfa(query, 2, is_prefix)
                        };

                        let mut stream = fst.search(automaton).into_stream();
                        let mut results = vec![];
                        while let Some(facet_value) = stream.next() {
                            let value = std::str::from_utf8(facet_value)?;
                            if self
                                .fetch_original_facets_using_normalized(
                                    fid,
                                    value,
                                    query,
                                    &search_candidates,
                                    &mut results,
                                )?
                                .is_break()
                            {
                                break;
                            }
                        }

                        Ok(results)
                    }
                } else {
                    let automaton = Str::new(query).starts_with();
                    let mut stream = fst.search(automaton).into_stream();
                    let mut results = vec![];
                    while let Some(facet_value) = stream.next() {
                        let value = std::str::from_utf8(facet_value)?;
                        if self
                            .fetch_original_facets_using_normalized(
                                fid,
                                value,
                                query,
                                &search_candidates,
                                &mut results,
                            )?
                            .is_break()
                        {
                            break;
                        }
                    }

                    Ok(results)
                }
            }
            None => {
                let prefix = FacetGroupKey { field_id: fid, level: 0, left_bound: "" };
                match sort_by {
                    OrderBy::Lexicographic => {
                        let mut results = vec![];
                        for result in index.facet_id_string_docids.prefix_iter(rtxn, &prefix)? {
                            let (FacetGroupKey { left_bound, .. }, FacetGroupValue { bitmap, .. }) =
                                result?;
                            let count = search_candidates.intersection_len(&bitmap);
                            if count != 0 {
                                let value = self
                                    .one_original_value_of(fid, left_bound, bitmap.min().unwrap())?
                                    .unwrap_or_else(|| left_bound.to_string());
                                results.push(FacetValueHit { value, count });
                            }
                            if results.len() >= self.max_values {
                                break;
                            }
                        }
                        Ok(results)
                    }
                    OrderBy::Count => {
                        let mut top_counts = BinaryHeap::new();
                        for result in index.facet_id_string_docids.prefix_iter(rtxn, &prefix)? {
                            let (FacetGroupKey { left_bound, .. }, FacetGroupValue { bitmap, .. }) =
                                result?;
                            let count = search_candidates.intersection_len(&bitmap);
                            if count != 0 {
                                let value = self
                                    .one_original_value_of(fid, left_bound, bitmap.min().unwrap())?
                                    .unwrap_or_else(|| left_bound.to_string());
                                if top_counts.len() >= self.max_values {
                                    top_counts.pop();
                                }
                                // It is a max heap and we need to move the smallest counts at the
                                // top to be able to pop them when we reach the max_values limit.
                                top_counts.push(Reverse(FacetValueHit { value, count }));
                            }
                        }

                        // Convert the heap into a vec of hits by removing the Reverse wrapper.
                        // Hits are already in the right order as they were reversed and there
                        // are output in ascending order.
                        Ok(top_counts
                            .into_sorted_vec()
                            .into_iter()
                            .map(|Reverse(hit)| hit)
                            .collect())
                    }
                }
            }
        }
    }

    fn fetch_original_facets_using_normalized(
        &self,
        fid: FieldId,
        value: &str,
        query: &str,
        search_candidates: &RoaringBitmap,
        results: &mut Vec<FacetValueHit>,
    ) -> Result<ControlFlow<()>> {
        let index = self.search_query.index;
        let rtxn = self.search_query.rtxn;

        let database = index.facet_id_normalized_string_strings;
        let key = (fid, value);
        let original_strings = match database.get(rtxn, &key)? {
            Some(original_strings) => original_strings,
            None => {
                error!("the facet value is missing from the facet database: {key:?}");
                return Ok(ControlFlow::Continue(()));
            }
        };
        for original in original_strings {
            let key = FacetGroupKey { field_id: fid, level: 0, left_bound: original.as_str() };
            let docids = match index.facet_id_string_docids.get(rtxn, &key)? {
                Some(FacetGroupValue { bitmap, .. }) => bitmap,
                None => {
                    error!("the facet value is missing from the facet database: {key:?}");
                    return Ok(ControlFlow::Continue(()));
                }
            };
            let count = search_candidates.intersection_len(&docids);
            if count != 0 {
                let value = self
                    .one_original_value_of(fid, &original, docids.min().unwrap())?
                    .unwrap_or_else(|| query.to_string());
                results.push(FacetValueHit { value, count });
            }
            if results.len() >= self.max_values {
                return Ok(ControlFlow::Break(()));
            }
        }

        Ok(ControlFlow::Continue(()))
    }
}

#[derive(Debug, Clone, serde::Serialize, PartialEq)]
pub struct FacetValueHit {
    /// The original facet value
    pub value: String,
    /// The number of documents associated to this facet
    pub count: u64,
}

impl PartialOrd for FacetValueHit {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for FacetValueHit {
    fn cmp(&self, other: &Self) -> Ordering {
        self.count.cmp(&other.count).then_with(|| self.value.cmp(&other.value))
    }
}

impl Eq for FacetValueHit {}
