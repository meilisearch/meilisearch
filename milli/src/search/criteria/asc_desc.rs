use std::mem::take;

use heed::BytesDecode;
use itertools::Itertools;
use log::debug;
use ordered_float::OrderedFloat;
use roaring::RoaringBitmap;

use super::{Criterion, CriterionParameters, CriterionResult};
use crate::facet::FacetType;
use crate::heed_codec::facet::{FacetGroupKeyCodec, OrderedF64Codec};
use crate::heed_codec::ByteSliceRefCodec;
use crate::search::criteria::{resolve_query_tree, CriteriaBuilder, InitialCandidates};
use crate::search::facet::{ascending_facet_sort, descending_facet_sort};
use crate::search::query_tree::Operation;
use crate::search::CriterionImplementationStrategy;
use crate::{FieldId, Index, Result};

/// Threshold on the number of candidates that will make
/// the system to choose between one algorithm or another.
const CANDIDATES_THRESHOLD: u64 = 1000;

pub struct AscDesc<'t> {
    index: &'t Index,
    rtxn: &'t heed::RoTxn<'t>,
    field_name: String,
    field_id: Option<FieldId>,
    is_ascending: bool,
    query_tree: Option<Operation>,
    candidates: Box<dyn Iterator<Item = heed::Result<RoaringBitmap>> + 't>,
    allowed_candidates: RoaringBitmap,
    initial_candidates: InitialCandidates,
    faceted_candidates: RoaringBitmap,
    implementation_strategy: CriterionImplementationStrategy,
    parent: Box<dyn Criterion + 't>,
}

impl<'t> AscDesc<'t> {
    pub fn asc(
        index: &'t Index,
        rtxn: &'t heed::RoTxn,
        parent: Box<dyn Criterion + 't>,
        field_name: String,
        implementation_strategy: CriterionImplementationStrategy,
    ) -> Result<Self> {
        Self::new(index, rtxn, parent, field_name, true, implementation_strategy)
    }

    pub fn desc(
        index: &'t Index,
        rtxn: &'t heed::RoTxn,
        parent: Box<dyn Criterion + 't>,
        field_name: String,
        implementation_strategy: CriterionImplementationStrategy,
    ) -> Result<Self> {
        Self::new(index, rtxn, parent, field_name, false, implementation_strategy)
    }

    fn new(
        index: &'t Index,
        rtxn: &'t heed::RoTxn,
        parent: Box<dyn Criterion + 't>,
        field_name: String,
        is_ascending: bool,
        implementation_strategy: CriterionImplementationStrategy,
    ) -> Result<Self> {
        let fields_ids_map = index.fields_ids_map(rtxn)?;
        let field_id = fields_ids_map.id(&field_name);
        let faceted_candidates = match field_id {
            Some(field_id) => {
                let number_faceted =
                    index.faceted_documents_ids(rtxn, field_id, FacetType::Number)?;
                let string_faceted =
                    index.faceted_documents_ids(rtxn, field_id, FacetType::String)?;
                number_faceted | string_faceted
            }
            None => RoaringBitmap::default(),
        };

        Ok(AscDesc {
            index,
            rtxn,
            field_name,
            field_id,
            is_ascending,
            query_tree: None,
            candidates: Box::new(std::iter::empty()),
            allowed_candidates: RoaringBitmap::new(),
            faceted_candidates,
            initial_candidates: InitialCandidates::Estimated(RoaringBitmap::new()),
            implementation_strategy,
            parent,
        })
    }
}

impl<'t> Criterion for AscDesc<'t> {
    #[logging_timer::time("AscDesc::{}")]
    fn next(&mut self, params: &mut CriterionParameters) -> Result<Option<CriterionResult>> {
        // remove excluded candidates when next is called, instead of doing it in the loop.
        self.allowed_candidates -= params.excluded_candidates;

        loop {
            debug!(
                "Facet {}({}) iteration",
                if self.is_ascending { "Asc" } else { "Desc" },
                self.field_name
            );

            match self.candidates.next().transpose()? {
                None if !self.allowed_candidates.is_empty() => {
                    return Ok(Some(CriterionResult {
                        query_tree: self.query_tree.clone(),
                        candidates: Some(take(&mut self.allowed_candidates)),
                        filtered_candidates: None,
                        initial_candidates: Some(self.initial_candidates.take()),
                    }));
                }
                None => match self.parent.next(params)? {
                    Some(CriterionResult {
                        query_tree,
                        candidates,
                        filtered_candidates,
                        initial_candidates,
                    }) => {
                        self.query_tree = query_tree;
                        let mut candidates = match (&self.query_tree, candidates) {
                            (_, Some(candidates)) => candidates,
                            (Some(qt), None) => {
                                let context = CriteriaBuilder::new(self.rtxn, self.index)?;
                                resolve_query_tree(&context, qt, params.wdcache)?
                            }
                            (None, None) => self.index.documents_ids(self.rtxn)?,
                        };

                        if let Some(filtered_candidates) = filtered_candidates {
                            candidates &= filtered_candidates;
                        }

                        match initial_candidates {
                            Some(initial_candidates) => {
                                self.initial_candidates |= initial_candidates
                            }
                            None => self.initial_candidates.map_inplace(|c| c | &candidates),
                        }

                        if candidates.is_empty() {
                            continue;
                        }

                        self.allowed_candidates = &candidates - params.excluded_candidates;
                        self.candidates = match self.field_id {
                            Some(field_id) => facet_ordered(
                                self.index,
                                self.rtxn,
                                field_id,
                                self.is_ascending,
                                candidates & &self.faceted_candidates,
                                self.implementation_strategy,
                            )?,
                            None => Box::new(std::iter::empty()),
                        };
                    }
                    None => return Ok(None),
                },
                Some(mut candidates) => {
                    candidates -= params.excluded_candidates;
                    self.allowed_candidates -= &candidates;
                    return Ok(Some(CriterionResult {
                        query_tree: self.query_tree.clone(),
                        candidates: Some(candidates),
                        filtered_candidates: None,
                        initial_candidates: Some(self.initial_candidates.take()),
                    }));
                }
            }
        }
    }
}

fn facet_ordered_iterative<'t>(
    index: &'t Index,
    rtxn: &'t heed::RoTxn,
    field_id: FieldId,
    is_ascending: bool,
    candidates: RoaringBitmap,
) -> Result<Box<dyn Iterator<Item = heed::Result<RoaringBitmap>> + 't>> {
    let number_iter = iterative_facet_number_ordered_iter(
        index,
        rtxn,
        field_id,
        is_ascending,
        candidates.clone(),
    )?;
    let string_iter =
        iterative_facet_string_ordered_iter(index, rtxn, field_id, is_ascending, candidates)?;
    Ok(Box::new(number_iter.chain(string_iter).map(Ok)) as Box<dyn Iterator<Item = _>>)
}

fn facet_extreme_value<'t>(
    mut extreme_it: impl Iterator<Item = heed::Result<(RoaringBitmap, &'t [u8])>> + 't,
) -> Result<Option<f64>> {
    let extreme_value =
        if let Some(extreme_value) = extreme_it.next() { extreme_value } else { return Ok(None) };
    let (_, extreme_value) = extreme_value?;

    Ok(OrderedF64Codec::bytes_decode(extreme_value))
}

pub fn facet_min_value<'t>(
    index: &'t Index,
    rtxn: &'t heed::RoTxn,
    field_id: FieldId,
    candidates: RoaringBitmap,
) -> Result<Option<f64>> {
    let db = index.facet_id_f64_docids.remap_key_type::<FacetGroupKeyCodec<ByteSliceRefCodec>>();
    let it = ascending_facet_sort(rtxn, db, field_id, candidates)?;
    facet_extreme_value(it)
}

pub fn facet_max_value<'t>(
    index: &'t Index,
    rtxn: &'t heed::RoTxn,
    field_id: FieldId,
    candidates: RoaringBitmap,
) -> Result<Option<f64>> {
    let db = index.facet_id_f64_docids.remap_key_type::<FacetGroupKeyCodec<ByteSliceRefCodec>>();
    let it = descending_facet_sort(rtxn, db, field_id, candidates)?;
    facet_extreme_value(it)
}

fn facet_ordered_set_based<'t>(
    index: &'t Index,
    rtxn: &'t heed::RoTxn,
    field_id: FieldId,
    is_ascending: bool,
    candidates: RoaringBitmap,
) -> Result<Box<dyn Iterator<Item = heed::Result<RoaringBitmap>> + 't>> {
    let number_db =
        index.facet_id_f64_docids.remap_key_type::<FacetGroupKeyCodec<ByteSliceRefCodec>>();
    let string_db =
        index.facet_id_string_docids.remap_key_type::<FacetGroupKeyCodec<ByteSliceRefCodec>>();

    let (number_iter, string_iter) = if is_ascending {
        let number_iter = ascending_facet_sort(rtxn, number_db, field_id, candidates.clone())?;
        let string_iter = ascending_facet_sort(rtxn, string_db, field_id, candidates)?;

        (itertools::Either::Left(number_iter), itertools::Either::Left(string_iter))
    } else {
        let number_iter = descending_facet_sort(rtxn, number_db, field_id, candidates.clone())?;
        let string_iter = descending_facet_sort(rtxn, string_db, field_id, candidates)?;

        (itertools::Either::Right(number_iter), itertools::Either::Right(string_iter))
    };

    Ok(Box::new(number_iter.chain(string_iter).map(|res| res.map(|(doc_ids, _)| doc_ids))))
}

/// Returns an iterator over groups of the given candidates in ascending or descending order.
///
/// It will either use an iterative or a recursive method on the whole facet database depending
/// on the number of candidates to rank.
fn facet_ordered<'t>(
    index: &'t Index,
    rtxn: &'t heed::RoTxn,
    field_id: FieldId,
    is_ascending: bool,
    candidates: RoaringBitmap,
    implementation_strategy: CriterionImplementationStrategy,
) -> Result<Box<dyn Iterator<Item = heed::Result<RoaringBitmap>> + 't>> {
    match implementation_strategy {
        CriterionImplementationStrategy::OnlyIterative => {
            facet_ordered_iterative(index, rtxn, field_id, is_ascending, candidates)
        }
        CriterionImplementationStrategy::OnlySetBased => {
            facet_ordered_set_based(index, rtxn, field_id, is_ascending, candidates)
        }
        CriterionImplementationStrategy::Dynamic => {
            if candidates.len() <= CANDIDATES_THRESHOLD {
                facet_ordered_iterative(index, rtxn, field_id, is_ascending, candidates)
            } else {
                facet_ordered_set_based(index, rtxn, field_id, is_ascending, candidates)
            }
        }
    }
}

/// Fetch the whole list of candidates facet number values one by one and order them by it.
///
/// This function is fast when the amount of candidates to rank is small.
fn iterative_facet_number_ordered_iter<'t>(
    index: &'t Index,
    rtxn: &'t heed::RoTxn,
    field_id: FieldId,
    is_ascending: bool,
    candidates: RoaringBitmap,
) -> Result<impl Iterator<Item = RoaringBitmap> + 't> {
    let mut docids_values = Vec::with_capacity(candidates.len() as usize);
    for docid in candidates.iter() {
        let left = (field_id, docid, f64::MIN);
        let right = (field_id, docid, f64::MAX);
        let mut iter = index.field_id_docid_facet_f64s.range(rtxn, &(left..=right))?;
        let entry = if is_ascending { iter.next() } else { iter.last() };
        if let Some(((_, _, value), ())) = entry.transpose()? {
            docids_values.push((docid, OrderedFloat(value)));
        }
    }
    docids_values.sort_unstable_by_key(|(_, v)| *v);
    let iter = docids_values.into_iter();
    let iter = if is_ascending {
        Box::new(iter) as Box<dyn Iterator<Item = _>>
    } else {
        Box::new(iter.rev())
    };

    // The itertools GroupBy iterator doesn't provide an owned version, we are therefore
    // required to collect the result into an owned collection (a Vec).
    // https://github.com/rust-itertools/itertools/issues/499
    #[allow(clippy::needless_collect)]
    let vec: Vec<_> = iter
        .group_by(|(_, v)| *v)
        .into_iter()
        .map(|(_, ids)| ids.map(|(id, _)| id).collect())
        .collect();

    Ok(vec.into_iter())
}

/// Fetch the whole list of candidates facet string values one by one and order them by it.
///
/// This function is fast when the amount of candidates to rank is small.
fn iterative_facet_string_ordered_iter<'t>(
    index: &'t Index,
    rtxn: &'t heed::RoTxn,
    field_id: FieldId,
    is_ascending: bool,
    candidates: RoaringBitmap,
) -> Result<impl Iterator<Item = RoaringBitmap> + 't> {
    let mut docids_values = Vec::with_capacity(candidates.len() as usize);
    for docid in candidates.iter() {
        let left = (field_id, docid, "");
        let right = (field_id, docid.saturating_add(1), "");
        // FIXME Doing this means that it will never be possible to retrieve
        //       the document with id 2^32, not sure this is a real problem.
        let mut iter = index.field_id_docid_facet_strings.range(rtxn, &(left..right))?;
        let entry = if is_ascending { iter.next() } else { iter.last() };
        if let Some(((_, _, value), _)) = entry.transpose()? {
            docids_values.push((docid, value));
        }
    }
    docids_values.sort_unstable_by_key(|(_, v)| *v);
    let iter = docids_values.into_iter();
    let iter = if is_ascending {
        Box::new(iter) as Box<dyn Iterator<Item = _>>
    } else {
        Box::new(iter.rev())
    };

    // The itertools GroupBy iterator doesn't provide an owned version, we are therefore
    // required to collect the result into an owned collection (a Vec).
    // https://github.com/rust-itertools/itertools/issues/499
    #[allow(clippy::needless_collect)]
    let vec: Vec<_> = iter
        .group_by(|(_, v)| *v)
        .into_iter()
        .map(|(_, ids)| ids.map(|(id, _)| id).collect())
        .collect();

    Ok(vec.into_iter())
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use big_s::S;
    use maplit::hashset;

    use crate::index::tests::TempIndex;
    use crate::{AscDesc, Criterion, Filter, Search, SearchResult};

    // Note that in this test, only the iterative sort algorithms are used. Set the CANDIDATES_THESHOLD
    // constant to 0 to ensure that the other sort algorithms are also correct.
    #[test]
    fn sort_criterion_placeholder() {
        let index = TempIndex::new();

        index
            .update_settings(|settings| {
                settings.set_primary_key("id".to_owned());
                settings
                    .set_sortable_fields(maplit::hashset! { S("id"), S("mod_10"), S("mod_20") });
                settings.set_criteria(vec![Criterion::Sort]);
            })
            .unwrap();

        let mut docs = vec![];
        for i in 0..100 {
            docs.push(
                serde_json::json!({ "id": i, "mod_10": format!("{}", i % 10), "mod_20": i % 20 }),
            );
        }

        index.add_documents(documents!(docs)).unwrap();

        let all_ids = (0..100).collect::<Vec<_>>();

        let rtxn = index.read_txn().unwrap();

        let mut search = Search::new(&rtxn, &index);
        search.sort_criteria(vec![AscDesc::from_str("mod_10:desc").unwrap()]);
        search.limit(100);

        let SearchResult { mut documents_ids, .. } = search.execute().unwrap();
        insta::assert_snapshot!(format!("{documents_ids:?}"), @"[9, 19, 29, 39, 49, 59, 69, 79, 89, 99, 8, 18, 28, 38, 48, 58, 68, 78, 88, 98, 7, 17, 27, 37, 47, 57, 67, 77, 87, 97, 6, 16, 26, 36, 46, 56, 66, 76, 86, 96, 5, 15, 25, 35, 45, 55, 65, 75, 85, 95, 4, 14, 24, 34, 44, 54, 64, 74, 84, 94, 3, 13, 23, 33, 43, 53, 63, 73, 83, 93, 2, 12, 22, 32, 42, 52, 62, 72, 82, 92, 1, 11, 21, 31, 41, 51, 61, 71, 81, 91, 0, 10, 20, 30, 40, 50, 60, 70, 80, 90]");
        documents_ids.sort();
        assert_eq!(all_ids, documents_ids);

        let mut search = Search::new(&rtxn, &index);
        search.sort_criteria(vec![
            AscDesc::from_str("mod_10:desc").unwrap(),
            AscDesc::from_str("id:desc").unwrap(),
        ]);
        search.limit(100);

        let SearchResult { mut documents_ids, .. } = search.execute().unwrap();
        insta::assert_snapshot!(format!("{documents_ids:?}"), @"[99, 89, 79, 69, 59, 49, 39, 29, 19, 9, 98, 88, 78, 68, 58, 48, 38, 28, 18, 8, 97, 87, 77, 67, 57, 47, 37, 27, 17, 7, 96, 86, 76, 66, 56, 46, 36, 26, 16, 6, 95, 85, 75, 65, 55, 45, 35, 25, 15, 5, 94, 84, 74, 64, 54, 44, 34, 24, 14, 4, 93, 83, 73, 63, 53, 43, 33, 23, 13, 3, 92, 82, 72, 62, 52, 42, 32, 22, 12, 2, 91, 81, 71, 61, 51, 41, 31, 21, 11, 1, 90, 80, 70, 60, 50, 40, 30, 20, 10, 0]");
        documents_ids.sort();
        assert_eq!(all_ids, documents_ids);

        let mut search = Search::new(&rtxn, &index);
        search.sort_criteria(vec![
            AscDesc::from_str("mod_10:desc").unwrap(),
            AscDesc::from_str("mod_20:asc").unwrap(),
        ]);
        search.limit(100);

        let SearchResult { mut documents_ids, .. } = search.execute().unwrap();
        insta::assert_snapshot!(format!("{documents_ids:?}"), @"[9, 29, 49, 69, 89, 19, 39, 59, 79, 99, 8, 28, 48, 68, 88, 18, 38, 58, 78, 98, 7, 27, 47, 67, 87, 17, 37, 57, 77, 97, 6, 26, 46, 66, 86, 16, 36, 56, 76, 96, 5, 25, 45, 65, 85, 15, 35, 55, 75, 95, 4, 24, 44, 64, 84, 14, 34, 54, 74, 94, 3, 23, 43, 63, 83, 13, 33, 53, 73, 93, 2, 22, 42, 62, 82, 12, 32, 52, 72, 92, 1, 21, 41, 61, 81, 11, 31, 51, 71, 91, 0, 20, 40, 60, 80, 10, 30, 50, 70, 90]");
        documents_ids.sort();
        assert_eq!(all_ids, documents_ids);

        let mut search = Search::new(&rtxn, &index);
        search.sort_criteria(vec![
            AscDesc::from_str("mod_10:desc").unwrap(),
            AscDesc::from_str("mod_20:desc").unwrap(),
        ]);
        search.limit(100);

        let SearchResult { mut documents_ids, .. } = search.execute().unwrap();
        insta::assert_snapshot!(format!("{documents_ids:?}"), @"[19, 39, 59, 79, 99, 9, 29, 49, 69, 89, 18, 38, 58, 78, 98, 8, 28, 48, 68, 88, 17, 37, 57, 77, 97, 7, 27, 47, 67, 87, 16, 36, 56, 76, 96, 6, 26, 46, 66, 86, 15, 35, 55, 75, 95, 5, 25, 45, 65, 85, 14, 34, 54, 74, 94, 4, 24, 44, 64, 84, 13, 33, 53, 73, 93, 3, 23, 43, 63, 83, 12, 32, 52, 72, 92, 2, 22, 42, 62, 82, 11, 31, 51, 71, 91, 1, 21, 41, 61, 81, 10, 30, 50, 70, 90, 0, 20, 40, 60, 80]");
        documents_ids.sort();
        assert_eq!(all_ids, documents_ids);

        let mut search = Search::new(&rtxn, &index);
        search.sort_criteria(vec![
            AscDesc::from_str("mod_10:desc").unwrap(),
            AscDesc::from_str("mod_20:desc").unwrap(),
            AscDesc::from_str("id:desc").unwrap(),
        ]);
        search.limit(100);

        let SearchResult { mut documents_ids, .. } = search.execute().unwrap();
        insta::assert_snapshot!(format!("{documents_ids:?}"), @"[99, 79, 59, 39, 19, 89, 69, 49, 29, 9, 98, 78, 58, 38, 18, 88, 68, 48, 28, 8, 97, 77, 57, 37, 17, 87, 67, 47, 27, 7, 96, 76, 56, 36, 16, 86, 66, 46, 26, 6, 95, 75, 55, 35, 15, 85, 65, 45, 25, 5, 94, 74, 54, 34, 14, 84, 64, 44, 24, 4, 93, 73, 53, 33, 13, 83, 63, 43, 23, 3, 92, 72, 52, 32, 12, 82, 62, 42, 22, 2, 91, 71, 51, 31, 11, 81, 61, 41, 21, 1, 90, 70, 50, 30, 10, 80, 60, 40, 20, 0]");
        documents_ids.sort();
        assert_eq!(all_ids, documents_ids);
    }

    // Note that in this test, only the iterative sort algorithms are used. Set the CANDIDATES_THESHOLD
    // constant to 0 to ensure that the other sort algorithms are also correct.
    #[test]
    fn sort_criterion_non_placeholder() {
        let index = TempIndex::new();

        index
            .update_settings(|settings| {
                settings.set_primary_key("id".to_owned());
                settings.set_filterable_fields(hashset! { S("id"), S("mod_10"), S("mod_20") });
                settings.set_sortable_fields(hashset! { S("id"), S("mod_10"), S("mod_20") });
                settings.set_criteria(vec![Criterion::Sort]);
            })
            .unwrap();

        let mut docs = vec![];
        for i in 0..100 {
            docs.push(
                serde_json::json!({ "id": i, "mod_10": format!("{}", i % 10), "mod_20": i % 20 }),
            );
        }

        index.add_documents(documents!(docs)).unwrap();

        let rtxn = index.read_txn().unwrap();

        let mut search = Search::new(&rtxn, &index);
        search.filter(
            Filter::from_str("mod_10 IN [1, 0, 2] OR mod_20 IN [10, 13] OR id IN [5, 6]")
                .unwrap()
                .unwrap(),
        );
        search.sort_criteria(vec![
            AscDesc::from_str("mod_10:desc").unwrap(),
            AscDesc::from_str("mod_20:asc").unwrap(),
            AscDesc::from_str("id:desc").unwrap(),
        ]);
        search.limit(100);

        let SearchResult { mut documents_ids, .. } = search.execute().unwrap();
        // The order should be in increasing value of the id modulo 10, followed by increasing value of the id modulo 20, followed by decreasing value of the id
        insta::assert_snapshot!(format!("{documents_ids:?}"), @"[6, 5, 93, 73, 53, 33, 13, 82, 62, 42, 22, 2, 92, 72, 52, 32, 12, 81, 61, 41, 21, 1, 91, 71, 51, 31, 11, 80, 60, 40, 20, 0, 90, 70, 50, 30, 10]");
        let expected_ids = (0..100)
            .filter(|id| {
                [1, 0, 2].contains(&(id % 10))
                    || [10, 13].contains(&(id % 20))
                    || [5, 6].contains(id)
            })
            .collect::<Vec<_>>();
        documents_ids.sort();
        assert_eq!(expected_ids, documents_ids);

        let mut search = Search::new(&rtxn, &index);
        search.filter(
            Filter::from_str("mod_10 IN [7, 8, 0] OR mod_20 IN [1, 15, 16] OR id IN [0, 4]")
                .unwrap()
                .unwrap(),
        );
        search.sort_criteria(vec![
            AscDesc::from_str("mod_10:asc").unwrap(),
            AscDesc::from_str("mod_20:asc").unwrap(),
            AscDesc::from_str("id:desc").unwrap(),
        ]);
        search.limit(100);

        let SearchResult { mut documents_ids, .. } = search.execute().unwrap();
        // The order should be in increasing value of the id modulo 10, followed by increasing value of the id modulo 20, followed by decreasing value of the id
        insta::assert_snapshot!(format!("{documents_ids:?}"), @"[80, 60, 40, 20, 0, 90, 70, 50, 30, 10, 81, 61, 41, 21, 1, 4, 95, 75, 55, 35, 15, 96, 76, 56, 36, 16, 87, 67, 47, 27, 7, 97, 77, 57, 37, 17, 88, 68, 48, 28, 8, 98, 78, 58, 38, 18]");
        let expected_ids = (0..100)
            .filter(|id| {
                [7, 8, 0].contains(&(id % 10))
                    || [1, 15, 16].contains(&(id % 20))
                    || [0, 4].contains(id)
            })
            .collect::<Vec<_>>();
        documents_ids.sort();
        assert_eq!(expected_ids, documents_ids);

        let mut search = Search::new(&rtxn, &index);
        search.filter(
            Filter::from_str("mod_10 IN [1, 0, 2] OR mod_20 IN [10, 13] OR id IN [5, 6]")
                .unwrap()
                .unwrap(),
        );
        search.sort_criteria(vec![AscDesc::from_str("id:desc").unwrap()]);
        search.limit(100);

        let SearchResult { documents_ids, .. } = search.execute().unwrap();
        // The order should be in decreasing value of the id
        let mut expected_ids = (0..100)
            .filter(|id| {
                [1, 0, 2].contains(&(id % 10))
                    || [10, 13].contains(&(id % 20))
                    || [5, 6].contains(id)
            })
            .collect::<Vec<_>>();
        expected_ids.sort();
        expected_ids.reverse();
        assert_eq!(expected_ids, documents_ids);
    }
}
