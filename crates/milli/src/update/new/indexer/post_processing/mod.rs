use std::cmp::Ordering;

use facet_bulk::generate_facet_levels;
use heed::types::{Bytes, DecodeIgnore, Str};
use heed::RwTxn;
use itertools::{merge_join_by, EitherOrBoth};

use super::document_changes::IndexingContext;
use crate::facet::FacetType;
use crate::index::main_key::{WORDS_FST_KEY, WORDS_PREFIXES_FST_KEY};
use crate::progress::Progress;
use crate::update::del_add::DelAdd;
use crate::update::facet::new_incremental::FacetsUpdateIncremental;
use crate::update::facet::{FACET_GROUP_SIZE, FACET_MAX_GROUP_SIZE, FACET_MIN_LEVEL_SIZE};
use crate::update::new::facet_search_builder::FacetSearchBuilder;
use crate::update::new::merger::FacetFieldIdDelta;
use crate::update::new::steps::{IndexingStep, PostProcessingFacets, PostProcessingWords};
use crate::update::new::word_fst_builder::{PrefixData, PrefixDelta, WordFstBuilder};
use crate::update::new::words_prefix_docids::{
    compute_exact_word_prefix_docids, compute_word_prefix_docids, compute_word_prefix_fid_docids,
    compute_word_prefix_position_docids,
};
use crate::update::new::FacetFieldIdsDelta;
use crate::update::{FacetsUpdateBulk, GrenadParameters};
use crate::{GlobalFieldsIdsMap, Index, Result};

mod facet_bulk;

pub(super) fn post_process<MSP>(
    indexing_context: IndexingContext<MSP>,
    wtxn: &mut RwTxn<'_>,
    mut global_fields_ids_map: GlobalFieldsIdsMap<'_>,
    facet_field_ids_delta: FacetFieldIdsDelta,
) -> Result<()>
where
    MSP: Fn() -> bool + Sync,
{
    let index = indexing_context.index;
    indexing_context.progress.update_progress(IndexingStep::PostProcessingFacets);
    compute_facet_level_database(
        index,
        wtxn,
        facet_field_ids_delta,
        &mut global_fields_ids_map,
        indexing_context.grenad_parameters,
        indexing_context.progress,
    )?;
    compute_facet_search_database(index, wtxn, global_fields_ids_map, indexing_context.progress)?;
    indexing_context.progress.update_progress(IndexingStep::PostProcessingWords);
    if let Some(prefix_delta) = compute_word_fst(index, wtxn, indexing_context.progress)? {
        compute_prefix_database(
            index,
            wtxn,
            prefix_delta,
            indexing_context.grenad_parameters,
            indexing_context.progress,
        )?;
    };
    Ok(())
}

#[tracing::instrument(level = "trace", skip_all, target = "indexing::prefix")]
fn compute_prefix_database(
    index: &Index,
    wtxn: &mut RwTxn,
    prefix_delta: PrefixDelta,
    grenad_parameters: &GrenadParameters,
    progress: &Progress,
) -> Result<()> {
    let PrefixDelta { modified, deleted } = prefix_delta;

    progress.update_progress(PostProcessingWords::WordPrefixDocids);
    compute_word_prefix_docids(wtxn, index, &modified, &deleted, grenad_parameters)?;

    progress.update_progress(PostProcessingWords::ExactWordPrefixDocids);
    compute_exact_word_prefix_docids(wtxn, index, &modified, &deleted, grenad_parameters)?;

    progress.update_progress(PostProcessingWords::WordPrefixFieldIdDocids);
    compute_word_prefix_fid_docids(wtxn, index, &modified, &deleted, grenad_parameters)?;

    progress.update_progress(PostProcessingWords::WordPrefixPositionDocids);
    compute_word_prefix_position_docids(wtxn, index, &modified, &deleted, grenad_parameters)
}

#[tracing::instrument(level = "trace", skip_all, target = "indexing")]
fn compute_word_fst(
    index: &Index,
    wtxn: &mut RwTxn,
    progress: &Progress,
) -> Result<Option<PrefixDelta>> {
    let rtxn = index.read_txn()?;
    progress.update_progress(PostProcessingWords::WordFst);

    let words_fst = index.words_fst(&rtxn)?;
    let mut word_fst_builder = WordFstBuilder::new(&words_fst)?;
    let prefix_settings = index.prefix_settings(&rtxn)?;
    word_fst_builder.with_prefix_settings(prefix_settings);

    let previous_words = index.word_docids.iter(&rtxn)?.remap_data_type::<Bytes>();
    let current_words = index.word_docids.iter(wtxn)?.remap_data_type::<Bytes>();
    for eob in merge_join_by(previous_words, current_words, |lhs, rhs| match (lhs, rhs) {
        (Ok((l, _)), Ok((r, _))) => l.cmp(r),
        (Err(_), _) | (_, Err(_)) => Ordering::Equal,
    }) {
        match eob {
            EitherOrBoth::Both(lhs, rhs) => {
                let (word, lhs_bytes) = lhs?;
                let (_, rhs_bytes) = rhs?;
                if lhs_bytes != rhs_bytes {
                    word_fst_builder.register_word(DelAdd::Addition, word.as_ref())?;
                }
            }
            EitherOrBoth::Left(result) => {
                let (word, _) = result?;
                word_fst_builder.register_word(DelAdd::Deletion, word.as_ref())?;
            }
            EitherOrBoth::Right(result) => {
                let (word, _) = result?;
                word_fst_builder.register_word(DelAdd::Addition, word.as_ref())?;
            }
        }
    }

    let (word_fst_mmap, prefix_data) = word_fst_builder.build(index, &rtxn)?;
    index.main.remap_types::<Str, Bytes>().put(wtxn, WORDS_FST_KEY, &word_fst_mmap)?;
    if let Some(PrefixData { prefixes_fst_mmap, prefix_delta }) = prefix_data {
        index.main.remap_types::<Str, Bytes>().put(
            wtxn,
            WORDS_PREFIXES_FST_KEY,
            &prefixes_fst_mmap,
        )?;
        Ok(Some(prefix_delta))
    } else {
        Ok(None)
    }
}

pub fn recompute_word_fst_from_word_docids_database(
    index: &Index,
    wtxn: &mut RwTxn,
    progress: &Progress,
) -> Result<()> {
    progress.update_progress(PostProcessingWords::WordFst);
    let fst = fst::Set::default().map_data(std::borrow::Cow::Owned)?;
    let mut word_fst_builder = WordFstBuilder::new(&fst)?;
    let words = index.word_docids.iter(wtxn)?.remap_data_type::<DecodeIgnore>();
    for res in words {
        let (word, _) = res?;
        word_fst_builder.register_word(DelAdd::Addition, word.as_ref())?;
    }
    let (word_fst_mmap, _) = word_fst_builder.build(index, wtxn)?;
    index.main.remap_types::<Str, Bytes>().put(wtxn, WORDS_FST_KEY, &word_fst_mmap)?;

    Ok(())
}

#[tracing::instrument(level = "trace", skip_all, target = "indexing::facet_search")]
fn compute_facet_search_database(
    index: &Index,
    wtxn: &mut RwTxn,
    global_fields_ids_map: GlobalFieldsIdsMap,
    progress: &Progress,
) -> Result<()> {
    let rtxn = index.read_txn()?;
    progress.update_progress(PostProcessingFacets::FacetSearch);

    // if the facet search is not enabled, we can skip the rest of the function
    if !index.facet_search(wtxn)? {
        return Ok(());
    }

    let localized_attributes_rules = index.localized_attributes_rules(&rtxn)?;
    let filterable_attributes_rules = index.filterable_attributes_rules(&rtxn)?;
    let mut facet_search_builder = FacetSearchBuilder::new(
        global_fields_ids_map,
        localized_attributes_rules.unwrap_or_default(),
        filterable_attributes_rules,
    );

    let previous_facet_id_string_docids = index
        .facet_id_string_docids
        .iter(&rtxn)?
        .remap_data_type::<DecodeIgnore>()
        .filter(|r| r.as_ref().map_or(true, |(k, _)| k.level == 0));
    let current_facet_id_string_docids = index
        .facet_id_string_docids
        .iter(wtxn)?
        .remap_data_type::<DecodeIgnore>()
        .filter(|r| r.as_ref().map_or(true, |(k, _)| k.level == 0));
    for eob in merge_join_by(
        previous_facet_id_string_docids,
        current_facet_id_string_docids,
        |lhs, rhs| match (lhs, rhs) {
            (Ok((l, _)), Ok((r, _))) => l.cmp(r),
            (Err(_), _) | (_, Err(_)) => Ordering::Equal,
        },
    ) {
        match eob {
            EitherOrBoth::Both(lhs, rhs) => {
                let (_, _) = lhs?;
                let (_, _) = rhs?;
            }
            EitherOrBoth::Left(result) => {
                let (key, _) = result?;
                facet_search_builder.register_from_key(DelAdd::Deletion, key)?;
            }
            EitherOrBoth::Right(result) => {
                let (key, _) = result?;
                facet_search_builder.register_from_key(DelAdd::Addition, key)?;
            }
        }
    }

    facet_search_builder.merge_and_write(index, wtxn, &rtxn)
}

#[tracing::instrument(level = "trace", skip_all, target = "indexing::facet_field_ids")]
fn compute_facet_level_database(
    index: &Index,
    wtxn: &mut RwTxn,
    mut facet_field_ids_delta: FacetFieldIdsDelta,
    global_fields_ids_map: &mut GlobalFieldsIdsMap,
    grenad_parameters: &GrenadParameters,
    progress: &Progress,
) -> Result<()> {
    let rtxn = index.read_txn()?;

    let filterable_attributes_rules = index.filterable_attributes_rules(&rtxn)?;
    let mut deltas: Vec<_> = facet_field_ids_delta.consume_facet_string_delta().collect();
    // We move all bulks at the front and incrementals (others) at the end.
    deltas.sort_by_key(|(_, delta)| if let FacetFieldIdDelta::Bulk = delta { 0 } else { 1 });

    for (fid, delta) in deltas {
        // skip field ids that should not be facet leveled
        let Some(metadata) = global_fields_ids_map.metadata(fid) else {
            continue;
        };
        if !metadata.require_facet_level_database(&filterable_attributes_rules) {
            continue;
        }

        let span = tracing::trace_span!(target: "indexing::facet_field_ids", "string");
        let _entered = span.enter();
        match delta {
            FacetFieldIdDelta::Bulk => {
                progress.update_progress(PostProcessingFacets::StringsBulk);
                if grenad_parameters.experimental_no_edition_2024_for_facet_post_processing {
                    tracing::debug!(%fid, "bulk string facet processing");
                    FacetsUpdateBulk::new_not_updating_level_0(index, vec![fid], FacetType::String)
                        .execute(wtxn)?
                } else {
                    tracing::debug!(%fid, "bulk string facet processing in parallel");
                    generate_facet_levels(index, wtxn, fid, FacetType::String)?
                }
            }
            FacetFieldIdDelta::Incremental(delta_data) => {
                progress.update_progress(PostProcessingFacets::StringsIncremental);
                tracing::debug!(%fid, len=%delta_data.len(), "incremental string facet processing");
                FacetsUpdateIncremental::new(
                    index,
                    FacetType::String,
                    fid,
                    delta_data,
                    FACET_GROUP_SIZE,
                    FACET_MIN_LEVEL_SIZE,
                    FACET_MAX_GROUP_SIZE,
                )
                .execute(wtxn)?
            }
        }
    }

    let mut deltas: Vec<_> = facet_field_ids_delta.consume_facet_number_delta().collect();
    // We move all bulks at the front and incrementals (others) at the end.
    deltas.sort_by_key(|(_, delta)| if let FacetFieldIdDelta::Bulk = delta { 0 } else { 1 });

    for (fid, delta) in deltas {
        let span = tracing::trace_span!(target: "indexing::facet_field_ids", "number");
        let _entered = span.enter();
        match delta {
            FacetFieldIdDelta::Bulk => {
                progress.update_progress(PostProcessingFacets::NumbersBulk);
                tracing::debug!(%fid, "bulk number facet processing");
                FacetsUpdateBulk::new_not_updating_level_0(index, vec![fid], FacetType::Number)
                    .execute(wtxn)?
            }
            FacetFieldIdDelta::Incremental(delta_data) => {
                progress.update_progress(PostProcessingFacets::NumbersIncremental);
                tracing::debug!(%fid, len=%delta_data.len(), "incremental number facet processing");
                FacetsUpdateIncremental::new(
                    index,
                    FacetType::Number,
                    fid,
                    delta_data,
                    FACET_GROUP_SIZE,
                    FACET_MIN_LEVEL_SIZE,
                    FACET_MAX_GROUP_SIZE,
                )
                .execute(wtxn)?
            }
        }
        debug_assert!(crate::update::facet::sanity_checks(
            index,
            wtxn,
            fid,
            FacetType::Number,
            FACET_GROUP_SIZE as usize,
            FACET_MIN_LEVEL_SIZE as usize,
            FACET_MAX_GROUP_SIZE as usize,
        )
        .is_ok());
    }

    Ok(())
}
