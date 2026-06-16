use std::cmp::Ordering;
use std::collections::BTreeSet;
use std::iter;

use either::Either;
use facet_bulk::generate_facet_levels;
use fst::Streamer;
use heed::types::{Bytes, DecodeIgnore, Str, Unit};
use heed::{RoTxn, RwTxn};
use itertools::{merge_join_by, EitherOrBoth};

use super::document_changes::IndexingContext;
use crate::facet::FacetType;
use crate::heed_codec::facet::{FacetGroupKey, FacetGroupKeyCodec};
use crate::heed_codec::StrRefCodec;
use crate::index::main_key::{WORDS_FST_KEY, WORDS_PREFIXES_FST_KEY};
use crate::progress::Progress;
use crate::update::del_add::DelAdd;
use crate::update::facet::new_incremental::FacetsUpdateIncremental;
use crate::update::facet::{FACET_GROUP_SIZE, FACET_MAX_GROUP_SIZE, FACET_MIN_LEVEL_SIZE};
use crate::update::new::facet_search_builder::FacetSearchBuilder;
use crate::update::new::indexer::{MiniString, WordDelta};
use crate::update::new::merger::FacetFieldIdDelta;
use crate::update::new::steps::{IndexingStep, PostProcessingFacets, PostProcessingWords};
use crate::update::new::word_fst_builder::{PrefixData, WordFstBuilder};
use crate::update::new::words_prefix_docids::{
    compute_exact_word_prefix_docids, compute_word_prefix_docids, compute_word_prefix_fid_docids,
    compute_word_prefix_position_docids,
};
use crate::update::new::FacetFieldIdsDelta;
use crate::update::FacetsUpdateBulk;
use crate::{FieldId, GlobalFieldsIdsMap, Index, Result};

mod facet_bulk;

#[tracing::instrument(level = "trace", skip_all, target = "indexing::post_processing")]
pub(super) fn post_process(
    indexing_context: IndexingContext,
    wtxn: &mut RwTxn<'_>,
    mut global_fields_ids_map: GlobalFieldsIdsMap<'_>,
    word_delta: &WordDelta,
    exact_word_delta: &WordDelta,
    facet_field_ids_delta: FacetFieldIdsDelta,
) -> Result<()> {
    let index = indexing_context.index;
    indexing_context.progress.update_progress(IndexingStep::PostProcessingFacets);
    compute_facet_level_database(
        index,
        wtxn,
        facet_field_ids_delta,
        &mut global_fields_ids_map,
        indexing_context.progress,
    )?;
    compute_facet_search_database(index, wtxn, global_fields_ids_map, indexing_context.progress)?;
    indexing_context.progress.update_progress(IndexingStep::PostProcessingWords);
    if let Some(prefix_data) =
        compute_word_fst(index, wtxn, word_delta, exact_word_delta, indexing_context.progress)?
    {
        compute_prefix_database(
            index,
            wtxn,
            word_delta,
            exact_word_delta,
            &prefix_data,
            indexing_context.progress,
        )?;
    }

    Ok(())
}

#[tracing::instrument(
    level = "trace",
    skip_all,
    target = "indexing::post_processing",
    name = "prefix"
)]
fn compute_prefix_database(
    index: &Index,
    wtxn: &mut RwTxn,
    word_delta: &WordDelta,
    exact_word_delta: &WordDelta,
    prefix_data: &PrefixData,
    progress: &Progress,
) -> Result<()> {
    progress.update_progress(PostProcessingWords::ComputePrefixes);
    let prefix_fst = fst::Set::new(&prefix_data.prefixes_fst_mmap[..])?;

    let tolerant_modified = compute_prefixes(&prefix_fst, word_delta.added_or_modified_words())?;
    let tolerant_deleted = compute_prefixes(&prefix_fst, word_delta.deleted_words())?;

    let exact_modified = compute_prefixes(&prefix_fst, exact_word_delta.added_or_modified_words())?;
    let exact_deleted = compute_prefixes(&prefix_fst, exact_word_delta.deleted_words())?;

    compute_prefix_database_from_sources(
        index,
        wtxn,
        &tolerant_modified,
        &tolerant_deleted,
        &exact_modified,
        &exact_deleted,
        progress,
    )
}

#[tracing::instrument(
    level = "trace",
    skip_all,
    target = "indexing::post_processing",
    name = "prefix_from_sources"
)]
pub(crate) fn compute_prefix_database_from_sources(
    index: &Index,
    wtxn: &mut RwTxn,
    tolerant_modified: &BTreeSet<MiniString>,
    tolerant_deleted: &BTreeSet<MiniString>,
    exact_modified: &BTreeSet<MiniString>,
    exact_deleted: &BTreeSet<MiniString>,
    progress: &Progress,
) -> Result<()> {
    let mut modified_for_fid_prefixes = tolerant_modified.clone();
    modified_for_fid_prefixes.extend(exact_modified.iter().cloned());
    let mut deleted_for_fid_prefixes = tolerant_deleted.clone();
    deleted_for_fid_prefixes.extend(exact_deleted.iter().cloned());

    progress.update_progress(PostProcessingWords::WordPrefixDocids);
    compute_word_prefix_docids(wtxn, index, tolerant_modified, tolerant_deleted)?;

    progress.update_progress(PostProcessingWords::ExactWordPrefixDocids);
    compute_exact_word_prefix_docids(wtxn, index, exact_modified, exact_deleted)?;

    progress.update_progress(PostProcessingWords::WordPrefixFieldIdDocids);
    compute_word_prefix_fid_docids(
        wtxn,
        index,
        &modified_for_fid_prefixes,
        &deleted_for_fid_prefixes,
    )?;

    progress.update_progress(PostProcessingWords::WordPrefixPositionDocids);
    compute_word_prefix_position_docids(
        wtxn,
        index,
        &modified_for_fid_prefixes,
        &deleted_for_fid_prefixes,
    )?;

    Ok(())
}

/// Recompute the exact-word prefix databases from the exact-word docids database.
pub fn recompute_exact_word_prefix_docids_from_database(
    index: &Index,
    wtxn: &mut RwTxn,
    progress: &Progress,
) -> Result<()> {
    let prefix_settings = index.prefix_settings(wtxn)?;
    if prefix_settings.compute_prefixes != crate::index::PrefixSearch::IndexingTime {
        return Ok(());
    }

    progress.update_progress(PostProcessingWords::ComputePrefixes);

    let exact_modified = exact_prefixes_from_database(index, wtxn)?;
    let exact_deleted = BTreeSet::new();

    index.exact_word_prefix_docids.clear(wtxn)?;

    compute_prefix_database_from_sources(
        index,
        wtxn,
        &BTreeSet::new(),
        &BTreeSet::new(),
        &exact_modified,
        &exact_deleted,
        progress,
    )
}

/// Collect all prefixes that meet the threshold from the exact-word docids database.
pub(crate) fn exact_prefixes_from_database(
    index: &Index,
    wtxn: &RwTxn,
) -> Result<BTreeSet<MiniString>> {
    let prefix_settings = index.prefix_settings(wtxn)?;
    let mut words = Vec::new();
    for result in index.exact_word_docids.iter(wtxn)?.remap_data_type::<DecodeIgnore>() {
        let (word, _) = result?;
        words.push(word.to_string());
    }

    prefixes_from_words(
        words.iter().map(|word| word.as_str()),
        prefix_settings.prefix_count_threshold,
        prefix_settings.max_prefix_length,
    )
}

/// Collect all prefixes that meet the threshold from a sorted list of words.
fn prefixes_from_words<'a, I>(
    words: I,
    prefix_count_threshold: usize,
    max_prefix_length: usize,
) -> Result<BTreeSet<MiniString>>
where
    I: IntoIterator<Item = &'a str>,
{
    let mut current_prefix = vec![String::new(); max_prefix_length];
    let mut current_prefix_count = vec![0; max_prefix_length];
    let mut builders: Vec<BTreeSet<String>> =
        (0..max_prefix_length).map(|_| BTreeSet::new()).collect();

    for word in words {
        for n in 0..max_prefix_length {
            let current_prefix = &mut current_prefix[n];
            let current_prefix_count = &mut current_prefix_count[n];
            let builder = &mut builders[n];

            let prefix = match word.get(..=n) {
                Some(prefix) => prefix,
                None => continue,
            };

            if *current_prefix_count == 0 || prefix != current_prefix.as_str() {
                *current_prefix = prefix.to_owned();
                *current_prefix_count = 0;
            }

            *current_prefix_count += 1;

            if *current_prefix_count == prefix_count_threshold {
                builder.insert(prefix.to_owned());
            }
        }
    }

    let mut output = BTreeSet::new();
    for prefix in builders.into_iter().flatten() {
        if let Some(prefix) = MiniString::new(&prefix) {
            output.insert(prefix);
        }
    }

    Ok(output)
}

/// The words must be sorted.
fn compute_prefixes<'a, I>(prefix_fst: &fst::Set<&[u8]>, words: I) -> Result<BTreeSet<MiniString>>
where
    I: IntoIterator<Item = &'a str>,
{
    let mut iter = words.into_iter();
    let mut prefix_stream = prefix_fst.stream();
    let mut current_prefix = match prefix_stream.next() {
        Some(current) => current,
        None => return Ok(BTreeSet::new()),
    };
    let mut current_word = match iter.next() {
        Some(current) => current,
        None => return Ok(BTreeSet::new()),
    };

    let mut output = BTreeSet::new();
    loop {
        // Current prefixes are only inserted once and each prefix is only inserted once.
        if current_word.as_bytes().starts_with(current_prefix) {
            let current_prefix = std::str::from_utf8(current_prefix)?;
            // safety: Prefixes are 3 bytes or less
            let current_prefix = MiniString::new(current_prefix).unwrap();
            output.insert(current_prefix);
        }

        if current_word.as_bytes() < current_prefix {
            current_word = match iter.next() {
                Some(current) => current,
                None => break,
            };
        } else {
            current_prefix = match prefix_stream.next() {
                Some(current) => current,
                None => break,
            };
        }
    }

    Ok(output)
}

#[tracing::instrument(level = "trace", skip_all, target = "indexing::post_processing")]
fn compute_word_fst(
    index: &Index,
    wtxn: &mut RwTxn,
    word_delta: &WordDelta,
    exact_word_delta: &WordDelta,
    progress: &Progress,
) -> Result<Option<PrefixData>> {
    progress.update_progress(PostProcessingWords::WordFst);

    let words_fst = index.words_fst(wtxn)?;
    let mut word_fst_builder = WordFstBuilder::new(&words_fst)?;
    let prefix_settings = index.prefix_settings(wtxn)?;
    word_fst_builder.with_prefix_settings(prefix_settings);

    // we ignore modifications when rebuilding the FST
    for either in word_delta.added_or_deleted_words() {
        match either {
            Either::Left(added_word) => {
                word_fst_builder.register_word(DelAdd::Addition, added_word.as_ref())?;
            }
            Either::Right(deleted_word) => {
                word_fst_builder.register_word(DelAdd::Deletion, deleted_word.as_ref())?;
            }
        }
    }

    for either in exact_word_delta.added_or_deleted_words() {
        match either {
            Either::Left(added_word) => {
                word_fst_builder
                    .register_exact_word_for_prefixes(DelAdd::Addition, added_word.as_ref())?;
            }
            Either::Right(deleted_word) => {
                word_fst_builder
                    .register_exact_word_for_prefixes(DelAdd::Deletion, deleted_word.as_ref())?;
            }
        }
    }

    let (word_fst_mmap, prefix_data) = word_fst_builder.build()?;
    index.main.remap_types::<Str, Bytes>().put(wtxn, WORDS_FST_KEY, &word_fst_mmap)?;

    if let Some(PrefixData { prefixes_fst_mmap }) = prefix_data {
        index.main.remap_types::<Str, Bytes>().put(
            wtxn,
            WORDS_PREFIXES_FST_KEY,
            &prefixes_fst_mmap,
        )?;
        Ok(Some(PrefixData { prefixes_fst_mmap }))
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
    let (word_fst_mmap, _) = word_fst_builder.build()?;
    index.main.remap_types::<Str, Bytes>().put(wtxn, WORDS_FST_KEY, &word_fst_mmap)?;

    Ok(())
}

#[tracing::instrument(
    level = "trace",
    skip_all,
    target = "indexing::post_processing",
    name = "facet_search"
)]
fn compute_facet_search_database(
    index: &Index,
    wtxn: &mut RwTxn,
    global_fields_ids_map: GlobalFieldsIdsMap,
    progress: &Progress,
) -> Result<()> {
    let rtxn = index.read_txn()?;
    progress.update_progress(PostProcessingFacets::FacetSearch);

    // if the facet search is not enabled, we can clear the
    // facet search data structures and skip the rest of the function
    if !index.facet_search(wtxn)? {
        index.facet_id_string_fst.clear(wtxn)?;
        index.facet_id_normalized_string_strings.clear(wtxn)?;
        return Ok(());
    }

    let localized_attributes_rules = index.localized_attributes_rules(wtxn)?;
    let filterable_attributes_rules = index.filterable_attributes_rules(wtxn)?;

    // Get the list of ranges corresponding to the facets that are marked as facet searchable.
    // This way we can avoid iterating over all the facet values.
    let mut facet_searchable_fids = BTreeSet::new();
    global_fields_ids_map.for_each_metadata(|field_id, _field_name, metadata| {
        if metadata
            .filterable_attributes_features(&filterable_attributes_rules)
            .is_facet_searchable()
        {
            facet_searchable_fids.insert(field_id);
        }
    });

    // We make sure that the list of filterable attributes is empty
    // if the facet search was disabled before this change.
    let empty_facet_searchable_fids;
    let old_facet_searchable_fids = if index.facet_search(&rtxn)? {
        &facet_searchable_fids
    } else {
        empty_facet_searchable_fids = BTreeSet::new();
        &empty_facet_searchable_fids
    };

    fn level_0_searchable_facets<'a>(
        txn: &'a RoTxn,
        index: &'a Index,
        facet_searchable_field_ids: &'a BTreeSet<FieldId>,
    ) -> impl Iterator<Item = Result<(FacetGroupKey<&'a str>, ()), heed::Error>> + 'a {
        facet_searchable_field_ids.iter().flat_map(|&field_id| {
            index
                .facet_id_string_docids
                .remap_types::<FacetGroupKeyCodec<Unit>, DecodeIgnore>()
                .prefix_iter(txn, &FacetGroupKey { field_id, level: 0, left_bound: () })
                .map_or_else(
                    |e| Either::Left(iter::once(Err(e))),
                    |it| Either::Right(it.remap_key_type::<FacetGroupKeyCodec<StrRefCodec>>()),
                )
        })
    }

    let previous_facet_id_string =
        level_0_searchable_facets(&rtxn, index, old_facet_searchable_fids);
    let current_facet_id_string = level_0_searchable_facets(wtxn, index, &facet_searchable_fids);

    let mut facet_search_builder = FacetSearchBuilder::new(
        global_fields_ids_map,
        localized_attributes_rules.unwrap_or_default(),
        filterable_attributes_rules,
    );

    for eob in merge_join_by(previous_facet_id_string, current_facet_id_string, |lhs, rhs| {
        match (lhs, rhs) {
            (Ok((l, _)), Ok((r, _))) => l.cmp(r),
            (Err(_), _) | (_, Err(_)) => Ordering::Equal,
        }
    }) {
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

#[tracing::instrument(
    level = "trace",
    skip_all,
    target = "indexing::post_processing",
    name = "facet_field_ids"
)]
fn compute_facet_level_database(
    index: &Index,
    wtxn: &mut RwTxn,
    mut facet_field_ids_delta: FacetFieldIdsDelta,
    global_fields_ids_map: &mut GlobalFieldsIdsMap,
    progress: &Progress,
) -> Result<()> {
    let filterable_attributes_rules = index.filterable_attributes_rules(wtxn)?;
    let mut deltas: Vec<_> = facet_field_ids_delta.consume_facet_string_delta().collect();
    // We move all bulks at the front and incrementals (others) at the end.
    deltas.sort_by_key(|(_, delta)| if let FacetFieldIdDelta::Bulk = delta { 0 } else { 1 });

    for (fid, delta) in deltas {
        // skip field ids that should not be facet leveled
        let Some(metadata) = global_fields_ids_map.metadata(fid) else {
            continue;
        };

        // Note in case of a settings change we will recompute the facet level database if the
        // user only enabled the facet search and the field is marked as comparable or sortable.
        if !metadata.require_facet_level_database(&filterable_attributes_rules) {
            continue;
        }

        let span =
            tracing::trace_span!(target: "indexing::post_processing::facet_field_ids", "string");
        let _entered = span.enter();
        match delta {
            FacetFieldIdDelta::Bulk => {
                progress.update_progress(PostProcessingFacets::StringsBulk);
                tracing::debug!(%fid, "bulk string facet processing in parallel");
                generate_facet_levels(index, wtxn, fid, FacetType::String)?
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
        let span =
            tracing::trace_span!(target: "indexing::post_processing::facet_field_ids", "number");
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
