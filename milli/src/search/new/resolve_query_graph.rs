#![allow(clippy::too_many_arguments)]

use std::collections::VecDeque;

use fxhash::FxHashMap;
use roaring::{MultiOps, RoaringBitmap};

use super::interner::Interned;
use super::query_graph::QueryNodeData;
use super::query_term::{Phrase, QueryTermSubset};
use super::small_bitmap::SmallBitmap;
use super::{QueryGraph, SearchContext, Word};
use crate::search::new::query_term::LocatedQueryTermSubset;
use crate::Result;

#[derive(Default)]
pub struct PhraseDocIdsCache {
    pub cache: FxHashMap<Interned<Phrase>, RoaringBitmap>,
}
impl<'ctx> SearchContext<'ctx> {
    /// Get the document ids associated with the given phrase
    pub fn get_phrase_docids(
        &mut self,
        universe: Option<&RoaringBitmap>,
        phrase: Interned<Phrase>,
    ) -> Result<&RoaringBitmap> {
        if self.phrase_docids.cache.contains_key(&phrase) {
            return Ok(&self.phrase_docids.cache[&phrase]);
        };
        let docids = compute_phrase_docids(self, universe, phrase)?;
        // TODO can we improve that? Because there is an issue, we keep that in cache...
        let _ = self.phrase_docids.cache.insert(phrase, docids);
        let docids = &self.phrase_docids.cache[&phrase];
        Ok(docids)
    }
}
pub fn compute_query_term_subset_docids(
    ctx: &mut SearchContext,
    universe: Option<&RoaringBitmap>,
    term: &QueryTermSubset,
) -> Result<RoaringBitmap> {
    let mut docids = RoaringBitmap::new();
    // TODO use the MultiOps trait to do large intersections
    for word in term.all_single_words_except_prefix_db(ctx)? {
        if let Some(word_docids) = ctx.word_docids(universe, word)? {
            docids |= word_docids;
        }
    }
    for phrase in term.all_phrases(ctx)? {
        docids |= ctx.get_phrase_docids(None, phrase)?;
    }

    if let Some(prefix) = term.use_prefix_db(ctx) {
        if let Some(prefix_docids) = ctx.word_prefix_docids(universe, prefix)? {
            docids |= prefix_docids;
        }
    }

    match universe {
        Some(universe) => Ok(docids & universe),
        None => Ok(docids),
    }
}

pub fn compute_query_term_subset_docids_within_field_id(
    ctx: &mut SearchContext,
    universe: Option<&RoaringBitmap>,
    term: &QueryTermSubset,
    fid: u16,
) -> Result<RoaringBitmap> {
    let mut docids = RoaringBitmap::new();
    for word in term.all_single_words_except_prefix_db(ctx)? {
        if let Some(word_fid_docids) = ctx.get_db_word_fid_docids(universe, word.interned(), fid)? {
            docids |= word_fid_docids;
        }
    }

    for phrase in term.all_phrases(ctx)? {
        // There may be false positives when resolving a phrase, so we're not
        // guaranteed that all of its words are within a single fid.
        if let Some(word) = phrase.words(ctx).iter().flatten().next() {
            if let Some(word_fid_docids) = ctx.get_db_word_fid_docids(universe, *word, fid)? {
                docids |= ctx.get_phrase_docids(None, phrase)? & word_fid_docids;
            }
        }
    }

    if let Some(word_prefix) = term.use_prefix_db(ctx) {
        if let Some(word_fid_docids) =
            ctx.get_db_word_prefix_fid_docids(universe, word_prefix.interned(), fid)?
        {
            docids |= word_fid_docids;
        }
    }

    Ok(docids)
}

pub fn compute_query_term_subset_docids_within_position(
    ctx: &mut SearchContext,
    universe: Option<&RoaringBitmap>,
    term: &QueryTermSubset,
    position: u16,
) -> Result<RoaringBitmap> {
    let mut docids = RoaringBitmap::new();
    for word in term.all_single_words_except_prefix_db(ctx)? {
        if let Some(word_position_docids) =
            ctx.get_db_word_position_docids(universe, word.interned(), position)?
        {
            docids |= word_position_docids;
        }
    }

    for phrase in term.all_phrases(ctx)? {
        // It's difficult to know the expected position of the words in the phrase,
        // so instead we just check the first one.
        if let Some(word) = phrase.words(ctx).iter().flatten().next() {
            if let Some(word_position_docids) =
                ctx.get_db_word_position_docids(universe, *word, position)?
            {
                docids |= ctx.get_phrase_docids(None, phrase)? & word_position_docids;
            }
        }
    }

    if let Some(word_prefix) = term.use_prefix_db(ctx) {
        if let Some(word_position_docids) =
            ctx.get_db_word_prefix_position_docids(universe, word_prefix.interned(), position)?
        {
            docids |= word_position_docids;
        }
    }
    Ok(docids)
}

/// Returns the subset of the input universe that satisfies the contraints of the input query graph.
pub fn compute_query_graph_docids(
    ctx: &mut SearchContext,
    q: &QueryGraph,
    universe: &RoaringBitmap,
) -> Result<RoaringBitmap> {
    let mut nodes_resolved = SmallBitmap::for_interned_values_in(&q.nodes);
    let mut path_nodes_docids = q.nodes.map(|_| RoaringBitmap::new());

    let mut next_nodes_to_visit = VecDeque::new();
    next_nodes_to_visit.push_back(q.root_node);

    while let Some(node_id) = next_nodes_to_visit.pop_front() {
        let node = q.nodes.get(node_id);
        let predecessors = &node.predecessors;
        if !predecessors.is_subset(&nodes_resolved) {
            next_nodes_to_visit.push_back(node_id);
            continue;
        }
        // Take union of all predecessors
        let predecessors_docids =
            MultiOps::union(predecessors.iter().map(|p| path_nodes_docids.get(p)));

        let node_docids = match &node.data {
            QueryNodeData::Term(LocatedQueryTermSubset {
                term_subset,
                positions: _,
                term_ids: _,
            }) => compute_query_term_subset_docids(ctx, Some(&predecessors_docids), term_subset)?,
            QueryNodeData::Deleted => {
                panic!()
            }
            QueryNodeData::Start => universe.clone(),
            QueryNodeData::End => {
                return Ok(predecessors_docids);
            }
        };
        nodes_resolved.insert(node_id);
        *path_nodes_docids.get_mut(node_id) = node_docids;

        for succ in node.successors.iter() {
            if !next_nodes_to_visit.contains(&succ) && !nodes_resolved.contains(succ) {
                next_nodes_to_visit.push_back(succ);
            }
        }

        for prec in node.predecessors.iter() {
            if q.nodes.get(prec).successors.is_subset(&nodes_resolved) {
                path_nodes_docids.get_mut(prec).clear();
            }
        }
    }
    panic!()
}

pub fn compute_phrase_docids(
    ctx: &mut SearchContext,
    universe: Option<&RoaringBitmap>,
    phrase: Interned<Phrase>,
) -> Result<RoaringBitmap> {
    let Phrase { words } = ctx.phrase_interner.get(phrase).clone();

    if words.is_empty() {
        return Ok(RoaringBitmap::new());
    }
    let mut candidates = RoaringBitmap::new();
    for word in words.iter().flatten().copied() {
        if let Some(word_docids) = ctx.word_docids(universe, Word::Original(word))? {
            candidates |= word_docids;
        } else {
            return Ok(RoaringBitmap::new());
        }
    }

    let winsize = words.len().min(3);

    for win in words.windows(winsize) {
        // Get all the documents with the matching distance for each word pairs.
        let mut bitmaps = Vec::with_capacity(winsize.pow(2));
        for (offset, &s1) in win
            .iter()
            .enumerate()
            .filter_map(|(index, word)| word.as_ref().map(|word| (index, word)))
        {
            for (dist, &s2) in win
                .iter()
                .skip(offset + 1)
                .enumerate()
                .filter_map(|(index, word)| word.as_ref().map(|word| (index, word)))
            {
                if dist == 0 {
                    match ctx.get_db_word_pair_proximity_docids(universe, s1, s2, 1)? {
                        Some(m) => bitmaps.push(m),
                        // If there are no documents for this pair, there will be no
                        // results for the phrase query.
                        None => return Ok(RoaringBitmap::new()),
                    }
                } else {
                    let mut bitmap = RoaringBitmap::new();
                    for dist in 0..=dist {
                        if let Some(m) =
                            ctx.get_db_word_pair_proximity_docids(universe, s1, s2, dist as u8 + 1)?
                        {
                            bitmap |= m;
                        }
                    }
                    if bitmap.is_empty() {
                        return Ok(bitmap);
                    } else {
                        bitmaps.push(bitmap);
                    }
                }
            }
        }

        // We sort the bitmaps so that we perform the small intersections first, which is faster.
        bitmaps.sort_unstable_by_key(|a| a.len());

        // TODO use MultiOps intersection which and remove the above sort
        for bitmap in bitmaps {
            candidates &= bitmap;

            // There will be no match, return early
            if candidates.is_empty() {
                break;
            }
        }
    }
    Ok(candidates)
}
