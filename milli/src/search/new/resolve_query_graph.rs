#![allow(clippy::too_many_arguments)]

use std::collections::VecDeque;

use fxhash::FxHashMap;
use heed::BytesDecode;
use roaring::RoaringBitmap;

use super::interner::Interned;
use super::query_graph::QueryNodeData;
use super::query_term::{Phrase, QueryTermSubset};
use super::small_bitmap::SmallBitmap;
use super::{QueryGraph, SearchContext};
use crate::search::new::query_term::LocatedQueryTermSubset;
use crate::{CboRoaringBitmapCodec, Result, RoaringBitmapCodec};

#[derive(Default)]
pub struct PhraseDocIdsCache {
    pub cache: FxHashMap<Interned<Phrase>, RoaringBitmap>,
}
impl<'ctx> SearchContext<'ctx> {
    /// Get the document ids associated with the given phrase
    pub fn get_phrase_docids(&mut self, phrase: Interned<Phrase>) -> Result<&RoaringBitmap> {
        if self.phrase_docids.cache.contains_key(&phrase) {
            return Ok(&self.phrase_docids.cache[&phrase]);
        };
        let docids = compute_phrase_docids(self, phrase)?;
        let _ = self.phrase_docids.cache.insert(phrase, docids);
        let docids = &self.phrase_docids.cache[&phrase];
        Ok(docids)
    }
}
pub fn compute_query_term_subset_docids(
    ctx: &mut SearchContext,
    term: &QueryTermSubset,
) -> Result<RoaringBitmap> {
    let mut docids = RoaringBitmap::new();
    for word in term.all_single_words_except_prefix_db(ctx)? {
        if let Some(word_docids) = ctx.get_db_word_docids(word)? {
            docids |= RoaringBitmapCodec::bytes_decode(word_docids).ok_or(heed::Error::Decoding)?;
        }
    }
    for phrase in term.all_phrases(ctx)? {
        docids |= ctx.get_phrase_docids(phrase)?;
    }

    if let Some(prefix) = term.use_prefix_db(ctx) {
        if let Some(prefix_docids) = ctx.get_db_word_prefix_docids(prefix)? {
            docids |=
                RoaringBitmapCodec::bytes_decode(prefix_docids).ok_or(heed::Error::Decoding)?;
        }
    }

    Ok(docids)
}

pub fn compute_query_graph_docids(
    ctx: &mut SearchContext,
    q: &QueryGraph,
    universe: &RoaringBitmap,
) -> Result<RoaringBitmap> {
    // TODO: there must be a faster way to compute this big
    // roaring bitmap expression

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
        let mut predecessors_docids = RoaringBitmap::new();
        for p in predecessors.iter() {
            predecessors_docids |= path_nodes_docids.get(p);
        }

        let node_docids = match &node.data {
            QueryNodeData::Term(LocatedQueryTermSubset {
                term_subset,
                positions: _,
                term_ids: _,
            }) => {
                let phrase_docids = compute_query_term_subset_docids(ctx, term_subset)?;
                predecessors_docids & phrase_docids
            }
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
    phrase: Interned<Phrase>,
) -> Result<RoaringBitmap> {
    let Phrase { words } = ctx.phrase_interner.get(phrase).clone();
    let mut candidates = RoaringBitmap::new();
    let mut first_iter = true;
    let winsize = words.len().min(3);

    if words.is_empty() {
        return Ok(candidates);
    }

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
                    match ctx.get_db_word_pair_proximity_docids(s1, s2, 1)? {
                        Some(m) => bitmaps.push(CboRoaringBitmapCodec::deserialize_from(m)?),
                        // If there are no documents for this pair, there will be no
                        // results for the phrase query.
                        None => return Ok(RoaringBitmap::new()),
                    }
                } else {
                    let mut bitmap = RoaringBitmap::new();
                    for dist in 0..=dist {
                        if let Some(m) =
                            ctx.get_db_word_pair_proximity_docids(s1, s2, dist as u8 + 1)?
                        {
                            bitmap |= CboRoaringBitmapCodec::deserialize_from(m)?;
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

        for bitmap in bitmaps {
            if first_iter {
                candidates = bitmap;
                first_iter = false;
            } else {
                candidates &= bitmap;
            }
            // There will be no match, return early
            if candidates.is_empty() {
                break;
            }
        }
    }
    Ok(candidates)
}
