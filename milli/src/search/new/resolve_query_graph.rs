#![allow(clippy::too_many_arguments)]

use std::collections::VecDeque;

use fxhash::FxHashMap;
use heed::{BytesDecode, RoTxn};
use roaring::{MultiOps, RoaringBitmap};

use super::db_cache::DatabaseCache;
use super::interner::{Interned, Interner};
use super::query_graph::QUERY_GRAPH_NODE_LENGTH_LIMIT;
use super::query_term::{Phrase, QueryTerm, WordDerivations};
use super::small_bitmap::SmallBitmap;
use super::{QueryGraph, QueryNode, SearchContext};
use crate::{CboRoaringBitmapCodec, Index, Result, RoaringBitmapCodec};

#[derive(Default)]
pub struct QueryTermDocIdsCache {
    pub phrases: FxHashMap<Interned<Phrase>, RoaringBitmap>,
    pub derivations: FxHashMap<Interned<WordDerivations>, RoaringBitmap>,
}
impl QueryTermDocIdsCache {
    /// Get the document ids associated with the given phrase
    pub fn get_phrase_docids<'s, 'ctx>(
        &'s mut self,
        index: &Index,
        txn: &'ctx RoTxn,
        db_cache: &mut DatabaseCache<'ctx>,
        word_interner: &Interner<String>,
        phrase_interner: &Interner<Phrase>,
        phrase: Interned<Phrase>,
    ) -> Result<&'s RoaringBitmap> {
        if self.phrases.contains_key(&phrase) {
            return Ok(&self.phrases[&phrase]);
        };
        let docids = resolve_phrase(index, txn, db_cache, word_interner, phrase_interner, phrase)?;
        let _ = self.phrases.insert(phrase, docids);
        let docids = &self.phrases[&phrase];
        Ok(docids)
    }

    /// Get the document ids associated with the given word derivations
    pub fn get_word_derivations_docids<'s, 'ctx>(
        &'s mut self,
        index: &Index,
        txn: &'ctx RoTxn,
        db_cache: &mut DatabaseCache<'ctx>,
        word_interner: &Interner<String>,
        derivations_interner: &Interner<WordDerivations>,
        phrase_interner: &Interner<Phrase>,
        derivations: Interned<WordDerivations>,
    ) -> Result<&'s RoaringBitmap> {
        if self.derivations.contains_key(&derivations) {
            return Ok(&self.derivations[&derivations]);
        };
        let WordDerivations {
            original,
            synonyms,
            split_words,
            zero_typo,
            one_typo,
            two_typos,
            use_prefix_db,
        } = derivations_interner.get(derivations);
        let mut or_docids = vec![];
        for word in zero_typo.iter().chain(one_typo.iter()).chain(two_typos.iter()).copied() {
            if let Some(word_docids) = db_cache.get_word_docids(index, txn, word_interner, word)? {
                or_docids.push(word_docids);
            }
        }
        if *use_prefix_db {
            // TODO: this will change if we decide to change from (original, zero_typo) to:
            // (debug_original, prefix_of, zero_typo)
            if let Some(prefix_docids) =
                db_cache.get_word_prefix_docids(index, txn, word_interner, *original)?
            {
                or_docids.push(prefix_docids);
            }
        }
        let mut docids = or_docids
            .into_iter()
            .map(|slice| RoaringBitmapCodec::bytes_decode(slice).unwrap())
            .collect::<Vec<_>>();
        for synonym in synonyms.iter().copied() {
            // TODO: cache resolve_phrase?
            docids.push(resolve_phrase(
                index,
                txn,
                db_cache,
                word_interner,
                phrase_interner,
                synonym,
            )?);
        }
        if let Some(split_words) = split_words {
            docids.push(resolve_phrase(
                index,
                txn,
                db_cache,
                word_interner,
                phrase_interner,
                *split_words,
            )?);
        }

        let docids = MultiOps::union(docids);
        let _ = self.derivations.insert(derivations, docids);
        let docids = &self.derivations[&derivations];
        Ok(docids)
    }

    /// Get the document ids associated with the given query term.
    fn get_query_term_docids<'s, 'ctx>(
        &'s mut self,
        index: &Index,
        txn: &'ctx RoTxn,
        db_cache: &mut DatabaseCache<'ctx>,
        word_interner: &Interner<String>,
        derivations_interner: &Interner<WordDerivations>,
        phrase_interner: &Interner<Phrase>,
        term: &QueryTerm,
    ) -> Result<&'s RoaringBitmap> {
        match *term {
            QueryTerm::Phrase { phrase } => {
                self.get_phrase_docids(index, txn, db_cache, word_interner, phrase_interner, phrase)
            }
            QueryTerm::Word { derivations } => self.get_word_derivations_docids(
                index,
                txn,
                db_cache,
                word_interner,
                derivations_interner,
                phrase_interner,
                derivations,
            ),
        }
    }
}

pub fn resolve_query_graph<'ctx>(
    ctx: &mut SearchContext<'ctx>,
    q: &QueryGraph,
    universe: &RoaringBitmap,
) -> Result<RoaringBitmap> {
    let SearchContext {
        index,
        txn,
        db_cache,
        word_interner,
        phrase_interner,
        derivations_interner,
        query_term_docids,
        ..
    } = ctx;
    // TODO: there is a faster way to compute this big
    // roaring bitmap expression

    let mut nodes_resolved = SmallBitmap::new(QUERY_GRAPH_NODE_LENGTH_LIMIT);
    let mut path_nodes_docids = vec![RoaringBitmap::new(); q.nodes.len()];

    let mut next_nodes_to_visit = VecDeque::new();
    next_nodes_to_visit.push_back(q.root_node);

    while let Some(node) = next_nodes_to_visit.pop_front() {
        let predecessors = &q.edges[node as usize].predecessors;
        if !predecessors.is_subset(&nodes_resolved) {
            next_nodes_to_visit.push_back(node);
            continue;
        }
        // Take union of all predecessors
        let mut predecessors_docids = RoaringBitmap::new();
        for p in predecessors.iter() {
            predecessors_docids |= &path_nodes_docids[p as usize];
        }

        let n = &q.nodes[node as usize];

        let node_docids = match n {
            QueryNode::Term(located_term) => {
                let derivations_docids = query_term_docids.get_query_term_docids(
                    index,
                    txn,
                    db_cache,
                    word_interner,
                    derivations_interner,
                    phrase_interner,
                    &located_term.value,
                )?;
                predecessors_docids & derivations_docids
            }
            QueryNode::Deleted => {
                panic!()
            }
            QueryNode::Start => universe.clone(),
            QueryNode::End => {
                return Ok(predecessors_docids);
            }
        };
        nodes_resolved.insert(node);
        path_nodes_docids[node as usize] = node_docids;

        for succ in q.edges[node as usize].successors.iter() {
            if !next_nodes_to_visit.contains(&succ) && !nodes_resolved.contains(succ) {
                next_nodes_to_visit.push_back(succ);
            }
        }

        for prec in q.edges[node as usize].predecessors.iter() {
            if q.edges[prec as usize].successors.is_subset(&nodes_resolved) {
                path_nodes_docids[prec as usize].clear();
            }
        }
    }
    panic!()
}

pub fn resolve_phrase<'ctx>(
    index: &Index,
    txn: &'ctx RoTxn,
    db_cache: &mut DatabaseCache<'ctx>,
    word_interner: &Interner<String>,
    phrase_interner: &Interner<Phrase>,
    phrase: Interned<Phrase>,
) -> Result<RoaringBitmap> {
    let Phrase { words } = phrase_interner.get(phrase).clone();
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
                    match db_cache.get_word_pair_proximity_docids(
                        index,
                        txn,
                        word_interner,
                        s1,
                        s2,
                        1,
                    )? {
                        Some(m) => bitmaps.push(CboRoaringBitmapCodec::deserialize_from(m)?),
                        // If there are no documents for this pair, there will be no
                        // results for the phrase query.
                        None => return Ok(RoaringBitmap::new()),
                    }
                } else {
                    let mut bitmap = RoaringBitmap::new();
                    for dist in 0..=dist {
                        if let Some(m) = db_cache.get_word_pair_proximity_docids(
                            index,
                            txn,
                            word_interner,
                            s1,
                            s2,
                            dist as u8 + 1,
                        )? {
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
