use std::collections::VecDeque;

use fxhash::FxHashMap;
use heed::{BytesDecode, RoTxn};
use roaring::{MultiOps, RoaringBitmap};

use super::db_cache::DatabaseCache;
use super::query_term::{Phrase, QueryTerm, WordDerivations};
use super::{QueryGraph, QueryNode};

use crate::{CboRoaringBitmapCodec, Index, Result, RoaringBitmapCodec};

// TODO: manual performance metrics: access to DB, bitmap deserializations/operations, etc.
#[derive(Default)]
pub struct NodeDocIdsCache {
    pub cache: FxHashMap<u32, RoaringBitmap>,
}
impl NodeDocIdsCache {
    fn get_docids<'cache, 'transaction>(
        &'cache mut self,
        index: &Index,
        txn: &'transaction RoTxn,
        db_cache: &mut DatabaseCache<'transaction>,
        term: &QueryTerm,
        node_idx: u32,
    ) -> Result<&'cache RoaringBitmap> {
        if self.cache.contains_key(&node_idx) {
            return Ok(&self.cache[&node_idx]);
        };
        let docids = match term {
            QueryTerm::Phrase { phrase } => resolve_phrase(index, txn, db_cache, phrase)?,
            QueryTerm::Word {
                derivations:
                    WordDerivations {
                        original,
                        zero_typo,
                        one_typo,
                        two_typos,
                        use_prefix_db,
                        synonyms,
                        split_words,
                    },
            } => {
                let mut or_docids = vec![];
                for word in zero_typo.iter().chain(one_typo.iter()).chain(two_typos.iter()) {
                    if let Some(word_docids) = db_cache.get_word_docids(index, txn, word)? {
                        or_docids.push(word_docids);
                    }
                }
                if *use_prefix_db {
                    if let Some(prefix_docids) =
                        db_cache.get_prefix_docids(index, txn, original.as_str())?
                    {
                        or_docids.push(prefix_docids);
                    }
                }
                let mut docids = or_docids
                    .into_iter()
                    .map(|slice| RoaringBitmapCodec::bytes_decode(slice).unwrap())
                    .collect::<Vec<_>>();
                for synonym in synonyms {
                    // TODO: cache resolve_phrase?
                    docids.push(resolve_phrase(index, txn, db_cache, synonym)?);
                }
                if let Some((left, right)) = split_words {
                    if let Some(split_word_docids) =
                        db_cache.get_word_pair_proximity_docids(index, txn, left, right, 1)?
                    {
                        docids.push(CboRoaringBitmapCodec::deserialize_from(split_word_docids)?);
                    }
                }

                MultiOps::union(docids)
            }
        };
        let _ = self.cache.insert(node_idx, docids);
        let docids = &self.cache[&node_idx];
        Ok(docids)
    }
}

pub fn resolve_query_graph<'transaction>(
    index: &Index,
    txn: &'transaction RoTxn,
    db_cache: &mut DatabaseCache<'transaction>,
    node_docids_cache: &mut NodeDocIdsCache,
    q: &QueryGraph,
    universe: &RoaringBitmap,
) -> Result<RoaringBitmap> {
    // TODO: there is definitely a faster way to compute this big
    // roaring bitmap expression

    let mut nodes_resolved = RoaringBitmap::new();
    let mut path_nodes_docids = vec![RoaringBitmap::new(); q.nodes.len()];

    let mut next_nodes_to_visit = VecDeque::new();
    next_nodes_to_visit.push_front(q.root_node);

    while let Some(node) = next_nodes_to_visit.pop_front() {
        let predecessors = &q.edges[node as usize].predecessors;
        if !predecessors.is_subset(&nodes_resolved) {
            next_nodes_to_visit.push_back(node);
            continue;
        }
        // Take union of all predecessors
        let predecessors_iter = predecessors.iter().map(|p| &path_nodes_docids[p as usize]);
        let predecessors_docids = MultiOps::union(predecessors_iter);

        let n = &q.nodes[node as usize];

        let node_docids = match n {
            QueryNode::Term(located_term) => {
                let term = &located_term.value;
                let derivations_docids =
                    node_docids_cache.get_docids(index, txn, db_cache, term, node)?;
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

        // This is currently slow but could easily be implemented very efficiently
        for prec in q.edges[node as usize].predecessors.iter() {
            if q.edges[prec as usize].successors.is_subset(&nodes_resolved) {
                path_nodes_docids[prec as usize].clear();
            }
        }
    }

    panic!()
}

pub fn resolve_phrase<'transaction>(
    index: &Index,
    txn: &'transaction RoTxn,
    db_cache: &mut DatabaseCache<'transaction>,
    phrase: &Phrase,
) -> Result<RoaringBitmap> {
    let Phrase { words } = phrase;
    let mut candidates = RoaringBitmap::new();
    let mut first_iter = true;
    let winsize = words.len().min(3);

    if words.is_empty() {
        return Ok(candidates);
    }

    for win in words.windows(winsize) {
        // Get all the documents with the matching distance for each word pairs.
        let mut bitmaps = Vec::with_capacity(winsize.pow(2));
        for (offset, s1) in win
            .iter()
            .enumerate()
            .filter_map(|(index, word)| word.as_ref().map(|word| (index, word)))
        {
            for (dist, s2) in win
                .iter()
                .skip(offset + 1)
                .enumerate()
                .filter_map(|(index, word)| word.as_ref().map(|word| (index, word)))
            {
                if dist == 0 {
                    match db_cache.get_word_pair_proximity_docids(index, txn, s1, s2, 1)? {
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
