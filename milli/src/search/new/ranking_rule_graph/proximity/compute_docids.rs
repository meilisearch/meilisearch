use roaring::RoaringBitmap;

use super::{ProximityEdge, WordPair};
use crate::search::new::SearchContext;
use crate::{CboRoaringBitmapCodec, Result};

pub fn compute_docids<'ctx>(
    ctx: &mut SearchContext<'ctx>,
    edge: &ProximityEdge,
    universe: &RoaringBitmap,
) -> Result<RoaringBitmap> {
    let SearchContext { index, txn, db_cache, word_interner, .. } = ctx;
    let ProximityEdge { pairs, proximity } = edge;
    let mut pair_docids = RoaringBitmap::new();
    for pair in pairs.iter() {
        let bytes = match pair {
            WordPair::Words { left, right } => db_cache.get_word_pair_proximity_docids(
                index,
                txn,
                word_interner,
                *left,
                *right,
                *proximity,
            ),
            WordPair::WordPrefix { left, right_prefix } => db_cache
                .get_word_prefix_pair_proximity_docids(
                    index,
                    txn,
                    word_interner,
                    *left,
                    *right_prefix,
                    *proximity,
                ),
            WordPair::WordPrefixSwapped { left_prefix, right } => db_cache
                .get_prefix_word_pair_proximity_docids(
                    index,
                    txn,
                    word_interner,
                    *left_prefix,
                    *right,
                    *proximity,
                ),
        }?;
        // TODO: deserialize bitmap within a universe, and (maybe) using a bump allocator?
        let bitmap = universe
            & bytes.map(CboRoaringBitmapCodec::deserialize_from).transpose()?.unwrap_or_default();
        pair_docids |= bitmap;
    }

    Ok(pair_docids)
}
