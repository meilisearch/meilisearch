use roaring::RoaringBitmap;

use super::{ProximityCondition, WordPair};
use crate::search::new::SearchContext;
use crate::{CboRoaringBitmapCodec, Result};

pub fn compute_docids<'ctx>(
    ctx: &mut SearchContext<'ctx>,
    condition: &ProximityCondition,
    universe: &RoaringBitmap,
) -> Result<RoaringBitmap> {
    let SearchContext {
        index,
        txn,
        db_cache,
        word_interner,
        term_docids,
        phrase_interner,
        term_interner,
    } = ctx;
    let pairs = match condition {
        ProximityCondition::Term { term } => {
            return term_docids
                .get_query_term_docids(
                    index,
                    txn,
                    db_cache,
                    word_interner,
                    term_interner,
                    phrase_interner,
                    *term,
                )
                .cloned()
        }
        ProximityCondition::Pairs { pairs } => pairs,
    };
    let mut pair_docids = RoaringBitmap::new();
    for pair in pairs.iter() {
        let pair = match pair {
            WordPair::Words { phrases, left, right, proximity } => {
                let mut docids = db_cache
                    .get_word_pair_proximity_docids(
                        index,
                        txn,
                        word_interner,
                        *left,
                        *right,
                        *proximity,
                    )?
                    .map(CboRoaringBitmapCodec::deserialize_from)
                    .transpose()?
                    .unwrap_or_default();
                if !docids.is_empty() {
                    for phrase in phrases {
                        docids &= ctx.term_docids.get_phrase_docids(
                            index,
                            txn,
                            db_cache,
                            word_interner,
                            &ctx.phrase_interner,
                            *phrase,
                        )?;
                    }
                }
                docids
            }
            WordPair::WordPrefix { phrases, left, right_prefix, proximity } => {
                let mut docids = db_cache
                    .get_word_prefix_pair_proximity_docids(
                        index,
                        txn,
                        word_interner,
                        *left,
                        *right_prefix,
                        *proximity,
                    )?
                    .map(CboRoaringBitmapCodec::deserialize_from)
                    .transpose()?
                    .unwrap_or_default();
                if !docids.is_empty() {
                    for phrase in phrases {
                        docids &= ctx.term_docids.get_phrase_docids(
                            index,
                            txn,
                            db_cache,
                            word_interner,
                            &ctx.phrase_interner,
                            *phrase,
                        )?;
                    }
                }
                docids
            }
            WordPair::WordPrefixSwapped { left_prefix, right, proximity } => db_cache
                .get_prefix_word_pair_proximity_docids(
                    index,
                    txn,
                    word_interner,
                    *left_prefix,
                    *right,
                    *proximity,
                )?
                .map(CboRoaringBitmapCodec::deserialize_from)
                .transpose()?
                .unwrap_or_default(),
        };
        // TODO: deserialize bitmap within a universe
        let bitmap = universe & pair;
        pair_docids |= bitmap;
    }

    Ok(pair_docids)
}
