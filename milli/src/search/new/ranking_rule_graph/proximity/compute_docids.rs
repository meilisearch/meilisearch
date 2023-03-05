use heed::RoTxn;
use roaring::{MultiOps, RoaringBitmap};

use super::{ProximityEdge, WordPair};
use crate::new::db_cache::DatabaseCache;
use crate::{CboRoaringBitmapCodec, Result};

pub fn compute_docids<'transaction>(
    index: &crate::Index,
    txn: &'transaction RoTxn,
    db_cache: &mut DatabaseCache<'transaction>,
    edge: &ProximityEdge,
) -> Result<RoaringBitmap> {
    let ProximityEdge { pairs, proximity } = edge;
    let mut pair_docids = vec![];
    for pair in pairs.iter() {
        let bytes = match pair {
            WordPair::Words { left, right } => {
                db_cache.get_word_pair_proximity_docids(index, txn, left, right, *proximity)
            }
            WordPair::WordPrefix { left, right_prefix } => db_cache
                .get_word_prefix_pair_proximity_docids(index, txn, left, right_prefix, *proximity),
            WordPair::WordPrefixSwapped { left_prefix, right } => db_cache
                .get_prefix_word_pair_proximity_docids(index, txn, left_prefix, right, *proximity),
        }?;
        let bitmap =
            bytes.map(CboRoaringBitmapCodec::deserialize_from).transpose()?.unwrap_or_default();
        pair_docids.push(bitmap);
    }
    let docids = MultiOps::union(pair_docids);
    Ok(docids)
}
