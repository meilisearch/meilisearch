use roaring::MultiOps;

use super::{ProximityEdge, WordPair};
use crate::new::db_cache::DatabaseCache;
use crate::CboRoaringBitmapCodec;

pub fn compute_docids<'transaction>(
    index: &crate::Index,
    txn: &'transaction heed::RoTxn,
    db_cache: &mut DatabaseCache<'transaction>,
    edge: &ProximityEdge,
) -> crate::Result<roaring::RoaringBitmap> {
    let ProximityEdge { pairs, proximity } = edge;
    // TODO: we should know already which pair of words to look for
    let mut pair_docids = vec![];
    for pair in pairs.iter() {
        let bytes = match pair {
            WordPair::Words { left, right } => {
                db_cache.get_word_pair_proximity_docids(index, txn, left, right, *proximity)
            }
            WordPair::WordPrefix { left, right_prefix } => db_cache
                .get_word_prefix_pair_proximity_docids(index, txn, left, right_prefix, *proximity),
        }?;
        let bitmap =
            bytes.map(CboRoaringBitmapCodec::deserialize_from).transpose()?.unwrap_or_default();
        pair_docids.push(bitmap);
    }
    pair_docids.sort_by_key(|rb| rb.len());
    let docids = MultiOps::union(pair_docids);
    Ok(docids)
}
