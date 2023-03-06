use super::{ProximityEdge, WordPair};
use crate::new::SearchContext;
use crate::{CboRoaringBitmapCodec, Result};
use roaring::{MultiOps, RoaringBitmap};

pub fn compute_docids<'search>(
    ctx: &mut SearchContext<'search>,
    edge: &ProximityEdge,
) -> Result<RoaringBitmap> {
    let ProximityEdge { pairs, proximity } = edge;
    let mut pair_docids = vec![];
    for pair in pairs.iter() {
        let bytes = match pair {
            WordPair::Words { left, right } => {
                ctx.get_word_pair_proximity_docids(*left, *right, *proximity)
            }
            WordPair::WordPrefix { left, right_prefix } => {
                ctx.get_word_prefix_pair_proximity_docids(*left, *right_prefix, *proximity)
            }
            WordPair::WordPrefixSwapped { left_prefix, right } => {
                ctx.get_prefix_word_pair_proximity_docids(*left_prefix, *right, *proximity)
            }
        }?;
        let bitmap =
            bytes.map(CboRoaringBitmapCodec::deserialize_from).transpose()?.unwrap_or_default();
        pair_docids.push(bitmap);
    }
    let docids = MultiOps::union(pair_docids);
    Ok(docids)
}
