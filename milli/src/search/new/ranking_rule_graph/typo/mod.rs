use heed::{BytesDecode, RoTxn};
use roaring::RoaringBitmap;

use super::empty_paths_cache::EmptyPathsCache;
use super::paths_map::PathsMap;
use super::{EdgeDetails, RankingRuleGraphTrait};
use crate::new::db_cache::DatabaseCache;
use crate::new::query_term::{LocatedQueryTerm, QueryTerm, WordDerivations};
use crate::new::QueryNode;
use crate::{Index, Result, RoaringBitmapCodec};

#[derive(Clone)]
pub enum TypoEdge {
    Phrase,
    Word { derivations: WordDerivations, nbr_typos: u8 },
}

pub enum TypoGraph {}

impl RankingRuleGraphTrait for TypoGraph {
    type EdgeDetails = TypoEdge;
    type BuildVisitedFromNode = ();

    fn graphviz_edge_details_label(edge: &Self::EdgeDetails) -> String {
        match edge {
            TypoEdge::Phrase => format!(", 0 typos"),
            TypoEdge::Word { nbr_typos, .. } => format!(", {nbr_typos} typos"),
        }
    }

    fn compute_docids<'db_cache, 'transaction>(
        index: &Index,
        txn: &'transaction RoTxn,
        db_cache: &mut DatabaseCache<'transaction>,
        edge: &Self::EdgeDetails,
    ) -> Result<roaring::RoaringBitmap> {
        match edge {
            TypoEdge::Phrase => todo!(),
            TypoEdge::Word { derivations, nbr_typos } => {
                let words = match nbr_typos {
                    0 => &derivations.zero_typo,
                    1 => &derivations.one_typo,
                    2 => &derivations.two_typos,
                    _ => panic!(),
                };
                let mut docids = RoaringBitmap::new();
                for word in words.iter() {
                    let Some(bytes) = db_cache.get_word_docids(index, txn, word)? else { continue };
                    let bitmap =
                        RoaringBitmapCodec::bytes_decode(bytes).ok_or(heed::Error::Decoding)?;
                    docids |= bitmap;
                }
                if *nbr_typos == 0 {
                    if let Some(bytes) =
                        db_cache.get_prefix_docids(index, txn, &derivations.original)?
                    {
                        let bitmap =
                            RoaringBitmapCodec::bytes_decode(bytes).ok_or(heed::Error::Decoding)?;
                        docids |= bitmap;
                    }
                }
                Ok(docids)
            }
        }
    }

    fn build_visit_from_node<'transaction>(
        _index: &Index,
        _txn: &'transaction RoTxn,
        _db_cache: &mut DatabaseCache<'transaction>,
        from_node: &QueryNode,
    ) -> Result<Option<Self::BuildVisitedFromNode>> {
        Ok(Some(()))
    }

    fn build_visit_to_node<'from_data, 'transaction: 'from_data>(
        index: &Index,
        txn: &'transaction RoTxn,
        db_cache: &mut DatabaseCache<'transaction>,
        to_node: &QueryNode,
        from_node_data: &'from_data Self::BuildVisitedFromNode,
    ) -> Result<Vec<(u8, EdgeDetails<Self::EdgeDetails>)>> {
        match to_node {
            QueryNode::Term(LocatedQueryTerm { value, .. }) => match value {
                QueryTerm::Phrase(_) => Ok(vec![(0, EdgeDetails::Data(TypoEdge::Phrase))]),
                QueryTerm::Word { derivations } => {
                    let mut edges = vec![];
                    if !derivations.zero_typo.is_empty() || derivations.use_prefix_db {
                        edges.push((
                            0,
                            EdgeDetails::Data(TypoEdge::Word {
                                derivations: derivations.clone(),
                                nbr_typos: 0,
                            }),
                        ))
                    }
                    if !derivations.one_typo.is_empty() {
                        edges.push((
                            1,
                            EdgeDetails::Data(TypoEdge::Word {
                                derivations: derivations.clone(),
                                nbr_typos: 1,
                            }),
                        ))
                    }
                    if !derivations.two_typos.is_empty() {
                        edges.push((
                            2,
                            EdgeDetails::Data(TypoEdge::Word {
                                derivations: derivations.clone(),
                                nbr_typos: 2,
                            }),
                        ))
                    }
                    Ok(edges)
                }
            },
            QueryNode::End => Ok(vec![(0, EdgeDetails::Unconditional)]),
            QueryNode::Deleted | QueryNode::Start => panic!(),
        }
    }

    fn log_state(
        graph: &super::RankingRuleGraph<Self>,
        paths: &PathsMap<u64>,
        empty_paths_cache: &EmptyPathsCache,
        logger: &mut dyn crate::new::logger::SearchLogger<crate::new::QueryGraph>,
    ) {
        logger.log_typo_state(graph, paths, empty_paths_cache);
    }
}
