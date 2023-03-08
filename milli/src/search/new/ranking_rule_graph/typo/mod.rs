use heed::BytesDecode;
use roaring::RoaringBitmap;

use super::empty_paths_cache::EmptyPathsCache;
use super::{EdgeCondition, RankingRuleGraph, RankingRuleGraphTrait};
use crate::search::new::interner::Interned;
use crate::search::new::logger::SearchLogger;
use crate::search::new::query_term::{LocatedQueryTerm, Phrase, QueryTerm, WordDerivations};
use crate::search::new::resolve_query_graph::resolve_phrase;
use crate::search::new::small_bitmap::SmallBitmap;
use crate::search::new::{QueryGraph, QueryNode, SearchContext};
use crate::{Result, RoaringBitmapCodec};

#[derive(Clone)]
pub enum TypoEdge {
    Phrase { phrase: Interned<Phrase> },
    Word { derivations: WordDerivations, nbr_typos: u8 },
}

pub enum TypoGraph {}

impl RankingRuleGraphTrait for TypoGraph {
    type EdgeCondition = TypoEdge;
    type BuildVisitedFromNode = ();

    fn label_for_edge_condition(edge: &Self::EdgeCondition) -> String {
        match edge {
            TypoEdge::Phrase { .. } => ", 0 typos".to_owned(),
            TypoEdge::Word { nbr_typos, .. } => format!(", {nbr_typos} typos"),
        }
    }

    fn resolve_edge_condition<'db_cache, 'search>(
        ctx: &mut SearchContext<'search>,
        edge: &Self::EdgeCondition,
        universe: &RoaringBitmap,
    ) -> Result<RoaringBitmap> {
        match edge {
            TypoEdge::Phrase { phrase } => resolve_phrase(ctx, *phrase),
            TypoEdge::Word { derivations, nbr_typos } => {
                let words = match nbr_typos {
                    0 => &derivations.zero_typo,
                    1 => &derivations.one_typo,
                    2 => &derivations.two_typos,
                    _ => panic!(),
                };
                let mut docids = RoaringBitmap::new();
                for word in words.iter().copied() {
                    let Some(bytes) = ctx.get_word_docids(word)? else { continue };
                    // TODO: deserialize bitmap within a universe
                    let bitmap = universe
                        & RoaringBitmapCodec::bytes_decode(bytes).ok_or(heed::Error::Decoding)?;
                    docids |= bitmap;
                }
                if *nbr_typos == 0 {
                    if let Some(bytes) = ctx.get_word_prefix_docids(derivations.original)? {
                        // TODO: deserialize bitmap within a universe
                        let bitmap = universe
                            & RoaringBitmapCodec::bytes_decode(bytes)
                                .ok_or(heed::Error::Decoding)?;
                        docids |= bitmap;
                    }
                }
                Ok(docids)
            }
        }
    }

    fn build_step_visit_source_node<'search>(
        _ctx: &mut SearchContext<'search>,
        _from_node: &QueryNode,
    ) -> Result<Option<Self::BuildVisitedFromNode>> {
        Ok(Some(()))
    }

    fn build_step_visit_destination_node<'from_data, 'search: 'from_data>(
        _ctx: &mut SearchContext<'search>,
        to_node: &QueryNode,
        _from_node_data: &'from_data Self::BuildVisitedFromNode,
    ) -> Result<Vec<(u8, EdgeCondition<Self::EdgeCondition>)>> {
        match to_node {
            QueryNode::Term(LocatedQueryTerm { value, .. }) => match value {
                &QueryTerm::Phrase { phrase } => {
                    Ok(vec![(0, EdgeCondition::Conditional(TypoEdge::Phrase { phrase }))])
                }
                QueryTerm::Word { derivations } => {
                    let mut edges = vec![];
                    if !derivations.zero_typo.is_empty() || derivations.use_prefix_db {
                        edges.push((
                            0,
                            EdgeCondition::Conditional(TypoEdge::Word {
                                derivations: derivations.clone(),
                                nbr_typos: 0,
                            }),
                        ))
                    }
                    if !derivations.one_typo.is_empty() {
                        edges.push((
                            1,
                            EdgeCondition::Conditional(TypoEdge::Word {
                                derivations: derivations.clone(),
                                nbr_typos: 1,
                            }),
                        ))
                    }
                    if !derivations.two_typos.is_empty() {
                        edges.push((
                            2,
                            EdgeCondition::Conditional(TypoEdge::Word {
                                derivations: derivations.clone(),
                                nbr_typos: 2,
                            }),
                        ))
                    }
                    Ok(edges)
                }
            },
            QueryNode::End => Ok(vec![(0, EdgeCondition::Unconditional)]),
            QueryNode::Deleted | QueryNode::Start => panic!(),
        }
    }

    fn log_state(
        graph: &RankingRuleGraph<Self>,
        paths: &[Vec<u16>],
        empty_paths_cache: &EmptyPathsCache,
        universe: &RoaringBitmap,
        distances: &[Vec<(u16, SmallBitmap)>],
        cost: u16,
        logger: &mut dyn SearchLogger<QueryGraph>,
    ) {
        logger.log_typo_state(graph, paths, empty_paths_cache, universe, distances.to_vec(), cost);
    }
}
