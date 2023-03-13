use roaring::RoaringBitmap;

use super::empty_paths_cache::EmptyPathsCache;
use super::{EdgeCondition, RankingRuleGraph, RankingRuleGraphTrait};
use crate::search::new::interner::{Interned, Interner};
use crate::search::new::logger::SearchLogger;
use crate::search::new::query_term::{LocatedQueryTerm, Phrase, QueryTerm, WordDerivations};
use crate::search::new::small_bitmap::SmallBitmap;
use crate::search::new::{QueryGraph, QueryNode, SearchContext};
use crate::Result;

#[derive(Clone, PartialEq, Eq, Hash)]
pub enum TypoEdge {
    Phrase { phrase: Interned<Phrase> },
    Word { derivations: Interned<WordDerivations>, nbr_typos: u8 },
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

    fn resolve_edge_condition<'db_cache, 'ctx>(
        ctx: &mut SearchContext<'ctx>,
        edge: &Self::EdgeCondition,
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
        } = ctx;
        match edge {
            &TypoEdge::Phrase { phrase } => Ok(universe
                & query_term_docids.get_phrase_docids(
                    index,
                    txn,
                    db_cache,
                    word_interner,
                    phrase_interner,
                    phrase,
                )?),
            TypoEdge::Word { derivations, .. } => {
                let docids = universe
                    & query_term_docids.get_word_derivations_docids(
                        index,
                        txn,
                        db_cache,
                        word_interner,
                        derivations_interner,
                        phrase_interner,
                        *derivations,
                    )?;

                Ok(docids)
            }
        }
    }

    fn build_step_visit_source_node<'ctx>(
        _ctx: &mut SearchContext<'ctx>,
        _from_node: &QueryNode,
    ) -> Result<Option<Self::BuildVisitedFromNode>> {
        Ok(Some(()))
    }

    fn build_step_visit_destination_node<'from_data, 'ctx: 'from_data>(
        ctx: &mut SearchContext<'ctx>,
        conditions_interner: &mut Interner<Self::EdgeCondition>,
        to_node: &QueryNode,
        _from_node_data: &'from_data Self::BuildVisitedFromNode,
    ) -> Result<Vec<(u8, EdgeCondition<Self::EdgeCondition>)>> {
        let SearchContext { derivations_interner, .. } = ctx;
        match to_node {
            QueryNode::Term(LocatedQueryTerm { value, .. }) => match *value {
                QueryTerm::Phrase { phrase } => Ok(vec![(
                    0,
                    EdgeCondition::Conditional(
                        conditions_interner.insert(TypoEdge::Phrase { phrase }),
                    ),
                )]),
                QueryTerm::Word { derivations } => {
                    let mut edges = vec![];

                    for nbr_typos in 0..=2 {
                        let derivations = derivations_interner.get(derivations).clone();
                        let new_derivations = match nbr_typos {
                            0 => {
                                // TODO: think about how split words and synonyms should be handled here
                                // TODO: what about ngrams?
                                // Maybe 2grams should have one typo by default and 3grams 2 typos by default
                                WordDerivations {
                                    original: derivations.original,
                                    synonyms: derivations.synonyms,
                                    split_words: None,
                                    zero_typo: derivations.zero_typo,
                                    one_typo: Box::new([]),
                                    two_typos: Box::new([]),
                                    use_prefix_db: derivations.use_prefix_db,
                                }
                            }
                            1 => {
                                // What about split words and synonyms here?
                                WordDerivations {
                                    original: derivations.original,
                                    synonyms: Box::new([]),
                                    split_words: derivations.split_words,
                                    zero_typo: Box::new([]),
                                    one_typo: derivations.one_typo,
                                    two_typos: Box::new([]),
                                    use_prefix_db: false, // false because all items from use_prefix_db haev 0 typos
                                }
                            }
                            2 => {
                                // What about split words and synonyms here?
                                WordDerivations {
                                    original: derivations.original,
                                    synonyms: Box::new([]),
                                    split_words: None,
                                    zero_typo: Box::new([]),
                                    one_typo: Box::new([]),
                                    two_typos: derivations.two_typos,
                                    use_prefix_db: false, // false because all items from use_prefix_db haev 0 typos
                                }
                            }
                            _ => panic!(),
                        };
                        if !new_derivations.is_empty() {
                            edges.push((
                                nbr_typos,
                                EdgeCondition::Conditional(conditions_interner.insert(
                                    TypoEdge::Word {
                                        derivations: derivations_interner.insert(new_derivations),
                                        nbr_typos,
                                    },
                                )),
                            ))
                        }
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
