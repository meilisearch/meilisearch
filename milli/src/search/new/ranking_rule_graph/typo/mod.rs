use roaring::RoaringBitmap;

use super::empty_paths_cache::EmptyPathsCache;
use super::{EdgeCondition, RankingRuleGraph, RankingRuleGraphTrait};
use crate::search::new::interner::{Interned, Interner};
use crate::search::new::logger::SearchLogger;
use crate::search::new::query_term::{LocatedQueryTerm, QueryTerm};
use crate::search::new::small_bitmap::SmallBitmap;
use crate::search::new::{QueryGraph, QueryNode, SearchContext};
use crate::Result;

#[derive(Clone, PartialEq, Eq, Hash)]
pub struct TypoEdge {
    term: Interned<QueryTerm>,
    nbr_typos: u8,
}

pub enum TypoGraph {}

impl RankingRuleGraphTrait for TypoGraph {
    type EdgeCondition = TypoEdge;

    fn label_for_edge_condition(edge: &Self::EdgeCondition) -> String {
        format!(", {} typos", edge.nbr_typos)
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
            term_interner,
            term_docids: query_term_docids,
        } = ctx;

        let docids = universe
            & query_term_docids.get_query_term_docids(
                index,
                txn,
                db_cache,
                word_interner,
                term_interner,
                phrase_interner,
                edge.term,
            )?;

        Ok(docids)
    }

    fn build_edges<'ctx>(
        ctx: &mut SearchContext<'ctx>,
        conditions_interner: &mut Interner<Self::EdgeCondition>,
        _from_node: &QueryNode,
        to_node: &QueryNode,
    ) -> Result<Vec<(u8, EdgeCondition<Self::EdgeCondition>)>> {
        let SearchContext { term_interner, .. } = ctx;
        match to_node {
            QueryNode::Term(LocatedQueryTerm { value, positions }) => {
                let mut edges = vec![];
                // Ngrams have a base typo cost
                // 2-gram -> equivalent to 1 typo
                // 3-gram -> equivalent to 2 typos
                let base_cost = positions.len().max(2) as u8;

                for nbr_typos in 0..=2 {
                    let term = term_interner.get(*value).clone();
                    let new_term = match nbr_typos {
                        0 => QueryTerm {
                            original: term.original,
                            is_prefix: term.is_prefix,
                            zero_typo: term.zero_typo,
                            prefix_of: term.prefix_of,
                            synonyms: term.synonyms,
                            split_words: None,
                            one_typo: Box::new([]),
                            two_typos: Box::new([]),
                            use_prefix_db: term.use_prefix_db,
                            is_ngram: term.is_ngram,
                            phrase: term.phrase,
                        },
                        1 => {
                            // What about split words and synonyms here?
                            QueryTerm {
                                original: term.original,
                                is_prefix: false,
                                zero_typo: None,
                                prefix_of: Box::new([]),
                                synonyms: Box::new([]),
                                split_words: term.split_words,
                                one_typo: term.one_typo,
                                two_typos: Box::new([]),
                                use_prefix_db: None, // false because all items from use_prefix_db have 0 typos
                                is_ngram: term.is_ngram,
                                phrase: None,
                            }
                        }
                        2 => {
                            // What about split words and synonyms here?
                            QueryTerm {
                                original: term.original,
                                zero_typo: None,
                                is_prefix: false,
                                prefix_of: Box::new([]),
                                synonyms: Box::new([]),
                                split_words: None,
                                one_typo: Box::new([]),
                                two_typos: term.two_typos,
                                use_prefix_db: None, // false because all items from use_prefix_db have 0 typos
                                is_ngram: term.is_ngram,
                                phrase: None,
                            }
                        }
                        _ => panic!(),
                    };
                    if !new_term.is_empty() {
                        edges.push((
                            nbr_typos as u8 + base_cost,
                            EdgeCondition::Conditional(conditions_interner.insert(TypoEdge {
                                term: term_interner.insert(new_term),
                                nbr_typos: nbr_typos as u8,
                            })),
                        ))
                    }
                }
                Ok(edges)
            }
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
