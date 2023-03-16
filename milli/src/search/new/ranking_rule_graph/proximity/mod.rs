pub mod build;
pub mod compute_docids;

use std::collections::HashSet;
use std::iter::FromIterator;

use roaring::RoaringBitmap;

use super::empty_paths_cache::DeadEndPathCache;
use super::{EdgeCondition, RankingRuleGraph, RankingRuleGraphTrait};
use crate::search::new::interner::{DedupInterner, Interned, MappedInterner};
use crate::search::new::logger::SearchLogger;
use crate::search::new::query_term::{Phrase, QueryTerm};
use crate::search::new::small_bitmap::SmallBitmap;
use crate::search::new::{QueryGraph, QueryNode, SearchContext};
use crate::Result;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum WordPair {
    Words {
        phrases: Vec<Interned<Phrase>>,
        left: Interned<String>,
        right: Interned<String>,
        proximity: u8,
    },
    WordPrefix {
        phrases: Vec<Interned<Phrase>>,
        left: Interned<String>,
        right_prefix: Interned<String>,
        proximity: u8,
    },
    WordPrefixSwapped {
        left_prefix: Interned<String>,
        right: Interned<String>,
        proximity: u8,
    },
}

#[derive(Clone, PartialEq, Eq, Hash)]
pub enum ProximityCondition {
    Term { term: Interned<QueryTerm> },
    Pairs { pairs: Box<[WordPair]> },
}

pub enum ProximityGraph {}

impl RankingRuleGraphTrait for ProximityGraph {
    type EdgeCondition = ProximityCondition;

    fn resolve_edge_condition<'ctx>(
        ctx: &mut SearchContext<'ctx>,
        condition: &Self::EdgeCondition,
        universe: &RoaringBitmap,
    ) -> Result<roaring::RoaringBitmap> {
        compute_docids::compute_docids(ctx, condition, universe)
    }

    fn build_edges<'ctx>(
        ctx: &mut SearchContext<'ctx>,
        conditions_interner: &mut DedupInterner<Self::EdgeCondition>,
        source_node: &QueryNode,
        dest_node: &QueryNode,
    ) -> Result<Vec<(u8, EdgeCondition<Self::EdgeCondition>)>> {
        build::build_edges(ctx, conditions_interner, source_node, dest_node)
    }

    fn log_state(
        graph: &RankingRuleGraph<Self>,
        paths: &[Vec<Interned<ProximityCondition>>],
        empty_paths_cache: &DeadEndPathCache<Self>,
        universe: &RoaringBitmap,
        distances: &MappedInterner<Vec<(u16, SmallBitmap<ProximityCondition>)>, QueryNode>,
        cost: u16,
        logger: &mut dyn SearchLogger<QueryGraph>,
    ) {
        logger.log_proximity_state(graph, paths, empty_paths_cache, universe, distances, cost);
    }

    fn label_for_edge_condition<'ctx>(
        ctx: &mut SearchContext<'ctx>,
        edge: &Self::EdgeCondition,
    ) -> Result<String> {
        match edge {
            ProximityCondition::Term { term } => {
                let term = ctx.term_interner.get(*term);
                Ok(format!("{} : exists", ctx.word_interner.get(term.original)))
            }
            ProximityCondition::Pairs { pairs } => {
                let mut s = String::new();
                for pair in pairs.iter() {
                    match pair {
                        WordPair::Words { phrases, left, right, proximity } => {
                            let left = ctx.word_interner.get(*left);
                            let right = ctx.word_interner.get(*right);
                            if !phrases.is_empty() {
                                s.push_str(&format!("{} phrases + ", phrases.len()));
                            }
                            s.push_str(&format!("\"{left} {right}\": {proximity}\n"));
                        }
                        WordPair::WordPrefix { phrases, left, right_prefix, proximity } => {
                            let left = ctx.word_interner.get(*left);
                            let right = ctx.word_interner.get(*right_prefix);
                            if !phrases.is_empty() {
                                s.push_str(&format!("{} phrases + ", phrases.len()));
                            }
                            s.push_str(&format!("\"{left} {right}...\" : {proximity}\n"));
                        }
                        WordPair::WordPrefixSwapped { left_prefix, right, proximity } => {
                            let left = ctx.word_interner.get(*left_prefix);
                            let right = ctx.word_interner.get(*right);
                            s.push_str(&format!("\"{left}... {right}\" : {proximity}\n"));
                        }
                    }
                }
                Ok(s)
            }
        }
    }

    fn words_used_by_edge_condition<'ctx>(
        ctx: &mut SearchContext<'ctx>,
        edge: &Self::EdgeCondition,
    ) -> Result<HashSet<Interned<String>>> {
        match edge {
            ProximityCondition::Term { term } => {
                let term = ctx.term_interner.get(*term);
                Ok(HashSet::from_iter(term.all_single_words_except_prefix_db()))
            }
            ProximityCondition::Pairs { pairs } => {
                let mut set = HashSet::new();
                for pair in pairs.iter() {
                    match pair {
                        WordPair::Words { phrases: _, left, right, proximity: _ } => {
                            set.insert(*left);
                            set.insert(*right);
                        }
                        WordPair::WordPrefix { phrases: _, left, right_prefix, proximity: _ } => {
                            set.insert(*left);
                            // TODO: this is not correct, there should be another trait method for collecting the prefixes
                            // to be used with the prefix DBs
                            set.insert(*right_prefix);
                        }
                        WordPair::WordPrefixSwapped { left_prefix, right, proximity: _ } => {
                            // TODO: this is not correct, there should be another trait method for collecting the prefixes
                            // to be used with the prefix DBs
                            set.insert(*left_prefix);
                            set.insert(*right);
                        }
                    }
                }
                Ok(set)
            }
        }
    }

    fn phrases_used_by_edge_condition<'ctx>(
        ctx: &mut SearchContext<'ctx>,
        edge: &Self::EdgeCondition,
    ) -> Result<HashSet<Interned<Phrase>>> {
        match edge {
            ProximityCondition::Term { term } => {
                let term = ctx.term_interner.get(*term);
                Ok(HashSet::from_iter(term.all_phrases()))
            }
            ProximityCondition::Pairs { pairs } => {
                let mut set = HashSet::new();
                for pair in pairs.iter() {
                    match pair {
                        WordPair::Words { phrases, left: _, right: _, proximity: _ } => {
                            set.extend(phrases.iter().copied());
                        }
                        WordPair::WordPrefix {
                            phrases,
                            left: _,
                            right_prefix: _,
                            proximity: _,
                        } => {
                            set.extend(phrases.iter().copied());
                        }
                        WordPair::WordPrefixSwapped { left_prefix: _, right: _, proximity: _ } => {}
                    }
                }
                Ok(set)
            }
        }
    }
}
