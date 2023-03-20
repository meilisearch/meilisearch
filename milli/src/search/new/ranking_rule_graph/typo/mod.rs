use std::collections::HashSet;
use std::fmt::Write;
use std::iter::FromIterator;

use roaring::RoaringBitmap;

use super::{DeadEndsCache, RankingRuleGraph, RankingRuleGraphTrait};
use crate::search::new::interner::{DedupInterner, Interned, MappedInterner};
use crate::search::new::logger::SearchLogger;
use crate::search::new::query_graph::QueryNodeData;
use crate::search::new::query_term::{LocatedQueryTerm, Phrase, QueryTerm};
use crate::search::new::{QueryGraph, QueryNode, SearchContext};
use crate::Result;

#[derive(Clone, PartialEq, Eq, Hash)]
pub struct TypoCondition {
    term: Interned<QueryTerm>,
}

pub enum TypoGraph {}

impl RankingRuleGraphTrait for TypoGraph {
    type Condition = TypoCondition;

    fn resolve_condition<'db_cache, 'ctx>(
        ctx: &mut SearchContext<'ctx>,
        condition: &Self::Condition,
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
                condition.term,
            )?;

        Ok(docids)
    }

    fn build_edges<'ctx>(
        ctx: &mut SearchContext<'ctx>,
        conditions_interner: &mut DedupInterner<Self::Condition>,
        _from_node: &QueryNode,
        to_node: &QueryNode,
    ) -> Result<Vec<(u8, Option<Interned<Self::Condition>>)>> {
        let SearchContext { term_interner, .. } = ctx;
        match &to_node.data {
            QueryNodeData::Term(LocatedQueryTerm { value, positions }) => {
                let mut edges = vec![];
                // Ngrams have a base typo cost
                // 2-gram -> equivalent to 1 typo
                // 3-gram -> equivalent to 2 typos
                let base_cost = positions.len().min(2) as u8;

                for nbr_typos in 0..=2 {
                    let term = term_interner.get(*value).clone();
                    let new_term = match nbr_typos {
                        0 => QueryTerm {
                            original: term.original,
                            is_prefix: term.is_prefix,
                            zero_typo: term.zero_typo,
                            prefix_of: term.prefix_of,
                            // TOOD: debatable
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
                            Some(
                                conditions_interner
                                    .insert(TypoCondition { term: term_interner.insert(new_term) }),
                            ),
                        ))
                    }
                }
                Ok(edges)
            }
            QueryNodeData::End => Ok(vec![(0, None)]),
            QueryNodeData::Deleted | QueryNodeData::Start => panic!(),
        }
    }

    fn log_state(
        graph: &RankingRuleGraph<Self>,
        paths: &[Vec<Interned<TypoCondition>>],
        dead_ends_cache: &DeadEndsCache<TypoCondition>,
        universe: &RoaringBitmap,
        distances: &MappedInterner<QueryNode, Vec<u16>>,
        cost: u16,
        logger: &mut dyn SearchLogger<QueryGraph>,
    ) {
        logger.log_typo_state(graph, paths, dead_ends_cache, universe, distances, cost);
    }

    fn label_for_condition<'ctx>(
        ctx: &mut SearchContext<'ctx>,
        condition: &Self::Condition,
    ) -> Result<String> {
        let TypoCondition { term } = condition;
        let term = ctx.term_interner.get(*term);
        let QueryTerm {
            original: _,
            is_ngram: _,
            is_prefix: _,
            phrase,
            zero_typo,
            prefix_of,
            synonyms,
            split_words,
            one_typo,
            two_typos,
            use_prefix_db,
        } = term;
        let mut s = String::new();
        if let Some(phrase) = phrase {
            let phrase = ctx.phrase_interner.get(*phrase).description(&ctx.word_interner);
            writeln!(&mut s, "\"{phrase}\" : phrase").unwrap();
        }
        if let Some(w) = zero_typo {
            let w = ctx.word_interner.get(*w);
            writeln!(&mut s, "\"{w}\" : 0 typo").unwrap();
        }
        for w in prefix_of.iter() {
            let w = ctx.word_interner.get(*w);
            writeln!(&mut s, "\"{w}\" : prefix").unwrap();
        }
        for w in one_typo.iter() {
            let w = ctx.word_interner.get(*w);
            writeln!(&mut s, "\"{w}\" : 1 typo").unwrap();
        }
        for w in two_typos.iter() {
            let w = ctx.word_interner.get(*w);
            writeln!(&mut s, "\"{w}\" : 2 typos").unwrap();
        }
        if let Some(phrase) = split_words {
            let phrase = ctx.phrase_interner.get(*phrase).description(&ctx.word_interner);
            writeln!(&mut s, "\"{phrase}\" : split words").unwrap();
        }
        for phrase in synonyms.iter() {
            let phrase = ctx.phrase_interner.get(*phrase).description(&ctx.word_interner);
            writeln!(&mut s, "\"{phrase}\" : synonym").unwrap();
        }
        if let Some(w) = use_prefix_db {
            let w = ctx.word_interner.get(*w);
            writeln!(&mut s, "\"{w}\" : use prefix db").unwrap();
        }

        Ok(s)
    }

    fn words_used_by_condition<'ctx>(
        ctx: &mut SearchContext<'ctx>,
        condition: &Self::Condition,
    ) -> Result<HashSet<Interned<String>>> {
        let TypoCondition { term, .. } = condition;
        let term = ctx.term_interner.get(*term);
        Ok(HashSet::from_iter(term.all_single_words_except_prefix_db()))
    }

    fn phrases_used_by_condition<'ctx>(
        ctx: &mut SearchContext<'ctx>,
        condition: &Self::Condition,
    ) -> Result<HashSet<Interned<Phrase>>> {
        let TypoCondition { term, .. } = condition;
        let term = ctx.term_interner.get(*term);
        Ok(HashSet::from_iter(term.all_phrases()))
    }
}
