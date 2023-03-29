mod db_cache;
mod distinct;
mod graph_based_ranking_rule;
mod interner;
mod logger;
mod query_graph;
mod query_term;
mod ranking_rule_graph;
mod ranking_rules;
mod resolve_query_graph;
// TODO: documentation + comments
mod small_bitmap;
// TODO: documentation + comments
// implementation is currently an adaptation of the previous implementation to fit with the new model
mod sort;
// TODO: documentation + comments
mod words;

use std::collections::{BTreeSet, HashSet};

use charabia::TokenizerBuilder;
use db_cache::DatabaseCache;
use graph_based_ranking_rule::{Proximity, Typo};
use heed::RoTxn;
use interner::DedupInterner;
pub use logger::detailed::DetailedSearchLogger;
pub use logger::{DefaultSearchLogger, SearchLogger};
use query_graph::{QueryGraph, QueryNode, QueryNodeData};
use query_term::{located_query_terms_from_string, Phrase, QueryTerm};
use ranking_rules::{bucket_sort, PlaceholderQuery, RankingRuleOutput, RankingRuleQueryTrait};
use resolve_query_graph::{resolve_query_graph, QueryTermDocIdsCache};
use roaring::RoaringBitmap;
use words::Words;

use self::ranking_rules::{BoxRankingRule, RankingRule};
use self::sort::Sort;
use crate::{
    AscDesc, CriterionError, Filter, Index, MatchingWords, Member, Result, SearchResult,
    TermsMatchingStrategy,
};

/// A structure used throughout the execution of a search query.
pub struct SearchContext<'ctx> {
    pub index: &'ctx Index,
    pub txn: &'ctx RoTxn<'ctx>,
    pub db_cache: DatabaseCache<'ctx>,
    pub word_interner: DedupInterner<String>,
    pub phrase_interner: DedupInterner<Phrase>,
    pub term_interner: DedupInterner<QueryTerm>,
    // think about memory usage of that field (roaring bitmaps in a hashmap)
    pub term_docids: QueryTermDocIdsCache,
}
impl<'ctx> SearchContext<'ctx> {
    pub fn new(index: &'ctx Index, txn: &'ctx RoTxn<'ctx>) -> Self {
        Self {
            index,
            txn,
            db_cache: <_>::default(),
            word_interner: <_>::default(),
            phrase_interner: <_>::default(),
            term_interner: <_>::default(),
            term_docids: <_>::default(),
        }
    }
}

/// Apply the [`TermsMatchingStrategy`] to the query graph and resolve it.
#[allow(clippy::too_many_arguments)]
fn resolve_maximally_reduced_query_graph(
    ctx: &mut SearchContext,
    universe: &RoaringBitmap,
    query_graph: &QueryGraph,
    matching_strategy: TermsMatchingStrategy,
    logger: &mut dyn SearchLogger<QueryGraph>,
) -> Result<RoaringBitmap> {
    let mut graph = query_graph.clone();
    let mut positions_to_remove = match matching_strategy {
        TermsMatchingStrategy::Last => {
            let mut all_positions = BTreeSet::new();
            for (_, n) in query_graph.nodes.iter() {
                match &n.data {
                    QueryNodeData::Term(term) => {
                        all_positions.extend(term.positions.clone());
                    }
                    QueryNodeData::Deleted | QueryNodeData::Start | QueryNodeData::End => {}
                }
            }
            all_positions.into_iter().collect()
        }
        TermsMatchingStrategy::All => vec![],
    };
    // don't remove the first term
    if !positions_to_remove.is_empty() {
        positions_to_remove.remove(0);
    }
    loop {
        if positions_to_remove.is_empty() {
            break;
        } else {
            let position_to_remove = positions_to_remove.pop().unwrap();
            let _ = graph.remove_words_starting_at_position(position_to_remove);
        }
    }
    logger.query_for_universe(&graph);
    let docids = resolve_query_graph(ctx, &graph, universe)?;

    Ok(docids)
}

/// Return the list of initialised ranking rules to be used for a placeholder search.
fn get_ranking_rules_for_placeholder_search<'ctx>(
    ctx: &SearchContext<'ctx>,
    sort_criteria: &Option<Vec<AscDesc>>,
) -> Result<Vec<BoxRankingRule<'ctx, PlaceholderQuery>>> {
    let mut sort = false;
    let mut asc = HashSet::new();
    let mut desc = HashSet::new();
    let mut ranking_rules: Vec<BoxRankingRule<PlaceholderQuery>> = vec![];
    let settings_ranking_rules = ctx.index.criteria(ctx.txn)?;
    for rr in settings_ranking_rules {
        match rr {
            // These rules need a query to have an effect; ignore them in placeholder search
            crate::Criterion::Words
            | crate::Criterion::Typo
            | crate::Criterion::Attribute
            | crate::Criterion::Proximity
            | crate::Criterion::Exactness => continue,
            crate::Criterion::Sort => {
                if sort {
                    continue;
                }
                resolve_sort_criteria(sort_criteria, ctx, &mut ranking_rules, &mut asc, &mut desc)?;
                sort = true;
            }
            crate::Criterion::Asc(field_name) => {
                if asc.contains(&field_name) {
                    continue;
                }
                asc.insert(field_name.clone());
                ranking_rules.push(Box::new(Sort::new(ctx.index, ctx.txn, field_name, true)?));
            }
            crate::Criterion::Desc(field_name) => {
                if desc.contains(&field_name) {
                    continue;
                }
                desc.insert(field_name.clone());
                ranking_rules.push(Box::new(Sort::new(ctx.index, ctx.txn, field_name, false)?));
            }
        }
    }
    Ok(ranking_rules)
}

/// Return the list of initialised ranking rules to be used for a query graph search.
fn get_ranking_rules_for_query_graph_search<'ctx>(
    ctx: &SearchContext<'ctx>,
    sort_criteria: &Option<Vec<AscDesc>>,
    terms_matching_strategy: TermsMatchingStrategy,
) -> Result<Vec<BoxRankingRule<'ctx, QueryGraph>>> {
    // query graph search
    let mut words = false;
    let mut typo = false;
    let mut proximity = false;
    let mut sort = false;
    let attribute = false;
    let exactness = false;
    let mut asc = HashSet::new();
    let mut desc = HashSet::new();

    let mut ranking_rules: Vec<BoxRankingRule<QueryGraph>> = vec![];
    let settings_ranking_rules = ctx.index.criteria(ctx.txn)?;
    for rr in settings_ranking_rules {
        // Add Words before any of: typo, proximity, attribute, exactness
        match rr {
            crate::Criterion::Typo
            | crate::Criterion::Attribute
            | crate::Criterion::Proximity
            // TODO: no exactness
            | crate::Criterion::Exactness => {
                if !words {
                    ranking_rules.push(Box::new(Words::new(terms_matching_strategy)));
                    words = true;
                }
            }
            _ => {}
        }
        match rr {
            crate::Criterion::Words => {
                if words {
                    continue;
                }
                ranking_rules.push(Box::new(Words::new(terms_matching_strategy)));
                words = true;
            }
            crate::Criterion::Typo => {
                if typo {
                    continue;
                }
                typo = true;
                ranking_rules.push(Box::<Typo>::default());
            }
            crate::Criterion::Proximity => {
                if proximity {
                    continue;
                }
                proximity = true;
                ranking_rules.push(Box::<Proximity>::default());
            }
            crate::Criterion::Attribute => {
                if attribute {
                    continue;
                }
                // todo!();
                // attribute = false;
            }
            crate::Criterion::Sort => {
                if sort {
                    continue;
                }
                resolve_sort_criteria(sort_criteria, ctx, &mut ranking_rules, &mut asc, &mut desc)?;
                sort = true;
            }
            crate::Criterion::Exactness => {
                if exactness {
                    continue;
                }
                // todo!();
                // exactness = false;
            }
            crate::Criterion::Asc(field_name) => {
                if asc.contains(&field_name) {
                    continue;
                }
                asc.insert(field_name.clone());
                ranking_rules.push(Box::new(Sort::new(ctx.index, ctx.txn, field_name, true)?));
            }
            crate::Criterion::Desc(field_name) => {
                if desc.contains(&field_name) {
                    continue;
                }
                desc.insert(field_name.clone());
                ranking_rules.push(Box::new(Sort::new(ctx.index, ctx.txn, field_name, false)?));
            }
        }
    }
    Ok(ranking_rules)
}

fn resolve_sort_criteria<'ctx, Query: RankingRuleQueryTrait>(
    sort_criteria: &Option<Vec<AscDesc>>,
    ctx: &SearchContext<'ctx>,
    ranking_rules: &mut Vec<BoxRankingRule<'ctx, Query>>,
    asc: &mut HashSet<String>,
    desc: &mut HashSet<String>,
) -> Result<()> {
    let sort_criteria = sort_criteria.clone().unwrap_or_default();
    ranking_rules.reserve(sort_criteria.len());
    for criterion in sort_criteria {
        let sort_ranking_rule = match criterion {
            AscDesc::Asc(Member::Field(field_name)) => {
                if asc.contains(&field_name) {
                    continue;
                }
                asc.insert(field_name.clone());
                Sort::new(ctx.index, ctx.txn, field_name, true)?
            }
            AscDesc::Desc(Member::Field(field_name)) => {
                if desc.contains(&field_name) {
                    continue;
                }
                desc.insert(field_name.clone());
                Sort::new(ctx.index, ctx.txn, field_name, false)?
            }
            _ => {
                return Err(
                    CriterionError::ReservedNameForSort { name: "_geoPoint".to_string() }.into()
                )
            }
        };
        ranking_rules.push(Box::new(sort_ranking_rule));
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
pub fn execute_search(
    ctx: &mut SearchContext,
    query: &Option<String>,
    terms_matching_strategy: TermsMatchingStrategy,
    filters: &Option<Filter>,
    sort_criteria: &Option<Vec<AscDesc>>,
    from: usize,
    length: usize,
    words_limit: Option<usize>,
    placeholder_search_logger: &mut dyn SearchLogger<PlaceholderQuery>,
    query_graph_logger: &mut dyn SearchLogger<QueryGraph>,
) -> Result<SearchResult> {
    let mut universe = if let Some(filters) = filters {
        filters.evaluate(ctx.txn, ctx.index)?
    } else {
        ctx.index.documents_ids(ctx.txn)?
    };

    let documents_ids = if let Some(query) = query {
        // We make sure that the analyzer is aware of the stop words
        // this ensures that the query builder is able to properly remove them.
        let mut tokbuilder = TokenizerBuilder::new();
        let stop_words = ctx.index.stop_words(ctx.txn)?;
        if let Some(ref stop_words) = stop_words {
            tokbuilder.stop_words(stop_words);
        }

        let script_lang_map = ctx.index.script_language(ctx.txn)?;
        if !script_lang_map.is_empty() {
            tokbuilder.allow_list(&script_lang_map);
        }

        let tokenizer = tokbuilder.build();
        let tokens = tokenizer.tokenize(query);

        let query_terms = located_query_terms_from_string(ctx, tokens, words_limit)?;
        let graph = QueryGraph::from_query(ctx, query_terms)?;

        universe = resolve_maximally_reduced_query_graph(
            ctx,
            &universe,
            &graph,
            terms_matching_strategy,
            query_graph_logger,
        )?;

        let ranking_rules =
            get_ranking_rules_for_query_graph_search(ctx, sort_criteria, terms_matching_strategy)?;

        bucket_sort(ctx, ranking_rules, &graph, &universe, from, length, query_graph_logger)?
    } else {
        let ranking_rules = get_ranking_rules_for_placeholder_search(ctx, sort_criteria)?;
        bucket_sort(
            ctx,
            ranking_rules,
            &PlaceholderQuery,
            &universe,
            from,
            length,
            placeholder_search_logger,
        )?
    };

    Ok(SearchResult {
        // TODO: correct matching words
        matching_words: MatchingWords::default(),
        // TODO: candidates with distinct
        candidates: universe,
        documents_ids,
    })
}
