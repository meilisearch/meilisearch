mod bucket_sort;
mod db_cache;
mod distinct;
mod geo_sort;
mod graph_based_ranking_rule;
mod interner;
mod limits;
mod logger;
pub mod matches;
mod query_graph;
mod query_term;
mod ranking_rule_graph;
mod ranking_rules;
mod resolve_query_graph;
mod small_bitmap;

mod exact_attribute;
mod sort;

#[cfg(test)]
mod tests;

use std::collections::HashSet;

use bucket_sort::{bucket_sort, BucketSortOutput};
use charabia::TokenizerBuilder;
use db_cache::DatabaseCache;
use exact_attribute::ExactAttribute;
use graph_based_ranking_rule::{Exactness, Fid, Position, Proximity, Typo};
use heed::RoTxn;
use interner::{DedupInterner, Interner};
pub use logger::visual::VisualSearchLogger;
pub use logger::{DefaultSearchLogger, SearchLogger};
use query_graph::{QueryGraph, QueryNode};
use query_term::{located_query_terms_from_tokens, LocatedQueryTerm, Phrase, QueryTerm};
use ranking_rules::{
    BoxRankingRule, PlaceholderQuery, RankingRule, RankingRuleOutput, RankingRuleQueryTrait,
};
use resolve_query_graph::{compute_query_graph_docids, PhraseDocIdsCache};
use roaring::RoaringBitmap;
use sort::Sort;

use self::geo_sort::GeoSort;
pub use self::geo_sort::Strategy as GeoSortStrategy;
use self::graph_based_ranking_rule::Words;
use self::interner::Interned;
use crate::score_details::{ScoreDetails, ScoringStrategy};
use crate::search::new::distinct::apply_distinct_rule;
use crate::{AscDesc, DocumentId, Filter, Index, Member, Result, TermsMatchingStrategy, UserError};

/// A structure used throughout the execution of a search query.
pub struct SearchContext<'ctx> {
    pub index: &'ctx Index,
    pub txn: &'ctx RoTxn<'ctx>,
    pub db_cache: DatabaseCache<'ctx>,
    pub word_interner: DedupInterner<String>,
    pub phrase_interner: DedupInterner<Phrase>,
    pub term_interner: Interner<QueryTerm>,
    pub phrase_docids: PhraseDocIdsCache,
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
            phrase_docids: <_>::default(),
        }
    }
}

#[derive(Clone, Copy, PartialEq, PartialOrd, Ord, Eq)]
pub enum Word {
    Original(Interned<String>),
    Derived(Interned<String>),
}

impl Word {
    pub fn interned(&self) -> Interned<String> {
        match self {
            Word::Original(word) => *word,
            Word::Derived(word) => *word,
        }
    }
}

/// Apply the [`TermsMatchingStrategy`] to the query graph and resolve it.
fn resolve_maximally_reduced_query_graph(
    ctx: &mut SearchContext,
    universe: &RoaringBitmap,
    query_graph: &QueryGraph,
    matching_strategy: TermsMatchingStrategy,
    logger: &mut dyn SearchLogger<QueryGraph>,
) -> Result<RoaringBitmap> {
    let mut graph = query_graph.clone();

    let nodes_to_remove = match matching_strategy {
        TermsMatchingStrategy::Last => query_graph
            .removal_order_for_terms_matching_strategy_last(ctx)
            .iter()
            .flat_map(|x| x.iter())
            .collect(),
        TermsMatchingStrategy::All => vec![],
    };
    graph.remove_nodes_keep_edges(&nodes_to_remove);

    logger.query_for_initial_universe(&graph);
    let docids = compute_query_graph_docids(ctx, &graph, universe)?;

    Ok(docids)
}

fn resolve_universe(
    ctx: &mut SearchContext,
    initial_universe: &RoaringBitmap,
    query_graph: &QueryGraph,
    matching_strategy: TermsMatchingStrategy,
    logger: &mut dyn SearchLogger<QueryGraph>,
) -> Result<RoaringBitmap> {
    resolve_maximally_reduced_query_graph(
        ctx,
        initial_universe,
        query_graph,
        matching_strategy,
        logger,
    )
}

/// Return the list of initialised ranking rules to be used for a placeholder search.
fn get_ranking_rules_for_placeholder_search<'ctx>(
    ctx: &SearchContext<'ctx>,
    sort_criteria: &Option<Vec<AscDesc>>,
    geo_strategy: geo_sort::Strategy,
) -> Result<Vec<BoxRankingRule<'ctx, PlaceholderQuery>>> {
    let mut sort = false;
    let mut sorted_fields = HashSet::new();
    let mut geo_sorted = false;
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
                resolve_sort_criteria(
                    sort_criteria,
                    ctx,
                    &mut ranking_rules,
                    &mut sorted_fields,
                    &mut geo_sorted,
                    geo_strategy,
                )?;
                sort = true;
            }
            crate::Criterion::Asc(field_name) => {
                if sorted_fields.contains(&field_name) {
                    continue;
                }
                sorted_fields.insert(field_name.clone());
                ranking_rules.push(Box::new(Sort::new(ctx.index, ctx.txn, field_name, true)?));
            }
            crate::Criterion::Desc(field_name) => {
                if sorted_fields.contains(&field_name) {
                    continue;
                }
                sorted_fields.insert(field_name.clone());
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
    geo_strategy: geo_sort::Strategy,
    terms_matching_strategy: TermsMatchingStrategy,
) -> Result<Vec<BoxRankingRule<'ctx, QueryGraph>>> {
    // query graph search
    let mut words = false;
    let mut typo = false;
    let mut proximity = false;
    let mut sort = false;
    let mut attribute = false;
    let mut exactness = false;
    let mut sorted_fields = HashSet::new();
    let mut geo_sorted = false;

    // Don't add the `words` ranking rule if the term matching strategy is `All`
    if matches!(terms_matching_strategy, TermsMatchingStrategy::All) {
        words = true;
    }

    let mut ranking_rules: Vec<BoxRankingRule<QueryGraph>> = vec![];
    let settings_ranking_rules = ctx.index.criteria(ctx.txn)?;
    for rr in settings_ranking_rules {
        // Add Words before any of: typo, proximity, attribute
        match rr {
            crate::Criterion::Typo
            | crate::Criterion::Attribute
            | crate::Criterion::Proximity
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
                ranking_rules.push(Box::new(Typo::new(None)));
            }
            crate::Criterion::Proximity => {
                if proximity {
                    continue;
                }
                proximity = true;
                ranking_rules.push(Box::new(Proximity::new(None)));
            }
            crate::Criterion::Attribute => {
                if attribute {
                    continue;
                }
                attribute = true;
                ranking_rules.push(Box::new(Fid::new(None)));
                ranking_rules.push(Box::new(Position::new(None)));
            }
            crate::Criterion::Sort => {
                if sort {
                    continue;
                }
                resolve_sort_criteria(
                    sort_criteria,
                    ctx,
                    &mut ranking_rules,
                    &mut sorted_fields,
                    &mut geo_sorted,
                    geo_strategy,
                )?;
                sort = true;
            }
            crate::Criterion::Exactness => {
                if exactness {
                    continue;
                }
                ranking_rules.push(Box::new(ExactAttribute::new()));
                ranking_rules.push(Box::new(Exactness::new()));
                exactness = true;
            }
            crate::Criterion::Asc(field_name) => {
                if sorted_fields.contains(&field_name) {
                    continue;
                }
                sorted_fields.insert(field_name.clone());
                ranking_rules.push(Box::new(Sort::new(ctx.index, ctx.txn, field_name, true)?));
            }
            crate::Criterion::Desc(field_name) => {
                if sorted_fields.contains(&field_name) {
                    continue;
                }
                sorted_fields.insert(field_name.clone());
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
    sorted_fields: &mut HashSet<String>,
    geo_sorted: &mut bool,
    geo_strategy: geo_sort::Strategy,
) -> Result<()> {
    let sort_criteria = sort_criteria.clone().unwrap_or_default();
    ranking_rules.reserve(sort_criteria.len());
    for criterion in sort_criteria {
        match criterion {
            AscDesc::Asc(Member::Field(field_name)) => {
                if sorted_fields.contains(&field_name) {
                    continue;
                }
                sorted_fields.insert(field_name.clone());
                ranking_rules.push(Box::new(Sort::new(ctx.index, ctx.txn, field_name, true)?));
            }
            AscDesc::Desc(Member::Field(field_name)) => {
                if sorted_fields.contains(&field_name) {
                    continue;
                }
                sorted_fields.insert(field_name.clone());
                ranking_rules.push(Box::new(Sort::new(ctx.index, ctx.txn, field_name, false)?));
            }
            AscDesc::Asc(Member::Geo(point)) => {
                if *geo_sorted {
                    continue;
                }
                let geo_faceted_docids = ctx.index.geo_faceted_documents_ids(ctx.txn)?;
                ranking_rules.push(Box::new(GeoSort::new(
                    geo_strategy,
                    geo_faceted_docids,
                    point,
                    true,
                )?));
            }
            AscDesc::Desc(Member::Geo(point)) => {
                if *geo_sorted {
                    continue;
                }
                let geo_faceted_docids = ctx.index.geo_faceted_documents_ids(ctx.txn)?;
                ranking_rules.push(Box::new(GeoSort::new(
                    geo_strategy,
                    geo_faceted_docids,
                    point,
                    false,
                )?));
            }
        };
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
pub fn execute_search(
    ctx: &mut SearchContext,
    query: &Option<String>,
    terms_matching_strategy: TermsMatchingStrategy,
    scoring_strategy: ScoringStrategy,
    exhaustive_number_hits: bool,
    filters: &Option<Filter>,
    sort_criteria: &Option<Vec<AscDesc>>,
    geo_strategy: geo_sort::Strategy,
    from: usize,
    length: usize,
    words_limit: Option<usize>,
    placeholder_search_logger: &mut dyn SearchLogger<PlaceholderQuery>,
    query_graph_logger: &mut dyn SearchLogger<QueryGraph>,
) -> Result<PartialSearchResult> {
    let mut universe = if let Some(filters) = filters {
        filters.evaluate(ctx.txn, ctx.index)?
    } else {
        ctx.index.documents_ids(ctx.txn)?
    };

    check_sort_criteria(ctx, sort_criteria.as_ref())?;

    let mut located_query_terms = None;

    let query_terms = if let Some(query) = query {
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

        let query_terms = located_query_terms_from_tokens(ctx, tokens, words_limit)?;
        if query_terms.is_empty() {
            // Do a placeholder search instead
            None
        } else {
            Some(query_terms)
        }
    } else {
        None
    };
    let bucket_sort_output = if let Some(query_terms) = query_terms {
        let (graph, new_located_query_terms) = QueryGraph::from_query(ctx, &query_terms)?;
        located_query_terms = Some(new_located_query_terms);

        let ranking_rules = get_ranking_rules_for_query_graph_search(
            ctx,
            sort_criteria,
            geo_strategy,
            terms_matching_strategy,
        )?;

        universe =
            resolve_universe(ctx, &universe, &graph, terms_matching_strategy, query_graph_logger)?;

        bucket_sort(
            ctx,
            ranking_rules,
            &graph,
            &universe,
            from,
            length,
            scoring_strategy,
            query_graph_logger,
        )?
    } else {
        let ranking_rules =
            get_ranking_rules_for_placeholder_search(ctx, sort_criteria, geo_strategy)?;
        bucket_sort(
            ctx,
            ranking_rules,
            &PlaceholderQuery,
            &universe,
            from,
            length,
            scoring_strategy,
            placeholder_search_logger,
        )?
    };

    let BucketSortOutput { docids, scores, mut all_candidates } = bucket_sort_output;

    let fields_ids_map = ctx.index.fields_ids_map(ctx.txn)?;

    // The candidates is the universe unless the exhaustive number of hits
    // is requested and a distinct attribute is set.
    if exhaustive_number_hits {
        if let Some(f) = ctx.index.distinct_field(ctx.txn)? {
            if let Some(distinct_fid) = fields_ids_map.id(f) {
                all_candidates = apply_distinct_rule(ctx, distinct_fid, &all_candidates)?.remaining;
            }
        }
    }

    Ok(PartialSearchResult {
        candidates: all_candidates,
        document_scores: scores,
        documents_ids: docids,
        located_query_terms,
    })
}

fn check_sort_criteria(ctx: &SearchContext, sort_criteria: Option<&Vec<AscDesc>>) -> Result<()> {
    let sort_criteria = if let Some(sort_criteria) = sort_criteria {
        sort_criteria
    } else {
        return Ok(());
    };

    if sort_criteria.is_empty() {
        return Ok(());
    }

    // We check that the sort ranking rule exists and throw an
    // error if we try to use it and that it doesn't.
    let sort_ranking_rule_missing = !ctx.index.criteria(ctx.txn)?.contains(&crate::Criterion::Sort);
    if sort_ranking_rule_missing {
        return Err(UserError::SortRankingRuleMissing.into());
    }

    // We check that we are allowed to use the sort criteria, we check
    // that they are declared in the sortable fields.
    let sortable_fields = ctx.index.sortable_fields(ctx.txn)?;
    for asc_desc in sort_criteria {
        match asc_desc.member() {
            Member::Field(ref field) if !crate::is_faceted(field, &sortable_fields) => {
                return Err(UserError::InvalidSortableAttribute {
                    field: field.to_string(),
                    valid_fields: sortable_fields.into_iter().collect(),
                })?
            }
            Member::Geo(_) if !sortable_fields.contains("_geo") => {
                return Err(UserError::InvalidSortableAttribute {
                    field: "_geo".to_string(),
                    valid_fields: sortable_fields.into_iter().collect(),
                })?
            }
            _ => (),
        }
    }

    Ok(())
}

pub struct PartialSearchResult {
    pub located_query_terms: Option<Vec<LocatedQueryTerm>>,
    pub candidates: RoaringBitmap,
    pub documents_ids: Vec<DocumentId>,
    pub document_scores: Vec<Vec<ScoreDetails>>,
}
