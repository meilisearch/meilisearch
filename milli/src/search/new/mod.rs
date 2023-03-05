mod db_cache;
mod graph_based_ranking_rule;
mod logger;
mod query_graph;
mod query_term;
mod ranking_rule_graph;
mod ranking_rules;
mod resolve_query_graph;
mod sort;
mod words;

use charabia::Tokenize;
use heed::RoTxn;

use query_graph::{QueryGraph, QueryNode};
pub use ranking_rules::{
    execute_search, RankingRule, RankingRuleOutput, RankingRuleOutputIter,
    RankingRuleOutputIterWrapper, RankingRuleQueryTrait,
};
use roaring::RoaringBitmap;

use self::db_cache::DatabaseCache;
use self::query_term::{word_derivations, LocatedQueryTerm};
use crate::{Index, Result};

pub enum BitmapOrAllRef<'s> {
    Bitmap(&'s RoaringBitmap),
    All,
}

pub fn make_query_graph<'transaction>(
    index: &Index,
    txn: &RoTxn,
    db_cache: &mut DatabaseCache<'transaction>,
    query: &str,
) -> Result<QueryGraph> {
    assert!(!query.is_empty());
    let authorize_typos = index.authorize_typos(txn)?;
    let min_len_one_typo = index.min_word_len_one_typo(txn)?;
    let min_len_two_typos = index.min_word_len_two_typos(txn)?;

    let exact_words = index.exact_words(txn)?;
    let fst = index.words_fst(txn)?;

    // TODO: get rid of this closure
    // also, ngrams can have one typo?
    let query = LocatedQueryTerm::from_query(query.tokenize(), None, move |word, is_prefix| {
        let typos = if !authorize_typos
            || word.len() < min_len_one_typo as usize
            || exact_words.as_ref().map_or(false, |fst| fst.contains(word))
        {
            0
        } else if word.len() < min_len_two_typos as usize {
            1
        } else {
            2
        };
        word_derivations(index, txn, word, typos, is_prefix, &fst)
    })
    .unwrap();
    let graph = QueryGraph::from_query(index, txn, db_cache, query)?;
    Ok(graph)
}
