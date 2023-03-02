pub mod db_cache;
pub mod graph_based_ranking_rule;
pub mod logger;
pub mod query_graph;
pub mod query_term;
pub mod ranking_rule_graph;
pub mod ranking_rules;
pub mod resolve_query_graph;
pub mod sort;
pub mod words;

use charabia::Tokenize;
use heed::RoTxn;
pub use query_graph::*;
pub use ranking_rules::*;
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
