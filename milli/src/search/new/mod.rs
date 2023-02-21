pub mod db_cache;
pub mod graph_based_ranking_rule;
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

use self::{
    db_cache::DatabaseCache,
    query_term::{word_derivations, LocatedQueryTerm},
};
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
    let fst = index.words_fst(txn).unwrap();
    let query = LocatedQueryTerm::from_query(query.tokenize(), None, |word, is_prefix| {
        word_derivations(
            index,
            txn,
            word,
            if word.len() < 4 {
                0
            } else if word.len() < 100 {
                1
            } else {
                2
            },
            is_prefix,
            &fst,
        )
    })
    .unwrap();
    let graph = QueryGraph::from_query(index, txn, db_cache, query)?;
    Ok(graph)
}
