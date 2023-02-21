use std::collections::BTreeMap;

use super::ProximityEdge;
use crate::new::db_cache::DatabaseCache;
use crate::new::query_term::{LocatedQueryTerm, QueryTerm, WordDerivations};
use crate::new::ranking_rule_graph::proximity::WordPair;
use crate::new::ranking_rule_graph::{Edge, EdgeDetails};
use crate::new::QueryNode;
use crate::{Index, Result};
use heed::RoTxn;
use itertools::Itertools;

pub fn visit_from_node(from_node: &QueryNode) -> Result<Option<(WordDerivations, i8)>> {
    Ok(Some(match from_node {
        QueryNode::Term(LocatedQueryTerm { value: value1, positions: pos1 }) => {
            match value1 {
                QueryTerm::Word { derivations } => (derivations.clone(), *pos1.end()),
                QueryTerm::Phrase(phrase1) => {
                    // TODO: remove second unwrap
                    let original = phrase1.last().unwrap().as_ref().unwrap().clone();
                    (
                        WordDerivations {
                            original: original.clone(),
                            zero_typo: vec![original],
                            one_typo: vec![],
                            two_typos: vec![],
                            use_prefix_db: false,
                        },
                        *pos1.end(),
                    )
                }
            }
        }
        QueryNode::Start => (
            WordDerivations {
                original: String::new(),
                zero_typo: vec![],
                one_typo: vec![],
                two_typos: vec![],
                use_prefix_db: false,
            },
            -100,
        ),
        _ => return Ok(None),
    }))
}

pub fn visit_to_node<'transaction, 'from_data>(
    index: &Index,
    txn: &'transaction RoTxn,
    db_cache: &mut DatabaseCache<'transaction>,
    to_node: &QueryNode,
    from_node_data: &'from_data (WordDerivations, i8),
) -> Result<Vec<(u8, EdgeDetails<ProximityEdge>)>> {
    let (derivations1, pos1) = from_node_data;
    let term2 = match &to_node {
        QueryNode::End => return Ok(vec![(0, EdgeDetails::Unconditional)]),
        QueryNode::Deleted | QueryNode::Start => return Ok(vec![]),
        QueryNode::Term(term) => term,
    };
    let LocatedQueryTerm { value: value2, positions: pos2 } = term2;

    let (derivations2, pos2, ngram_len2) = match value2 {
        QueryTerm::Word { derivations } => (derivations.clone(), *pos2.start(), pos2.len()),
        QueryTerm::Phrase(phrase2) => {
            // TODO: remove second unwrap
            let original = phrase2.last().unwrap().as_ref().unwrap().clone();
            (
                WordDerivations {
                    original: original.clone(),
                    zero_typo: vec![original],
                    one_typo: vec![],
                    two_typos: vec![],
                    use_prefix_db: false,
                },
                *pos2.start(),
                1,
            )
        }
    };

    // TODO: here we would actually do it for each combination of word1 and word2
    // and take the union of them
    if pos1 + 1 != pos2 {
        // TODO: how should this actually be handled?
        // We want to effectively ignore this pair of terms
        // Unconditionally walk through the edge without computing the docids
        // But also what should the cost be?
        return Ok(vec![(0, EdgeDetails::Unconditional)]);
    }

    let updb1 = derivations1.use_prefix_db;
    let updb2 = derivations2.use_prefix_db;

    // left term cannot be a prefix
    assert!(!updb1);

    let derivations1 = derivations1.all_derivations_except_prefix_db();
    let original_word_2 = derivations2.original.clone();
    let mut cost_proximity_word_pairs = BTreeMap::<u8, BTreeMap<u8, Vec<WordPair>>>::new();

    if updb2 {
        for word1 in derivations1.clone() {
            for proximity in 0..(7 - ngram_len2) {
                let cost = (proximity + ngram_len2 - 1) as u8;
                if db_cache
                    .get_word_prefix_pair_proximity_docids(
                        index,
                        txn,
                        word1,
                        original_word_2.as_str(),
                        proximity as u8,
                    )?
                    .is_some()
                {
                    cost_proximity_word_pairs
                        .entry(cost)
                        .or_default()
                        .entry(proximity as u8)
                        .or_default()
                        .push(WordPair::WordPrefix {
                            left: word1.to_owned(),
                            right_prefix: original_word_2.to_owned(),
                        });
                }
            }
        }
    }

    let derivations2 = derivations2.all_derivations_except_prefix_db();
    // TODO: safeguard in case the cartesian product is too large?
    let product_derivations = derivations1.cartesian_product(derivations2);

    for (word1, word2) in product_derivations {
        for proximity in 0..(7 - ngram_len2) {
            let cost = (proximity + ngram_len2 - 1) as u8;
            // TODO: do the opposite way with a proximity penalty as well!
            //       search for (word2, word1, proximity-1), I guess?
            if db_cache
                .get_word_pair_proximity_docids(index, txn, word1, word2, proximity as u8)?
                .is_some()
            {
                cost_proximity_word_pairs
                    .entry(cost)
                    .or_default()
                    .entry(proximity as u8)
                    .or_default()
                    .push(WordPair::Words { left: word1.to_owned(), right: word2.to_owned() });
            }
        }
    }
    let mut new_edges = cost_proximity_word_pairs
        .into_iter()
        .flat_map(|(cost, proximity_word_pairs)| {
            let mut edges = vec![];
            for (proximity, word_pairs) in proximity_word_pairs {
                edges
                    .push((cost, EdgeDetails::Data(ProximityEdge { pairs: word_pairs, proximity })))
            }
            edges
        })
        .collect::<Vec<_>>();
    new_edges.push((8 + (ngram_len2 - 1) as u8, EdgeDetails::Unconditional));
    Ok(new_edges)
}
