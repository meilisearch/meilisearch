use std::collections::BTreeMap;

use itertools::Itertools;

use super::ProximityEdge;
use crate::search::new::interner::Interner;
use crate::search::new::query_term::{LocatedQueryTerm, QueryTerm, WordDerivations};
use crate::search::new::ranking_rule_graph::proximity::WordPair;
use crate::search::new::ranking_rule_graph::EdgeCondition;
use crate::search::new::{QueryNode, SearchContext};
use crate::Result;

pub fn visit_from_node(
    ctx: &mut SearchContext,
    from_node: &QueryNode,
) -> Result<Option<(WordDerivations, i8)>> {
    Ok(Some(match from_node {
        QueryNode::Term(LocatedQueryTerm { value: value1, positions: pos1 }) => {
            match value1 {
                QueryTerm::Word { derivations } => {
                    (ctx.derivations_interner.get(*derivations).clone(), *pos1.end())
                }
                QueryTerm::Phrase { phrase: phrase1 } => {
                    let phrase1 = ctx.phrase_interner.get(*phrase1);
                    if let Some(original) = *phrase1.words.last().unwrap() {
                        (
                            WordDerivations {
                                original,
                                zero_typo: Box::new([original]),
                                one_typo: Box::new([]),
                                two_typos: Box::new([]),
                                use_prefix_db: false,
                                synonyms: Box::new([]),
                                split_words: None,
                            },
                            *pos1.end(),
                        )
                    } else {
                        // No word pairs if the phrase does not have a regular word as its last term
                        return Ok(None);
                    }
                }
            }
        }
        QueryNode::Start => (
            WordDerivations {
                original: ctx.word_interner.insert(String::new()),
                zero_typo: Box::new([]),
                one_typo: Box::new([]),
                two_typos: Box::new([]),
                use_prefix_db: false,
                synonyms: Box::new([]),
                split_words: None,
            },
            -100,
        ),
        _ => return Ok(None),
    }))
}

pub fn visit_to_node<'ctx, 'from_data>(
    ctx: &mut SearchContext<'ctx>,
    conditions_interner: &mut Interner<ProximityEdge>,
    to_node: &QueryNode,
    from_node_data: &'from_data (WordDerivations, i8),
) -> Result<Vec<(u8, EdgeCondition<ProximityEdge>)>> {
    let SearchContext { index, txn, db_cache, word_interner, derivations_interner, .. } = ctx;

    // IMPORTANT! TODO: split words support

    let (derivations1, pos1) = from_node_data;
    let term2 = match &to_node {
        QueryNode::End => return Ok(vec![(0, EdgeCondition::Unconditional)]),
        QueryNode::Deleted | QueryNode::Start => return Ok(vec![]),
        QueryNode::Term(term) => term,
    };
    let LocatedQueryTerm { value: value2, positions: pos2 } = term2;

    let (derivations2, pos2, ngram_len2) = match value2 {
        QueryTerm::Word { derivations } => {
            (derivations_interner.get(*derivations).clone(), *pos2.start(), pos2.len())
        }
        QueryTerm::Phrase { phrase: phrase2 } => {
            let phrase2 = ctx.phrase_interner.get(*phrase2);
            if let Some(original) = *phrase2.words.first().unwrap() {
                (
                    WordDerivations {
                        original,
                        zero_typo: Box::new([original]),
                        one_typo: Box::new([]),
                        two_typos: Box::new([]),
                        use_prefix_db: false,
                        synonyms: Box::new([]),
                        split_words: None,
                    },
                    *pos2.start(),
                    1,
                )
            } else {
                // No word pairs if the phrase does not have a regular word as its first term
                return Ok(vec![]);
            }
        }
    };

    if pos1 + 1 != pos2 {
        // TODO: how should this actually be handled?
        // We want to effectively ignore this pair of terms
        // Unconditionally walk through the edge without computing the docids
        // But also what should the cost be?
        return Ok(vec![(0, EdgeCondition::Unconditional)]);
    }

    let updb1 = derivations1.use_prefix_db;
    let updb2 = derivations2.use_prefix_db;

    // left term cannot be a prefix
    assert!(!updb1);

    // TODO: IMPORTANT! split words and synonyms support
    let derivations1 = derivations1.all_single_word_derivations_except_prefix_db();
    // TODO: eventually, we want to get rid of the uses from `orginal`
    let mut cost_proximity_word_pairs = BTreeMap::<u8, BTreeMap<u8, Vec<WordPair>>>::new();

    if updb2 {
        for word1 in derivations1.clone() {
            for proximity in 1..=(8 - ngram_len2) {
                let cost = (proximity + ngram_len2 - 1) as u8;
                // TODO: if we had access to the universe here, we could already check whether
                // the bitmap corresponding to this word pair is disjoint with the universe or not
                if db_cache
                    .get_word_prefix_pair_proximity_docids(
                        index,
                        txn,
                        word_interner,
                        word1,
                        derivations2.original,
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
                            left: word1,
                            right_prefix: derivations2.original,
                        });
                }
                if db_cache
                    .get_prefix_word_pair_proximity_docids(
                        index,
                        txn,
                        word_interner,
                        derivations2.original,
                        word1,
                        proximity as u8 - 1,
                    )?
                    .is_some()
                {
                    cost_proximity_word_pairs
                        .entry(cost)
                        .or_default()
                        .entry(proximity as u8)
                        .or_default()
                        .push(WordPair::WordPrefixSwapped {
                            left_prefix: derivations2.original,
                            right: word1,
                        });
                }
            }
        }
    }

    // TODO: important! support split words and synonyms as well
    let derivations2 = derivations2.all_single_word_derivations_except_prefix_db();
    // TODO: add safeguard in case the cartesian product is too large!
    // even if we restrict the word derivations to a maximum of 100, the size of the
    // caterisan product could reach a maximum of 10_000 derivations, which is way too much.
    // mMaybe prioritise the product of zero typo derivations, then the product of zero-typo/one-typo
    // + one-typo/zero-typo, then one-typo/one-typo, then ... until an arbitrary limit has been
    // reached
    let product_derivations = derivations1.cartesian_product(derivations2);

    for (word1, word2) in product_derivations {
        for proximity in 1..=(8 - ngram_len2) {
            let cost = (proximity + ngram_len2 - 1) as u8;
            if db_cache
                .get_word_pair_proximity_docids(
                    index,
                    txn,
                    word_interner,
                    word1,
                    word2,
                    proximity as u8,
                )?
                .is_some()
            {
                cost_proximity_word_pairs
                    .entry(cost)
                    .or_default()
                    .entry(proximity as u8)
                    .or_default()
                    .push(WordPair::Words { left: word1, right: word2 });
            }
            if proximity > 1
                && db_cache
                    .get_word_pair_proximity_docids(
                        index,
                        txn,
                        word_interner,
                        word2,
                        word1,
                        proximity as u8 - 1,
                    )?
                    .is_some()
            {
                cost_proximity_word_pairs
                    .entry(cost)
                    .or_default()
                    .entry(proximity as u8 - 1)
                    .or_default()
                    .push(WordPair::Words { left: word2, right: word1 });
            }
        }
    }
    let mut new_edges =
        cost_proximity_word_pairs
            .into_iter()
            .flat_map(|(cost, proximity_word_pairs)| {
                let mut edges = vec![];
                for (proximity, word_pairs) in proximity_word_pairs {
                    edges.push((
                        cost,
                        EdgeCondition::Conditional(conditions_interner.insert(ProximityEdge {
                            pairs: word_pairs.into_boxed_slice(),
                            proximity,
                        })),
                    ))
                }
                edges
            })
            .collect::<Vec<_>>();
    new_edges.push((8 + (ngram_len2 - 1) as u8, EdgeCondition::Unconditional));
    Ok(new_edges)
}
