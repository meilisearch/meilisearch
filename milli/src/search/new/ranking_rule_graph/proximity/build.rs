#![allow(clippy::too_many_arguments)]
use std::collections::BTreeMap;

use super::ProximityEdge;
use crate::search::new::db_cache::DatabaseCache;
use crate::search::new::interner::{Interned, Interner};
use crate::search::new::query_term::{LocatedQueryTerm, Phrase, QueryTerm, WordDerivations};
use crate::search::new::ranking_rule_graph::proximity::WordPair;
use crate::search::new::ranking_rule_graph::EdgeCondition;
use crate::search::new::{QueryNode, SearchContext};
use crate::Result;
use heed::RoTxn;

pub fn visit_from_node(
    ctx: &mut SearchContext,
    from_node: &QueryNode,
) -> Result<Option<(Vec<(Option<Interned<Phrase>>, Interned<String>)>, i8)>> {
    let SearchContext { derivations_interner, .. } = ctx;

    let (left_phrase, left_derivations, left_end_position) = match from_node {
        QueryNode::Term(LocatedQueryTerm { value: value1, positions: pos1 }) => {
            match value1 {
                QueryTerm::Word { derivations } => {
                    (None, derivations_interner.get(*derivations).clone(), *pos1.end())
                }
                QueryTerm::Phrase { phrase: phrase_interned } => {
                    let phrase = ctx.phrase_interner.get(*phrase_interned);
                    if let Some(original) = *phrase.words.last().unwrap() {
                        (
                            Some(*phrase_interned),
                            WordDerivations {
                                original,
                                zero_typo: Some(original),
                                one_typo: Box::new([]),
                                two_typos: Box::new([]),
                                use_prefix_db: None,
                                synonyms: Box::new([]),
                                split_words: None,
                                is_prefix: false,
                                prefix_of: Box::new([]),
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
        QueryNode::Start => (None, WordDerivations::empty(&mut ctx.word_interner, ""), -1),
        _ => return Ok(None),
    };

    // left term cannot be a prefix
    assert!(left_derivations.use_prefix_db.is_none() && !left_derivations.is_prefix);

    let last_word_left_phrase = if let Some(left_phrase_interned) = left_phrase {
        let left_phrase = ctx.phrase_interner.get(left_phrase_interned);
        left_phrase.words.last().copied().unwrap()
    } else {
        None
    };
    let left_single_word_iter: Vec<(Option<Interned<Phrase>>, Interned<String>)> = left_derivations
        .all_single_word_derivations_except_prefix_db()
        .chain(last_word_left_phrase.iter().copied())
        .map(|w| (left_phrase, w))
        .collect();
    let left_phrase_iter: Vec<(Option<Interned<Phrase>>, Interned<String>)> = left_derivations
        .all_phrase_derivations()
        .map(|left_phrase_interned: Interned<Phrase>| {
            let left_phrase = ctx.phrase_interner.get(left_phrase_interned);
            let last_word_left_phrase: Interned<String> =
                left_phrase.words.last().unwrap().unwrap();
            let r: (Option<Interned<Phrase>>, Interned<String>) =
                (Some(left_phrase_interned), last_word_left_phrase);
            r
        })
        .collect();
    let mut left_word_iter = left_single_word_iter;
    left_word_iter.extend(left_phrase_iter);

    Ok(Some((left_word_iter, left_end_position)))
}

pub fn build_step_visit_destination_node<'ctx, 'from_data>(
    ctx: &mut SearchContext<'ctx>,
    conditions_interner: &mut Interner<ProximityEdge>,
    from_node_data: &'from_data (Vec<(Option<Interned<Phrase>>, Interned<String>)>, i8),
    to_node: &QueryNode,
) -> Result<Vec<(u8, EdgeCondition<ProximityEdge>)>> {
    let SearchContext {
        index,
        txn,
        db_cache,
        word_interner,
        phrase_interner,
        derivations_interner,
        query_term_docids: _,
    } = ctx;
    let right_term = match &to_node {
        QueryNode::End => return Ok(vec![(0, EdgeCondition::Unconditional)]),
        QueryNode::Deleted | QueryNode::Start => return Ok(vec![]),
        QueryNode::Term(term) => term,
    };
    let LocatedQueryTerm { value: right_value, positions: right_positions } = right_term;

    let (right_phrase, right_derivations, right_start_position, right_ngram_length) =
        match right_value {
            QueryTerm::Word { derivations } => (
                None,
                derivations_interner.get(*derivations).clone(),
                *right_positions.start(),
                right_positions.len(),
            ),
            QueryTerm::Phrase { phrase: right_phrase_interned } => {
                let right_phrase = phrase_interner.get(*right_phrase_interned);
                if let Some(original) = *right_phrase.words.first().unwrap() {
                    (
                        Some(*right_phrase_interned),
                        WordDerivations {
                            original,
                            zero_typo: Some(original),
                            one_typo: Box::new([]),
                            two_typos: Box::new([]),
                            use_prefix_db: None,
                            synonyms: Box::new([]),
                            split_words: None,
                            is_prefix: false,
                            prefix_of: Box::new([]),
                        },
                        *right_positions.start(),
                        1,
                    )
                } else {
                    // No word pairs if the phrase does not have a regular word as its first term
                    return Ok(vec![]);
                }
            }
        };

    let (left_derivations, left_end_position) = from_node_data;

    if left_end_position + 1 != right_start_position {
        // We want to ignore this pair of terms
        // Unconditionally walk through the edge without computing the docids
        // This can happen when, in a query like `the sun flowers are beautiful`, the term
        // `flowers` is removed by the words ranking rule due to the terms matching strategy.
        // The remaining query graph represents `the sun .. are beautiful`
        // but `sun` and `are` have no proximity condition between them
        return Ok(vec![(0, EdgeCondition::Unconditional)]);
    }

    let mut cost_proximity_word_pairs = BTreeMap::<u8, BTreeMap<u8, Vec<WordPair>>>::new();

    if let Some(right_prefix) = right_derivations.use_prefix_db {
        for (left_phrase, left_word) in left_derivations.iter().copied() {
            add_prefix_edges(
                index,
                txn,
                db_cache,
                word_interner,
                right_ngram_length,
                left_word,
                right_prefix,
                &mut cost_proximity_word_pairs,
                left_phrase,
            )?;
        }
    }

    // TODO: add safeguard in case the cartesian product is too large!
    // even if we restrict the word derivations to a maximum of 100, the size of the
    // caterisan product could reach a maximum of 10_000 derivations, which is way too much.
    // mMaybe prioritise the product of zero typo derivations, then the product of zero-typo/one-typo
    // + one-typo/zero-typo, then one-typo/one-typo, then ... until an arbitrary limit has been
    // reached
    let first_word_right_phrase = if let Some(right_phrase_interned) = right_phrase {
        let right_phrase = phrase_interner.get(right_phrase_interned);
        right_phrase.words.first().copied().unwrap()
    } else {
        None
    };
    let right_single_word_iter: Vec<(Option<Interned<Phrase>>, Interned<String>)> =
        right_derivations
            .all_single_word_derivations_except_prefix_db()
            .chain(first_word_right_phrase.iter().copied())
            .map(|w| (right_phrase, w))
            .collect();
    let right_phrase_iter: Vec<(Option<Interned<Phrase>>, Interned<String>)> = right_derivations
        .all_phrase_derivations()
        .map(|right_phrase_interned: Interned<Phrase>| {
            let right_phrase = phrase_interner.get(right_phrase_interned);
            let first_word_right_phrase: Interned<String> =
                right_phrase.words.first().unwrap().unwrap();
            let r: (Option<Interned<Phrase>>, Interned<String>) =
                (Some(right_phrase_interned), first_word_right_phrase);
            r
        })
        .collect();
    let mut right_word_iter = right_single_word_iter;
    right_word_iter.extend(right_phrase_iter);

    for (left_phrase, left_word) in left_derivations.iter().copied() {
        for (right_phrase, right_word) in right_word_iter.iter().copied() {
            add_non_prefix_edges(
                index,
                txn,
                db_cache,
                word_interner,
                right_ngram_length,
                left_word,
                right_word,
                &mut cost_proximity_word_pairs,
                &[left_phrase, right_phrase].iter().copied().flatten().collect::<Vec<_>>(),
            )?;
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
    new_edges.push((8 + (right_ngram_length - 1) as u8, EdgeCondition::Unconditional));
    Ok(new_edges)
}

fn add_prefix_edges<'ctx>(
    index: &mut &crate::Index,
    txn: &'ctx RoTxn,
    db_cache: &mut DatabaseCache<'ctx>,
    word_interner: &mut Interner<String>,
    right_ngram_length: usize,
    left_word: Interned<String>,
    right_prefix: Interned<String>,
    cost_proximity_word_pairs: &mut BTreeMap<u8, BTreeMap<u8, Vec<WordPair>>>,
    left_phrase: Option<Interned<Phrase>>,
) -> Result<()> {
    for proximity in 1..=(8 - right_ngram_length) {
        let cost = (proximity + right_ngram_length - 1) as u8;
        // TODO: if we had access to the universe here, we could already check whether
        // the bitmap corresponding to this word pair is disjoint with the universe or not
        if db_cache
            .get_word_prefix_pair_proximity_docids(
                index,
                txn,
                word_interner,
                left_word,
                right_prefix,
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
                    phrases: left_phrase.into_iter().collect(),
                    left: left_word,
                    right_prefix,
                });
        }

        // No swapping when computing the proximity between a phrase and a word
        if left_phrase.is_none()
            && db_cache
                .get_prefix_word_pair_proximity_docids(
                    index,
                    txn,
                    word_interner,
                    right_prefix,
                    left_word,
                    proximity as u8 - 1,
                )?
                .is_some()
        {
            cost_proximity_word_pairs
                .entry(cost)
                .or_default()
                .entry(proximity as u8)
                .or_default()
                .push(WordPair::WordPrefixSwapped { left_prefix: right_prefix, right: left_word });
        }
    }
    Ok(())
}

fn add_non_prefix_edges<'ctx>(
    index: &mut &crate::Index,
    txn: &'ctx RoTxn,
    db_cache: &mut DatabaseCache<'ctx>,
    word_interner: &mut Interner<String>,
    right_ngram_length: usize,
    word1: Interned<String>,
    word2: Interned<String>,
    cost_proximity_word_pairs: &mut BTreeMap<u8, BTreeMap<u8, Vec<WordPair>>>,
    phrases: &[Interned<Phrase>],
) -> Result<()> {
    for proximity in 1..=(8 - right_ngram_length) {
        let cost = (proximity + right_ngram_length - 1) as u8;
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
                .push(WordPair::Words { phrases: phrases.to_vec(), left: word1, right: word2 });
        }
        if proximity > 1
            // no swapping when either term is a phrase
            && phrases.is_empty()
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
                .push(WordPair::Words { phrases: vec![], left: word2, right: word1 });
        }
    }
    Ok(())
}
