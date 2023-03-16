#![allow(clippy::too_many_arguments)]
use std::collections::BTreeMap;

use super::ProximityCondition;
use crate::search::new::db_cache::DatabaseCache;
use crate::search::new::interner::{DedupInterner, Interned};
use crate::search::new::query_graph::QueryNodeData;
use crate::search::new::query_term::{LocatedQueryTerm, Phrase, QueryTerm};
use crate::search::new::ranking_rule_graph::proximity::WordPair;
use crate::search::new::{QueryNode, SearchContext};
use crate::Result;
use heed::RoTxn;

fn last_word_of_term_iter<'t>(
    t: &'t QueryTerm,
    phrase_interner: &'t DedupInterner<Phrase>,
) -> impl Iterator<Item = (Option<Interned<Phrase>>, Interned<String>)> + 't {
    t.all_single_words_except_prefix_db().map(|w| (None, w)).chain(t.all_phrases().flat_map(
        move |p| {
            let phrase = phrase_interner.get(p);
            phrase.words.last().unwrap().map(|last| (Some(p), last))
        },
    ))
}
fn first_word_of_term_iter<'t>(
    t: &'t QueryTerm,
    phrase_interner: &'t DedupInterner<Phrase>,
) -> impl Iterator<Item = (Interned<String>, Option<Interned<Phrase>>)> + 't {
    t.all_single_words_except_prefix_db().map(|w| (w, None)).chain(t.all_phrases().flat_map(
        move |p| {
            let phrase = phrase_interner.get(p);
            phrase.words.first().unwrap().map(|first| (first, Some(p)))
        },
    ))
}

pub fn build_edges<'ctx>(
    ctx: &mut SearchContext<'ctx>,
    conditions_interner: &mut DedupInterner<ProximityCondition>,
    from_node: &QueryNode,
    to_node: &QueryNode,
) -> Result<Vec<(u8, Option<Interned<ProximityCondition>>)>> {
    let SearchContext {
        index,
        txn,
        db_cache,
        word_interner,
        phrase_interner,
        term_interner,
        term_docids: _,
    } = ctx;

    let right_term = match &to_node.data {
        QueryNodeData::End => return Ok(vec![(0, None)]),
        QueryNodeData::Deleted | QueryNodeData::Start => return Ok(vec![]),
        QueryNodeData::Term(term) => term,
    };

    let LocatedQueryTerm { value: right_term_interned, positions: right_positions } = right_term;

    let (right_term, right_start_position, right_ngram_length) =
        (term_interner.get(*right_term_interned), *right_positions.start(), right_positions.len());

    let (left_term, left_end_position) = match &from_node.data {
        QueryNodeData::Term(LocatedQueryTerm { value, positions }) => {
            (term_interner.get(*value), *positions.end())
        }
        QueryNodeData::Deleted => return Ok(vec![]),
        QueryNodeData::Start => {
            return Ok(vec![(
                (right_ngram_length - 1) as u8,
                Some(
                    conditions_interner
                        .insert(ProximityCondition::Term { term: *right_term_interned }),
                ),
            )])
        }
        QueryNodeData::End => return Ok(vec![]),
    };

    if left_end_position + 1 != right_start_position {
        // We want to ignore this pair of terms
        // Unconditionally walk through the edge without computing the docids
        // This can happen when, in a query like `the sun flowers are beautiful`, the term
        // `flowers` is removed by the `words` ranking rule.
        // The remaining query graph represents `the sun .. are beautiful`
        // but `sun` and `are` have no proximity condition between them
        return Ok(vec![(
            (right_ngram_length - 1) as u8,
            Some(
                conditions_interner.insert(ProximityCondition::Term { term: *right_term_interned }),
            ),
        )]);
    }

    let mut cost_word_pairs = BTreeMap::<u8, Vec<WordPair>>::new();

    if let Some(right_prefix) = right_term.use_prefix_db {
        for (left_phrase, left_word) in last_word_of_term_iter(left_term, phrase_interner) {
            add_prefix_edges(
                index,
                txn,
                db_cache,
                word_interner,
                right_ngram_length,
                left_word,
                right_prefix,
                &mut cost_word_pairs,
                left_phrase,
            )?;
        }
    }

    // TODO: add safeguard in case the cartesian product is too large!
    // even if we restrict the word derivations to a maximum of 100, the size of the
    // caterisan product could reach a maximum of 10_000 derivations, which is way too much.
    // Maybe prioritise the product of zero typo derivations, then the product of zero-typo/one-typo
    // + one-typo/zero-typo, then one-typo/one-typo, then ... until an arbitrary limit has been
    // reached

    for (left_phrase, left_word) in last_word_of_term_iter(left_term, phrase_interner) {
        for (right_word, right_phrase) in first_word_of_term_iter(right_term, phrase_interner) {
            add_non_prefix_edges(
                index,
                txn,
                db_cache,
                word_interner,
                right_ngram_length,
                left_word,
                right_word,
                &mut cost_word_pairs,
                &[left_phrase, right_phrase].iter().copied().flatten().collect::<Vec<_>>(),
            )?;
        }
    }

    let mut new_edges = cost_word_pairs
        .into_iter()
        .map(|(cost, word_pairs)| {
            (
                cost,
                Some(
                    conditions_interner
                        .insert(ProximityCondition::Pairs { pairs: word_pairs.into_boxed_slice() }),
                ),
            )
        })
        .collect::<Vec<_>>();
    new_edges.push((
        8 + (right_ngram_length - 1) as u8,
        Some(conditions_interner.insert(ProximityCondition::Term { term: *right_term_interned })),
    ));
    Ok(new_edges)
}

fn add_prefix_edges<'ctx>(
    index: &mut &crate::Index,
    txn: &'ctx RoTxn,
    db_cache: &mut DatabaseCache<'ctx>,
    word_interner: &mut DedupInterner<String>,
    right_ngram_length: usize,
    left_word: Interned<String>,
    right_prefix: Interned<String>,
    cost_proximity_word_pairs: &mut BTreeMap<u8, Vec<WordPair>>,
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
            cost_proximity_word_pairs.entry(cost).or_default().push(WordPair::WordPrefix {
                phrases: left_phrase.into_iter().collect(),
                left: left_word,
                right_prefix,
                proximity: proximity as u8,
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
            cost_proximity_word_pairs.entry(cost).or_default().push(WordPair::WordPrefixSwapped {
                left_prefix: right_prefix,
                right: left_word,
                proximity: proximity as u8 - 1,
            });
        }
    }
    Ok(())
}

fn add_non_prefix_edges<'ctx>(
    index: &mut &crate::Index,
    txn: &'ctx RoTxn,
    db_cache: &mut DatabaseCache<'ctx>,
    word_interner: &mut DedupInterner<String>,
    right_ngram_length: usize,
    word1: Interned<String>,
    word2: Interned<String>,
    cost_proximity_word_pairs: &mut BTreeMap<u8, Vec<WordPair>>,
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
            cost_proximity_word_pairs.entry(cost).or_default().push(WordPair::Words {
                phrases: phrases.to_vec(),
                left: word1,
                right: word2,
                proximity: proximity as u8,
            });
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
            cost_proximity_word_pairs.entry(cost).or_default().push(WordPair::Words {
                phrases: vec![],
                left: word2,
                right: word1,
                proximity: proximity as u8 - 1,
            });
        }
    }
    Ok(())
}
