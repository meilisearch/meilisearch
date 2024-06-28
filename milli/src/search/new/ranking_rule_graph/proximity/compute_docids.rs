#![allow(clippy::too_many_arguments)]

use std::collections::BTreeSet;

use roaring::RoaringBitmap;

use super::ProximityCondition;
use crate::search::new::interner::Interned;
use crate::search::new::query_term::{Phrase, QueryTermSubset};
use crate::search::new::ranking_rule_graph::ComputedCondition;
use crate::search::new::resolve_query_graph::compute_query_term_subset_docids;
use crate::search::new::{SearchContext, Word};
use crate::Result;

pub fn compute_docids(
    ctx: &mut SearchContext,
    condition: &ProximityCondition,
    universe: &RoaringBitmap,
) -> Result<ComputedCondition> {
    let (left_term, right_term, cost) = match condition {
        ProximityCondition::Uninit { left_term, right_term, cost } => {
            (left_term, right_term, *cost)
        }
        ProximityCondition::Term { term } => {
            return Ok(ComputedCondition {
                docids: compute_query_term_subset_docids(ctx, Some(universe), &term.term_subset)?,
                universe_len: universe.len(),
                start_term_subset: None,
                end_term_subset: term.clone(),
            });
        }
    };

    let right_term_ngram_len = right_term.term_ids.len() as u8;

    // e.g. for the simple words `sun .. flower`
    // the cost is 5
    // the forward proximity is 5
    // the backward proximity is 4
    //
    // for the 2gram `the sunflower`
    // the cost is 5
    // the forward proximity is 4
    // the backward proximity is 3
    let forward_proximity = 1 + cost - right_term_ngram_len;
    let backward_proximity = cost - right_term_ngram_len;

    let mut docids = RoaringBitmap::new();

    if let Some(right_prefix) = right_term.term_subset.use_prefix_db(ctx) {
        for (left_phrase, left_word) in last_words_of_term_derivations(ctx, &left_term.term_subset)?
        {
            compute_prefix_edges(
                ctx,
                left_word.interned(),
                right_prefix.interned(),
                left_phrase,
                forward_proximity,
                backward_proximity,
                &mut docids,
                universe,
            )?;
        }
    }

    for (left_phrase, left_word) in last_words_of_term_derivations(ctx, &left_term.term_subset)? {
        // Before computing the edges, check that the left word and left phrase
        // aren't disjoint with the universe, but only do it if there is more than
        // one word derivation to the right.
        //
        // This is an optimisation to avoid checking for an excessive number of
        // pairs.
        let right_derivs = first_word_of_term_iter(ctx, &right_term.term_subset)?;
        if right_derivs.len() > 1 {
            let universe = &universe;
            if let Some(left_phrase) = left_phrase {
                if universe.is_disjoint(ctx.get_phrase_docids(None, left_phrase)?) {
                    continue;
                }
            } else if let Some(left_word_docids) = ctx.word_docids(Some(universe), left_word)? {
                if universe.is_disjoint(&left_word_docids) {
                    continue;
                }
            }
        }

        for (right_word, right_phrase) in right_derivs {
            compute_non_prefix_edges(
                ctx,
                left_word.interned(),
                right_word,
                left_phrase,
                right_phrase,
                forward_proximity,
                backward_proximity,
                &mut docids,
                universe,
            )?;
        }
    }

    Ok(ComputedCondition {
        docids,
        universe_len: universe.len(),
        start_term_subset: Some(left_term.clone()),
        end_term_subset: right_term.clone(),
    })
}

fn compute_prefix_edges(
    ctx: &mut SearchContext,
    left_word: Interned<String>,
    right_prefix: Interned<String>,
    left_phrase: Option<Interned<Phrase>>,
    forward_proximity: u8,
    backward_proximity: u8,
    docids: &mut RoaringBitmap,
    universe: &RoaringBitmap,
) -> Result<()> {
    let mut used_left_words = BTreeSet::new();
    let mut used_left_phrases = BTreeSet::new();
    let mut used_right_prefix = BTreeSet::new();

    let mut universe = universe.clone();
    if let Some(phrase) = left_phrase {
        // TODO we can clearly give the universe to this method
        //      Unfortunately, it is deserializing/computing stuff and
        //      keeping the result as a materialized bitmap.
        let phrase_docids = ctx.get_phrase_docids(None, phrase)?;
        if !phrase_docids.is_empty() {
            used_left_phrases.insert(phrase);
        }
        universe &= phrase_docids;
        if universe.is_empty() {
            return Ok(());
        }
    }

    // TODO check that the fact that the universe always changes is not an issue, e.g. caching stuff.
    if let Some(new_docids) = ctx.get_db_word_prefix_pair_proximity_docids(
        Some(&universe),
        left_word,
        right_prefix,
        forward_proximity,
    )? {
        if !new_docids.is_empty() {
            used_left_words.insert(left_word);
            used_right_prefix.insert(right_prefix);
            *docids |= new_docids;
        }
    }

    // No swapping when computing the proximity between a phrase and a word
    if left_phrase.is_none() {
        // TODO check that the fact that the universe always changes is not an issue, e.g. caching stuff.
        if let Some(new_docids) = ctx.get_db_prefix_word_pair_proximity_docids(
            Some(&universe),
            right_prefix,
            left_word,
            backward_proximity,
        )? {
            if !new_docids.is_empty() {
                used_left_words.insert(left_word);
                used_right_prefix.insert(right_prefix);
                *docids |= new_docids;
            }
        }
    }

    Ok(())
}

fn compute_non_prefix_edges(
    ctx: &mut SearchContext,
    word1: Interned<String>,
    word2: Interned<String>,
    left_phrase: Option<Interned<Phrase>>,
    right_phrase: Option<Interned<Phrase>>,
    forward_proximity: u8,
    backward_proximity: u8,
    docids: &mut RoaringBitmap,
    universe: &RoaringBitmap,
) -> Result<()> {
    let mut universe = universe.clone();

    for phrase in left_phrase.iter().chain(right_phrase.iter()).copied() {
        universe &= ctx.get_phrase_docids(None, phrase)?;
        if universe.is_empty() {
            return Ok(());
        }
    }

    // TODO check that it is not an issue to alterate the universe
    if let Some(new_docids) =
        ctx.get_db_word_pair_proximity_docids(Some(&universe), word1, word2, forward_proximity)?
    {
        if !new_docids.is_empty() {
            *docids |= new_docids;
        }
    }
    if backward_proximity >= 1 && left_phrase.is_none() && right_phrase.is_none() {
        if let Some(new_docids) = ctx.get_db_word_pair_proximity_docids(
            Some(&universe),
            word2,
            word1,
            backward_proximity,
        )? {
            if !new_docids.is_empty() {
                *docids |= new_docids;
            }
        }
    }

    Ok(())
}

fn last_words_of_term_derivations(
    ctx: &mut SearchContext,
    t: &QueryTermSubset,
) -> Result<BTreeSet<(Option<Interned<Phrase>>, Word)>> {
    let mut result = BTreeSet::new();

    for w in t.all_single_words_except_prefix_db(ctx)? {
        result.insert((None, w));
    }
    for p in t.all_phrases(ctx)? {
        let phrase = ctx.phrase_interner.get(p);
        let last_term_of_phrase = phrase.words.last().unwrap();
        if let Some(last_word) = last_term_of_phrase {
            result.insert((Some(p), Word::Original(*last_word)));
        }
    }

    Ok(result)
}
fn first_word_of_term_iter(
    ctx: &mut SearchContext,
    t: &QueryTermSubset,
) -> Result<BTreeSet<(Interned<String>, Option<Interned<Phrase>>)>> {
    let mut result = BTreeSet::new();
    let all_words = t.all_single_words_except_prefix_db(ctx)?;
    for w in all_words {
        result.insert((w.interned(), None));
    }
    for p in t.all_phrases(ctx)? {
        let phrase = ctx.phrase_interner.get(p);
        let first_term_of_phrase = phrase.words.first().unwrap();
        if let Some(first_word) = first_term_of_phrase {
            result.insert((*first_word, Some(p)));
        }
    }

    Ok(result)
}
