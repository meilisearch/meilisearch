#![allow(clippy::too_many_arguments)]

use super::ProximityCondition;
use crate::search::new::interner::Interned;
use crate::search::new::query_term::{Phrase, QueryTermSubset};
use crate::search::new::ranking_rule_graph::ComputedCondition;
use crate::search::new::resolve_query_graph::compute_query_term_subset_docids;
use crate::search::new::SearchContext;
use crate::{CboRoaringBitmapCodec, Result};
use roaring::RoaringBitmap;
use std::collections::BTreeSet;

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
            let mut docids = compute_query_term_subset_docids(ctx, &term.term_subset)?;
            docids &= universe;
            return Ok(ComputedCondition {
                docids,
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
                left_word,
                right_prefix,
                left_phrase,
                forward_proximity,
                backward_proximity,
                &mut docids,
                universe,
            )?;
        }
    }

    // TODO: add safeguard in case the cartesian product is too large!
    // even if we restrict the word derivations to a maximum of 100, the size of the
    // caterisan product could reach a maximum of 10_000 derivations, which is way too much.
    // Maybe prioritise the product of zero typo derivations, then the product of zero-typo/one-typo
    // + one-typo/zero-typo, then one-typo/one-typo, then ... until an arbitrary limit has been
    // reached

    for (left_phrase, left_word) in last_words_of_term_derivations(ctx, &left_term.term_subset)? {
        // Before computing the edges, check that the left word and left phrase
        // aren't disjoint with the universe, but only do it if there is more than
        // one word derivation to the right.
        //
        // This is an optimisation to avoid checking for an excessive number of
        // pairs.
        // WAIT, NO.
        // This should only be done once per node.
        // Here, we'll potentially do is.. 16 times?
        // Maybe we should do it at edge-build time instead.
        // Same for the future attribute ranking rule.
        let right_derivs = first_word_of_term_iter(ctx, &right_term.term_subset)?;
        if right_derivs.len() > 1 {
            let universe = &universe;
            if let Some(left_phrase) = left_phrase {
                if universe.is_disjoint(ctx.get_phrase_docids(left_phrase)?) {
                    continue;
                }
            } else if let Some(lw_bytes) = ctx.get_db_word_docids(left_word)? {
                let left_word_docids = CboRoaringBitmapCodec::deserialize_from(lw_bytes)?;
                if universe.is_disjoint(&left_word_docids) {
                    continue;
                }
            }
        }

        for (right_word, right_phrase) in right_derivs {
            compute_non_prefix_edges(
                ctx,
                left_word,
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
        // TODO: think about whether we want to reduce the subset,
        // we probably should!
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
        let phrase_docids = ctx.get_phrase_docids(phrase)?;
        if !phrase_docids.is_empty() {
            used_left_phrases.insert(phrase);
        }
        universe &= phrase_docids;
        if universe.is_empty() {
            return Ok(());
        }
    }

    if let Some(new_docids) =
        ctx.get_db_word_prefix_pair_proximity_docids(left_word, right_prefix, forward_proximity)?
    {
        let new_docids = &universe & CboRoaringBitmapCodec::deserialize_from(new_docids)?;
        if !new_docids.is_empty() {
            used_left_words.insert(left_word);
            used_right_prefix.insert(right_prefix);
            *docids |= new_docids;
        }
    }

    // No swapping when computing the proximity between a phrase and a word
    if left_phrase.is_none() {
        if let Some(new_docids) = ctx.get_db_prefix_word_pair_proximity_docids(
            right_prefix,
            left_word,
            backward_proximity,
        )? {
            let new_docids = &universe & CboRoaringBitmapCodec::deserialize_from(new_docids)?;
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
    let mut used_left_phrases = BTreeSet::new();
    let mut used_right_phrases = BTreeSet::new();
    let mut used_left_words = BTreeSet::new();
    let mut used_right_words = BTreeSet::new();

    let mut universe = universe.clone();

    for phrase in left_phrase.iter().chain(right_phrase.iter()).copied() {
        let phrase_docids = ctx.get_phrase_docids(phrase)?;
        universe &= phrase_docids;
        if universe.is_empty() {
            return Ok(());
        }
    }
    if let Some(left_phrase) = left_phrase {
        used_left_phrases.insert(left_phrase);
    }
    if let Some(right_phrase) = right_phrase {
        used_right_phrases.insert(right_phrase);
    }

    if let Some(new_docids) =
        ctx.get_db_word_pair_proximity_docids(word1, word2, forward_proximity)?
    {
        let new_docids = &universe & CboRoaringBitmapCodec::deserialize_from(new_docids)?;
        if !new_docids.is_empty() {
            used_left_words.insert(word1);
            used_right_words.insert(word2);
            *docids |= new_docids;
        }
    }
    if backward_proximity >= 1
            // no swapping when either term is a phrase
            && left_phrase.is_none() && right_phrase.is_none()
    {
        if let Some(new_docids) =
            ctx.get_db_word_pair_proximity_docids(word2, word1, backward_proximity)?
        {
            let new_docids = &universe & CboRoaringBitmapCodec::deserialize_from(new_docids)?;
            if !new_docids.is_empty() {
                used_left_words.insert(word2);
                used_right_words.insert(word1);
                *docids |= new_docids;
            }
        }
    }

    Ok(())
}

fn last_words_of_term_derivations(
    ctx: &mut SearchContext,
    t: &QueryTermSubset,
) -> Result<BTreeSet<(Option<Interned<Phrase>>, Interned<String>)>> {
    let mut result = BTreeSet::new();

    for w in t.all_single_words_except_prefix_db(ctx)? {
        result.insert((None, w));
    }
    for p in t.all_phrases(ctx)? {
        let phrase = ctx.phrase_interner.get(p);
        let last_term_of_phrase = phrase.words.last().unwrap();
        if let Some(last_word) = last_term_of_phrase {
            result.insert((Some(p), *last_word));
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
        result.insert((w, None));
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
