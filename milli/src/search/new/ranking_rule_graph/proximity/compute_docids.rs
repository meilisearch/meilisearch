#![allow(clippy::too_many_arguments)]

use std::iter::FromIterator;

use super::ProximityCondition;
use crate::search::new::db_cache::DatabaseCache;
use crate::search::new::interner::{DedupInterner, Interned};
use crate::search::new::query_term::{Phrase, QueryTerm};
use crate::search::new::resolve_query_graph::QueryTermDocIdsCache;
use crate::search::new::SearchContext;
use crate::{CboRoaringBitmapCodec, Index, Result};
use fxhash::FxHashSet;
use heed::RoTxn;
use roaring::RoaringBitmap;

pub fn compute_docids<'ctx>(
    ctx: &mut SearchContext<'ctx>,
    condition: &ProximityCondition,
    universe: &RoaringBitmap,
) -> Result<(RoaringBitmap, FxHashSet<Interned<String>>, FxHashSet<Interned<Phrase>>)> {
    let (left_term, right_term, right_term_ngram_len, cost) = match condition {
        ProximityCondition::Uninit { left_term, right_term, right_term_ngram_len, cost } => {
            (*left_term, *right_term, *right_term_ngram_len, *cost)
        }
        ProximityCondition::Term { term } => {
            let term_v = ctx.term_interner.get(*term);
            return Ok((
                ctx.term_docids
                    .get_query_term_docids(
                        ctx.index,
                        ctx.txn,
                        &mut ctx.db_cache,
                        &ctx.word_interner,
                        &ctx.term_interner,
                        &ctx.phrase_interner,
                        *term,
                    )?
                    .clone(),
                FxHashSet::from_iter(term_v.all_single_words_except_prefix_db()),
                FxHashSet::from_iter(term_v.all_phrases()),
            ));
        }
    };

    let left_term = ctx.term_interner.get(left_term);
    let right_term = ctx.term_interner.get(right_term);

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

    let mut used_words = FxHashSet::default();
    let mut used_phrases = FxHashSet::default();

    let mut docids = RoaringBitmap::new();

    if let Some(right_prefix) = right_term.use_prefix_db {
        for (left_phrase, left_word) in last_word_of_term_iter(left_term, &ctx.phrase_interner) {
            compute_prefix_edges(
                ctx.index,
                ctx.txn,
                &mut ctx.db_cache,
                &mut ctx.term_docids,
                &ctx.word_interner,
                &ctx.phrase_interner,
                left_word,
                right_prefix,
                left_phrase,
                forward_proximity,
                backward_proximity,
                &mut docids,
                universe,
                &mut used_words,
                &mut used_phrases,
            )?;
        }
    }

    // TODO: add safeguard in case the cartesian product is too large!
    // even if we restrict the word derivations to a maximum of 100, the size of the
    // caterisan product could reach a maximum of 10_000 derivations, which is way too much.
    // Maybe prioritise the product of zero typo derivations, then the product of zero-typo/one-typo
    // + one-typo/zero-typo, then one-typo/one-typo, then ... until an arbitrary limit has been
    // reached

    for (left_phrase, left_word) in last_word_of_term_iter(left_term, &ctx.phrase_interner) {
        for (right_word, right_phrase) in first_word_of_term_iter(right_term, &ctx.phrase_interner)
        {
            compute_non_prefix_edges(
                ctx.index,
                ctx.txn,
                &mut ctx.db_cache,
                &mut ctx.term_docids,
                &ctx.word_interner,
                &ctx.phrase_interner,
                left_word,
                right_word,
                &[left_phrase, right_phrase].iter().copied().flatten().collect::<Vec<_>>(),
                forward_proximity,
                backward_proximity,
                &mut docids,
                universe,
                &mut used_words,
                &mut used_phrases,
            )?;
        }
    }

    Ok((docids, used_words, used_phrases))
}

fn compute_prefix_edges<'ctx>(
    index: &Index,
    txn: &'ctx RoTxn,
    db_cache: &mut DatabaseCache<'ctx>,
    term_docids: &mut QueryTermDocIdsCache,
    word_interner: &DedupInterner<String>,
    phrase_interner: &DedupInterner<Phrase>,
    left_word: Interned<String>,
    right_prefix: Interned<String>,
    left_phrase: Option<Interned<Phrase>>,
    forward_proximity: u8,
    backward_proximity: u8,
    docids: &mut RoaringBitmap,
    universe: &RoaringBitmap,
    used_words: &mut FxHashSet<Interned<String>>,
    used_phrases: &mut FxHashSet<Interned<Phrase>>,
) -> Result<()> {
    let mut universe = universe.clone();
    if let Some(phrase) = left_phrase {
        let phrase_docids = term_docids.get_phrase_docids(
            index,
            txn,
            db_cache,
            word_interner,
            phrase_interner,
            phrase,
        )?;
        if !phrase_docids.is_empty() {
            used_phrases.insert(phrase);
        }
        universe &= phrase_docids;
        if universe.is_empty() {
            return Ok(());
        }
    }

    if let Some(new_docids) = db_cache.get_word_prefix_pair_proximity_docids(
        index,
        txn,
        word_interner,
        left_word,
        right_prefix,
        forward_proximity,
    )? {
        let new_docids = &universe & CboRoaringBitmapCodec::deserialize_from(new_docids)?;
        if !new_docids.is_empty() {
            used_words.insert(left_word);
            used_words.insert(right_prefix);
            *docids |= new_docids;
        }
    }

    // No swapping when computing the proximity between a phrase and a word
    if left_phrase.is_none() {
        if let Some(new_docids) = db_cache.get_prefix_word_pair_proximity_docids(
            index,
            txn,
            word_interner,
            right_prefix,
            left_word,
            backward_proximity,
        )? {
            let new_docids = &universe & CboRoaringBitmapCodec::deserialize_from(new_docids)?;
            if !new_docids.is_empty() {
                used_words.insert(left_word);
                used_words.insert(right_prefix);
                *docids |= new_docids;
            }
        }
    }

    Ok(())
}

fn compute_non_prefix_edges<'ctx>(
    index: &Index,
    txn: &'ctx RoTxn,
    db_cache: &mut DatabaseCache<'ctx>,
    term_docids: &mut QueryTermDocIdsCache,
    word_interner: &DedupInterner<String>,
    phrase_interner: &DedupInterner<Phrase>,
    word1: Interned<String>,
    word2: Interned<String>,
    phrases: &[Interned<Phrase>],
    forward_proximity: u8,
    backward_proximity: u8,
    docids: &mut RoaringBitmap,
    universe: &RoaringBitmap,
    used_words: &mut FxHashSet<Interned<String>>,
    used_phrases: &mut FxHashSet<Interned<Phrase>>,
) -> Result<()> {
    let mut universe = universe.clone();
    for phrase in phrases {
        let phrase_docids = term_docids.get_phrase_docids(
            index,
            txn,
            db_cache,
            word_interner,
            phrase_interner,
            *phrase,
        )?;
        if !phrase_docids.is_empty() {
            used_phrases.insert(*phrase);
        }
        universe &= phrase_docids;
        if universe.is_empty() {
            return Ok(());
        }
    }
    if let Some(new_docids) = db_cache.get_word_pair_proximity_docids(
        index,
        txn,
        word_interner,
        word1,
        word2,
        forward_proximity,
    )? {
        let new_docids = &universe & CboRoaringBitmapCodec::deserialize_from(new_docids)?;
        if !new_docids.is_empty() {
            used_words.insert(word1);
            used_words.insert(word2);
            *docids |= new_docids;
        }
    }
    if backward_proximity >= 1
            // no swapping when either term is a phrase
            && phrases.is_empty()
    {
        if let Some(new_docids) = db_cache.get_word_pair_proximity_docids(
            index,
            txn,
            word_interner,
            word2,
            word1,
            backward_proximity,
        )? {
            let new_docids = &universe & CboRoaringBitmapCodec::deserialize_from(new_docids)?;
            if !new_docids.is_empty() {
                used_words.insert(word1);
                used_words.insert(word2);
                *docids |= new_docids;
            }
        }
    }

    Ok(())
}

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
