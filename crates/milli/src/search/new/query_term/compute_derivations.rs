use std::borrow::Cow;
use std::cmp::Ordering;
use std::collections::BTreeSet;

use fst::automaton::Str;
use fst::{IntoStreamer, Streamer};
use heed::types::DecodeIgnore;
use itertools::{merge_join_by, EitherOrBoth};

use super::{OneTypoTerm, Phrase, QueryTerm, ZeroTypoTerm};
use crate::search::fst_utils::{Complement, Intersection, StartsWith, Union};
use crate::search::new::interner::{DedupInterner, Interned};
use crate::search::new::query_term::{Lazy, TwoTypoTerm};
use crate::search::new::{limits, SearchContext};
use crate::search::{build_dfa, get_first};
use crate::{Result, MAX_WORD_LENGTH};

impl Interned<QueryTerm> {
    pub fn compute_fully_if_needed(self, ctx: &mut SearchContext<'_>) -> Result<()> {
        let s = ctx.term_interner.get_mut(self);
        if s.max_levenshtein_distance <= 1 && s.one_typo.is_uninit() {
            assert!(s.two_typo.is_uninit());
            // Initialize one_typo subterm even if max_nbr_typo is 0 because of split words
            self.initialize_one_typo_subterm(ctx)?;
            let s = ctx.term_interner.get_mut(self);
            assert!(s.one_typo.is_init());
            s.two_typo = Lazy::Init(TwoTypoTerm::default());
        } else if s.max_levenshtein_distance > 1 && s.two_typo.is_uninit() {
            assert!(s.two_typo.is_uninit());
            self.initialize_one_and_two_typo_subterm(ctx)?;
            let s = ctx.term_interner.get_mut(self);
            assert!(s.one_typo.is_init() && s.two_typo.is_init());
        }
        Ok(())
    }
}

fn find_zero_typo_prefix_derivations(
    ctx: &mut SearchContext<'_>,
    word_interned: Interned<String>,
    prefix_of: &mut BTreeSet<Interned<String>>,
) -> Result<()> {
    let word = ctx.word_interner.get(word_interned).to_owned();
    let word = word.as_str();

    let words =
        ctx.index.word_docids.remap_data_type::<DecodeIgnore>().prefix_iter(ctx.txn, word)?;
    let exact_words =
        ctx.index.exact_word_docids.remap_data_type::<DecodeIgnore>().prefix_iter(ctx.txn, word)?;

    for eob in merge_join_by(words, exact_words, |lhs, rhs| match (lhs, rhs) {
        (Ok((word, _)), Ok((exact_word, _))) => word.cmp(exact_word),
        (Err(_), _) | (_, Err(_)) => Ordering::Equal,
    }) {
        match eob {
            EitherOrBoth::Both(kv, _) | EitherOrBoth::Left(kv) | EitherOrBoth::Right(kv) => {
                let (derived_word, _) = kv?;
                let derived_word = derived_word.to_string();
                let derived_word_interned = ctx.word_interner.insert(derived_word);
                if derived_word_interned != word_interned {
                    prefix_of.insert(derived_word_interned);
                    if prefix_of.len() >= limits::MAX_PREFIX_COUNT {
                        break;
                    }
                }
            }
        }
    }

    Ok(())
}

fn find_one_typo_derivations(
    ctx: &mut SearchContext<'_>,
    word_interned: Interned<String>,
    is_prefix: bool,
    one_typo_words: &mut BTreeSet<Interned<String>>,
) -> Result<()> {
    let fst = ctx.get_words_fst()?;
    let word = ctx.word_interner.get(word_interned).to_owned();
    let word = word.as_str();

    let dfa = build_dfa(word, 1, is_prefix);
    let starts = StartsWith(Str::new(get_first(word)));
    let mut stream = fst.search_with_state(Intersection(starts, &dfa)).into_stream();

    while let Some((derived_word, state)) = stream.next() {
        let d = dfa.distance(state.1);
        match d.to_u8() {
            0 => (),
            1 => {
                let derived_word = std::str::from_utf8(derived_word)?;
                let derived_word = ctx.word_interner.insert(derived_word.to_owned());
                one_typo_words.insert(derived_word);
                if one_typo_words.len() >= limits::MAX_ONE_TYPO_COUNT {
                    break;
                }
            }
            _ => {
                unreachable!("One typo dfa produced multiple typos")
            }
        }
    }
    Ok(())
}

fn find_one_two_typo_derivations(
    word_interned: Interned<String>,
    is_prefix: bool,
    fst: fst::Set<Cow<'_, [u8]>>,
    word_interner: &mut DedupInterner<String>,
    one_typo_words: &mut BTreeSet<Interned<String>>,
    two_typo_words: &mut BTreeSet<Interned<String>>,
) -> Result<()> {
    let word = word_interner.get(word_interned).to_owned();
    let word = word.as_str();

    let starts = StartsWith(Str::new(get_first(word)));
    let first = Intersection(build_dfa(word, 1, is_prefix), Complement(&starts));
    let second_dfa = build_dfa(word, 2, is_prefix);
    let second = Intersection(&second_dfa, &starts);
    let automaton = Union(first, &second);

    let mut stream = fst.search_with_state(automaton).into_stream();

    while let Some((derived_word, state)) = stream.next() {
        let finished_one_typo_words = one_typo_words.len() >= limits::MAX_ONE_TYPO_COUNT;
        let finished_two_typo_words = two_typo_words.len() >= limits::MAX_TWO_TYPOS_COUNT;
        if finished_one_typo_words && finished_two_typo_words {
            // No chance we will add either one- or two-typo derivations anymore, stop iterating.
            break;
        }
        let derived_word = std::str::from_utf8(derived_word)?;
        // No need to intern here
        // in the case the typo is on the first letter, we know the number of typo
        // is two
        if get_first(derived_word) != get_first(word) && !finished_two_typo_words {
            let derived_word_interned = word_interner.insert(derived_word.to_owned());
            two_typo_words.insert(derived_word_interned);
            continue;
        } else {
            // Else, we know that it is the second dfa that matched and compute the
            // correct distance
            let d = second_dfa.distance((state.1).0);
            match d.to_u8() {
                0 => (),
                1 => {
                    if finished_one_typo_words {
                        continue;
                    }
                    let derived_word_interned = word_interner.insert(derived_word.to_owned());
                    one_typo_words.insert(derived_word_interned);
                }
                2 => {
                    if finished_two_typo_words {
                        continue;
                    }
                    let derived_word_interned = word_interner.insert(derived_word.to_owned());
                    two_typo_words.insert(derived_word_interned);
                }
                _ => unreachable!("2 typos DFA produced a distance greater than 2"),
            }
        }
    }
    Ok(())
}

pub fn partially_initialized_term_from_word(
    ctx: &mut SearchContext<'_>,
    word: &str,
    max_typo: u8,
    is_prefix: bool,
    is_ngram: bool,
) -> Result<QueryTerm> {
    let word_interned = ctx.word_interner.insert(word.to_owned());

    if word.len() > MAX_WORD_LENGTH {
        return Ok({
            QueryTerm {
                original: ctx.word_interner.insert(word.to_owned()),
                ngram_words: None,
                is_prefix: false,
                max_levenshtein_distance: 0,
                zero_typo: <_>::default(),
                one_typo: Lazy::Init(<_>::default()),
                two_typo: Lazy::Init(<_>::default()),
            }
        });
    }

    let use_prefix_db = is_prefix
        && (ctx
            .index
            .word_prefix_docids
            .remap_data_type::<DecodeIgnore>()
            .get(ctx.txn, word)?
            .is_some()
            || (!is_ngram
                && ctx
                    .index
                    .exact_word_prefix_docids
                    .remap_data_type::<DecodeIgnore>()
                    .get(ctx.txn, word)?
                    .is_some()));
    let use_prefix_db = if use_prefix_db { Some(word_interned) } else { None };

    let mut zero_typo = None;
    let mut prefix_of = BTreeSet::new();

    if ctx.index.contains_word(ctx.txn, word)? {
        zero_typo = Some(word_interned);
    }

    if is_prefix && use_prefix_db.is_none() {
        find_zero_typo_prefix_derivations(ctx, word_interned, &mut prefix_of)?;
    }
    let synonyms = ctx.get_synonyms()?;
    let mut synonym_word_count = 0;
    let synonyms = synonyms
        .get(&vec![word.to_owned()])
        .cloned()
        .unwrap_or_default()
        .into_iter()
        .take(limits::MAX_SYNONYM_PHRASE_COUNT)
        .filter_map(|words| {
            if synonym_word_count + words.len() > limits::MAX_SYNONYM_WORD_COUNT {
                return None;
            }
            synonym_word_count += words.len();
            let words = words.into_iter().map(|w| Some(ctx.word_interner.insert(w))).collect();
            Some(ctx.phrase_interner.insert(Phrase { words }))
        })
        .collect();
    let zero_typo =
        ZeroTypoTerm { phrase: None, exact: zero_typo, prefix_of, synonyms, use_prefix_db };

    Ok(QueryTerm {
        original: word_interned,
        ngram_words: None,
        max_levenshtein_distance: max_typo,
        is_prefix,
        zero_typo,
        one_typo: Lazy::Uninit,
        two_typo: Lazy::Uninit,
    })
}

fn find_split_words(ctx: &mut SearchContext<'_>, word: &str) -> Result<Option<Interned<Phrase>>> {
    if let Some((l, r)) = split_best_frequency(ctx, word)? {
        Ok(Some(ctx.phrase_interner.insert(Phrase { words: vec![Some(l), Some(r)] })))
    } else {
        Ok(None)
    }
}

impl Interned<QueryTerm> {
    fn initialize_one_typo_subterm(self, ctx: &mut SearchContext<'_>) -> Result<()> {
        let self_mut = ctx.term_interner.get_mut(self);

        let allows_split_words = self_mut.allows_split_words();
        let QueryTerm {
            original,
            is_prefix,
            one_typo,
            max_levenshtein_distance: max_nbr_typos,
            ..
        } = self_mut;

        let original = *original;
        let is_prefix = *is_prefix;
        // let original_str = ctx.word_interner.get(*original).to_owned();
        if one_typo.is_init() {
            return Ok(());
        }
        let mut one_typo_words = BTreeSet::new();

        if *max_nbr_typos > 0 {
            find_one_typo_derivations(ctx, original, is_prefix, &mut one_typo_words)?;
        }

        let split_words = if allows_split_words {
            let original_str = ctx.word_interner.get(original).to_owned();
            find_split_words(ctx, original_str.as_str())?
        } else {
            None
        };

        let self_mut = ctx.term_interner.get_mut(self);

        // Only add the split words to the derivations if:
        // 1. the term is neither an ngram nor a phrase; OR
        // 2. the term is an ngram, but the split words are different from the ngram's component words
        let split_words = if let Some((ngram_words, split_words)) =
            self_mut.ngram_words.as_ref().zip(split_words.as_ref())
        {
            let Phrase { words } = ctx.phrase_interner.get(*split_words);
            if ngram_words.iter().ne(words.iter().flatten()) {
                Some(*split_words)
            } else {
                None
            }
        } else {
            split_words
        };
        let one_typo = OneTypoTerm { split_words, one_typo: one_typo_words };

        self_mut.one_typo = Lazy::Init(one_typo);

        Ok(())
    }
    fn initialize_one_and_two_typo_subterm(self, ctx: &mut SearchContext<'_>) -> Result<()> {
        let self_mut = ctx.term_interner.get_mut(self);
        let QueryTerm {
            original,
            is_prefix,
            two_typo,
            max_levenshtein_distance: max_nbr_typos,
            ..
        } = self_mut;
        let original_str = ctx.word_interner.get(*original).to_owned();
        if two_typo.is_init() {
            return Ok(());
        }
        let mut one_typo_words = BTreeSet::new();
        let mut two_typo_words = BTreeSet::new();

        if *max_nbr_typos > 0 {
            find_one_two_typo_derivations(
                *original,
                *is_prefix,
                ctx.index.words_fst(ctx.txn)?,
                &mut ctx.word_interner,
                &mut one_typo_words,
                &mut two_typo_words,
            )?;
        }

        let split_words = find_split_words(ctx, original_str.as_str())?;
        let self_mut = ctx.term_interner.get_mut(self);

        let one_typo = OneTypoTerm { one_typo: one_typo_words, split_words };

        let two_typo = TwoTypoTerm { two_typos: two_typo_words };

        self_mut.one_typo = Lazy::Init(one_typo);
        self_mut.two_typo = Lazy::Init(two_typo);

        Ok(())
    }
}

/// Split the original word into the two words that appear the
/// most next to each other in the index.
///
/// Return `None` if the original word cannot be split.
fn split_best_frequency(
    ctx: &mut SearchContext<'_>,
    original: &str,
) -> Result<Option<(Interned<String>, Interned<String>)>> {
    let chars = original.char_indices().skip(1);
    let mut best = None;

    for (i, _) in chars {
        let (left, right) = original.split_at(i);
        let left = ctx.word_interner.insert(left.to_owned());
        let right = ctx.word_interner.insert(right.to_owned());

        if let Some(frequency) = ctx.get_db_word_pair_proximity_docids_len(None, left, right, 1)? {
            if best.is_none_or(|(old, _, _)| frequency > old) {
                best = Some((frequency, left, right));
            }
        }
    }

    Ok(best.map(|(_, left, right)| (left, right)))
}
