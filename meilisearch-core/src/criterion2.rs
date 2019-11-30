use std::cmp::{self, Ordering, Reverse};
use std::borrow::Cow;
use std::sync::atomic::{self, AtomicUsize};

use slice_group_by::{GroupBy, GroupByMut};
use compact_arena::SmallArena;
use sdset::{Set, SetBuf};

use crate::{DocIndex, DocumentId};
use crate::bucket_sort::{BareMatch, SimpleMatch, RawDocument, PostingsListView};
use crate::automaton::QueryEnhancer;

type PostingsListsArena<'tag, 'txn> = SmallArena<'tag, PostingsListView<'txn>>;

pub trait Criterion {
    fn name(&self) -> &str;

    fn prepare<'a, 'tag, 'txn>(
        &self,
        documents: &mut [RawDocument<'a, 'tag>],
        postings_lists: &mut PostingsListsArena<'tag, 'txn>,
        query_enhancer: &QueryEnhancer,
    );

    fn evaluate<'a, 'tag, 'txn>(
        &self,
        lhs: &RawDocument<'a, 'tag>,
        rhs: &RawDocument<'a, 'tag>,
        postings_lists: &PostingsListsArena<'tag, 'txn>,
    ) -> Ordering;

    #[inline]
    fn eq<'a, 'tag, 'txn>(
        &self,
        lhs: &RawDocument<'a, 'tag>,
        rhs: &RawDocument<'a, 'tag>,
        postings_lists: &PostingsListsArena<'tag, 'txn>,
    ) -> bool
    {
        self.evaluate(lhs, rhs, postings_lists) == Ordering::Equal
    }
}

pub struct Typo;

impl Criterion for Typo {
    fn name(&self) -> &str { "typo" }

    fn prepare(
        &self,
        documents: &mut [RawDocument],
        postings_lists: &mut PostingsListsArena,
        query_enhancer: &QueryEnhancer,
    ) {
        for document in documents {
            document.raw_matches.sort_unstable_by_key(|bm| (bm.query_index, bm.distance));
        }
    }

    fn evaluate(
        &self,
        lhs: &RawDocument,
        rhs: &RawDocument,
        postings_lists: &PostingsListsArena,
    ) -> Ordering
    {
        // This function is a wrong logarithmic 10 function.
        // It is safe to panic on input number higher than 3,
        // the number of typos is never bigger than that.
        #[inline]
        fn custom_log10(n: u8) -> f32 {
            match n {
                0 => 0.0,     // log(1)
                1 => 0.30102, // log(2)
                2 => 0.47712, // log(3)
                3 => 0.60205, // log(4)
                _ => panic!("invalid number"),
            }
        }

        #[inline]
        fn compute_typos(matches: &[BareMatch]) -> usize {
            let mut number_words: usize = 0;
            let mut sum_typos = 0.0;

            for group in matches.linear_group_by_key(|bm| bm.query_index) {
                sum_typos += custom_log10(group[0].distance);
                number_words += 1;
            }

            (number_words as f32 / (sum_typos + 1.0) * 1000.0) as usize
        }

        let lhs = compute_typos(&lhs.raw_matches);
        let rhs = compute_typos(&rhs.raw_matches);

        lhs.cmp(&rhs).reverse()
    }
}

pub struct Words;

impl Criterion for Words {
    fn name(&self) -> &str { "words" }

    fn prepare(
        &self,
        documents: &mut [RawDocument],
        postings_lists: &mut PostingsListsArena,
        query_enhancer: &QueryEnhancer,
    ) {
        for document in documents {
            document.raw_matches.sort_unstable_by_key(|bm| bm.query_index);
        }
    }

    fn evaluate(
        &self,
        lhs: &RawDocument,
        rhs: &RawDocument,
        postings_lists: &PostingsListsArena,
    ) -> Ordering
    {
        #[inline]
        fn number_of_query_words(matches: &[BareMatch]) -> usize {
            matches.linear_group_by_key(|bm| bm.query_index).count()
        }

        let lhs = number_of_query_words(&lhs.raw_matches);
        let rhs = number_of_query_words(&rhs.raw_matches);

        lhs.cmp(&rhs).reverse()
    }
}

fn process_raw_matches<'a, 'tag, 'txn>(
    documents: &mut [RawDocument<'a, 'tag>],
    postings_lists: &mut PostingsListsArena<'tag, 'txn>,
    query_enhancer: &QueryEnhancer,
) {
    for document in documents {
        if document.processed_matches.is_some() { continue }

        let mut processed = Vec::new();
        let document_id = document.raw_matches[0].document_id;

        for m in document.raw_matches.iter() {
            let postings_list = &postings_lists[m.postings_list];
            processed.reserve(postings_list.len());
            for di in postings_list.as_ref() {
                let simple_match = SimpleMatch {
                    query_index: m.query_index,
                    distance: m.distance,
                    attribute: di.attribute,
                    word_index: di.word_index,
                    is_exact: m.is_exact,
                };
                processed.push(simple_match);
            }
        }

        let processed = multiword_rewrite_matches(&mut processed, query_enhancer);
        document.processed_matches = Some(processed.into_vec());
    }
}

pub struct Proximity;

impl Criterion for Proximity {
    fn name(&self) -> &str { "proximity" }

    fn prepare<'a, 'tag, 'txn>(
        &self,
        documents: &mut [RawDocument<'a, 'tag>],
        postings_lists: &mut PostingsListsArena<'tag, 'txn>,
        query_enhancer: &QueryEnhancer,
    ) {
        process_raw_matches(documents, postings_lists, query_enhancer);
    }

    fn evaluate<'a, 'tag, 'txn>(
        &self,
        lhs: &RawDocument<'a, 'tag>,
        rhs: &RawDocument<'a, 'tag>,
        postings_lists: &PostingsListsArena<'tag, 'txn>,
    ) -> Ordering
    {
        const MAX_DISTANCE: u16 = 8;

        fn index_proximity(lhs: u16, rhs: u16) -> u16 {
            if lhs < rhs {
                cmp::min(rhs - lhs, MAX_DISTANCE)
            } else {
                cmp::min(lhs - rhs, MAX_DISTANCE) + 1
            }
        }

        fn attribute_proximity(lhs: SimpleMatch, rhs: SimpleMatch) -> u16 {
            if lhs.attribute != rhs.attribute { MAX_DISTANCE }
            else { index_proximity(lhs.word_index, rhs.word_index) }
        }

        fn min_proximity(lhs: &[SimpleMatch], rhs: &[SimpleMatch]) -> u16 {
            let mut min_prox = u16::max_value();
            for a in lhs {
                for b in rhs {
                    let prox = attribute_proximity(*a, *b);
                    min_prox = cmp::min(min_prox, prox);
                }
            }
            min_prox
        }

        fn matches_proximity(matches: &[SimpleMatch],) -> u16 {
            let mut proximity = 0;
            let mut iter = matches.linear_group_by_key(|m| m.query_index);

            // iterate over groups by windows of size 2
            let mut last = iter.next();
            while let (Some(lhs), Some(rhs)) = (last, iter.next()) {
                proximity += min_proximity(lhs, rhs);
                last = Some(rhs);
            }

            proximity
        }

        let lhs = matches_proximity(&lhs.processed_matches.as_ref().unwrap());
        let rhs = matches_proximity(&rhs.processed_matches.as_ref().unwrap());

        lhs.cmp(&rhs)
    }
}

pub struct Attribute;

impl Criterion for Attribute {
    fn name(&self) -> &str { "attribute" }

    fn prepare<'a, 'tag, 'txn>(
        &self,
        documents: &mut [RawDocument<'a, 'tag>],
        postings_lists: &mut PostingsListsArena<'tag, 'txn>,
        query_enhancer: &QueryEnhancer,
    ) {
        process_raw_matches(documents, postings_lists, query_enhancer);
    }

    fn evaluate<'a, 'tag, 'txn>(
        &self,
        lhs: &RawDocument<'a, 'tag>,
        rhs: &RawDocument<'a, 'tag>,
        postings_lists: &PostingsListsArena<'tag, 'txn>,
    ) -> Ordering
    {
        #[inline]
        fn sum_attribute(matches: &[SimpleMatch]) -> usize {
            let mut sum_attribute = 0;
            for group in matches.linear_group_by_key(|bm| bm.query_index) {
                sum_attribute += group[0].attribute as usize;
            }
            sum_attribute
        }

        let lhs = sum_attribute(&lhs.processed_matches.as_ref().unwrap());
        let rhs = sum_attribute(&rhs.processed_matches.as_ref().unwrap());

        lhs.cmp(&rhs)
    }
}

pub struct WordsPosition;

impl Criterion for WordsPosition {
    fn name(&self) -> &str { "words position" }

    fn prepare<'a, 'tag, 'txn>(
        &self,
        documents: &mut [RawDocument<'a, 'tag>],
        postings_lists: &mut PostingsListsArena<'tag, 'txn>,
        query_enhancer: &QueryEnhancer,
    ) {
        process_raw_matches(documents, postings_lists, query_enhancer);
    }

    fn evaluate<'a, 'tag, 'txn>(
        &self,
        lhs: &RawDocument<'a, 'tag>,
        rhs: &RawDocument<'a, 'tag>,
        postings_lists: &PostingsListsArena<'tag, 'txn>,
    ) -> Ordering
    {
        #[inline]
        fn sum_words_position(matches: &[SimpleMatch]) -> usize {
            let mut sum_words_position = 0;
            for group in matches.linear_group_by_key(|bm| bm.query_index) {
                sum_words_position += group[0].word_index as usize;
            }
            sum_words_position
        }

        let lhs = sum_words_position(&lhs.processed_matches.as_ref().unwrap());
        let rhs = sum_words_position(&rhs.processed_matches.as_ref().unwrap());

        lhs.cmp(&rhs)
    }
}

pub struct Exact;

impl Criterion for Exact {
    fn name(&self) -> &str { "exact" }

    fn prepare(
        &self,
        documents: &mut [RawDocument],
        postings_lists: &mut PostingsListsArena,
        query_enhancer: &QueryEnhancer,
    ) {
        for document in documents {
            document.raw_matches.sort_unstable_by_key(|bm| (bm.query_index, Reverse(bm.is_exact)));
        }
    }

    fn evaluate(
        &self,
        lhs: &RawDocument,
        rhs: &RawDocument,
        postings_lists: &PostingsListsArena,
    ) -> Ordering
    {
        #[inline]
        fn sum_exact_query_words(matches: &[BareMatch]) -> usize {
            let mut sum_exact_query_words = 0;

            for group in matches.linear_group_by_key(|bm| bm.query_index) {
                sum_exact_query_words += group[0].is_exact as usize;
            }

            sum_exact_query_words
        }

        let lhs = sum_exact_query_words(&lhs.raw_matches);
        let rhs = sum_exact_query_words(&rhs.raw_matches);

        lhs.cmp(&rhs).reverse()
    }
}

pub struct StableDocId;

impl Criterion for StableDocId {
    fn name(&self) -> &str { "stable document id" }

    fn prepare(
        &self,
        documents: &mut [RawDocument],
        postings_lists: &mut PostingsListsArena,
        query_enhancer: &QueryEnhancer,
    ) {
        // ...
    }

    fn evaluate(
        &self,
        lhs: &RawDocument,
        rhs: &RawDocument,
        postings_lists: &PostingsListsArena,
    ) -> Ordering
    {
        let lhs = &lhs.raw_matches[0].document_id;
        let rhs = &rhs.raw_matches[0].document_id;

        lhs.cmp(rhs)
    }
}

pub fn multiword_rewrite_matches(
    matches: &mut [SimpleMatch],
    query_enhancer: &QueryEnhancer,
) -> SetBuf<SimpleMatch>
{
    let mut padded_matches = Vec::with_capacity(matches.len());

    // let before_sort = Instant::now();
    // we sort the matches by word index to make them rewritable
    matches.sort_unstable_by_key(|m| (m.attribute, m.word_index));
    // debug!("sorting dirty matches took {:.02?}", before_sort.elapsed());

    // let before_padding = Instant::now();
    // for each attribute of each document
    for same_document_attribute in matches.linear_group_by_key(|m| m.attribute) {
        // padding will only be applied
        // to word indices in the same attribute
        let mut padding = 0;
        let mut iter = same_document_attribute.linear_group_by_key(|m| m.word_index);

        // for each match at the same position
        // in this document attribute
        while let Some(same_word_index) = iter.next() {
            // find the biggest padding
            let mut biggest = 0;
            for match_ in same_word_index {
                let mut replacement = query_enhancer.replacement(match_.query_index as u32);
                let replacement_len = replacement.len();
                let nexts = iter.remainder().linear_group_by_key(|m| m.word_index);

                if let Some(query_index) = replacement.next() {
                    let word_index = match_.word_index + padding as u16;
                    let query_index = query_index as u16;
                    let match_ = SimpleMatch { query_index, word_index, ..*match_ };
                    padded_matches.push(match_);
                }

                let mut found = false;

                // look ahead and if there already is a match
                // corresponding to this padding word, abort the padding
                'padding: for (x, next_group) in nexts.enumerate() {
                    for (i, query_index) in replacement.clone().enumerate().skip(x) {
                        let word_index = match_.word_index + padding as u16 + (i + 1) as u16;
                        let query_index = query_index as u16;
                        let padmatch = SimpleMatch { query_index, word_index, ..*match_ };

                        for nmatch_ in next_group {
                            let mut rep = query_enhancer.replacement(nmatch_.query_index as u32);
                            let query_index = rep.next().unwrap() as u16;
                            if query_index == padmatch.query_index {
                                if !found {
                                    // if we find a corresponding padding for the
                                    // first time we must push preceding paddings
                                    for (i, query_index) in replacement.clone().enumerate().take(i)
                                    {
                                        let word_index = match_.word_index + padding as u16 + (i + 1) as u16;
                                        let query_index = query_index as u16;
                                        let match_ = SimpleMatch { query_index, word_index, ..*match_ };
                                        padded_matches.push(match_);
                                        biggest = biggest.max(i + 1);
                                    }
                                }

                                padded_matches.push(padmatch);
                                found = true;
                                continue 'padding;
                            }
                        }
                    }

                    // if we do not find a corresponding padding in the
                    // next groups so stop here and pad what was found
                    break;
                }

                if !found {
                    // if no padding was found in the following matches
                    // we must insert the entire padding
                    for (i, query_index) in replacement.enumerate() {
                        let word_index = match_.word_index + padding as u16 + (i + 1) as u16;
                        let query_index = query_index as u16;
                        let match_ = SimpleMatch { query_index, word_index, ..*match_ };
                        padded_matches.push(match_);
                    }

                    biggest = biggest.max(replacement_len - 1);
                }
            }

            padding += biggest;
        }
    }

    // debug!("padding matches took {:.02?}", before_padding.elapsed());

    // With this check we can see that the loop above takes something
    // like 43% of the search time even when no rewrite is needed.
    // assert_eq!(before_matches, padded_matches);

    SetBuf::from_dirty(padded_matches)
}
