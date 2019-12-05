use std::cmp::{self, Ordering, Reverse};
use std::borrow::Cow;
use std::sync::atomic::{self, AtomicUsize};

use slice_group_by::{GroupBy, GroupByMut};
use compact_arena::SmallArena;
use sdset::Set;

use crate::{DocIndex, DocumentId};
use crate::bucket_sort::{BareMatch, SimpleMatch, RawDocument, PostingsListView};

type PostingsListsArena<'tag, 'txn> = SmallArena<'tag, PostingsListView<'txn>>;

pub trait Criterion {
    fn name(&self) -> &str;

    fn prepare<'a, 'tag, 'txn>(
        &self,
        documents: &mut [RawDocument<'a, 'tag>],
        postings_lists: &mut PostingsListsArena<'tag, 'txn>,
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
        processed.sort_unstable();
        document.processed_matches = Some(processed);
    }
}

pub struct Proximity;

impl Criterion for Proximity {
    fn name(&self) -> &str { "proximity" }

    fn prepare<'a, 'tag, 'txn>(
        &self,
        documents: &mut [RawDocument<'a, 'tag>],
        postings_lists: &mut PostingsListsArena<'tag, 'txn>,
    ) {
        process_raw_matches(documents, postings_lists);
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
                    min_prox = cmp::min(min_prox, attribute_proximity(*a, *b));
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
    ) {
        process_raw_matches(documents, postings_lists);
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
    ) {
        process_raw_matches(documents, postings_lists);
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

    fn prepare(&self, documents: &mut [RawDocument], postings_lists: &mut PostingsListsArena) {
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
