use std::cmp::{self, Ordering, Reverse};
use std::borrow::Cow;
use std::sync::atomic::{self, AtomicUsize};

use slice_group_by::{GroupBy, GroupByMut};
use compact_arena::SmallArena;
use sdset::Set;

use crate::{DocIndex, DocumentId};
use crate::bucket_sort::BareMatch;
use crate::bucket_sort::RawDocument;

type PostingsListsArena<'tag, 'txn> = SmallArena<'tag, Cow<'txn, Set<DocIndex>>>;

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
            document.matches.sort_unstable_by_key(|bm| (bm.query_index, bm.distance));
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

        let lhs = compute_typos(&lhs.matches);
        let rhs = compute_typos(&rhs.matches);

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
            document.matches.sort_unstable_by_key(|bm| bm.query_index);
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

        let lhs = number_of_query_words(&lhs.matches);
        let rhs = number_of_query_words(&rhs.matches);

        lhs.cmp(&rhs).reverse()
    }
}

pub struct Proximity;

impl Criterion for Proximity {
    fn name(&self) -> &str { "proximity" }

    fn prepare(
        &self,
        documents: &mut [RawDocument],
        postings_lists: &mut PostingsListsArena,
    ) {
        for document in documents {
            document.matches.sort_unstable_by_key(|bm| (bm.query_index, bm.distance));
        }
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

        fn attribute_proximity((lattr, lwi): (u16, u16), (rattr, rwi): (u16, u16)) -> u16 {
            if lattr != rattr {
                return MAX_DISTANCE;
            }
            index_proximity(lwi, rwi)
        }

        // fn min_proximity<'tag, 'txn>(
        //     lhs: &[BareMatch<'tag>],
        //     rhs: &[BareMatch<'tag>],
        //     postings_lists: &PostingsListsArena<'tag, 'txn>) -> u16
        // {
        //     let mut min_prox = u16::max_value();

        //     for a in lhs {
        //         let pla = &postings_lists[a.postings_list];
        //         for b in rhs {
        //             let plb = &postings_lists[b.postings_list];

        //             // let a = (a.pos );
        //             min_prox = cmp::min(min_prox, attribute_proximity(a, b));
        //         }
        //     }

        //     // for a in lattr.iter().zip(lwi) {
        //     //     for b in rattr.iter().zip(rwi) {
        //     //         let a = clone_tuple(a);
        //     //         let b = clone_tuple(b);
        //     //         min_prox = cmp::min(min_prox, attribute_proximity(a, b));
        //     //     }
        //     // }

        //     min_prox
        // }

        unimplemented!()
    }
}

pub static ATTRIBUTE_CALLED_NUMBER: AtomicUsize = AtomicUsize::new(0);

pub struct Attribute;

impl Criterion for Attribute {
    fn name(&self) -> &str { "attribute" }

    fn prepare(&self, documents: &mut [RawDocument], postings_lists: &mut PostingsListsArena) {
        for document in documents {
            document.matches.sort_unstable_by_key(|bm| bm.query_index);
        }
    }

    fn evaluate<'a, 'tag, 'txn>(
        &self,
        lhs: &RawDocument<'a, 'tag>,
        rhs: &RawDocument<'a, 'tag>,
        postings_lists: &PostingsListsArena<'tag, 'txn>,
    ) -> Ordering
    {
        #[inline]
        fn sum_attribute<'tag, 'txn>(
            matches: &[BareMatch<'tag>],
            postings_lists: &PostingsListsArena<'tag, 'txn>,
        ) -> usize
        {
            let mut sum_attribute = 0;

            for group in matches.linear_group_by_key(|bm| bm.query_index) {
                let document_id = &group[0].document_id;
                let index = group[0].postings_list;
                let postings_list = &postings_lists[index];
                // sum_attribute += postings_list[0].attribute as usize;
                if let Ok(index) = postings_list.binary_search_by_key(document_id, |p| p.document_id) {
                    sum_attribute += postings_list[index].attribute as usize;
                }
            }

            sum_attribute
        }

        ATTRIBUTE_CALLED_NUMBER.fetch_add(1, atomic::Ordering::SeqCst);

        let lhs = sum_attribute(&lhs.matches, postings_lists);
        let rhs = sum_attribute(&rhs.matches, postings_lists);

        lhs.cmp(&rhs)
    }
}

pub struct WordsPosition;

impl Criterion for WordsPosition {
    fn name(&self) -> &str { "words position" }

    fn prepare(&self, documents: &mut [RawDocument], postings_lists: &mut PostingsListsArena) {
        for document in documents {
            document.matches.sort_unstable_by_key(|bm| bm.query_index);
        }
    }

    fn evaluate<'a, 'tag, 'txn>(
        &self,
        lhs: &RawDocument<'a, 'tag>,
        rhs: &RawDocument<'a, 'tag>,
        postings_lists: &PostingsListsArena<'tag, 'txn>,
    ) -> Ordering
    {
        #[inline]
        fn sum_words_position<'tag, 'txn>(
            matches: &[BareMatch<'tag>],
            postings_lists: &PostingsListsArena<'tag, 'txn>,
        ) -> usize
        {
            let mut sum_words_position = 0;

            for group in matches.linear_group_by_key(|bm| bm.query_index) {
                let document_id = &group[0].document_id;
                let index = group[0].postings_list;
                let postings_list = &postings_lists[index];
                // sum_words_position += postings_list[0].word_index as usize;
                if let Ok(index) = postings_list.binary_search_by_key(document_id, |p| p.document_id) {
                    sum_words_position += postings_list[index].word_index as usize;
                }
            }

            sum_words_position
        }

        let lhs = sum_words_position(&lhs.matches, postings_lists);
        let rhs = sum_words_position(&rhs.matches, postings_lists);

        lhs.cmp(&rhs)
    }
}

pub struct Exact;

impl Criterion for Exact {
    fn name(&self) -> &str { "exact" }

    fn prepare(&self, documents: &mut [RawDocument], postings_lists: &mut PostingsListsArena) {
        for document in documents {
            document.matches.sort_unstable_by_key(|bm| (bm.query_index, Reverse(bm.is_exact)));
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

        let lhs = sum_exact_query_words(&lhs.matches);
        let rhs = sum_exact_query_words(&rhs.matches);

        lhs.cmp(&rhs).reverse()
    }
}
