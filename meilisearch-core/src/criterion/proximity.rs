use std::cmp::{self, Ordering};

use compact_arena::SmallArena;
use slice_group_by::GroupBy;

use crate::automaton::QueryEnhancer;
use crate::bucket_sort::{PostingsListView, SimpleMatch, QueryWordAutomaton};
use crate::RawDocument;

use super::{Criterion, prepare_raw_matches};

pub struct Proximity;

impl Criterion for Proximity {
    fn name(&self) -> &str { "proximity" }

    fn prepare<'a, 'tag, 'txn>(
        &self,
        documents: &mut [RawDocument<'a, 'tag>],
        postings_lists: &mut SmallArena<'tag, PostingsListView<'txn>>,
        query_enhancer: &QueryEnhancer,
        automatons: &[QueryWordAutomaton],
    ) {
        prepare_raw_matches(documents, postings_lists, query_enhancer, automatons);
    }

    fn evaluate<'a, 'tag, 'txn>(
        &self,
        lhs: &RawDocument<'a, 'tag>,
        rhs: &RawDocument<'a, 'tag>,
        postings_lists: &SmallArena<'tag, PostingsListView<'txn>>,
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

        let lhs = matches_proximity(&lhs.processed_matches);
        let rhs = matches_proximity(&rhs.processed_matches);

        lhs.cmp(&rhs)
    }
}
