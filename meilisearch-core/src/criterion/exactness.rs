use std::cmp::{Ordering, Reverse};
use std::collections::hash_map::{HashMap, Entry};
use meilisearch_schema::IndexedPos;
use slice_group_by::GroupBy;
use crate::{RawDocument, MResult};
use crate::bucket_sort::BareMatch;
use super::{Criterion, Context, ContextMut};

pub struct Exactness;

impl Criterion for Exactness {
    fn name(&self) -> &str { "exactness" }

    fn prepare<'h, 'p, 'tag, 'txn, 'q, 'r>(
        &self,
        ctx: ContextMut<'h, 'p, 'tag, 'txn, 'q>,
        documents: &mut [RawDocument<'r, 'tag>],
    ) -> MResult<()>
    {
        let store = ctx.documents_fields_counts_store;
        let reader = ctx.reader;

        'documents: for doc in documents {
            doc.bare_matches.sort_unstable_by_key(|bm| (bm.query_index, Reverse(bm.is_exact)));

            // mark the document if we find a "one word field" that matches
            let mut fields_counts = HashMap::new();
            for group in doc.bare_matches.linear_group_by_key(|bm| bm.query_index) {
                for group in group.linear_group_by_key(|bm| bm.is_exact) {
                    if !group[0].is_exact { break }

                    for bm in group {
                        for di in ctx.postings_lists[bm.postings_list].as_ref() {

                            let attr = IndexedPos(di.attribute);
                            let count = match fields_counts.entry(attr) {
                                Entry::Occupied(entry) => *entry.get(),
                                Entry::Vacant(entry) => {
                                    let count = store.document_field_count(reader, doc.id, attr)?;
                                    *entry.insert(count)
                                },
                            };

                            if count == Some(1) {
                                doc.contains_one_word_field = true;
                                continue 'documents
                            }
                        }
                    }
                }
            }
        }

        Ok(())
    }

    fn evaluate(&self, _ctx: &Context, lhs: &RawDocument, rhs: &RawDocument) -> Ordering {
        #[inline]
        fn sum_exact_query_words(matches: &[BareMatch]) -> usize {
            let mut sum_exact_query_words = 0;

            for group in matches.linear_group_by_key(|bm| bm.query_index) {
                sum_exact_query_words += group[0].is_exact as usize;
            }

            sum_exact_query_words
        }

        // does it contains a "one word field"
        lhs.contains_one_word_field.cmp(&rhs.contains_one_word_field).reverse()
        // if not, with document contains the more exact words
        .then_with(|| {
            let lhs = sum_exact_query_words(&lhs.bare_matches);
            let rhs = sum_exact_query_words(&rhs.bare_matches);
            lhs.cmp(&rhs).reverse()
        })
    }
}
