use std::collections::HashSet;

use big_s::S;
use milli::update::Settings;
use milli::{Criterion, Search, SearchResult, TermsMatchingStrategy};
use Criterion::*;

use crate::search::{self, EXTERNAL_DOCUMENTS_IDS};

macro_rules! test_distinct {
    ($func:ident, $distinct:ident, $criteria:expr, $n_res:expr) => {
        #[test]
        fn $func() {
            let criteria = $criteria;
            let index = search::setup_search_index_with_criteria(&criteria);

            // update distinct attribute
            let mut wtxn = index.write_txn().unwrap();
            let config = milli::update::IndexerConfig::default();
            let mut builder = Settings::new(&mut wtxn, &index, &config);
            builder.set_distinct_field(S(stringify!($distinct)));
            builder.execute(|_| (), || false).unwrap();
            wtxn.commit().unwrap();

            let rtxn = index.read_txn().unwrap();

            let mut search = Search::new(&rtxn, &index);
            search.query(search::TEST_QUERY);
            search.limit(EXTERNAL_DOCUMENTS_IDS.len());
            search.authorize_typos(true);
            search.terms_matching_strategy(TermsMatchingStrategy::default());

            let SearchResult { documents_ids, candidates, .. } = search.execute().unwrap();

            assert_eq!(candidates.len(), $n_res);

            let mut distinct_values = HashSet::new();
            let expected_external_ids: Vec<_> =
                search::expected_order(&criteria, true, TermsMatchingStrategy::default(), &[])
                    .into_iter()
                    .filter_map(|d| {
                        if distinct_values.contains(&d.$distinct) {
                            None
                        } else {
                            distinct_values.insert(d.$distinct.to_owned());
                            Some(d.id)
                        }
                    })
                    .collect();

            let documents_ids = search::internal_to_external_ids(&index, &documents_ids);
            assert_eq!(documents_ids, expected_external_ids);
        }
    };
}

test_distinct!(
    distinct_string_default_criteria,
    tag,
    vec![Words, Typo, Proximity, Attribute, Exactness],
    3
);
test_distinct!(
    distinct_number_default_criteria,
    asc_desc_rank,
    vec![Words, Typo, Proximity, Attribute, Exactness],
    7
);
test_distinct!(distinct_string_criterion_words, tag, vec![Words], 3);
test_distinct!(distinct_number_criterion_words, asc_desc_rank, vec![Words], 7);
test_distinct!(distinct_string_criterion_words_typo, tag, vec![Words, Typo], 3);
test_distinct!(distinct_number_criterion_words_typo, asc_desc_rank, vec![Words, Typo], 7);
test_distinct!(distinct_string_criterion_words_proximity, tag, vec![Words, Proximity], 3);
test_distinct!(distinct_number_criterion_words_proximity, asc_desc_rank, vec![Words, Proximity], 7);
test_distinct!(distinct_string_criterion_words_attribute, tag, vec![Words, Attribute], 3);
test_distinct!(distinct_number_criterion_words_attribute, asc_desc_rank, vec![Words, Attribute], 7);
test_distinct!(distinct_string_criterion_words_exactness, tag, vec![Words, Exactness], 3);
test_distinct!(distinct_number_criterion_words_exactness, asc_desc_rank, vec![Words, Exactness], 7);
