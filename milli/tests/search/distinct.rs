use std::collections::HashSet;

use big_s::S;
use milli::update::Settings;
use milli::{Criterion, Search, SearchResult};
use Criterion::*;

use crate::search::{self, EXTERNAL_DOCUMENTS_IDS};

macro_rules! test_distinct {
    ($func:ident, $distinct:ident, $criteria:expr) => {
        #[test]
        fn $func() {
            let criteria = $criteria;
            let index = search::setup_search_index_with_criteria(&criteria);

            // update distinct attribute
            let mut wtxn = index.write_txn().unwrap();
            let mut builder = Settings::new(&mut wtxn, &index);
            builder.set_distinct_field(S(stringify!($distinct)));
            builder.execute(|_| ()).unwrap();
            wtxn.commit().unwrap();

            let rtxn = index.read_txn().unwrap();

            let mut search = Search::new(&rtxn, &index);
            search.query(search::TEST_QUERY);
            search.limit(EXTERNAL_DOCUMENTS_IDS.len());
            search.authorize_typos(true);
            search.optional_words(true);

            let SearchResult { documents_ids, .. } = search.execute().unwrap();

            let mut distinct_values = HashSet::new();
            let expected_external_ids: Vec<_> = search::expected_order(&criteria, true, true, &[])
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
    vec![Words, Typo, Proximity, Attribute, Exactness]
);
test_distinct!(
    distinct_number_default_criteria,
    asc_desc_rank,
    vec![Words, Typo, Proximity, Attribute, Exactness]
);
test_distinct!(distinct_string_criterion_words, tag, vec![Words]);
test_distinct!(distinct_number_criterion_words, asc_desc_rank, vec![Words]);
test_distinct!(distinct_string_criterion_words_typo, tag, vec![Words, Typo]);
test_distinct!(distinct_number_criterion_words_typo, asc_desc_rank, vec![Words, Typo]);
test_distinct!(distinct_string_criterion_words_proximity, tag, vec![Words, Proximity]);
test_distinct!(distinct_number_criterion_words_proximity, asc_desc_rank, vec![Words, Proximity]);
test_distinct!(distinct_string_criterion_words_attribute, tag, vec![Words, Attribute]);
test_distinct!(distinct_number_criterion_words_attribute, asc_desc_rank, vec![Words, Attribute]);
test_distinct!(distinct_string_criterion_words_exactness, tag, vec![Words, Exactness]);
test_distinct!(distinct_number_criterion_words_exactness, asc_desc_rank, vec![Words, Exactness]);
