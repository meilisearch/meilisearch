use either::{Either, Left, Right};
use milli::{Criterion, FilterCondition, Search, SearchResult};
use Criterion::*;

use crate::search::{self, EXTERNAL_DOCUMENTS_IDS};

macro_rules! test_filter {
    ($func:ident, $filter:expr) => {
        #[test]
        fn $func() {
            let criteria = vec![Words, Typo, Proximity, Attribute, Exactness];
            let index = search::setup_search_index_with_criteria(&criteria);
            let mut rtxn = index.read_txn().unwrap();

            let filter_conditions =
                FilterCondition::from_array::<Vec<Either<Vec<&str>, &str>>, _, _, _>(
                    &rtxn, &index, $filter,
                )
                .unwrap()
                .unwrap();

            let mut search = Search::new(&mut rtxn, &index);
            search.query(search::TEST_QUERY);
            search.limit(EXTERNAL_DOCUMENTS_IDS.len());
            search.authorize_typos(true);
            search.optional_words(true);
            search.filter(filter_conditions);

            let SearchResult { documents_ids, .. } = search.execute().unwrap();

            let filtered_ids = search::expected_filtered_ids($filter);
            let expected_external_ids: Vec<_> = search::expected_order(&criteria, true, true)
                .into_iter()
                .filter_map(|d| if filtered_ids.contains(&d.id) { Some(d.id) } else { None })
                .collect();

            let documents_ids = search::internal_to_external_ids(&index, &documents_ids);
            assert_eq!(documents_ids, expected_external_ids);
        }
    };
}

#[rustfmt::skip]
test_filter!(eq_simple_string_filter,           vec![Right("tag=red")]);
#[rustfmt::skip]
test_filter!(eq_simple_number_filter,           vec![Right("asc_desc_rank=1")]);
#[rustfmt::skip]
test_filter!(eq_string_and_filter_return_empty, vec![Right("tag=red"), Right("tag=green")]);
#[rustfmt::skip]
test_filter!(eq_mix_and_filter,                 vec![Right("tag=red"), Right("asc_desc_rank=1")]);
#[rustfmt::skip]
test_filter!(eq_string_or_filter,               vec![Left(vec!["tag=red", "tag=green"])]);
#[rustfmt::skip]
test_filter!(eq_mix_or_filter,                  vec![Left(vec!["tag=red", "asc_desc_rank=1"])]);
#[rustfmt::skip]
test_filter!(eq_number_or_filter,               vec![Left(vec!["asc_desc_rank=3", "asc_desc_rank=1"])]);
#[rustfmt::skip]
test_filter!(eq_complex_filter,                 vec![Left(vec!["tag=red", "tag=green"]), Right("asc_desc_rank=3")]);
#[rustfmt::skip]
test_filter!(eq_complex_filter_2,               vec![Left(vec!["tag=red", "tag=green"]), Left(vec!["asc_desc_rank=3", "asc_desc_rank=1"])]);
#[rustfmt::skip]
test_filter!(greater_simple_number_filter,      vec![Right("asc_desc_rank>1")]);
#[rustfmt::skip]
test_filter!(greater_mix_and_filter,            vec![Right("tag=red"), Right("asc_desc_rank>1")]);
#[rustfmt::skip]
test_filter!(greater_mix_or_filter,             vec![Left(vec!["tag=red", "asc_desc_rank>1"])]);
#[rustfmt::skip]
test_filter!(greater_number_or_filter,          vec![Left(vec!["asc_desc_rank>3", "asc_desc_rank>1"])]);
#[rustfmt::skip]
test_filter!(greater_complex_filter,            vec![Left(vec!["tag=red", "tag=green"]), Right("asc_desc_rank>3")]);
#[rustfmt::skip]
test_filter!(greater_complex_filter_2,          vec![Left(vec!["tag=red", "tag=green"]), Left(vec!["asc_desc_rank>3", "asc_desc_rank>1"])]);
#[rustfmt::skip]
test_filter!(lower_simple_number_filter,        vec![Right("asc_desc_rank<1")]);
#[rustfmt::skip]
test_filter!(lower_mix_and_filter,              vec![Right("tag=red"), Right("asc_desc_rank<1")]);
#[rustfmt::skip]
test_filter!(lower_mix_or_filter,               vec![Left(vec!["tag=red", "asc_desc_rank<1"])]);
#[rustfmt::skip]
test_filter!(lower_number_or_filter,            vec![Left(vec!["asc_desc_rank<3", "asc_desc_rank<1"])]);
#[rustfmt::skip]
test_filter!(lower_complex_filter,              vec![Left(vec!["tag=red", "tag=green"]), Right("asc_desc_rank<3")]);
#[rustfmt::skip]
test_filter!(lower_complex_filter_2,            vec![Left(vec!["tag=red", "tag=green"]), Left(vec!["asc_desc_rank<3", "asc_desc_rank<1"])]);
