use milli::{Search, SearchResult, Criterion};
use big_s::S;

use crate::search::{self, EXTERNAL_DOCUMENTS_IDS};

#[test]
fn none() {
    let criteria = vec![];
    let index = search::setup_search_index_with_criteria(&criteria);
    let mut rtxn = index.read_txn().unwrap();

    let mut search = Search::new(&mut rtxn, &index);
    search.query(search::TEST_QUERY);
    search.limit(EXTERNAL_DOCUMENTS_IDS.len());

    let SearchResult { matching_words: _matching_words, candidates: _candidates, documents_ids } = search.execute().unwrap();

    let expected_external_ids: Vec<_> = search::expected_order(&criteria, true, true).into_iter().map(|d| d.id).collect();
    let documents_ids = search::internal_to_external_ids(&index, &documents_ids);

    assert_eq!(documents_ids, expected_external_ids);
}

#[test]
fn words() {
    let criteria = vec![Criterion::Words];
    let index = search::setup_search_index_with_criteria(&criteria);
    let mut rtxn = index.read_txn().unwrap();

    let mut search = Search::new(&mut rtxn, &index);
    search.query(search::TEST_QUERY);
    search.limit(EXTERNAL_DOCUMENTS_IDS.len());

    let SearchResult { matching_words: _matching_words, candidates: _candidates, documents_ids } = search.execute().unwrap();

    let expected_external_ids: Vec<_> = search::expected_order(&criteria, true, true).into_iter().map(|d| d.id).collect();
    let documents_ids = search::internal_to_external_ids(&index, &documents_ids);

    assert_eq!(documents_ids, expected_external_ids);
}

#[test]
fn attribute() {
    let criteria = vec![Criterion::Attribute];
    let index = search::setup_search_index_with_criteria(&criteria);
    let mut rtxn = index.read_txn().unwrap();

    let mut search = Search::new(&mut rtxn, &index);
    search.query(search::TEST_QUERY);
    search.limit(EXTERNAL_DOCUMENTS_IDS.len());
    search.optional_words(false);

    let SearchResult { matching_words: _matching_words, candidates: _candidates, documents_ids } = search.execute().unwrap();

    let expected_external_ids: Vec<_> = search::expected_order(&criteria, true, false).into_iter().map(|d| d.id).collect();
    let documents_ids = search::internal_to_external_ids(&index, &documents_ids);

    assert_eq!(documents_ids, expected_external_ids);
}

#[test]
fn exactness() {
    let criteria = vec![Criterion::Exactness];
    let index = search::setup_search_index_with_criteria(&criteria);
    let mut rtxn = index.read_txn().unwrap();

    let mut search = Search::new(&mut rtxn, &index);
    search.query(search::TEST_QUERY);
    search.limit(EXTERNAL_DOCUMENTS_IDS.len());
    search.optional_words(false);

    let SearchResult { matching_words: _matching_words, candidates: _candidates, documents_ids } = search.execute().unwrap();

    let expected_external_ids: Vec<_> = search::expected_order(&criteria, true, false).into_iter().map(|d| d.id).collect();
    let documents_ids = search::internal_to_external_ids(&index, &documents_ids);

    assert_eq!(documents_ids, expected_external_ids);
}

#[test]
fn proximity() {
    let criteria = vec![Criterion::Proximity];
    let index = search::setup_search_index_with_criteria(&criteria);
    let mut rtxn = index.read_txn().unwrap();

    let mut search = Search::new(&mut rtxn, &index);
    search.query(search::TEST_QUERY);
    search.limit(EXTERNAL_DOCUMENTS_IDS.len());
    search.optional_words(false);

    let SearchResult { matching_words: _matching_words, candidates: _candidates, documents_ids } = search.execute().unwrap();

    let expected_external_ids: Vec<_> = search::expected_order(&criteria, true, false).into_iter().map(|d| d.id).collect();
    let documents_ids = search::internal_to_external_ids(&index, &documents_ids);

    assert_eq!(documents_ids, expected_external_ids);
}

#[test]
fn typo() {
    let criteria = vec![Criterion::Typo];
    let index = search::setup_search_index_with_criteria(&criteria);
    let mut rtxn = index.read_txn().unwrap();

    let mut search = Search::new(&mut rtxn, &index);
    search.query(search::TEST_QUERY);
    search.limit(EXTERNAL_DOCUMENTS_IDS.len());
    search.optional_words(false);

    let SearchResult { matching_words: _matching_words, candidates: _candidates, documents_ids } = search.execute().unwrap();

    let expected_external_ids: Vec<_> = search::expected_order(&criteria, true, false).into_iter().map(|d| d.id).collect();
    let documents_ids = search::internal_to_external_ids(&index, &documents_ids);

    assert_eq!(documents_ids, expected_external_ids);
}

#[test]
fn asc() {
    let criteria = vec![Criterion::Asc(S("asc_desc_rank"))];
    let index = search::setup_search_index_with_criteria(&criteria);
    let mut rtxn = index.read_txn().unwrap();

    let mut search = Search::new(&mut rtxn, &index);
    search.query(search::TEST_QUERY);
    search.limit(EXTERNAL_DOCUMENTS_IDS.len());
    search.optional_words(false);

    let SearchResult { matching_words: _matching_words, candidates: _candidates, documents_ids } = search.execute().unwrap();

    let expected_external_ids: Vec<_> = search::expected_order(&criteria, true, false).into_iter().map(|d| d.id).collect();
    let documents_ids = search::internal_to_external_ids(&index, &documents_ids);

    assert_eq!(documents_ids, expected_external_ids);
}

#[test]
fn desc() {
    let criteria = vec![Criterion::Desc(S("asc_desc_rank"))];
    let index = search::setup_search_index_with_criteria(&criteria);
    let mut rtxn = index.read_txn().unwrap();

    let mut search = Search::new(&mut rtxn, &index);
    search.query(search::TEST_QUERY);
    search.limit(EXTERNAL_DOCUMENTS_IDS.len());
    search.optional_words(false);

    let SearchResult { matching_words: _matching_words, candidates: _candidates, documents_ids } = search.execute().unwrap();

    let expected_external_ids: Vec<_> = search::expected_order(&criteria, true, false).into_iter().map(|d| d.id).collect();
    let documents_ids = search::internal_to_external_ids(&index, &documents_ids);

    assert_eq!(documents_ids, expected_external_ids);
}

#[test]
fn none_0_typo() {
    let criteria = vec![];
    let index = search::setup_search_index_with_criteria(&criteria);
    let mut rtxn = index.read_txn().unwrap();

    let mut search = Search::new(&mut rtxn, &index);
    search.query(search::TEST_QUERY);
    search.limit(EXTERNAL_DOCUMENTS_IDS.len());
    search.authorize_typos(false);
    search.optional_words(false);

    let SearchResult { matching_words: _matching_words, candidates: _candidates, documents_ids } = search.execute().unwrap();

    let expected_external_ids: Vec<_> = search::expected_order(&criteria, false, false).into_iter().map(|d| d.id).collect();
    let documents_ids = search::internal_to_external_ids(&index, &documents_ids);

    assert_eq!(documents_ids, expected_external_ids);
}

#[test]
fn attribute_0_typo() {
    let criteria = vec![Criterion::Attribute];
    let index = search::setup_search_index_with_criteria(&criteria);
    let mut rtxn = index.read_txn().unwrap();

    let mut search = Search::new(&mut rtxn, &index);
    search.query(search::TEST_QUERY);
    search.limit(EXTERNAL_DOCUMENTS_IDS.len());
    search.optional_words(false);
    search.authorize_typos(false);

    let SearchResult { matching_words: _matching_words, candidates: _candidates, documents_ids } = search.execute().unwrap();

    let expected_external_ids: Vec<_> = search::expected_order(&criteria, false, false).into_iter().map(|d| d.id).collect();
    let documents_ids = search::internal_to_external_ids(&index, &documents_ids);

    assert_eq!(documents_ids, expected_external_ids);
}

#[test]
fn exactness_0_typo() {
    let criteria = vec![Criterion::Exactness];
    let index = search::setup_search_index_with_criteria(&criteria);
    let mut rtxn = index.read_txn().unwrap();

    let mut search = Search::new(&mut rtxn, &index);
    search.query(search::TEST_QUERY);
    search.limit(EXTERNAL_DOCUMENTS_IDS.len());
    search.optional_words(false);
    search.authorize_typos(false);

    let SearchResult { matching_words: _matching_words, candidates: _candidates, documents_ids } = search.execute().unwrap();

    let expected_external_ids: Vec<_> = search::expected_order(&criteria, false, false).into_iter().map(|d| d.id).collect();
    let documents_ids = search::internal_to_external_ids(&index, &documents_ids);

    assert_eq!(documents_ids, expected_external_ids);
}

#[test]
fn proximity_0_typo() {
    let criteria = vec![Criterion::Proximity];
    let index = search::setup_search_index_with_criteria(&criteria);
    let mut rtxn = index.read_txn().unwrap();

    let mut search = Search::new(&mut rtxn, &index);
    search.query(search::TEST_QUERY);
    search.limit(EXTERNAL_DOCUMENTS_IDS.len());
    search.optional_words(false);
    search.authorize_typos(false);

    let SearchResult { matching_words: _matching_words, candidates: _candidates, documents_ids } = search.execute().unwrap();

    let expected_external_ids: Vec<_> = search::expected_order(&criteria, false, false).into_iter().map(|d| d.id).collect();
    let documents_ids = search::internal_to_external_ids(&index, &documents_ids);

    assert_eq!(documents_ids, expected_external_ids);
}

#[test]
fn typo_0_typo() {
    let criteria = vec![Criterion::Typo];
    let index = search::setup_search_index_with_criteria(&criteria);
    let mut rtxn = index.read_txn().unwrap();

    let mut search = Search::new(&mut rtxn, &index);
    search.query(search::TEST_QUERY);
    search.limit(EXTERNAL_DOCUMENTS_IDS.len());
    search.optional_words(false);
    search.authorize_typos(false);

    let SearchResult { matching_words: _matching_words, candidates: _candidates, documents_ids } = search.execute().unwrap();

    let expected_external_ids: Vec<_> = search::expected_order(&criteria, false, false).into_iter().map(|d| d.id).collect();
    let documents_ids = search::internal_to_external_ids(&index, &documents_ids);

    assert_eq!(documents_ids, expected_external_ids);
}

#[test]
fn asc_0_typo() {
    let criteria = vec![Criterion::Asc(S("asc_desc_rank"))];
    let index = search::setup_search_index_with_criteria(&criteria);
    let mut rtxn = index.read_txn().unwrap();

    let mut search = Search::new(&mut rtxn, &index);
    search.query(search::TEST_QUERY);
    search.limit(EXTERNAL_DOCUMENTS_IDS.len());
    search.optional_words(false);
    search.authorize_typos(false);

    let SearchResult { matching_words: _matching_words, candidates: _candidates, documents_ids } = search.execute().unwrap();

    let expected_external_ids: Vec<_> = search::expected_order(&criteria, false, false).into_iter().map(|d| d.id).collect();
    let documents_ids = search::internal_to_external_ids(&index, &documents_ids);

    assert_eq!(documents_ids, expected_external_ids);
}

#[test]
fn desc_0_typo() {
    let criteria = vec![Criterion::Desc(S("asc_desc_rank"))];
    let index = search::setup_search_index_with_criteria(&criteria);
    let mut rtxn = index.read_txn().unwrap();

    let mut search = Search::new(&mut rtxn, &index);
    search.query(search::TEST_QUERY);
    search.limit(EXTERNAL_DOCUMENTS_IDS.len());
    search.optional_words(false);
    search.authorize_typos(false);

    let SearchResult { matching_words: _matching_words, candidates: _candidates, documents_ids } = search.execute().unwrap();

    let expected_external_ids: Vec<_> = search::expected_order(&criteria, false, false).into_iter().map(|d| d.id).collect();
    let documents_ids = search::internal_to_external_ids(&index, &documents_ids);

    assert_eq!(documents_ids, expected_external_ids);
}

#[test]
fn test_desc_on_unexisting_field_should_return_all_1() {
    let criteria = vec![Criterion::Desc(S("unexisting_field"))];
    let index = search::setup_search_index_with_criteria(&criteria);
    let mut rtxn = index.read_txn().unwrap();

    let mut search = Search::new(&mut rtxn, &index);
    search.query(search::TEST_QUERY);
    search.limit(EXTERNAL_DOCUMENTS_IDS.len());
    search.optional_words(false);
    search.authorize_typos(false);

    let SearchResult { matching_words: _matching_words, candidates: _candidates, documents_ids } = search.execute().unwrap();

    let criteria = vec![];
    let expected_external_ids: Vec<_> = search::expected_order(&criteria, false, false).into_iter().map(|d| d.id).collect();
    let documents_ids = search::internal_to_external_ids(&index, &documents_ids);

    assert_eq!(documents_ids, expected_external_ids);
}

#[test]
fn test_asc_on_unexisting_field_should_return_all_1() {
    let criteria = vec![Criterion::Asc(S("unexisting_field"))];
    let index = search::setup_search_index_with_criteria(&criteria);
    let mut rtxn = index.read_txn().unwrap();

    let mut search = Search::new(&mut rtxn, &index);
    search.query(search::TEST_QUERY);
    search.limit(EXTERNAL_DOCUMENTS_IDS.len());
    search.optional_words(false);
    search.authorize_typos(false);

    let SearchResult { matching_words: _matching_words, candidates: _candidates, documents_ids } = search.execute().unwrap();

    let criteria = vec![];
    let expected_external_ids: Vec<_> = search::expected_order(&criteria, false, false).into_iter().map(|d| d.id).collect();
    let documents_ids = search::internal_to_external_ids(&index, &documents_ids);

    assert_eq!(documents_ids, expected_external_ids);
}

#[test]
fn test_desc_on_unexisting_field_should_return_all_2() {
    let criteria = vec![Criterion::Desc(S("unexisting_field"))];
    let index = search::setup_search_index_with_criteria(&criteria);
    let mut rtxn = index.read_txn().unwrap();

    let mut search = Search::new(&mut rtxn, &index);
    search.query(search::TEST_QUERY);
    search.limit(EXTERNAL_DOCUMENTS_IDS.len());

    let SearchResult { matching_words: _matching_words, candidates: _candidates, documents_ids } = search.execute().unwrap();

    let criteria = vec![];
    let expected_external_ids: Vec<_> = search::expected_order(&criteria, true, true).into_iter().map(|d| d.id).collect();
    let documents_ids = search::internal_to_external_ids(&index, &documents_ids);

    assert_eq!(documents_ids, expected_external_ids);
}

#[test]
fn test_asc_on_unexisting_field_should_return_all_2() {
    let criteria = vec![Criterion::Asc(S("unexisting_field"))];
    let index = search::setup_search_index_with_criteria(&criteria);
    let mut rtxn = index.read_txn().unwrap();

    let mut search = Search::new(&mut rtxn, &index);
    search.query(search::TEST_QUERY);
    search.limit(EXTERNAL_DOCUMENTS_IDS.len());

    let SearchResult { matching_words: _matching_words, candidates: _candidates, documents_ids } = search.execute().unwrap();

    let criteria = vec![];
    let expected_external_ids: Vec<_> = search::expected_order(&criteria, true, true).into_iter().map(|d| d.id).collect();
    let documents_ids = search::internal_to_external_ids(&index, &documents_ids);

    assert_eq!(documents_ids, expected_external_ids);
}

#[test]
fn criteria_mixup() {
    use Criterion::*;
    let index = search::setup_search_index_with_criteria(&vec![Words, Attribute, Desc(S("asc_desc_rank")), Exactness, Proximity, Typo]);


    let criteria_mix = {
        let desc = || Desc(S("asc_desc_rank"));
        // all possible criteria order
        vec![
            vec![Words, Attribute,  desc(),     Exactness,  Proximity,  Typo],
            vec![Words, Attribute,  desc(),     Exactness,  Typo,       Proximity],
            vec![Words, Attribute,  desc(),     Proximity,  Exactness,  Typo],
            vec![Words, Attribute,  desc(),     Proximity,  Typo,       Exactness],
            vec![Words, Attribute,  desc(),     Typo,       Exactness,  Proximity],
            vec![Words, Attribute,  desc(),     Typo,       Proximity,  Exactness],
            vec![Words, Attribute,  Exactness,  desc(),     Proximity,  Typo],
            vec![Words, Attribute,  Exactness,  desc(),     Typo,       Proximity],
            vec![Words, Attribute,  Exactness,  Proximity,  desc(),     Typo],
            vec![Words, Attribute,  Exactness,  Proximity,  Typo,       desc()],
            vec![Words, Attribute,  Exactness,  Typo,       desc(),     Proximity],
            vec![Words, Attribute,  Exactness,  Typo,       Proximity,  desc()],
            vec![Words, Attribute,  Proximity,  desc(),     Exactness,  Typo],
            vec![Words, Attribute,  Proximity,  desc(),     Typo,       Exactness],
            vec![Words, Attribute,  Proximity,  Exactness,  desc(),     Typo],
            vec![Words, Attribute,  Proximity,  Exactness,  Typo,       desc()],
            vec![Words, Attribute,  Proximity,  Typo,       desc(),     Exactness],
            vec![Words, Attribute,  Proximity,  Typo,       Exactness,  desc()],
            vec![Words, Attribute,  Typo,       desc(),     Exactness,  Proximity],
            vec![Words, Attribute,  Typo,       desc(),     Proximity,  Exactness],
            vec![Words, Attribute,  Typo,       Exactness,  desc(),     Proximity],
            vec![Words, Attribute,  Typo,       Exactness,  Proximity,  desc()],
            vec![Words, Attribute,  Typo,       Proximity,  desc(),     Exactness],
            vec![Words, Attribute,  Typo,       Proximity,  Exactness,  desc()],
            vec![Words, desc(),     Attribute,  Exactness,  Proximity,  Typo],
            vec![Words, desc(),     Attribute,  Exactness,  Typo,       Proximity],
            vec![Words, desc(),     Attribute,  Proximity,  Exactness,  Typo],
            vec![Words, desc(),     Attribute,  Proximity,  Typo,       Exactness],
            vec![Words, desc(),     Attribute,  Typo,       Exactness,  Proximity],
            vec![Words, desc(),     Attribute,  Typo,       Proximity,  Exactness],
            vec![Words, desc(),     Exactness,  Attribute,  Proximity,  Typo],
            vec![Words, desc(),     Exactness,  Attribute,  Typo,       Proximity],
            vec![Words, desc(),     Exactness,  Proximity,  Attribute,  Typo],
            vec![Words, desc(),     Exactness,  Proximity,  Typo,       Attribute],
            vec![Words, desc(),     Exactness,  Typo,       Attribute,  Proximity],
            vec![Words, desc(),     Exactness,  Typo,       Proximity,  Attribute],
            vec![Words, desc(),     Proximity,  Attribute,  Exactness,  Typo],
            vec![Words, desc(),     Proximity,  Attribute,  Typo,       Exactness],
            vec![Words, desc(),     Proximity,  Exactness,  Attribute,  Typo],
            vec![Words, desc(),     Proximity,  Exactness,  Typo,       Attribute],
            vec![Words, desc(),     Proximity,  Typo,       Attribute,  Exactness],
            vec![Words, desc(),     Proximity,  Typo,       Exactness,  Attribute],
            vec![Words, desc(),     Typo,       Attribute,  Exactness,  Proximity],
            vec![Words, desc(),     Typo,       Attribute,  Proximity,  Exactness],
            vec![Words, desc(),     Typo,       Exactness,  Attribute,  Proximity],
            vec![Words, desc(),     Typo,       Exactness,  Proximity,  Attribute],
            vec![Words, desc(),     Typo,       Proximity,  Attribute,  Exactness],
            vec![Words, desc(),     Typo,       Proximity,  Exactness,  Attribute],
            vec![Words, Exactness,  Attribute,  desc(),     Proximity,  Typo],
            vec![Words, Exactness,  Attribute,  desc(),     Typo,       Proximity],
            vec![Words, Exactness,  Attribute,  Proximity,  desc(),     Typo],
            vec![Words, Exactness,  Attribute,  Proximity,  Typo,       desc()],
            vec![Words, Exactness,  Attribute,  Typo,       desc(),     Proximity],
            vec![Words, Exactness,  Attribute,  Typo,       Proximity,  desc()],
            vec![Words, Exactness,  desc(),     Attribute,  Proximity,  Typo],
            vec![Words, Exactness,  desc(),     Attribute,  Typo,       Proximity],
            vec![Words, Exactness,  desc(),     Proximity,  Attribute,  Typo],
            vec![Words, Exactness,  desc(),     Proximity,  Typo,       Attribute],
            vec![Words, Exactness,  desc(),     Typo,       Attribute,  Proximity],
            vec![Words, Exactness,  desc(),     Typo,       Proximity,  Attribute],
            vec![Words, Exactness,  Proximity,  Attribute,  desc(),     Typo],
            vec![Words, Exactness,  Proximity,  Attribute,  Typo,       desc()],
            vec![Words, Exactness,  Proximity,  desc(),     Attribute,  Typo],
            vec![Words, Exactness,  Proximity,  desc(),     Typo,       Attribute],
            vec![Words, Exactness,  Proximity,  Typo,       Attribute,  desc()],
            vec![Words, Exactness,  Proximity,  Typo,       desc(),     Attribute],
            vec![Words, Exactness,  Typo,       Attribute,  desc(),     Proximity],
            vec![Words, Exactness,  Typo,       Attribute,  Proximity,  desc()],
            vec![Words, Exactness,  Typo,       desc(),     Attribute,  Proximity],
            vec![Words, Exactness,  Typo,       desc(),     Proximity,  Attribute],
            vec![Words, Exactness,  Typo,       Proximity,  Attribute,  desc()],
            vec![Words, Exactness,  Typo,       Proximity,  desc(),     Attribute],
            vec![Words, Proximity,  Attribute,  desc(),     Exactness,  Typo],
            vec![Words, Proximity,  Attribute,  desc(),     Typo,       Exactness],
            vec![Words, Proximity,  Attribute,  Exactness,  desc(),     Typo],
            vec![Words, Proximity,  Attribute,  Exactness,  Typo,       desc()],
            vec![Words, Proximity,  Attribute,  Typo,       desc(),     Exactness],
            vec![Words, Proximity,  Attribute,  Typo,       Exactness,  desc()],
            vec![Words, Proximity,  desc(),     Attribute,  Exactness,  Typo],
            vec![Words, Proximity,  desc(),     Attribute,  Typo,       Exactness],
            vec![Words, Proximity,  desc(),     Exactness,  Attribute,  Typo],
            vec![Words, Proximity,  desc(),     Exactness,  Typo,       Attribute],
            vec![Words, Proximity,  desc(),     Typo,       Attribute,  Exactness],
            vec![Words, Proximity,  desc(),     Typo,       Exactness,  Attribute],
            vec![Words, Proximity,  Exactness,  Attribute,  desc(),     Typo],
            vec![Words, Proximity,  Exactness,  Attribute,  Typo,       desc()],
            vec![Words, Proximity,  Exactness,  desc(),     Attribute,  Typo],
            vec![Words, Proximity,  Exactness,  desc(),     Typo,       Attribute],
            vec![Words, Proximity,  Exactness,  Typo,       Attribute,  desc()],
            vec![Words, Proximity,  Exactness,  Typo,       desc(),     Attribute],
            vec![Words, Proximity,  Typo,       Attribute,  desc(),     Exactness],
            vec![Words, Proximity,  Typo,       Attribute,  Exactness,  desc()],
            vec![Words, Proximity,  Typo,       desc(),     Attribute,  Exactness],
            vec![Words, Proximity,  Typo,       desc(),     Exactness,  Attribute],
            vec![Words, Proximity,  Typo,       Exactness,  Attribute,  desc()],
            vec![Words, Proximity,  Typo,       Exactness,  desc(),     Attribute],
            vec![Words, Typo,       Attribute,  desc(),     Exactness,  Proximity],
            vec![Words, Typo,       Attribute,  desc(),     Proximity,  Exactness],
            vec![Words, Typo,       Attribute,  Exactness,  desc(),     Proximity],
            vec![Words, Typo,       Attribute,  Exactness,  Proximity,  desc()],
            vec![Words, Typo,       Attribute,  Proximity,  desc(),     Exactness],
            vec![Words, Typo,       Attribute,  Proximity,  Exactness,  desc()],
            vec![Words, Typo,       desc(),     Attribute,  Proximity,  Exactness],
            vec![Words, Typo,       desc(),     Exactness,  Attribute,  Proximity],
            vec![Words, Typo,       desc(),     Exactness,  Attribute,  Proximity],
            vec![Words, Typo,       desc(),     Exactness,  Proximity,  Attribute],
            vec![Words, Typo,       desc(),     Proximity,  Attribute,  Exactness],
            vec![Words, Typo,       desc(),     Proximity,  Exactness,  Attribute],
            vec![Words, Typo,       Exactness,  Attribute,  desc(),     Proximity],
            vec![Words, Typo,       Exactness,  Attribute,  Proximity,  desc()],
            vec![Words, Typo,       Exactness,  desc(),     Attribute,  Proximity],
            vec![Words, Typo,       Exactness,  desc(),     Proximity,  Attribute],
            vec![Words, Typo,       Exactness,  Proximity,  Attribute,  desc()],
            vec![Words, Typo,       Exactness,  Proximity,  desc(),     Attribute],
            vec![Words, Typo,       Proximity,  Attribute,  desc(),     Exactness],
            vec![Words, Typo,       Proximity,  Attribute,  Exactness,  desc()],
            vec![Words, Typo,       Proximity,  desc(),     Attribute,  Exactness],
            vec![Words, Typo,       Proximity,  desc(),     Exactness,  Attribute],
            vec![Words, Typo,       Proximity,  Exactness,  Attribute,  desc()],
            vec![Words, Typo,       Proximity,  Exactness,  desc(),     Attribute],
        ]
    };

    for criteria in criteria_mix {
        eprintln!("Testing with criteria order: {:?}", &criteria);
        //update criteria
        let mut wtxn = index.write_txn().unwrap();
        index.put_criteria(&mut wtxn, &criteria).unwrap();
        wtxn.commit().unwrap();

        let mut rtxn = index.read_txn().unwrap();

        let mut search = Search::new(&mut rtxn, &index);
        search.query(search::TEST_QUERY);
        search.limit(EXTERNAL_DOCUMENTS_IDS.len());
        search.optional_words(true);
        search.authorize_typos(true);

        let SearchResult { matching_words: _matching_words, candidates: _candidates, documents_ids } = search.execute().unwrap();

        let expected_external_ids: Vec<_> = search::expected_order(&criteria, true, true).into_iter().map(|d| d.id).collect();
        let documents_ids = search::internal_to_external_ids(&index, &documents_ids);

        assert_eq!(documents_ids, expected_external_ids);
    }
}
