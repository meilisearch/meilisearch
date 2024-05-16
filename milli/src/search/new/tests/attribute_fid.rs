use crate::index::tests::TempIndex;
use crate::{db_snap, Criterion, Search, SearchResult, TermsMatchingStrategy};

fn create_index() -> TempIndex {
    let index = TempIndex::new();

    index
        .update_settings(|s| {
            s.set_primary_key("id".to_owned());
            s.set_searchable_fields(vec![
                "title".to_owned(),
                "description".to_owned(),
                "plot".to_owned(),
            ]);
            s.set_criteria(vec![Criterion::Attribute]);
        })
        .unwrap();

    index
        .add_documents(documents!([
            {
                "id": 0,
                "title": "",
                "description": "",
                "plot": "the quick brown fox jumps over the lazy dog",
            },
            {
                "id": 1,
                "title": "",
                "description": "the quick brown foxes jump over the lazy dog",
                "plot": "",
            },
            {
                "id": 2,
                "title": "the quick brown fox jumps over the lazy dog",
                "description": "",
                "plot": "",
            },
            {
                "id": 3,
                "title": "the",
                "description": "quick brown fox jumps over the lazy dog",
                "plot": "",
            },
            {
                "id": 4,
                "title": "the quick",
                "description": "brown fox jumps over the lazy dog",
                "plot": "",
            },
            {
                "id": 5,
                "title": "the quick brown",
                "description": "fox jumps over the lazy dog",
                "plot": "",
            },
            {
                "id": 6,
                "title": "the quick brown fox",
                "description": "jumps over the lazy dog",
                "plot": "",
            },
            {
                "id": 7,
                "title": "the quick",
                "description": "brown fox jumps",
                "plot": "over the lazy dog",
            },
            {
                "id": 8,
                "title": "the quick brown",
                "description": "fox",
                "plot": "jumps over the lazy dog",
            },
            {
                "id": 9,
                "title": "the quick brown",
                "description": "fox jumps",
                "plot": "over the lazy dog",
            },
            {
                "id": 10,
                "title": "",
                "description": "the quick brown fox",
                "plot": "jumps over the lazy dog",
            },
            {
                "id": 11,
                "title": "the quick",
                "description": "",
                "plot": "brown fox jumps over the lazy dog",
            },
            {
                "id": 12,
                "title": "",
                "description": "the quickbrownfox",
                "plot": "jumps over the lazy dog",
            },
            {
                "id": 13,
                "title": "",
                "description": "the quick brown fox",
                "plot": "jumps over the lazy dog",
            },
            {
                "id": 14,
                "title": "",
                "description": "the quickbrownfox",
                "plot": "jumps overthelazy dog",
            },
        ]))
        .unwrap();
    index
}

#[test]
fn test_attribute_fid_simple() {
    let index = create_index();

    let txn = index.read_txn().unwrap();

    let mut s = Search::new(&txn, &index);
    s.terms_matching_strategy(TermsMatchingStrategy::All);
    s.query("the quick brown fox jumps over the lazy dog");
    s.scoring_strategy(crate::score_details::ScoringStrategy::Detailed);
    let SearchResult { documents_ids, document_scores, .. } = s.execute().unwrap();
    let document_ids_scores: Vec<_> = documents_ids.iter().zip(document_scores).collect();
    insta::assert_snapshot!(format!("{document_ids_scores:#?}"));
}

#[test]
fn test_attribute_fid_ngrams() {
    let index = create_index();
    db_snap!(index, fields_ids_map, @r###"
    0   id               |
    1   title            |
    2   description      |
    3   plot             |
    "###);
    db_snap!(index, searchable_fields, @r###"["title", "description", "plot"]"###);
    db_snap!(index, fieldids_weights_map, @r###"
    fid weight
    1   0   |
    2   1   |
    3   2   |
    "###);

    let txn = index.read_txn().unwrap();

    let mut s = Search::new(&txn, &index);
    s.terms_matching_strategy(TermsMatchingStrategy::All);
    s.query("the quick brown fox jumps over the lazy dog");
    s.scoring_strategy(crate::score_details::ScoringStrategy::Detailed);

    let SearchResult { documents_ids, document_scores, .. } = s.execute().unwrap();

    let document_ids_scores: Vec<_> = documents_ids.iter().zip(document_scores).collect();
    insta::assert_snapshot!(format!("{document_ids_scores:#?}"));
}
