use crate::{index::tests::TempIndex, Criterion, Search, SearchResult, TermsMatchingStrategy};

fn create_index() -> TempIndex {
    let index = TempIndex::new();

    index
        .update_settings(|s| {
            s.set_primary_key("id".to_owned());
            s.set_searchable_fields(vec!["text".to_owned()]);
            s.set_criteria(vec![Criterion::Attribute]);
        })
        .unwrap();

    index
        .add_documents(documents!([
            {
                "id": 0,
                "text": "do you know about the quick and talented brown fox",
            },
            {
                "id": 1,
                "text": "do you know about the quick brown fox",
            },
            {
                "id": 2,
                "text": "the quick and talented brown fox",
            },
            {
                "id": 3,
                "text": "fox brown quick the",
            },
            {
                "id": 4,
                "text": "the quick brown fox",
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
    s.query("the quick brown fox");
    let SearchResult { documents_ids, .. } = s.execute().unwrap();
    insta::assert_snapshot!(format!("{documents_ids:?}"), @"[3, 4, 2, 1, 0]");
}
