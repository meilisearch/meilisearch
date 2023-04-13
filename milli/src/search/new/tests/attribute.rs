use std::collections::HashMap;

use crate::{
    index::tests::TempIndex, search::new::tests::collect_field_values, Criterion, Search,
    SearchResult, TermsMatchingStrategy,
};

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
                "title": "the quick brown fox jumps over the lazy dog",
                "description": "Pack my box with five dozen liquor jugs",
                "plot": "How vexingly quick daft zebras jump",
            },
            {
                "id": 1,
                "title": "Pack my box with five dozen liquor jugs",
                "description": "the quick brown foxes jump over the lazy dog",
                "plot": "How vexingly quick daft zebras jump",
            },
            {
                "id": 2,
                "title": "How vexingly quick daft zebras jump",
                "description": "Pack my box with five dozen liquor jugs",
                "plot": "the quick brown fox jumps over the lazy dog",
            }
        ]))
        .unwrap();
    index
}

#[test]
fn test_attributes_are_ranked_correctly() {
    let index = create_index();
    let txn = index.read_txn().unwrap();

    let mut s = Search::new(&txn, &index);
    s.terms_matching_strategy(TermsMatchingStrategy::All);
    s.query("the quick brown fox");
    let SearchResult { documents_ids, .. } = s.execute().unwrap();
    insta::assert_snapshot!(format!("{documents_ids:?}"), @"[0, 1, 2]");
}
