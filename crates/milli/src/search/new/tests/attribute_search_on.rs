use crate::index::tests::TempIndex;

fn create_empty_index() -> TempIndex {
    let index = TempIndex::new();

    index
        .update_settings(|s| {
            s.set_primary_key("id".to_string());
            s.set_searchable_fields(vec!["name".to_string(), "title".to_string()]);
        })
        .unwrap();

    index
}

#[test]
fn test_attributes_to_search_on_empty_index() {
    let index = create_empty_index();
    let txn = index.read_txn().unwrap();

    let mut search = index.search(&txn);
    let attrs = ["title".to_string()];
    search.searchable_attributes(&attrs);
    search.query("doc");

    let result = search.execute().unwrap();
    assert!(result.documents_ids.is_empty());
}
