use crate::{
    db_snap, index::tests::TempIndex, Criterion, Search, SearchResult, TermsMatchingStrategy,
};

fn create_index() -> TempIndex {
    let index = TempIndex::new();

    index
        .update_settings(|s| {
            s.set_primary_key("id".to_owned());
            s.set_searchable_fields(vec!["text".to_owned(), "other".to_owned()]);
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
            {
                "id": 5,
                "text": "a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a 
                a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a 
                a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a 
                a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a 
                a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a 
                a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a 
                a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a 
                a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a 
                a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a 
                a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a 
                a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a 
                a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a 
                a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a 
                a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a 
                a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a 
                a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a
                a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a 
                a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a 
                a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a 
                a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a 
                a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a 
                a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a 
                a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a 
                a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a 
                a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a 
                a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a 
                a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a 
                a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a 
                a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a 
                a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a 
                the quick brown fox",
            },
            {
                "id": 6,
                "text": "quick a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a 
                a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a 
                a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a 
                a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a 
                brown",
            },
            {
                "id": 7,
                "text": "a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a 
                a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a 
                a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a 
                a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a  
                quick brown",
            },
        ]))
        .unwrap();
    index
}

#[test]
fn test_attribute_position_simple() {
    let index = create_index();

    db_snap!(index, word_position_docids, @"fe86911166fa4c0903c512fd86ec65e4");

    let txn = index.read_txn().unwrap();

    let mut s = Search::new(&txn, &index);
    s.terms_matching_strategy(TermsMatchingStrategy::All);
    s.query("quick brown");
    let SearchResult { documents_ids, .. } = s.execute().unwrap();
    insta::assert_snapshot!(format!("{documents_ids:?}"), @"[3, 4, 2, 1, 0, 6, 7, 5]");
}
#[test]
fn test_attribute_position_repeated() {
    let index = create_index();

    let txn = index.read_txn().unwrap();

    let mut s = Search::new(&txn, &index);
    s.terms_matching_strategy(TermsMatchingStrategy::All);
    s.query("a a a a a");
    let SearchResult { documents_ids, .. } = s.execute().unwrap();
    insta::assert_snapshot!(format!("{documents_ids:?}"), @"[5, 7, 6]");
}
