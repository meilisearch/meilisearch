use crate::index::tests::TempIndex;
use crate::{db_snap, Criterion, Search, SearchResult, TermsMatchingStrategy};

fn create_index() -> TempIndex {
    let index = TempIndex::new();

    index
        .update_settings(|s| {
            s.set_primary_key("id".to_owned());
            s.set_searchable_fields(vec![
                "text".to_owned(),
                "text2".to_owned(),
                "other".to_owned(),
            ]);
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
                quickbrown",
            },
            {
                "id": 8,
                "text": "a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a
                a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a
                a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a
                a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a
                quick brown",
            },
            {
                "id": 9,
                "text": "a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a
                a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a
                a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a
                a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a a
                quickbrown",
            },
            {
                "id": 10,
                "text": "quick brown",
                "text2": "brown quick",
            },
            {
                "id": 11,
                "text": "quickbrown",
            },
            {
                "id": 12,
                "text": "quick brown",
            },
            {
                "id": 13,
                "text": "quickbrown",
            },
        ]))
        .unwrap();
    index
}

#[test]
fn test_attribute_position_simple() {
    let index = create_index();

    db_snap!(index, word_position_docids, @"1ad58847d772924d8aab5e92be8cf0cc");

    let txn = index.read_txn().unwrap();

    let mut s = Search::new(&txn, &index);
    s.terms_matching_strategy(TermsMatchingStrategy::All);
    s.query("quick brown");
    s.scoring_strategy(crate::score_details::ScoringStrategy::Detailed);

    let SearchResult { documents_ids, document_scores, .. } = s.execute().unwrap();

    let document_ids_scores: Vec<_> = documents_ids.iter().zip(document_scores).collect();
    insta::assert_snapshot!(format!("{document_ids_scores:#?}"));
}
#[test]
fn test_attribute_position_repeated() {
    let index = create_index();

    let txn = index.read_txn().unwrap();

    let mut s = Search::new(&txn, &index);
    s.terms_matching_strategy(TermsMatchingStrategy::All);
    s.query("a a a a a");
    s.scoring_strategy(crate::score_details::ScoringStrategy::Detailed);

    let SearchResult { documents_ids, document_scores, .. } = s.execute().unwrap();

    let document_ids_scores: Vec<_> = documents_ids.iter().zip(document_scores).collect();
    insta::assert_snapshot!(format!("{document_ids_scores:#?}"));
}

#[test]
fn test_attribute_position_different_fields() {
    let index = create_index();

    let txn = index.read_txn().unwrap();

    let mut s = Search::new(&txn, &index);
    s.terms_matching_strategy(TermsMatchingStrategy::All);
    s.query("quick brown");
    s.scoring_strategy(crate::score_details::ScoringStrategy::Detailed);

    let SearchResult { documents_ids, document_scores, .. } = s.execute().unwrap();

    let document_ids_scores: Vec<_> = documents_ids.iter().zip(document_scores).collect();
    insta::assert_snapshot!(format!("{document_ids_scores:#?}"));
}

#[test]
fn test_attribute_position_ngrams() {
    let index = create_index();

    let txn = index.read_txn().unwrap();

    let mut s = Search::new(&txn, &index);
    s.terms_matching_strategy(TermsMatchingStrategy::All);
    s.query("quick brown");
    s.scoring_strategy(crate::score_details::ScoringStrategy::Detailed);

    let SearchResult { documents_ids, document_scores, .. } = s.execute().unwrap();

    let document_ids_scores: Vec<_> = documents_ids.iter().zip(document_scores).collect();
    insta::assert_snapshot!(format!("{document_ids_scores:#?}"));
}
