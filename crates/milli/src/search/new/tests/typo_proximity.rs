/*!
This module tests the interactions between the typo and proximity ranking rules.

The typo ranking rule should transform the query graph such that it only contains
the combinations of word derivations that it used to compute its bucket.

The proximity ranking rule should then look for proximities only between those specific derivations.
For example, given the search query `beautiful summer` and the dataset:
```text
{ "id": 0, "text": "beautigul summer...... beautiful day in the summer" }
{ "id": 1, "text": "beautiful summer" }
```
Then the document with id `1` should be returned before `0`.
The proximity ranking rule is not allowed to look for the proximity between `beautigul` and `summer`
because the typo ranking rule before it only used the derivation `beautiful`.
*/

use crate::index::tests::TempIndex;
use crate::search::new::tests::collect_field_values;
use crate::{Criterion, Search, SearchResult, TermsMatchingStrategy};

fn create_index() -> TempIndex {
    let index = TempIndex::new();

    index
        .update_settings(|s| {
            s.set_primary_key("id".to_owned());
            s.set_searchable_fields(vec!["text".to_owned()]);
            s.set_criteria(vec![Criterion::Words, Criterion::Typo, Criterion::Proximity]);
        })
        .unwrap();

    index
        .add_documents(documents!([
            // trap explained in the module documentation
            {
                "id": 0,
                "text": "beautigul summer. beautiful x y z summer"
            },
            {
                "id": 1,
                "text": "beautiful summer"
            },
            // the next 2 documents set up a more complicated trap
            // with the query `beautiful summer`, we will have:
            // 1. documents with no typos, id 0 and 1
            // 2. documents with 1 typos: id 2 and 3, those are interpreted as EITHER
            //      - id 2: "beautigul + summer" ; OR
            //      - id 3: "beautiful + sommer"
            // To sort these two documents, the proximity ranking rule must use only the
            // word pairs: `beautigul -- summer` and `beautiful -- sommer` even though
            // all variations of `beautiful` and `sommer` were used by the typo ranking rule.
            {
                "id": 2,
                "text": "beautigul sommer. beautigul x summer"
            },
            {
                "id": 3,
                "text": "beautiful sommer"
            },
            // The next two documents lay out an even more complex trap.
            // With the user query `delicious sweet dessert`, the typo ranking rule will return one bucket of:
            // - id 4: delicitous + sweet + dessert
            // - id 5: beautiful + sweet + desgert
            // The word pairs that the proximity ranking rules is allowed to use are
            // EITHER:
            //      delicitous -- sweet AND sweet -- dessert
            // OR
            //      delicious -- sweet AND sweet -- desgert
            // So the word pair to use for the terms `summer` and `dessert` depend on the
            // word pairs explored before them.
            {
                "id": 4,
                "text": "delicitous. sweet. dessert. delicitous sweet desgert",
            },
            {
                "id": 5,
                "text": "delicious. sweet desgert. delicious sweet desgert",
            },
        ]))
        .unwrap();
    index
}

#[test]
fn test_trap_basic_and_complex1() {
    let index = create_index();
    let txn = index.read_txn().unwrap();

    let mut s = Search::new(&txn, &index);
    s.terms_matching_strategy(TermsMatchingStrategy::All);
    s.query("beautiful summer");
    s.scoring_strategy(crate::score_details::ScoringStrategy::Detailed);
    let SearchResult { documents_ids, document_scores, .. } = s.execute().unwrap();
    insta::assert_snapshot!(format!("{documents_ids:?}"), @"[1, 0, 3, 2]");
    insta::assert_snapshot!(format!("{document_scores:#?}"));
    let texts = collect_field_values(&index, &txn, "text", &documents_ids);
    insta::assert_debug_snapshot!(texts, @r###"
    [
        "\"beautiful summer\"",
        "\"beautigul summer. beautiful x y z summer\"",
        "\"beautiful sommer\"",
        "\"beautigul sommer. beautigul x summer\"",
    ]
    "###);
}

#[test]
fn test_trap_complex2() {
    let index = create_index();
    let txn = index.read_txn().unwrap();

    let mut s = Search::new(&txn, &index);
    s.terms_matching_strategy(TermsMatchingStrategy::All);
    s.query("delicious sweet dessert");
    s.scoring_strategy(crate::score_details::ScoringStrategy::Detailed);
    let SearchResult { documents_ids, document_scores, .. } = s.execute().unwrap();
    insta::assert_snapshot!(format!("{documents_ids:?}"), @"[5, 4]");
    insta::assert_snapshot!(format!("{document_scores:#?}"));
    let texts = collect_field_values(&index, &txn, "text", &documents_ids);
    insta::assert_debug_snapshot!(texts, @r###"
    [
        "\"delicious. sweet desgert. delicious sweet desgert\"",
        "\"delicitous. sweet. dessert. delicitous sweet desgert\"",
    ]
    "###);
}
