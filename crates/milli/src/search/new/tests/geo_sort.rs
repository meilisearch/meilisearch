/*!
This module tests the `geo_sort` ranking rule
*/

use big_s::S;
use heed::RoTxn;
use itertools::Itertools;
use maplit::hashset;

use crate::constants::RESERVED_GEO_FIELD_NAME;
use crate::index::tests::TempIndex;
use crate::score_details::ScoreDetails;
use crate::search::new::tests::collect_field_values;
use crate::{AscDesc, Criterion, GeoSortStrategy, Member, Search, SearchResult};

fn create_index() -> TempIndex {
    let index = TempIndex::new();

    index
        .update_settings(|s| {
            s.set_primary_key("id".to_owned());
            s.set_sortable_fields(hashset! { S(RESERVED_GEO_FIELD_NAME), S("score") });
            s.set_criteria(vec![Criterion::Words, Criterion::Sort]);
        })
        .unwrap();
    index
}

#[track_caller]
fn execute_iterative_and_rtree_returns_the_same<'a>(
    rtxn: &RoTxn<'a>,
    index: &TempIndex,
    search: &mut Search<'a>,
) -> (Vec<usize>, Vec<Vec<ScoreDetails>>) {
    search.geo_sort_strategy(GeoSortStrategy::AlwaysIterative(2));
    let SearchResult { documents_ids, document_scores: iterative_scores_bucketed, .. } =
        search.execute().unwrap();
    let iterative_ids_bucketed = collect_field_values(index, rtxn, "id", &documents_ids);

    search.geo_sort_strategy(GeoSortStrategy::AlwaysIterative(1000));
    let SearchResult { documents_ids, document_scores: iterative_scores, .. } =
        search.execute().unwrap();
    let iterative_ids = collect_field_values(index, rtxn, "id", &documents_ids);

    assert_eq!(iterative_ids_bucketed, iterative_ids, "iterative bucket");
    assert_eq!(iterative_scores_bucketed, iterative_scores, "iterative bucket score");

    search.geo_sort_strategy(GeoSortStrategy::AlwaysRtree(2));
    let SearchResult { documents_ids, document_scores: rtree_scores_bucketed, .. } =
        search.execute().unwrap();
    let rtree_ids_bucketed = collect_field_values(index, rtxn, "id", &documents_ids);

    search.geo_sort_strategy(GeoSortStrategy::AlwaysRtree(1000));
    let SearchResult { documents_ids, document_scores: rtree_scores, .. } =
        search.execute().unwrap();
    let rtree_ids = collect_field_values(index, rtxn, "id", &documents_ids);

    assert_eq!(rtree_ids_bucketed, rtree_ids, "rtree bucket");
    assert_eq!(rtree_scores_bucketed, rtree_scores, "rtree bucket score");

    assert_eq!(iterative_ids, rtree_ids, "iterative vs rtree");
    assert_eq!(iterative_scores, rtree_scores, "iterative vs rtree scores");

    (iterative_ids.into_iter().map(|id| id.parse().unwrap()).collect(), iterative_scores)
}

#[test]
fn test_geo_sort() {
    let index = create_index();

    index
        .add_documents(documents!([
            { "id": 2, RESERVED_GEO_FIELD_NAME: { "lat": 2, "lng": -1 } },
            { "id": 3, RESERVED_GEO_FIELD_NAME: { "lat": -2, "lng": -2 } },
            { "id": 5, RESERVED_GEO_FIELD_NAME: { "lat": 6, "lng": -5 } },
            { "id": 4, RESERVED_GEO_FIELD_NAME: { "lat": 3, "lng": 5 } },
            { "id": 0, RESERVED_GEO_FIELD_NAME: { "lat": 0, "lng": 0 } },
            { "id": 1, RESERVED_GEO_FIELD_NAME: { "lat": 1, "lng": 1 } },
            { "id": 6 }, { "id": 8 }, { "id": 7 }, { "id": 10 }, { "id": 9 },
        ]))
        .unwrap();

    let rtxn = index.read_txn().unwrap();

    let mut s = Search::new(&rtxn, &index);
    s.scoring_strategy(crate::score_details::ScoringStrategy::Detailed);

    s.sort_criteria(vec![AscDesc::Asc(Member::Geo([0., 0.]))]);
    let (ids, scores) = execute_iterative_and_rtree_returns_the_same(&rtxn, &index, &mut s);
    insta::assert_snapshot!(format!("{ids:?}"), @"[0, 1, 2, 3, 4, 5, 6, 8, 7, 10, 9]");
    insta::assert_snapshot!(format!("{scores:#?}"));

    s.sort_criteria(vec![AscDesc::Desc(Member::Geo([0., 0.]))]);
    let (ids, scores) = execute_iterative_and_rtree_returns_the_same(&rtxn, &index, &mut s);
    insta::assert_snapshot!(format!("{ids:?}"), @"[5, 4, 3, 2, 1, 0, 6, 8, 7, 10, 9]");
    insta::assert_snapshot!(format!("{scores:#?}"));
}

#[test]
fn test_geo_sort_with_following_ranking_rules() {
    let index = create_index();

    index
        .add_documents(documents!([
            { "id": 1 }, { "id": 4 }, { "id": 3 }, { "id": 2 }, { "id": 5 },
            { "id": 6, RESERVED_GEO_FIELD_NAME: { "lat": 2, "lng": 2 }, "score": 10 },
            { "id": 7, RESERVED_GEO_FIELD_NAME: { "lat": 2, "lng": 2 }, "score": 9 },
            { "id": 8, RESERVED_GEO_FIELD_NAME: { "lat": 2, "lng": 2 }, "score": 8 },
            { "id": 9, RESERVED_GEO_FIELD_NAME: { "lat": 2, "lng": 2 }, "score": 7 },
            { "id": 10, RESERVED_GEO_FIELD_NAME: { "lat": 2, "lng": 2 }, "score":6 },
            { "id": 11, RESERVED_GEO_FIELD_NAME: { "lat": 2, "lng": 2 }, "score": 5 },
            { "id": 12, RESERVED_GEO_FIELD_NAME: { "lat": 5, "lng": 5 }, "score": 10 },
            { "id": 13, RESERVED_GEO_FIELD_NAME: { "lat": 5, "lng": 5 }, "score": 9 },
            { "id": 14, RESERVED_GEO_FIELD_NAME: { "lat": 5, "lng": 5 }, "score": 8 },
            { "id": 15, RESERVED_GEO_FIELD_NAME: { "lat": 5, "lng": 5 }, "score": 7 },
        ]))
        .unwrap();

    let rtxn = index.read_txn().unwrap();

    let mut s = Search::new(&rtxn, &index);
    s.scoring_strategy(crate::score_details::ScoringStrategy::Detailed);
    s.sort_criteria(vec![
        AscDesc::Asc(Member::Geo([0., 0.])),
        AscDesc::Desc(Member::Field("score".to_string())),
    ]);
    let (ids, scores) = execute_iterative_and_rtree_returns_the_same(&rtxn, &index, &mut s);
    insta::assert_snapshot!(format!("{ids:?}"), @"[6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 1, 4, 3, 2, 5]");
    insta::assert_snapshot!(format!("{scores:#?}"));

    s.sort_criteria(vec![
        AscDesc::Desc(Member::Geo([0., 0.])),
        AscDesc::Desc(Member::Field("score".to_string())),
    ]);
    let (ids, scores) = execute_iterative_and_rtree_returns_the_same(&rtxn, &index, &mut s);
    insta::assert_snapshot!(format!("{ids:?}"), @"[12, 13, 14, 15, 6, 7, 8, 9, 10, 11, 1, 4, 3, 2, 5]");
    insta::assert_snapshot!(format!("{scores:#?}"));
}

#[test]
fn test_geo_sort_reached_max_bucket_size() {
    let index = create_index();

    index
        .add_documents(documents!([
            { "id": 1 }, { "id": 4 }, { "id": 3 }, { "id": 2 }, { "id": 5 },
            { "id": 6, RESERVED_GEO_FIELD_NAME: { "lat": 2, "lng": 2 }, "score": 10 },
            { "id": 7, RESERVED_GEO_FIELD_NAME: { "lat": 2, "lng": 2 }, "score": 9 },
            { "id": 8, RESERVED_GEO_FIELD_NAME: { "lat": 2, "lng": 2 }, "score": 8 },
            { "id": 9, RESERVED_GEO_FIELD_NAME: { "lat": 2, "lng": 2 }, "score": 7 },
            { "id": 10, RESERVED_GEO_FIELD_NAME: { "lat": 2, "lng": 2 }, "score":6 },
            { "id": 11, RESERVED_GEO_FIELD_NAME: { "lat": 2, "lng": 2 }, "score": 5 },
            { "id": 12, RESERVED_GEO_FIELD_NAME: { "lat": 5, "lng": 5 }, "score": 10 },
            { "id": 13, RESERVED_GEO_FIELD_NAME: { "lat": 5, "lng": 5 }, "score": 9 },
            { "id": 14, RESERVED_GEO_FIELD_NAME: { "lat": 5, "lng": 5 }, "score": 8 },
            { "id": 15, RESERVED_GEO_FIELD_NAME: { "lat": 5, "lng": 5 }, "score": 7 },
        ]))
        .unwrap();

    let rtxn = index.read_txn().unwrap();

    let mut s = Search::new(&rtxn, &index);
    s.geo_max_bucket_size(2);
    s.scoring_strategy(crate::score_details::ScoringStrategy::Detailed);
    s.sort_criteria(vec![
        AscDesc::Asc(Member::Geo([0., 0.])),
        AscDesc::Desc(Member::Field("score".to_string())),
    ]);

    /* We should not expect the results to obey the following ranking rules when the bucket size limit is reached,
     * nor should we expect Iteration and rtree to give exactly the same order for the same bucket in this case.*/
    s.geo_sort_strategy(GeoSortStrategy::AlwaysIterative(1000));
    let SearchResult { documents_ids, .. } = s.execute().unwrap();
    let iterative_ids = collect_field_values(&index, &rtxn, "id", &documents_ids);

    assert_eq!(iterative_ids.len(), 15);
    for id_str in &iterative_ids[0..6] {
        let id = id_str.parse::<u32>().unwrap();
        assert!((6..=11).contains(&id))
    }
    for id_str in &iterative_ids[6..10] {
        let id = id_str.parse::<u32>().unwrap();
        assert!((12..=15).contains(&id))
    }
    let no_geo_ids = iterative_ids[10..].iter().collect_vec();
    insta::assert_snapshot!(format!("{no_geo_ids:?}"), @r#"["1", "4", "3", "2", "5"]"#);

    s.geo_sort_strategy(GeoSortStrategy::AlwaysRtree(1000));
    let SearchResult { documents_ids, .. } = s.execute().unwrap();
    let rtree_ids = collect_field_values(&index, &rtxn, "id", &documents_ids);

    assert_eq!(rtree_ids.len(), 15);
    for id_str in &rtree_ids[0..6] {
        let id = id_str.parse::<u32>().unwrap();
        assert!((6..=11).contains(&id))
    }
    for id_str in &rtree_ids[6..10] {
        let id = id_str.parse::<u32>().unwrap();
        assert!((12..=15).contains(&id))
    }
    let no_geo_ids = rtree_ids[10..].iter().collect_vec();
    insta::assert_snapshot!(format!("{no_geo_ids:?}"), @r#"["1", "4", "3", "2", "5"]"#);
}

#[test]
fn test_geo_sort_around_the_edge_of_the_flat_earth() {
    let index = create_index();

    index
        .add_documents(documents!([
            { "id": 0, RESERVED_GEO_FIELD_NAME: { "lat": 0, "lng": 0 } },
            { "id": 1, RESERVED_GEO_FIELD_NAME: { "lat": 88, "lng": 0 } },
            { "id": 2, RESERVED_GEO_FIELD_NAME: { "lat": -89, "lng": 0 } },

            { "id": 3, RESERVED_GEO_FIELD_NAME: { "lat": 0, "lng": 178 } },
            { "id": 4, RESERVED_GEO_FIELD_NAME: { "lat": 0, "lng": -179 } },
        ]))
        .unwrap();

    let rtxn = index.read_txn().unwrap();

    let mut s = Search::new(&rtxn, &index);
    s.scoring_strategy(crate::score_details::ScoringStrategy::Detailed);

    // --- asc
    s.sort_criteria(vec![AscDesc::Asc(Member::Geo([0., 0.]))]);
    let (ids, scores) = execute_iterative_and_rtree_returns_the_same(&rtxn, &index, &mut s);
    insta::assert_snapshot!(format!("{ids:?}"), @"[0, 1, 2, 3, 4]");
    insta::assert_snapshot!(format!("{scores:#?}"));

    // ensuring the lat doesn't wrap around
    s.sort_criteria(vec![AscDesc::Asc(Member::Geo([85., 0.]))]);
    let (ids, scores) = execute_iterative_and_rtree_returns_the_same(&rtxn, &index, &mut s);
    insta::assert_snapshot!(format!("{ids:?}"), @"[1, 0, 3, 4, 2]");
    insta::assert_snapshot!(format!("{scores:#?}"));

    s.sort_criteria(vec![AscDesc::Asc(Member::Geo([-85., 0.]))]);
    let (ids, scores) = execute_iterative_and_rtree_returns_the_same(&rtxn, &index, &mut s);
    insta::assert_snapshot!(format!("{ids:?}"), @"[2, 0, 3, 4, 1]");
    insta::assert_snapshot!(format!("{scores:#?}"));

    // ensuring the lng does wrap around
    s.sort_criteria(vec![AscDesc::Asc(Member::Geo([0., 175.]))]);
    let (ids, scores) = execute_iterative_and_rtree_returns_the_same(&rtxn, &index, &mut s);
    insta::assert_snapshot!(format!("{ids:?}"), @"[3, 4, 2, 1, 0]");
    insta::assert_snapshot!(format!("{scores:#?}"));

    s.sort_criteria(vec![AscDesc::Asc(Member::Geo([0., -175.]))]);
    let (ids, scores) = execute_iterative_and_rtree_returns_the_same(&rtxn, &index, &mut s);
    insta::assert_snapshot!(format!("{ids:?}"), @"[4, 3, 2, 1, 0]");
    insta::assert_snapshot!(format!("{scores:#?}"));

    // --- desc
    s.sort_criteria(vec![AscDesc::Desc(Member::Geo([0., 0.]))]);
    let (ids, scores) = execute_iterative_and_rtree_returns_the_same(&rtxn, &index, &mut s);
    insta::assert_snapshot!(format!("{ids:?}"), @"[4, 3, 2, 1, 0]");
    insta::assert_snapshot!(format!("{scores:#?}"));

    // ensuring the lat doesn't wrap around
    s.sort_criteria(vec![AscDesc::Desc(Member::Geo([85., 0.]))]);
    let (ids, scores) = execute_iterative_and_rtree_returns_the_same(&rtxn, &index, &mut s);
    insta::assert_snapshot!(format!("{ids:?}"), @"[2, 4, 3, 0, 1]");
    insta::assert_snapshot!(format!("{scores:#?}"));

    s.sort_criteria(vec![AscDesc::Desc(Member::Geo([-85., 0.]))]);
    let (ids, scores) = execute_iterative_and_rtree_returns_the_same(&rtxn, &index, &mut s);
    insta::assert_snapshot!(format!("{ids:?}"), @"[1, 4, 3, 0, 2]");
    insta::assert_snapshot!(format!("{scores:#?}"));

    // ensuring the lng does wrap around
    s.sort_criteria(vec![AscDesc::Desc(Member::Geo([0., 175.]))]);
    let (ids, scores) = execute_iterative_and_rtree_returns_the_same(&rtxn, &index, &mut s);
    insta::assert_snapshot!(format!("{ids:?}"), @"[0, 1, 2, 4, 3]");
    insta::assert_snapshot!(format!("{scores:#?}"));

    s.sort_criteria(vec![AscDesc::Desc(Member::Geo([0., -175.]))]);
    let (ids, scores) = execute_iterative_and_rtree_returns_the_same(&rtxn, &index, &mut s);
    insta::assert_snapshot!(format!("{ids:?}"), @"[0, 1, 2, 3, 4]");
    insta::assert_snapshot!(format!("{scores:#?}"));
}

#[test]
fn geo_sort_mixed_with_words() {
    let index = create_index();

    index
        .add_documents(documents!([
            { "id": 0, "doggo": "jean", RESERVED_GEO_FIELD_NAME: { "lat": 0, "lng": 0 } },
            { "id": 1, "doggo": "intel", RESERVED_GEO_FIELD_NAME: { "lat": 88, "lng": 0 } },
            { "id": 2, "doggo": "jean bob", RESERVED_GEO_FIELD_NAME: { "lat": -89, "lng": 0 } },
            { "id": 3, "doggo": "jean michel", RESERVED_GEO_FIELD_NAME: { "lat": 0, "lng": 178 } },
            { "id": 4, "doggo": "bob marley", RESERVED_GEO_FIELD_NAME: { "lat": 0, "lng": -179 } },
        ]))
        .unwrap();

    let rtxn = index.read_txn().unwrap();

    let mut s = Search::new(&rtxn, &index);
    s.scoring_strategy(crate::score_details::ScoringStrategy::Detailed);
    s.sort_criteria(vec![AscDesc::Asc(Member::Geo([0., 0.]))]);

    s.query("jean");
    let (ids, scores) = execute_iterative_and_rtree_returns_the_same(&rtxn, &index, &mut s);
    insta::assert_snapshot!(format!("{ids:?}"), @"[0, 2, 3]");
    insta::assert_snapshot!(format!("{scores:#?}"));

    s.query("bob");
    let (ids, scores) = execute_iterative_and_rtree_returns_the_same(&rtxn, &index, &mut s);
    insta::assert_snapshot!(format!("{ids:?}"), @"[2, 4]");
    insta::assert_snapshot!(format!("{scores:#?}"), @r###"
    [
        [
            Words(
                Words {
                    matching_words: 1,
                    max_matching_words: 1,
                },
            ),
            GeoSort(
                GeoSort {
                    target_point: [
                        0.0,
                        0.0,
                    ],
                    ascending: true,
                    value: Some(
                        [
                            -89.0,
                            0.0,
                        ],
                    ),
                },
            ),
        ],
        [
            Words(
                Words {
                    matching_words: 1,
                    max_matching_words: 1,
                },
            ),
            GeoSort(
                GeoSort {
                    target_point: [
                        0.0,
                        0.0,
                    ],
                    ascending: true,
                    value: Some(
                        [
                            0.0,
                            -179.0,
                        ],
                    ),
                },
            ),
        ],
    ]
    "###);

    s.query("intel");
    let (ids, scores) = execute_iterative_and_rtree_returns_the_same(&rtxn, &index, &mut s);
    insta::assert_snapshot!(format!("{ids:?}"), @"[1]");
    insta::assert_snapshot!(format!("{scores:#?}"), @r###"
    [
        [
            Words(
                Words {
                    matching_words: 1,
                    max_matching_words: 1,
                },
            ),
            GeoSort(
                GeoSort {
                    target_point: [
                        0.0,
                        0.0,
                    ],
                    ascending: true,
                    value: Some(
                        [
                            88.0,
                            0.0,
                        ],
                    ),
                },
            ),
        ],
    ]
    "###);
}

#[test]
fn geo_sort_without_any_geo_faceted_documents() {
    let index = create_index();

    index
        .add_documents(documents!([
            { "id": 0, "doggo": "jean" },
            { "id": 1, "doggo": "intel" },
            { "id": 2, "doggo": "jean bob" },
            { "id": 3, "doggo": "jean michel" },
            { "id": 4, "doggo": "bob marley" },
        ]))
        .unwrap();

    let rtxn = index.read_txn().unwrap();

    let mut s = Search::new(&rtxn, &index);
    s.scoring_strategy(crate::score_details::ScoringStrategy::Detailed);
    s.sort_criteria(vec![AscDesc::Asc(Member::Geo([0., 0.]))]);

    s.query("jean");
    let (ids, scores) = execute_iterative_and_rtree_returns_the_same(&rtxn, &index, &mut s);
    insta::assert_snapshot!(format!("{ids:?}"), @"[0, 2, 3]");
    insta::assert_snapshot!(format!("{scores:#?}"));
}
