use meili_snap::{json_string, snapshot};

use crate::common::{shared_index_with_score_documents, Server};
use crate::json;

#[actix_rt::test]
async fn federated_search_with_metadata_header() {
    let server = Server::new_shared();
    let index = shared_index_with_score_documents().await;

    let (response, code) = server
        .multi_search_with_headers(
            json!({"federation": {}, "queries": [
            {"indexUid": index.uid, "q": "the bat"},
            {"indexUid": index.uid, "q": "badman returns"},
            {"indexUid" : index.uid, "q": "batman"},
            {"indexUid": index.uid, "q": "batman returns"},
            ]}),
            vec![("Meili-Include-Metadata", "true")],
        )
        .await;
    snapshot!(code, @"200 OK");
    snapshot!(json_string!(response, { ".processingTimeMs" => "[duration]", ".**._rankingScore" => "[score]", ".**.requestUid" => "[uuid]", ".metadata.**.queryUid" => "[queryUid]" }), @r###"
    {
      "hits": [
        {
          "title": "Batman",
          "id": "D",
          "_federation": {
            "indexUid": "SHARED_SCORE_DOCUMENTS",
            "queriesPosition": 2,
            "weightedRankingScore": 1.0
          }
        },
        {
          "title": "Batman Returns",
          "id": "C",
          "_federation": {
            "indexUid": "SHARED_SCORE_DOCUMENTS",
            "queriesPosition": 3,
            "weightedRankingScore": 1.0
          }
        },
        {
          "title": "Batman the dark knight returns: Part 1",
          "id": "A",
          "_federation": {
            "indexUid": "SHARED_SCORE_DOCUMENTS",
            "queriesPosition": 2,
            "weightedRankingScore": 0.9848484848484848
          }
        },
        {
          "title": "Batman the dark knight returns: Part 2",
          "id": "B",
          "_federation": {
            "indexUid": "SHARED_SCORE_DOCUMENTS",
            "queriesPosition": 2,
            "weightedRankingScore": 0.9848484848484848
          }
        },
        {
          "title": "Badman",
          "id": "E",
          "_federation": {
            "indexUid": "SHARED_SCORE_DOCUMENTS",
            "queriesPosition": 1,
            "weightedRankingScore": 0.5
          }
        }
      ],
      "processingTimeMs": "[duration]",
      "limit": 20,
      "offset": 0,
      "estimatedTotalHits": 5,
      "requestUid": "[uuid]",
      "metadata": [
        {
          "query": "the bat",
          "queryUid": "[queryUid]",
          "indexUid": "SHARED_SCORE_DOCUMENTS",
          "primaryKey": "id"
        },
        {
          "query": "badman returns",
          "queryUid": "[queryUid]",
          "indexUid": "SHARED_SCORE_DOCUMENTS",
          "primaryKey": "id"
        },
        {
          "query": "batman",
          "queryUid": "[queryUid]",
          "indexUid": "SHARED_SCORE_DOCUMENTS",
          "primaryKey": "id"
        },
        {
          "query": "batman returns",
          "queryUid": "[queryUid]",
          "indexUid": "SHARED_SCORE_DOCUMENTS",
          "primaryKey": "id"
        }
      ]
    }
    "###);
}
