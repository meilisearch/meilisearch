use meili_snap::*;

use crate::common::{
    shared_empty_index, shared_index_with_documents, shared_index_with_geo_documents,
    shared_index_with_nested_documents, Server,
};
use crate::json;

#[actix_rt::test]
async fn swap_indexes_bad_format() {
    let server = Server::new_shared();

    let (response, code) = server.index_swap(json!("doggo")).await;
    snapshot!(code, @"400 Bad Request");
    snapshot!(json_string!(response), @r###"
    {
      "message": "Invalid value type: expected an array, but found a string: `\"doggo\"`",
      "code": "bad_request",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#bad_request"
    }
    "###);

    let (response, code) = server.index_swap(json!(["doggo"])).await;
    snapshot!(code, @"400 Bad Request");
    snapshot!(json_string!(response), @r###"
    {
      "message": "Invalid value type at `[0]`: expected an object, but found a string: `\"doggo\"`",
      "code": "bad_request",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#bad_request"
    }
    "###);
}

#[actix_rt::test]
async fn swap_indexes_bad_indexes() {
    let server = Server::new_shared();

    let (response, code) = server.index_swap(json!([{ "indexes": "doggo"}])).await;
    snapshot!(code, @"400 Bad Request");
    snapshot!(json_string!(response), @r###"
    {
      "message": "Invalid value type at `[0].indexes`: expected an array, but found a string: `\"doggo\"`",
      "code": "invalid_swap_indexes",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#invalid_swap_indexes"
    }
    "###);

    let (response, code) = server.index_swap(json!([{ "indexes": ["doggo"]}])).await;
    snapshot!(code, @"400 Bad Request");
    snapshot!(json_string!(response), @r###"
    {
      "message": "Two indexes must be given for each swap. The list `[\"doggo\"]` contains 1 indexes.",
      "code": "invalid_swap_indexes",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#invalid_swap_indexes"
    }
    "###);

    let (response, code) =
        server.index_swap(json!([{ "indexes": ["doggo", "crabo", "croco"]}])).await;
    snapshot!(code, @"400 Bad Request");
    snapshot!(json_string!(response), @r###"
    {
      "message": "Two indexes must be given for each swap. The list `[\"doggo\", \"crabo\", \"croco\"]` contains 3 indexes.",
      "code": "invalid_swap_indexes",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#invalid_swap_indexes"
    }
    "###);

    let (response, code) = server.index_swap(json!([{ "indexes": ["doggo", "doggo"]}])).await;
    snapshot!(code, @"400 Bad Request");
    snapshot!(json_string!(response), @r###"
    {
      "message": "Indexes must be declared only once during a swap. `doggo` was specified several times.",
      "code": "invalid_swap_duplicate_index_found",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#invalid_swap_duplicate_index_found"
    }
    "###);

    let (response, code) = server
        .index_swap(json!([{ "indexes": ["doggo", "catto"]}, { "indexes": ["girafo", "doggo"]}]))
        .await;
    snapshot!(code, @"400 Bad Request");
    snapshot!(json_string!(response), @r###"
    {
      "message": "Indexes must be declared only once during a swap. `doggo` was specified several times.",
      "code": "invalid_swap_duplicate_index_found",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#invalid_swap_duplicate_index_found"
    }
    "###);
}

#[actix_rt::test]
async fn swap_indexes_bad_rename() {
    let server = Server::new_shared();

    let (response, code) =
        server.index_swap(json!([{ "indexes": ["kefir", "intel"], "rename": "hello" }])).await;
    snapshot!(code, @"400 Bad Request");
    snapshot!(json_string!(response), @r#"
    {
      "message": "Invalid value type at `[0].rename`: expected a boolean, but found a string: `\"hello\"`",
      "code": "invalid_swap_rename",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#invalid_swap_rename"
    }
    "#);
}

#[actix_rt::test]
async fn swap_indexes_rename_to_already_existing_index() {
    let server = Server::new_shared();
    let already_existing_index = shared_empty_index().await;
    let base_index = shared_index_with_documents().await;

    let (response, _code) = server
        .index_swap(
            json!([{ "indexes": [base_index.uid, already_existing_index.uid], "rename": true }]),
        )
        .await;
    let response = server.wait_task(response.uid()).await;
    snapshot!(response, @r#"
    {
      "uid": "[uid]",
      "batchUid": "[batch_uid]",
      "indexUid": null,
      "status": "failed",
      "type": "indexSwap",
      "canceledBy": null,
      "details": {
        "swaps": [
          {
            "indexes": [
              "SHARED_DOCUMENTS",
              "EMPTY_INDEX"
            ],
            "rename": true
          }
        ]
      },
      "error": {
        "message": "Cannot rename `SHARED_DOCUMENTS` to `EMPTY_INDEX` as the index already exists. Hint: You can remove `EMPTY_INDEX` first and then do your remove.",
        "code": "index_already_exists",
        "type": "invalid_request",
        "link": "https://docs.meilisearch.com/errors#index_already_exists"
      },
      "duration": "[duration]",
      "enqueuedAt": "[date]",
      "startedAt": "[date]",
      "finishedAt": "[date]"
    }
    "#);

    let base_index_2 = shared_index_with_geo_documents().await;
    let already_existing_index_2 = shared_index_with_nested_documents().await;
    let (response, _code) = server
        .index_swap(
            json!([{ "indexes": [base_index.uid, already_existing_index.uid], "rename": true }, { "indexes": [base_index_2.uid, already_existing_index_2.uid], "rename": true }]),
        )
        .await;
    let response = server.wait_task(response.uid()).await;
    snapshot!(response, @r#"
    {
      "uid": "[uid]",
      "batchUid": "[batch_uid]",
      "indexUid": null,
      "status": "failed",
      "type": "indexSwap",
      "canceledBy": null,
      "details": {
        "swaps": [
          {
            "indexes": [
              "SHARED_DOCUMENTS",
              "EMPTY_INDEX"
            ],
            "rename": true
          },
          {
            "indexes": [
              "SHARED_GEO_DOCUMENTS",
              "SHARED_NESTED_DOCUMENTS"
            ],
            "rename": true
          }
        ]
      },
      "error": {
        "message": "The following indexes are being renamed but cannot because their new name conflicts with an already existing index: `EMPTY_INDEX`, `SHARED_NESTED_DOCUMENTS`. Renaming doesn't overwrite the other index name.",
        "code": "index_already_exists",
        "type": "invalid_request",
        "link": "https://docs.meilisearch.com/errors#index_already_exists"
      },
      "duration": "[duration]",
      "enqueuedAt": "[date]",
      "startedAt": "[date]",
      "finishedAt": "[date]"
    }
    "#);
}
