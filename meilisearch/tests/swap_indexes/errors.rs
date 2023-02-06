use meili_snap::*;
use serde_json::json;

use crate::common::Server;

#[actix_rt::test]
async fn swap_indexes_bad_format() {
    let server = Server::new().await;

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
    let server = Server::new().await;

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
