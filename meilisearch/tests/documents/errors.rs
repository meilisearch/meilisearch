use meili_snap::*;
use urlencoding::encode;

use crate::common::Server;
use crate::json;

#[actix_rt::test]
async fn get_all_documents_bad_offset() {
    let server = Server::new_shared();
    let index = server.index("test");

    let (response, code) = index.get_all_documents_raw("?offset").await;
    snapshot!(code, @"400 Bad Request");
    snapshot!(json_string!(response), @r###"
    {
      "message": "Invalid value in parameter `offset`: could not parse `` as a positive integer",
      "code": "invalid_document_offset",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#invalid_document_offset"
    }
    "###);

    let (response, code) = index.get_all_documents_raw("?offset=doggo").await;
    snapshot!(code, @"400 Bad Request");
    snapshot!(json_string!(response), @r###"
    {
      "message": "Invalid value in parameter `offset`: could not parse `doggo` as a positive integer",
      "code": "invalid_document_offset",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#invalid_document_offset"
    }
    "###);

    let (response, code) = index.get_all_documents_raw("?offset=-1").await;
    snapshot!(code, @"400 Bad Request");
    snapshot!(json_string!(response), @r###"
    {
      "message": "Invalid value in parameter `offset`: could not parse `-1` as a positive integer",
      "code": "invalid_document_offset",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#invalid_document_offset"
    }
    "###);
}

#[actix_rt::test]
async fn get_all_documents_bad_limit() {
    let server = Server::new_shared();
    let index = server.index("test");

    let (response, code) = index.get_all_documents_raw("?limit").await;
    snapshot!(code, @"400 Bad Request");
    snapshot!(json_string!(response), @r###"
    {
      "message": "Invalid value in parameter `limit`: could not parse `` as a positive integer",
      "code": "invalid_document_limit",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#invalid_document_limit"
    }
    "###);

    let (response, code) = index.get_all_documents_raw("?limit=doggo").await;
    snapshot!(code, @"400 Bad Request");
    snapshot!(json_string!(response), @r###"
    {
      "message": "Invalid value in parameter `limit`: could not parse `doggo` as a positive integer",
      "code": "invalid_document_limit",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#invalid_document_limit"
    }
    "###);

    let (response, code) = index.get_all_documents_raw("?limit=-1").await;
    snapshot!(code, @"400 Bad Request");
    snapshot!(json_string!(response), @r###"
    {
      "message": "Invalid value in parameter `limit`: could not parse `-1` as a positive integer",
      "code": "invalid_document_limit",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#invalid_document_limit"
    }
    "###);
}

#[actix_rt::test]
async fn get_all_documents_bad_filter() {
    let server = Server::new_shared();
    let index = server.index("test");

    // Since the filter can't be parsed automatically by deserr, we have the wrong error message
    // if the index does not exist: we could expect to get an error message about the invalid filter before
    // the existence of the index is checked, but it is not the case.
    let (response, code) = index.get_all_documents_raw("?filter").await;
    snapshot!(code, @"404 Not Found");
    snapshot!(json_string!(response), @r###"
    {
      "message": "Index `test` not found.",
      "code": "index_not_found",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#index_not_found"
    }
    "###);

    let (response, code) = index.get_all_documents_raw("?filter=doggo").await;
    snapshot!(code, @"404 Not Found");
    snapshot!(json_string!(response), @r###"
    {
      "message": "Index `test` not found.",
      "code": "index_not_found",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#index_not_found"
    }
    "###);

    let (response, code) = index.get_all_documents_raw("?filter=doggo=bernese").await;
    snapshot!(code, @"404 Not Found");
    snapshot!(json_string!(response), @r###"
    {
      "message": "Index `test` not found.",
      "code": "index_not_found",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#index_not_found"
    }
    "###);

    let (response, _code) = index.create(None).await;
    server.wait_task(response.uid()).await.succeeded();

    let (response, code) = index.get_all_documents_raw("?filter").await;
    snapshot!(code, @"200 OK");
    snapshot!(json_string!(response), @r###"
    {
      "results": [],
      "offset": 0,
      "limit": 20,
      "total": 0
    }
    "###);

    let (response, code) = index.get_all_documents_raw("?filter=doggo").await;
    snapshot!(code, @"400 Bad Request");
    snapshot!(json_string!(response), @r###"
    {
      "message": "Was expecting an operation `=`, `!=`, `>=`, `>`, `<=`, `<`, `IN`, `NOT IN`, `TO`, `EXISTS`, `NOT EXISTS`, `IS NULL`, `IS NOT NULL`, `IS EMPTY`, `IS NOT EMPTY`, `CONTAINS`, `NOT CONTAINS`, `_geoRadius`, or `_geoBoundingBox` at `doggo`.\n1:6 doggo",
      "code": "invalid_document_filter",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#invalid_document_filter"
    }
    "###);

    let (response, code) = index.get_all_documents_raw("?filter=doggo=bernese").await;
    snapshot!(code, @"400 Bad Request");
    snapshot!(json_string!(response), @r###"
    {
      "message": "Attribute `doggo` is not filterable. This index does not have configured filterable attributes.\n1:6 doggo=bernese",
      "code": "invalid_document_filter",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#invalid_document_filter"
    }
    "###);
}

#[actix_rt::test]
async fn delete_documents_batch() {
    let server = Server::new_shared();
    let index = server.index("test");

    let (response, code) = index.delete_batch_raw(json!("doggo")).await;
    snapshot!(code, @"400 Bad Request");
    snapshot!(json_string!(response), @r###"
    {
      "message": "Json deserialize error: invalid type: string \"doggo\", expected a sequence at line 1 column 7",
      "code": "bad_request",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#bad_request"
    }
    "###);
}

#[actix_rt::test]
async fn replace_documents_missing_payload() {
    let server = Server::new_shared();
    let index = server.index("test");

    let (response, code) =
        index.raw_add_documents("", vec![("Content-Type", "application/json")], "").await;
    snapshot!(code, @"400 Bad Request");
    snapshot!(json_string!(response), @r###"
    {
      "message": "A json payload is missing.",
      "code": "missing_payload",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#missing_payload"
    }
    "###);

    let (response, code) =
        index.raw_add_documents("", vec![("Content-Type", "application/x-ndjson")], "").await;
    snapshot!(code, @"400 Bad Request");
    snapshot!(json_string!(response), @r###"
    {
      "message": "A ndjson payload is missing.",
      "code": "missing_payload",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#missing_payload"
    }
    "###);

    let (response, code) =
        index.raw_add_documents("", vec![("Content-Type", "text/csv")], "").await;
    snapshot!(code, @"400 Bad Request");
    snapshot!(json_string!(response), @r###"
    {
      "message": "A csv payload is missing.",
      "code": "missing_payload",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#missing_payload"
    }
    "###);
}

#[actix_rt::test]
async fn update_documents_missing_payload() {
    let server = Server::new_shared();
    let index = server.index("test");

    let (response, code) = index.raw_update_documents("", Some("application/json"), "").await;
    snapshot!(code, @"400 Bad Request");
    snapshot!(json_string!(response), @r###"
    {
      "message": "A json payload is missing.",
      "code": "missing_payload",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#missing_payload"
    }
    "###);

    let (response, code) = index.raw_update_documents("", Some("application/x-ndjson"), "").await;
    snapshot!(code, @"400 Bad Request");
    snapshot!(json_string!(response), @r###"
    {
      "message": "A ndjson payload is missing.",
      "code": "missing_payload",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#missing_payload"
    }
    "###);

    let (response, code) = index.raw_update_documents("", Some("text/csv"), "").await;
    snapshot!(code, @"400 Bad Request");
    snapshot!(json_string!(response), @r###"
    {
      "message": "A csv payload is missing.",
      "code": "missing_payload",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#missing_payload"
    }
    "###);
}

#[actix_rt::test]
async fn replace_documents_missing_content_type() {
    let server = Server::new_shared();
    let index = server.index("test");

    let (response, code) = index.raw_add_documents("", Vec::new(), "").await;
    snapshot!(code, @"415 Unsupported Media Type");
    snapshot!(json_string!(response), @r###"
    {
      "message": "A Content-Type header is missing. Accepted values for the Content-Type header are: `application/json`, `application/x-ndjson`, `text/csv`",
      "code": "missing_content_type",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#missing_content_type"
    }
    "###);

    // even with a csv delimiter specified this error is triggered first
    let (response, code) = index.raw_add_documents("", Vec::new(), "?csvDelimiter=;").await;
    snapshot!(code, @"415 Unsupported Media Type");
    snapshot!(json_string!(response), @r###"
    {
      "message": "A Content-Type header is missing. Accepted values for the Content-Type header are: `application/json`, `application/x-ndjson`, `text/csv`",
      "code": "missing_content_type",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#missing_content_type"
    }
    "###);
}

#[actix_rt::test]
async fn update_documents_missing_content_type() {
    let server = Server::new_shared();
    let index = server.index("test");

    let (response, code) = index.raw_update_documents("", None, "").await;
    snapshot!(code, @"415 Unsupported Media Type");
    snapshot!(json_string!(response), @r###"
    {
      "message": "A Content-Type header is missing. Accepted values for the Content-Type header are: `application/json`, `application/x-ndjson`, `text/csv`",
      "code": "missing_content_type",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#missing_content_type"
    }
    "###);

    // even with a csv delimiter specified this error is triggered first
    let (response, code) = index.raw_update_documents("", None, "?csvDelimiter=;").await;
    snapshot!(code, @"415 Unsupported Media Type");
    snapshot!(json_string!(response), @r###"
    {
      "message": "A Content-Type header is missing. Accepted values for the Content-Type header are: `application/json`, `application/x-ndjson`, `text/csv`",
      "code": "missing_content_type",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#missing_content_type"
    }
    "###);
}

#[actix_rt::test]
async fn replace_documents_bad_content_type() {
    let server = Server::new_shared();
    let index = server.index("test");

    let (response, code) = index.raw_add_documents("", vec![("Content-Type", "doggo")], "").await;
    snapshot!(code, @"415 Unsupported Media Type");
    snapshot!(json_string!(response), @r###"
    {
      "message": "The Content-Type `doggo` is invalid. Accepted values for the Content-Type header are: `application/json`, `application/x-ndjson`, `text/csv`",
      "code": "invalid_content_type",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#invalid_content_type"
    }
    "###);
}

#[actix_rt::test]
async fn update_documents_bad_content_type() {
    let server = Server::new_shared();
    let index = server.index("test");

    let (response, code) = index.raw_update_documents("", Some("doggo"), "").await;
    snapshot!(code, @"415 Unsupported Media Type");
    snapshot!(json_string!(response), @r###"
    {
      "message": "The Content-Type `doggo` is invalid. Accepted values for the Content-Type header are: `application/json`, `application/x-ndjson`, `text/csv`",
      "code": "invalid_content_type",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#invalid_content_type"
    }
    "###);
}

#[actix_rt::test]
async fn replace_documents_bad_csv_delimiter() {
    let server = Server::new_shared();
    let index = server.index("test");

    let (response, code) = index
        .raw_add_documents("", vec![("Content-Type", "application/json")], "?csvDelimiter")
        .await;
    snapshot!(code, @"400 Bad Request");
    snapshot!(json_string!(response), @r###"
    {
      "message": "Invalid value in parameter `csvDelimiter`: expected a string of one character, but found an empty string",
      "code": "invalid_document_csv_delimiter",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#invalid_document_csv_delimiter"
    }
    "###);

    let (response, code) = index
        .raw_add_documents("", vec![("Content-Type", "application/json")], "?csvDelimiter=doggo")
        .await;
    snapshot!(code, @"400 Bad Request");
    snapshot!(json_string!(response), @r###"
    {
      "message": "Invalid value in parameter `csvDelimiter`: expected a string of one character, but found the following string of 5 characters: `doggo`",
      "code": "invalid_document_csv_delimiter",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#invalid_document_csv_delimiter"
    }
    "###);

    let (response, code) = index
        .raw_add_documents(
            "",
            vec![("Content-Type", "application/json")],
            &format!("?csvDelimiter={}", encode("🍰")),
        )
        .await;
    snapshot!(code, @"400 Bad Request");
    snapshot!(json_string!(response), @r###"
    {
      "message": "csv delimiter must be an ascii character. Found: `🍰`",
      "code": "invalid_document_csv_delimiter",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#invalid_document_csv_delimiter"
    }
    "###);
}

#[actix_rt::test]
async fn update_documents_bad_csv_delimiter() {
    let server = Server::new_shared();
    let index = server.index("test");

    let (response, code) =
        index.raw_update_documents("", Some("application/json"), "?csvDelimiter").await;
    snapshot!(code, @"400 Bad Request");
    snapshot!(json_string!(response), @r###"
    {
      "message": "Invalid value in parameter `csvDelimiter`: expected a string of one character, but found an empty string",
      "code": "invalid_document_csv_delimiter",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#invalid_document_csv_delimiter"
    }
    "###);

    let (response, code) =
        index.raw_update_documents("", Some("application/json"), "?csvDelimiter=doggo").await;
    snapshot!(code, @"400 Bad Request");
    snapshot!(json_string!(response), @r###"
    {
      "message": "Invalid value in parameter `csvDelimiter`: expected a string of one character, but found the following string of 5 characters: `doggo`",
      "code": "invalid_document_csv_delimiter",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#invalid_document_csv_delimiter"
    }
    "###);

    let (response, code) = index
        .raw_update_documents(
            "",
            Some("application/json"),
            &format!("?csvDelimiter={}", encode("🍰")),
        )
        .await;
    snapshot!(code, @"400 Bad Request");
    snapshot!(json_string!(response), @r###"
    {
      "message": "csv delimiter must be an ascii character. Found: `🍰`",
      "code": "invalid_document_csv_delimiter",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#invalid_document_csv_delimiter"
    }
    "###);
}

#[actix_rt::test]
async fn replace_documents_csv_delimiter_with_bad_content_type() {
    let server = Server::new_shared();
    let index = server.index("test");

    let (response, code) = index
        .raw_add_documents("", vec![("Content-Type", "application/json")], "?csvDelimiter=a")
        .await;
    snapshot!(code, @"415 Unsupported Media Type");
    snapshot!(json_string!(response), @r###"
    {
      "message": "The Content-Type `application/json` does not support the use of a csv delimiter. The csv delimiter can only be used with the Content-Type `text/csv`.",
      "code": "invalid_content_type",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#invalid_content_type"
    }
    "###);

    let (response, code) = index
        .raw_add_documents("", vec![("Content-Type", "application/x-ndjson")], "?csvDelimiter=a")
        .await;
    snapshot!(code, @"415 Unsupported Media Type");
    snapshot!(json_string!(response), @r###"
    {
      "message": "The Content-Type `application/x-ndjson` does not support the use of a csv delimiter. The csv delimiter can only be used with the Content-Type `text/csv`.",
      "code": "invalid_content_type",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#invalid_content_type"
    }
    "###);
}

#[actix_rt::test]
async fn update_documents_csv_delimiter_with_bad_content_type() {
    let server = Server::new_shared();
    let index = server.index("test");

    let (response, code) =
        index.raw_update_documents("", Some("application/json"), "?csvDelimiter=a").await;
    snapshot!(code, @"415 Unsupported Media Type");
    snapshot!(json_string!(response), @r###"
    {
      "message": "The Content-Type `application/json` does not support the use of a csv delimiter. The csv delimiter can only be used with the Content-Type `text/csv`.",
      "code": "invalid_content_type",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#invalid_content_type"
    }
    "###);

    let (response, code) =
        index.raw_update_documents("", Some("application/x-ndjson"), "?csvDelimiter=a").await;
    snapshot!(code, @"415 Unsupported Media Type");
    snapshot!(json_string!(response), @r###"
    {
      "message": "The Content-Type `application/x-ndjson` does not support the use of a csv delimiter. The csv delimiter can only be used with the Content-Type `text/csv`.",
      "code": "invalid_content_type",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#invalid_content_type"
    }
    "###);
}

#[actix_rt::test]
async fn delete_document_by_filter() {
    let server = Server::new_shared();
    let index = server.index("tests-documents-errors-delete_document_by_filter");

    // send a bad payload type
    let (response, code) = index.delete_document_by_filter(json!("hello")).await;
    snapshot!(code, @"400 Bad Request");
    snapshot!(response, @r###"
    {
      "message": "Invalid value type: expected an object, but found a string: `\"hello\"`",
      "code": "bad_request",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#bad_request"
    }
    "###);

    // send bad payload type
    let (response, code) = index.delete_document_by_filter(json!({ "filter": true })).await;
    snapshot!(code, @"400 Bad Request");
    snapshot!(response, @r###"
    {
      "message": "Invalid syntax for the filter parameter: `expected String, Array, found: true`.",
      "code": "invalid_document_filter",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#invalid_document_filter"
    }
    "###);

    // send bad filter
    let (response, code) = index.delete_document_by_filter(json!({ "filter": "hello"})).await;
    snapshot!(code, @"400 Bad Request");
    snapshot!(response, @r###"
    {
      "message": "Was expecting an operation `=`, `!=`, `>=`, `>`, `<=`, `<`, `IN`, `NOT IN`, `TO`, `EXISTS`, `NOT EXISTS`, `IS NULL`, `IS NOT NULL`, `IS EMPTY`, `IS NOT EMPTY`, `CONTAINS`, `NOT CONTAINS`, `_geoRadius`, or `_geoBoundingBox` at `hello`.\n1:6 hello",
      "code": "invalid_document_filter",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#invalid_document_filter"
    }
    "###);

    // send empty filter
    let (response, code) = index.delete_document_by_filter(json!({ "filter": ""})).await;
    snapshot!(code, @"400 Bad Request");
    snapshot!(response, @r###"
    {
      "message": "Sending an empty filter is forbidden.",
      "code": "invalid_document_filter",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#invalid_document_filter"
    }
    "###);

    // do not send any filter
    let (response, code) = index.delete_document_by_filter(json!({})).await;
    snapshot!(code, @"400 Bad Request");
    snapshot!(response, @r###"
    {
      "message": "Missing field `filter`",
      "code": "missing_document_filter",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#missing_document_filter"
    }
    "###);

    // index does not exists
    let (response, code) =
        index.delete_document_by_filter(json!({ "filter": "doggo = bernese"})).await;
    snapshot!(code, @"202 Accepted");
    let response = server.wait_task(response.uid()).await;
    snapshot!(response, @r###"
    {
      "uid": "[uid]",
      "indexUid": "tests-documents-errors-delete_document_by_filter",
      "status": "failed",
      "type": "documentDeletion",
      "canceledBy": null,
      "details": {
        "providedIds": 0,
        "deletedDocuments": 0,
        "originalFilter": "\"doggo = bernese\""
      },
      "error": {
        "message": "Index `tests-documents-errors-delete_document_by_filter` not found.",
        "code": "index_not_found",
        "type": "invalid_request",
        "link": "https://docs.meilisearch.com/errors#index_not_found"
      },
      "duration": "[duration]",
      "enqueuedAt": "[date]",
      "startedAt": "[date]",
      "finishedAt": "[date]"
    }
    "###);

    let (response, code) = index.create(None).await;
    snapshot!(code, @"202 Accepted");
    server.wait_task(response.uid()).await.succeeded();

    // no filterable are set
    let (response, code) =
        index.delete_document_by_filter(json!({ "filter": "doggo = bernese"})).await;
    snapshot!(code, @"202 Accepted");
    let response = server.wait_task(response.uid()).await;
    snapshot!(response, @r###"
    {
      "uid": "[uid]",
      "indexUid": "tests-documents-errors-delete_document_by_filter",
      "status": "failed",
      "type": "documentDeletion",
      "canceledBy": null,
      "details": {
        "providedIds": 0,
        "deletedDocuments": 0,
        "originalFilter": "\"doggo = bernese\""
      },
      "error": {
        "message": "Attribute `doggo` is not filterable. This index does not have configured filterable attributes.\n1:6 doggo = bernese",
        "code": "invalid_document_filter",
        "type": "invalid_request",
        "link": "https://docs.meilisearch.com/errors#invalid_document_filter"
      },
      "duration": "[duration]",
      "enqueuedAt": "[date]",
      "startedAt": "[date]",
      "finishedAt": "[date]"
    }
    "###);

    let (response, code) = index.update_settings_filterable_attributes(json!(["doggo"])).await;
    snapshot!(code, @"202 Accepted");
    server.wait_task(response.uid()).await.succeeded();

    // not filterable while there is a filterable attribute
    let (response, code) =
        index.delete_document_by_filter(json!({ "filter": "catto = jorts"})).await;
    snapshot!(code, @"202 Accepted");
    let response = server.wait_task(response.uid()).await;
    snapshot!(response, @r###"
    {
      "uid": "[uid]",
      "indexUid": "tests-documents-errors-delete_document_by_filter",
      "status": "failed",
      "type": "documentDeletion",
      "canceledBy": null,
      "details": {
        "providedIds": 0,
        "deletedDocuments": 0,
        "originalFilter": "\"catto = jorts\""
      },
      "error": {
        "message": "Attribute `catto` is not filterable. Available filterable attributes are: `doggo`.\n1:6 catto = jorts",
        "code": "invalid_document_filter",
        "type": "invalid_request",
        "link": "https://docs.meilisearch.com/errors#invalid_document_filter"
      },
      "duration": "[duration]",
      "enqueuedAt": "[date]",
      "startedAt": "[date]",
      "finishedAt": "[date]"
    }
    "###);
}

#[actix_rt::test]
async fn fetch_document_by_filter() {
    let server = Server::new_shared();
    let index = server.index("doggo");
    index.update_settings_filterable_attributes(json!(["color"])).await;
    index
        .add_documents(
            json!([
                { "id": 0, "color": "red" },
                { "id": 1, "color": "blue" },
                { "id": 2, "color": "blue" },
                { "id": 3 },
            ]),
            Some("id"),
        )
        .await;
    index.wait_task(1).await;

    let (response, code) = index.get_document_by_filter(json!(null)).await;
    snapshot!(code, @"400 Bad Request");
    snapshot!(response, @r###"
    {
      "message": "Invalid value type: expected an object, but found null",
      "code": "bad_request",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#bad_request"
    }
    "###);

    let (response, code) = index.get_document_by_filter(json!({ "offset": "doggo" })).await;
    snapshot!(code, @"400 Bad Request");
    snapshot!(response, @r###"
    {
      "message": "Invalid value type at `.offset`: expected a positive integer, but found a string: `\"doggo\"`",
      "code": "invalid_document_offset",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#invalid_document_offset"
    }
    "###);

    let (response, code) = index.get_document_by_filter(json!({ "limit": "doggo" })).await;
    snapshot!(code, @"400 Bad Request");
    snapshot!(response, @r###"
    {
      "message": "Invalid value type at `.limit`: expected a positive integer, but found a string: `\"doggo\"`",
      "code": "invalid_document_limit",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#invalid_document_limit"
    }
    "###);

    let (response, code) = index.get_document_by_filter(json!({ "fields": "doggo" })).await;
    snapshot!(code, @"400 Bad Request");
    snapshot!(response, @r###"
    {
      "message": "Invalid value type at `.fields`: expected an array, but found a string: `\"doggo\"`",
      "code": "invalid_document_fields",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#invalid_document_fields"
    }
    "###);

    let (response, code) = index.get_document_by_filter(json!({ "filter": true })).await;
    snapshot!(code, @"400 Bad Request");
    snapshot!(response, @r###"
    {
      "message": "Invalid syntax for the filter parameter: `expected String, Array, found: true`.",
      "code": "invalid_document_filter",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#invalid_document_filter"
    }
    "###);

    let (response, code) = index.get_document_by_filter(json!({ "filter": "cool doggo" })).await;
    snapshot!(code, @"400 Bad Request");
    snapshot!(response, @r###"
    {
      "message": "Was expecting an operation `=`, `!=`, `>=`, `>`, `<=`, `<`, `IN`, `NOT IN`, `TO`, `EXISTS`, `NOT EXISTS`, `IS NULL`, `IS NOT NULL`, `IS EMPTY`, `IS NOT EMPTY`, `CONTAINS`, `NOT CONTAINS`, `_geoRadius`, or `_geoBoundingBox` at `cool doggo`.\n1:11 cool doggo",
      "code": "invalid_document_filter",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#invalid_document_filter"
    }
    "###);

    let (response, code) =
        index.get_document_by_filter(json!({ "filter": "doggo = bernese" })).await;
    snapshot!(code, @"400 Bad Request");
    snapshot!(response, @r###"
    {
      "message": "Attribute `doggo` is not filterable. Available filterable attributes are: `color`.\n1:6 doggo = bernese",
      "code": "invalid_document_filter",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#invalid_document_filter"
    }
    "###);
}

#[actix_rt::test]
async fn retrieve_vectors() {
    let server = Server::new_shared();
    let index = server.index("doggo");

    // GET ALL DOCUMENTS BY QUERY
    let (response, _code) = index.get_all_documents_raw("?retrieveVectors=tamo").await;
    snapshot!(response, @r###"
    {
      "message": "Invalid value in parameter `retrieveVectors`: could not parse `tamo` as a boolean, expected either `true` or `false`",
      "code": "invalid_document_retrieve_vectors",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#invalid_document_retrieve_vectors"
    }
    "###);
    let (response, _code) = index.get_all_documents_raw("?retrieveVectors=true").await;
    snapshot!(response, @r###"
    {
      "message": "Passing `retrieveVectors` as a parameter requires enabling the `vector store` experimental feature. See https://github.com/meilisearch/product/discussions/677",
      "code": "feature_not_enabled",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#feature_not_enabled"
    }
    "###);

    // FETCH ALL DOCUMENTS BY POST
    let (response, _code) =
        index.get_document_by_filter(json!({ "retrieveVectors": "tamo" })).await;
    snapshot!(response, @r###"
    {
      "message": "Invalid value type at `.retrieveVectors`: expected a boolean, but found a string: `\"tamo\"`",
      "code": "invalid_document_retrieve_vectors",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#invalid_document_retrieve_vectors"
    }
    "###);
    let (response, _code) = index.get_document_by_filter(json!({ "retrieveVectors": true })).await;
    snapshot!(response, @r###"
    {
      "message": "Passing `retrieveVectors` as a parameter requires enabling the `vector store` experimental feature. See https://github.com/meilisearch/product/discussions/677",
      "code": "feature_not_enabled",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#feature_not_enabled"
    }
    "###);

    // GET A SINGLE DOCUMENT
    let (response, _code) = index.get_document(0, Some(json!({"retrieveVectors": "tamo"}))).await;
    snapshot!(response, @r###"
    {
      "message": "Invalid value in parameter `retrieveVectors`: could not parse `tamo` as a boolean, expected either `true` or `false`",
      "code": "invalid_document_retrieve_vectors",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#invalid_document_retrieve_vectors"
    }
    "###);
    let (response, _code) = index.get_document(0, Some(json!({"retrieveVectors": true}))).await;
    snapshot!(response, @r###"
    {
      "message": "Passing `retrieveVectors` as a parameter requires enabling the `vector store` experimental feature. See https://github.com/meilisearch/product/discussions/677",
      "code": "feature_not_enabled",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#feature_not_enabled"
    }
    "###);
}
