use meili_snap::*;
use serde_json::json;
use urlencoding::encode;

use crate::common::Server;

#[actix_rt::test]
async fn get_all_documents_bad_offset() {
    let server = Server::new().await;
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
    let server = Server::new().await;
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
async fn delete_documents_batch() {
    let server = Server::new().await;
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
    let server = Server::new().await;
    let index = server.index("test");

    let (response, code) = index.raw_add_documents("", Some("application/json"), "").await;
    snapshot!(code, @"400 Bad Request");
    snapshot!(json_string!(response), @r###"
    {
      "message": "A json payload is missing.",
      "code": "missing_payload",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#missing_payload"
    }
    "###);

    let (response, code) = index.raw_add_documents("", Some("application/x-ndjson"), "").await;
    snapshot!(code, @"400 Bad Request");
    snapshot!(json_string!(response), @r###"
    {
      "message": "A ndjson payload is missing.",
      "code": "missing_payload",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#missing_payload"
    }
    "###);

    let (response, code) = index.raw_add_documents("", Some("text/csv"), "").await;
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
    let server = Server::new().await;
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
    let server = Server::new().await;
    let index = server.index("test");

    let (response, code) = index.raw_add_documents("", None, "").await;
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
    let (response, code) = index.raw_add_documents("", None, "?csvDelimiter=;").await;
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
    let server = Server::new().await;
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
    let server = Server::new().await;
    let index = server.index("test");

    let (response, code) = index.raw_add_documents("", Some("doggo"), "").await;
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
    let server = Server::new().await;
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
    let server = Server::new().await;
    let index = server.index("test");

    let (response, code) =
        index.raw_add_documents("", Some("application/json"), "?csvDelimiter").await;
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
        index.raw_add_documents("", Some("application/json"), "?csvDelimiter=doggo").await;
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
        .raw_add_documents("", Some("application/json"), &format!("?csvDelimiter={}", encode("🍰")))
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
    let server = Server::new().await;
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
    let server = Server::new().await;
    let index = server.index("test");

    let (response, code) =
        index.raw_add_documents("", Some("application/json"), "?csvDelimiter=a").await;
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
        index.raw_add_documents("", Some("application/x-ndjson"), "?csvDelimiter=a").await;
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
    let server = Server::new().await;
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
