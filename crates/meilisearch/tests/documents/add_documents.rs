use actix_web::test;
use meili_snap::{json_string, snapshot};
use meilisearch::Opt;
use time::format_description::well_known::Rfc3339;
use time::OffsetDateTime;

use crate::common::encoder::Encoder;
use crate::common::{default_settings, GetAllDocumentsOptions, Server, Value};
use crate::json;

/// This is the basic usage of our API and every other tests uses the content-type application/json
#[actix_rt::test]
async fn add_documents_test_json_content_types() {
    let document = json!([
        {
            "id": 1,
            "content": "Bouvier Bernois",
        }
    ]);

    // this is a what is expected and should work
    let server = Server::new().await;
    let app = server.init_web_app().await;

    // post
    let req = test::TestRequest::post()
        .uri("/indexes/dog/documents")
        .set_payload(document.to_string())
        .insert_header(("content-type", "application/json"))
        .to_request();
    let res = test::call_service(&app, req).await;
    let status_code = res.status();
    let body = test::read_body(res).await;
    let response: Value = serde_json::from_slice(&body).unwrap_or_default();
    snapshot!(status_code, @"202 Accepted");
    snapshot!(json_string!(response, { ".duration" => "[duration]", ".enqueuedAt" => "[date]", ".startedAt" => "[date]", ".finishedAt" => "[date]" }),
        @r###"
    {
      "taskUid": 0,
      "indexUid": "dog",
      "status": "enqueued",
      "type": "documentAdditionOrUpdate",
      "enqueuedAt": "[date]"
    }
    "###);

    // put
    let req = test::TestRequest::put()
        .uri("/indexes/dog/documents")
        .set_payload(document.to_string())
        .insert_header(("content-type", "application/json"))
        .to_request();
    let res = test::call_service(&app, req).await;
    let status_code = res.status();
    let body = test::read_body(res).await;
    let response: Value = serde_json::from_slice(&body).unwrap_or_default();
    snapshot!(status_code, @"202 Accepted");
    snapshot!(json_string!(response, { ".duration" => "[duration]", ".enqueuedAt" => "[date]", ".startedAt" => "[date]", ".finishedAt" => "[date]" }),
        @r###"
    {
      "taskUid": 1,
      "indexUid": "dog",
      "status": "enqueued",
      "type": "documentAdditionOrUpdate",
      "enqueuedAt": "[date]"
    }
    "###);
}

/// Here we try to send a single document instead of an array with a single document inside.
#[actix_rt::test]
async fn add_single_document_test_json_content_types() {
    let document = json!({
        "id": 1,
        "content": "Bouvier Bernois",
    });

    // this is a what is expected and should work
    let server = Server::new().await;
    let app = server.init_web_app().await;

    // post
    let req = test::TestRequest::post()
        .uri("/indexes/dog/documents")
        .set_payload(document.to_string())
        .insert_header(("content-type", "application/json"))
        .to_request();
    let res = test::call_service(&app, req).await;
    let status_code = res.status();
    let body = test::read_body(res).await;
    let response: Value = serde_json::from_slice(&body).unwrap_or_default();
    snapshot!(status_code, @"202 Accepted");
    snapshot!(json_string!(response, { ".duration" => "[duration]", ".enqueuedAt" => "[date]", ".startedAt" => "[date]", ".finishedAt" => "[date]" }),
        @r###"
    {
      "taskUid": 0,
      "indexUid": "dog",
      "status": "enqueued",
      "type": "documentAdditionOrUpdate",
      "enqueuedAt": "[date]"
    }
    "###);

    // put
    let req = test::TestRequest::put()
        .uri("/indexes/dog/documents")
        .set_payload(document.to_string())
        .insert_header(("content-type", "application/json"))
        .to_request();
    let res = test::call_service(&app, req).await;
    let status_code = res.status();
    let body = test::read_body(res).await;
    let response: Value = serde_json::from_slice(&body).unwrap_or_default();
    snapshot!(status_code, @"202 Accepted");
    snapshot!(json_string!(response, { ".duration" => "[duration]", ".enqueuedAt" => "[date]", ".startedAt" => "[date]", ".finishedAt" => "[date]" }),
        @r###"
    {
      "taskUid": 1,
      "indexUid": "dog",
      "status": "enqueued",
      "type": "documentAdditionOrUpdate",
      "enqueuedAt": "[date]"
    }
    "###);
}

/// Here we try sending encoded (compressed) document request
#[actix_rt::test]
async fn add_single_document_gzip_encoded() {
    let document = json!({
        "id": 1,
        "content": "Bouvier Bernois",
    });

    // this is a what is expected and should work
    let server = Server::new().await;
    let app = server.init_web_app().await;
    // post
    let document = serde_json::to_string(&document).unwrap();
    let encoder = Encoder::Gzip;
    let req = test::TestRequest::post()
        .uri("/indexes/dog/documents")
        .set_payload(encoder.encode(document.clone()))
        .insert_header(("content-type", "application/json"))
        .insert_header(encoder.header().unwrap())
        .to_request();
    let res = test::call_service(&app, req).await;
    let status_code = res.status();
    let body = test::read_body(res).await;
    let response: Value = serde_json::from_slice(&body).unwrap_or_default();
    snapshot!(status_code, @"202 Accepted");
    snapshot!(json_string!(response, { ".duration" => "[duration]", ".enqueuedAt" => "[date]", ".startedAt" => "[date]", ".finishedAt" => "[date]" }),
        @r###"
    {
      "taskUid": 0,
      "indexUid": "dog",
      "status": "enqueued",
      "type": "documentAdditionOrUpdate",
      "enqueuedAt": "[date]"
    }
    "###);

    // put
    let req = test::TestRequest::put()
        .uri("/indexes/dog/documents")
        .set_payload(encoder.encode(document))
        .insert_header(("content-type", "application/json"))
        .insert_header(encoder.header().unwrap())
        .to_request();
    let res = test::call_service(&app, req).await;
    let status_code = res.status();
    let body = test::read_body(res).await;
    let response: Value = serde_json::from_slice(&body).unwrap_or_default();
    snapshot!(status_code, @"202 Accepted");
    snapshot!(json_string!(response, { ".duration" => "[duration]", ".enqueuedAt" => "[date]", ".startedAt" => "[date]", ".finishedAt" => "[date]" }),
        @r###"
    {
      "taskUid": 1,
      "indexUid": "dog",
      "status": "enqueued",
      "type": "documentAdditionOrUpdate",
      "enqueuedAt": "[date]"
    }
    "###);
}
#[actix_rt::test]
async fn add_single_document_gzip_encoded_with_incomplete_error() {
    let document = json!("kefir");

    // this is a what is expected and should work
    let server = Server::new().await;
    let app = server.init_web_app().await;
    // post
    let document = serde_json::to_string(&document).unwrap();
    let req = test::TestRequest::post()
        .uri("/indexes/dog/documents")
        .set_payload(document.to_string())
        .insert_header(("content-type", "application/json"))
        .insert_header(("content-encoding", "gzip"))
        .to_request();
    let res = test::call_service(&app, req).await;
    let status_code = res.status();
    let body = test::read_body(res).await;
    let response: Value = serde_json::from_slice(&body).unwrap_or_default();
    snapshot!(status_code, @"400 Bad Request");
    snapshot!(json_string!(response),
        @r###"
    {
      "message": "The provided payload is incomplete and cannot be parsed",
      "code": "bad_request",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#bad_request"
    }
    "###);

    // put
    let req = test::TestRequest::put()
        .uri("/indexes/dog/documents")
        .set_payload(document.to_string())
        .insert_header(("content-type", "application/json"))
        .insert_header(("content-encoding", "gzip"))
        .to_request();
    let res = test::call_service(&app, req).await;
    let status_code = res.status();
    let body = test::read_body(res).await;
    let response: Value = serde_json::from_slice(&body).unwrap_or_default();
    snapshot!(status_code, @"400 Bad Request");
    snapshot!(json_string!(response),
        @r###"
    {
      "message": "The provided payload is incomplete and cannot be parsed",
      "code": "bad_request",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#bad_request"
    }
    "###);
}

/// Here we try document request with every encoding
#[actix_rt::test]
async fn add_single_document_with_every_encoding() {
    let document = json!({
        "id": 1,
        "content": "Bouvier Bernois",
    });

    // this is a what is expected and should work
    let server = Server::new().await;
    let app = server.init_web_app().await;
    // post
    let document = serde_json::to_string(&document).unwrap();

    for (task_uid, encoder) in Encoder::iterator().enumerate() {
        let mut req = test::TestRequest::post()
            .uri("/indexes/dog/documents")
            .set_payload(encoder.encode(document.clone()))
            .insert_header(("content-type", "application/json"));
        req = match encoder.header() {
            Some(header) => req.insert_header(header),
            None => req,
        };
        let req = req.to_request();
        let res = test::call_service(&app, req).await;
        let status_code = res.status();
        let body = test::read_body(res).await;
        let response: Value = serde_json::from_slice(&body).unwrap_or_default();
        assert_eq!(status_code, 202);
        assert_eq!(response["taskUid"], task_uid);
    }
}

#[actix_rt::test]
async fn add_csv_document() {
    let server = Server::new().await;
    let index = server.index("pets");

    let document = "#id,name,race
0,jean,bernese mountain
1,jorts,orange cat";

    let (response, code) = index.raw_update_documents(document, Some("text/csv"), "").await;
    snapshot!(code, @"202 Accepted");
    snapshot!(json_string!(response, { ".enqueuedAt" => "[date]" }), @r###"
    {
      "taskUid": 0,
      "indexUid": "pets",
      "status": "enqueued",
      "type": "documentAdditionOrUpdate",
      "enqueuedAt": "[date]"
    }
    "###);
    let response = index.wait_task(response["taskUid"].as_u64().unwrap()).await;
    snapshot!(json_string!(response, { ".enqueuedAt" => "[date]", ".startedAt" => "[date]", ".finishedAt" => "[date]", ".duration" => "[duration]" }), @r###"
    {
      "uid": 0,
      "batchUid": 0,
      "indexUid": "pets",
      "status": "succeeded",
      "type": "documentAdditionOrUpdate",
      "canceledBy": null,
      "details": {
        "receivedDocuments": 2,
        "indexedDocuments": 2
      },
      "error": null,
      "duration": "[duration]",
      "enqueuedAt": "[date]",
      "startedAt": "[date]",
      "finishedAt": "[date]"
    }
    "###);

    let (documents, code) = index.get_all_documents(GetAllDocumentsOptions::default()).await;
    snapshot!(code, @"200 OK");
    snapshot!(json_string!(documents), @r###"
    {
      "results": [
        {
          "#id": "0",
          "name": "jean",
          "race": "bernese mountain"
        },
        {
          "#id": "1",
          "name": "jorts",
          "race": "orange cat"
        }
      ],
      "offset": 0,
      "limit": 20,
      "total": 2
    }
    "###);
}

#[actix_rt::test]
async fn add_csv_document_with_types() {
    let server = Server::new().await;
    let index = server.index("pets");

    let document = "#id:number,name:string,race:string,age:number,cute:boolean
0,jean,bernese mountain,2.5,true
1,,,,
2,lilou,pug,-2,false";

    let (response, code) = index.raw_update_documents(document, Some("text/csv"), "").await;
    snapshot!(code, @"202 Accepted");
    snapshot!(json_string!(response, { ".enqueuedAt" => "[date]" }), @r###"
    {
      "taskUid": 0,
      "indexUid": "pets",
      "status": "enqueued",
      "type": "documentAdditionOrUpdate",
      "enqueuedAt": "[date]"
    }
    "###);
    let response = index.wait_task(response["taskUid"].as_u64().unwrap()).await;
    snapshot!(json_string!(response, { ".enqueuedAt" => "[date]", ".startedAt" => "[date]", ".finishedAt" => "[date]", ".duration" => "[duration]" }), @r###"
    {
      "uid": 0,
      "batchUid": 0,
      "indexUid": "pets",
      "status": "succeeded",
      "type": "documentAdditionOrUpdate",
      "canceledBy": null,
      "details": {
        "receivedDocuments": 3,
        "indexedDocuments": 3
      },
      "error": null,
      "duration": "[duration]",
      "enqueuedAt": "[date]",
      "startedAt": "[date]",
      "finishedAt": "[date]"
    }
    "###);

    let (documents, code) = index.get_all_documents(GetAllDocumentsOptions::default()).await;
    snapshot!(code, @"200 OK");
    snapshot!(json_string!(documents), @r###"
    {
      "results": [
        {
          "#id": 0,
          "name": "jean",
          "race": "bernese mountain",
          "age": 2.5,
          "cute": true
        },
        {
          "#id": 1,
          "name": null,
          "race": null,
          "age": null,
          "cute": null
        },
        {
          "#id": 2,
          "name": "lilou",
          "race": "pug",
          "age": -2,
          "cute": false
        }
      ],
      "offset": 0,
      "limit": 20,
      "total": 3
    }
    "###);
}

#[actix_rt::test]
async fn add_csv_document_with_custom_delimiter() {
    let server = Server::new().await;
    let index = server.index("pets");

    let document = "#id|name|race
0|jean|bernese mountain
1|jorts|orange cat";

    let (response, code) =
        index.raw_update_documents(document, Some("text/csv"), "?csvDelimiter=|").await;
    snapshot!(code, @"202 Accepted");
    snapshot!(json_string!(response, { ".enqueuedAt" => "[date]" }), @r###"
    {
      "taskUid": 0,
      "indexUid": "pets",
      "status": "enqueued",
      "type": "documentAdditionOrUpdate",
      "enqueuedAt": "[date]"
    }
    "###);
    let response = index.wait_task(response["taskUid"].as_u64().unwrap()).await;
    snapshot!(json_string!(response, { ".enqueuedAt" => "[date]", ".startedAt" => "[date]", ".finishedAt" => "[date]", ".duration" => "[duration]" }), @r###"
    {
      "uid": 0,
      "batchUid": 0,
      "indexUid": "pets",
      "status": "succeeded",
      "type": "documentAdditionOrUpdate",
      "canceledBy": null,
      "details": {
        "receivedDocuments": 2,
        "indexedDocuments": 2
      },
      "error": null,
      "duration": "[duration]",
      "enqueuedAt": "[date]",
      "startedAt": "[date]",
      "finishedAt": "[date]"
    }
    "###);

    let (documents, code) = index.get_all_documents(GetAllDocumentsOptions::default()).await;
    snapshot!(code, @"200 OK");
    snapshot!(json_string!(documents), @r###"
    {
      "results": [
        {
          "#id": "0",
          "name": "jean",
          "race": "bernese mountain"
        },
        {
          "#id": "1",
          "name": "jorts",
          "race": "orange cat"
        }
      ],
      "offset": 0,
      "limit": 20,
      "total": 2
    }
    "###);
}

#[actix_rt::test]
async fn add_csv_document_with_types_error() {
    let server = Server::new().await;
    let index = server.index("pets");

    let document = "#id:number,a:boolean,b:number
0,doggo,1";

    let (response, code) = index.raw_update_documents(document, Some("text/csv"), "").await;
    snapshot!(code, @"400 Bad Request");
    snapshot!(json_string!(response, { ".enqueuedAt" => "[date]" }), @r###"
    {
      "message": "The `csv` payload provided is malformed: `Error parsing boolean \"doggo\" at line 1: provided string was not `true` or `false``.",
      "code": "malformed_payload",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#malformed_payload"
    }
    "###);

    let document = "#id:number,a:boolean,b:number
0,true,doggo";

    let (response, code) = index.raw_update_documents(document, Some("text/csv"), "").await;
    snapshot!(code, @"400 Bad Request");
    snapshot!(json_string!(response, { ".enqueuedAt" => "[date]" }), @r###"
    {
      "message": "The `csv` payload provided is malformed: `Error parsing number \"doggo\" at line 1: invalid float literal`.",
      "code": "malformed_payload",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#malformed_payload"
    }
    "###);
}

/// any other content-type is must be refused
#[actix_rt::test]
async fn error_add_documents_test_bad_content_types() {
    let document = json!([
        {
            "id": 1,
            "content": "Leonberg",
        }
    ]);

    let server = Server::new().await;
    let app = server.init_web_app().await;

    // post
    let req = test::TestRequest::post()
        .uri("/indexes/dog/documents")
        .set_payload(document.to_string())
        .insert_header(("content-type", "text/plain"))
        .to_request();
    let res = test::call_service(&app, req).await;
    let status_code = res.status();
    let body = test::read_body(res).await;
    let response: Value = serde_json::from_slice(&body).unwrap_or_default();
    snapshot!(status_code, @"415 Unsupported Media Type");
    snapshot!(json_string!(response),
        @r###"
    {
      "message": "The Content-Type `text/plain` is invalid. Accepted values for the Content-Type header are: `application/json`, `application/x-ndjson`, `text/csv`",
      "code": "invalid_content_type",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#invalid_content_type"
    }
    "###);

    // put
    let req = test::TestRequest::put()
        .uri("/indexes/dog/documents")
        .set_payload(document.to_string())
        .insert_header(("content-type", "text/plain"))
        .to_request();
    let res = test::call_service(&app, req).await;
    let status_code = res.status();
    let body = test::read_body(res).await;
    let response: Value = serde_json::from_slice(&body).unwrap_or_default();
    snapshot!(status_code, @"415 Unsupported Media Type");
    snapshot!(json_string!(response),
        @r###"
    {
      "message": "The Content-Type `text/plain` is invalid. Accepted values for the Content-Type header are: `application/json`, `application/x-ndjson`, `text/csv`",
      "code": "invalid_content_type",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#invalid_content_type"
    }
    "###);
}

/// missing content-type must be refused
#[actix_rt::test]
async fn error_add_documents_test_no_content_type() {
    let document = json!([
        {
            "id": 1,
            "content": "Leonberg",
        }
    ]);

    let server = Server::new().await;
    let app = server.init_web_app().await;

    // post
    let req = test::TestRequest::post()
        .uri("/indexes/dog/documents")
        .set_payload(document.to_string())
        .to_request();
    let res = test::call_service(&app, req).await;
    let status_code = res.status();
    let body = test::read_body(res).await;
    let response: Value = serde_json::from_slice(&body).unwrap_or_default();
    snapshot!(status_code, @"415 Unsupported Media Type");
    snapshot!(json_string!(response),
        @r###"
    {
      "message": "A Content-Type header is missing. Accepted values for the Content-Type header are: `application/json`, `application/x-ndjson`, `text/csv`",
      "code": "missing_content_type",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#missing_content_type"
    }
    "###);

    // put
    let req = test::TestRequest::put()
        .uri("/indexes/dog/documents")
        .set_payload(document.to_string())
        .to_request();
    let res = test::call_service(&app, req).await;
    let status_code = res.status();
    let body = test::read_body(res).await;
    let response: Value = serde_json::from_slice(&body).unwrap_or_default();
    snapshot!(status_code, @"415 Unsupported Media Type");
    snapshot!(json_string!(response),
        @r###"
    {
      "message": "A Content-Type header is missing. Accepted values for the Content-Type header are: `application/json`, `application/x-ndjson`, `text/csv`",
      "code": "missing_content_type",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#missing_content_type"
    }
    "###);
}

#[actix_rt::test]
async fn error_add_malformed_csv_documents() {
    let document = "id, content\n1234, hello, world\n12, hello world";

    let server = Server::new().await;
    let app = server.init_web_app().await;

    // post
    let req = test::TestRequest::post()
        .uri("/indexes/dog/documents")
        .set_payload(document.to_string())
        .insert_header(("content-type", "text/csv"))
        .to_request();
    let res = test::call_service(&app, req).await;
    let status_code = res.status();
    let body = test::read_body(res).await;
    let response: Value = serde_json::from_slice(&body).unwrap_or_default();
    snapshot!(status_code, @"400 Bad Request");
    snapshot!(json_string!(response),
        @r###"
    {
      "message": "The `csv` payload provided is malformed: `CSV error: record 1 (line: 2, byte: 12): found record with 3 fields, but the previous record has 2 fields`.",
      "code": "malformed_payload",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#malformed_payload"
    }
    "###);

    // put
    let req = test::TestRequest::put()
        .uri("/indexes/dog/documents")
        .set_payload(document.to_string())
        .insert_header(("content-type", "text/csv"))
        .to_request();
    let res = test::call_service(&app, req).await;
    let status_code = res.status();
    let body = test::read_body(res).await;
    let response: Value = serde_json::from_slice(&body).unwrap_or_default();
    snapshot!(status_code, @"400 Bad Request");
    snapshot!(json_string!(response),
        @r###"
    {
      "message": "The `csv` payload provided is malformed: `CSV error: record 1 (line: 2, byte: 12): found record with 3 fields, but the previous record has 2 fields`.",
      "code": "malformed_payload",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#malformed_payload"
    }
    "###);
}

#[actix_rt::test]
async fn error_add_malformed_json_documents() {
    let document = r#"[{"id": 1}, {id: 2}]"#;

    let server = Server::new().await;
    let app = server.init_web_app().await;

    // post
    let req = test::TestRequest::post()
        .uri("/indexes/dog/documents")
        .set_payload(document.to_string())
        .insert_header(("content-type", "application/json"))
        .to_request();
    let res = test::call_service(&app, req).await;
    let status_code = res.status();
    let body = test::read_body(res).await;
    let response: Value = serde_json::from_slice(&body).unwrap_or_default();
    snapshot!(status_code, @"400 Bad Request");
    snapshot!(json_string!(response),
        @r###"
    {
      "message": "The `json` payload provided is malformed. `Couldn't serialize document value: key must be a string at line 1 column 14`.",
      "code": "malformed_payload",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#malformed_payload"
    }
    "###);

    // put
    let req = test::TestRequest::put()
        .uri("/indexes/dog/documents")
        .set_payload(document.to_string())
        .insert_header(("content-type", "application/json"))
        .to_request();
    let res = test::call_service(&app, req).await;
    let status_code = res.status();
    let body = test::read_body(res).await;
    let response: Value = serde_json::from_slice(&body).unwrap_or_default();
    snapshot!(status_code, @"400 Bad Request");
    snapshot!(json_string!(response),
        @r###"
    {
      "message": "The `json` payload provided is malformed. `Couldn't serialize document value: key must be a string at line 1 column 14`.",
      "code": "malformed_payload",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#malformed_payload"
    }
    "###);

    // truncate

    // length = 100
    let long = "0123456789".repeat(10);

    let document = format!("\"{}\"", long);
    let req = test::TestRequest::put()
        .uri("/indexes/dog/documents")
        .set_payload(document)
        .insert_header(("content-type", "application/json"))
        .to_request();
    let res = test::call_service(&app, req).await;
    let body = test::read_body(res).await;
    let response: Value = serde_json::from_slice(&body).unwrap_or_default();
    snapshot!(status_code, @"400 Bad Request");
    snapshot!(json_string!(response),
        @r###"
    {
      "message": "The `json` payload provided is malformed. `Couldn't serialize document value: data are neither an object nor a list of objects`.",
      "code": "malformed_payload",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#malformed_payload"
    }
    "###);

    // add one more char to the long string to test if the truncating works.
    let document = format!("\"{}m\"", long);
    let req = test::TestRequest::put()
        .uri("/indexes/dog/documents")
        .set_payload(document)
        .insert_header(("content-type", "application/json"))
        .to_request();
    let res = test::call_service(&app, req).await;
    let body = test::read_body(res).await;
    let response: Value = serde_json::from_slice(&body).unwrap_or_default();
    snapshot!(status_code, @"400 Bad Request");
    snapshot!(json_string!(response),
        @r###"
    {
      "message": "The `json` payload provided is malformed. `Couldn't serialize document value: data are neither an object nor a list of objects`.",
      "code": "malformed_payload",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#malformed_payload"
    }
    "###);
}

#[actix_rt::test]
async fn error_add_malformed_ndjson_documents() {
    let document = "{\"id\": 1}\n{id: 2}";

    let server = Server::new().await;
    let app = server.init_web_app().await;

    // post
    let req = test::TestRequest::post()
        .uri("/indexes/dog/documents")
        .set_payload(document.to_string())
        .insert_header(("content-type", "application/x-ndjson"))
        .to_request();
    let res = test::call_service(&app, req).await;
    let status_code = res.status();
    let body = test::read_body(res).await;
    let response: Value = serde_json::from_slice(&body).unwrap_or_default();
    snapshot!(status_code, @"400 Bad Request");
    snapshot!(json_string!(response),
        @r###"
    {
      "message": "The `ndjson` payload provided is malformed. `Couldn't serialize document value: key must be a string at line 2 column 2`.",
      "code": "malformed_payload",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#malformed_payload"
    }
    "###);

    // put
    let req = test::TestRequest::put()
        .uri("/indexes/dog/documents")
        .set_payload(document.to_string())
        .insert_header(("content-type", "application/x-ndjson"))
        .to_request();
    let res = test::call_service(&app, req).await;
    let status_code = res.status();
    let body = test::read_body(res).await;
    let response: Value = serde_json::from_slice(&body).unwrap_or_default();
    snapshot!(status_code, @"400 Bad Request");
    snapshot!(json_string!(response),
        @r###"
    {
      "message": "The `ndjson` payload provided is malformed. `Couldn't serialize document value: key must be a string at line 2 column 2`.",
      "code": "malformed_payload",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#malformed_payload"
    }
    "###);
}

#[actix_rt::test]
async fn error_add_missing_payload_csv_documents() {
    let document = "";

    let server = Server::new().await;
    let app = server.init_web_app().await;

    // post
    let req = test::TestRequest::post()
        .uri("/indexes/dog/documents")
        .set_payload(document.to_string())
        .insert_header(("content-type", "text/csv"))
        .to_request();
    let res = test::call_service(&app, req).await;
    let status_code = res.status();
    let body = test::read_body(res).await;
    let response: Value = serde_json::from_slice(&body).unwrap_or_default();
    snapshot!(status_code, @"400 Bad Request");
    snapshot!(json_string!(response),
        @r###"
    {
      "message": "A csv payload is missing.",
      "code": "missing_payload",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#missing_payload"
    }
    "###);

    // put
    let req = test::TestRequest::put()
        .uri("/indexes/dog/documents")
        .set_payload(document.to_string())
        .insert_header(("content-type", "text/csv"))
        .to_request();
    let res = test::call_service(&app, req).await;
    let status_code = res.status();
    let body = test::read_body(res).await;
    let response: Value = serde_json::from_slice(&body).unwrap_or_default();
    snapshot!(status_code, @"400 Bad Request");
    snapshot!(json_string!(response),
        @r###"
    {
      "message": "A csv payload is missing.",
      "code": "missing_payload",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#missing_payload"
    }
    "###);
}

#[actix_rt::test]
async fn error_add_missing_payload_json_documents() {
    let document = "";

    let server = Server::new().await;
    let app = server.init_web_app().await;

    // post
    let req = test::TestRequest::post()
        .uri("/indexes/dog/documents")
        .set_payload(document.to_string())
        .insert_header(("content-type", "application/json"))
        .to_request();
    let res = test::call_service(&app, req).await;
    let status_code = res.status();
    let body = test::read_body(res).await;
    let response: Value = serde_json::from_slice(&body).unwrap_or_default();
    snapshot!(status_code, @"400 Bad Request");
    snapshot!(json_string!(response),
        @r###"
    {
      "message": "A json payload is missing.",
      "code": "missing_payload",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#missing_payload"
    }
    "###);

    // put
    let req = test::TestRequest::put()
        .uri("/indexes/dog/documents")
        .set_payload(document.to_string())
        .insert_header(("content-type", "application/json"))
        .to_request();
    let res = test::call_service(&app, req).await;
    let status_code = res.status();
    let body = test::read_body(res).await;
    let response: Value = serde_json::from_slice(&body).unwrap_or_default();
    snapshot!(status_code, @"400 Bad Request");
    snapshot!(json_string!(response),
        @r###"
    {
      "message": "A json payload is missing.",
      "code": "missing_payload",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#missing_payload"
    }
    "###);
}

#[actix_rt::test]
async fn error_add_missing_payload_ndjson_documents() {
    let document = "";

    let server = Server::new().await;
    let app = server.init_web_app().await;

    // post
    let req = test::TestRequest::post()
        .uri("/indexes/dog/documents")
        .set_payload(document.to_string())
        .insert_header(("content-type", "application/x-ndjson"))
        .to_request();
    let res = test::call_service(&app, req).await;
    let status_code = res.status();
    let body = test::read_body(res).await;
    let response: Value = serde_json::from_slice(&body).unwrap_or_default();
    snapshot!(status_code, @"400 Bad Request");
    snapshot!(json_string!(response),
        @r###"
    {
      "message": "A ndjson payload is missing.",
      "code": "missing_payload",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#missing_payload"
    }
    "###);

    // put
    let req = test::TestRequest::put()
        .uri("/indexes/dog/documents")
        .set_payload(document.to_string())
        .insert_header(("content-type", "application/x-ndjson"))
        .to_request();
    let res = test::call_service(&app, req).await;
    let status_code = res.status();
    let body = test::read_body(res).await;
    let response: Value = serde_json::from_slice(&body).unwrap_or_default();
    snapshot!(status_code, @"400 Bad Request");
    snapshot!(json_string!(response),
        @r###"
    {
      "message": "A ndjson payload is missing.",
      "code": "missing_payload",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#missing_payload"
    }
    "###);
}

#[actix_rt::test]
async fn add_documents_no_index_creation() {
    let server = Server::new().await;
    let index = server.index("test");

    let documents = json!([
        {
            "id": 1,
            "content": "foo",
        }
    ]);

    let (response, code) = index.add_documents(documents, None).await;
    snapshot!(code, @"202 Accepted");
    assert_eq!(response["taskUid"], 0);

    index.wait_task(response.uid()).await.succeeded();

    let (response, code) = index.get_task(0).await;
    snapshot!(code, @"200 OK");
    snapshot!(json_string!(response, { ".duration" => "[duration]", ".enqueuedAt" => "[date]", ".startedAt" => "[date]", ".finishedAt" => "[date]" }),
        @r###"
    {
      "uid": 0,
      "batchUid": 0,
      "indexUid": "test",
      "status": "succeeded",
      "type": "documentAdditionOrUpdate",
      "canceledBy": null,
      "details": {
        "receivedDocuments": 1,
        "indexedDocuments": 1
      },
      "error": null,
      "duration": "[duration]",
      "enqueuedAt": "[date]",
      "startedAt": "[date]",
      "finishedAt": "[date]"
    }
    "###);

    let processed_at =
        OffsetDateTime::parse(response["finishedAt"].as_str().unwrap(), &Rfc3339).unwrap();
    let enqueued_at =
        OffsetDateTime::parse(response["enqueuedAt"].as_str().unwrap(), &Rfc3339).unwrap();
    assert!(processed_at > enqueued_at);

    // index was created, and primary key was inferred.
    let (response, code) = index.get().await;
    snapshot!(code, @"200 OK");
    assert_eq!(response["primaryKey"], "id");
}

#[actix_rt::test]
async fn error_document_add_create_index_bad_uid() {
    let server = Server::new().await;
    let index = server.index("883  fj!");
    let (response, code) = index.add_documents(json!([{"id": 1}]), None).await;

    snapshot!(code, @"400 Bad Request");
    snapshot!(json_string!(response),
        @r###"
    {
      "message": "`883  fj!` is not a valid index uid. Index uid can be an integer or a string containing only alphanumeric characters, hyphens (-) and underscores (_), and can not be more than 512 bytes.",
      "code": "invalid_index_uid",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#invalid_index_uid"
    }
    "###);
}

#[actix_rt::test]
async fn document_addition_with_primary_key() {
    let server = Server::new().await;
    let index = server.index("test");

    let documents = json!([
        {
            "primary": 1,
            "content": "foo",
        }
    ]);
    let (response, code) = index.add_documents(documents, Some("primary")).await;
    snapshot!(code, @"202 Accepted");
    snapshot!(json_string!(response, { ".duration" => "[duration]", ".enqueuedAt" => "[date]", ".startedAt" => "[date]", ".finishedAt" => "[date]" }),
        @r###"
    {
      "taskUid": 0,
      "indexUid": "test",
      "status": "enqueued",
      "type": "documentAdditionOrUpdate",
      "enqueuedAt": "[date]"
    }
    "###);

    index.wait_task(response.uid()).await.succeeded();

    let (response, code) = index.get_task(response.uid()).await;
    snapshot!(code, @"200 OK");
    snapshot!(json_string!(response, { ".duration" => "[duration]", ".enqueuedAt" => "[date]", ".startedAt" => "[date]", ".finishedAt" => "[date]" }),
        @r###"
    {
      "uid": 0,
      "batchUid": 0,
      "indexUid": "test",
      "status": "succeeded",
      "type": "documentAdditionOrUpdate",
      "canceledBy": null,
      "details": {
        "receivedDocuments": 1,
        "indexedDocuments": 1
      },
      "error": null,
      "duration": "[duration]",
      "enqueuedAt": "[date]",
      "startedAt": "[date]",
      "finishedAt": "[date]"
    }
    "###);

    let (response, code) = index.get().await;
    snapshot!(code, @"200 OK");
    snapshot!(json_string!(response, { ".createdAt" => "[date]", ".updatedAt" => "[date]" }),
        @r###"
    {
      "uid": "test",
      "createdAt": "[date]",
      "updatedAt": "[date]",
      "primaryKey": "primary"
    }
    "###);
}

#[actix_rt::test]
async fn document_addition_with_huge_int_primary_key() {
    let server = Server::new().await;
    let index = server.index("test");

    let documents = json!([
        {
            "primary": 14630868576586246730u64,
            "content": "foo",
        }
    ]);
    let (response, code) = index.add_documents(documents, Some("primary")).await;
    snapshot!(code, @"202 Accepted");

    let response = index.wait_task(response.uid()).await;
    snapshot!(response,
        @r###"
    {
      "uid": "[uid]",
      "batchUid": "[batch_uid]",
      "indexUid": "test",
      "status": "succeeded",
      "type": "documentAdditionOrUpdate",
      "canceledBy": null,
      "details": {
        "receivedDocuments": 1,
        "indexedDocuments": 1
      },
      "error": null,
      "duration": "[duration]",
      "enqueuedAt": "[date]",
      "startedAt": "[date]",
      "finishedAt": "[date]"
    }
    "###);

    let (response, code) = index.get_document(14630868576586246730u64, None).await;
    snapshot!(code, @"200 OK");
    snapshot!(json_string!(response),
        @r###"
    {
      "primary": 14630868576586246730,
      "content": "foo"
    }
    "###);
}

#[actix_rt::test]
async fn replace_document() {
    let server = Server::new().await;
    let index = server.index("test");

    let documents = json!([
        {
            "doc_id": 1,
            "content": "foo",
        }
    ]);

    let (response, code) = index.add_documents(documents, None).await;
    snapshot!(code,@"202 Accepted");
    snapshot!(json_string!(response, { ".duration" => "[duration]", ".enqueuedAt" => "[date]", ".startedAt" => "[date]", ".finishedAt" => "[date]" }),
        @r###"
    {
      "taskUid": 0,
      "indexUid": "test",
      "status": "enqueued",
      "type": "documentAdditionOrUpdate",
      "enqueuedAt": "[date]"
    }
    "###);

    index.wait_task(response.uid()).await.succeeded();

    let documents = json!([
        {
            "doc_id": 1,
            "other": "bar",
        }
    ]);

    let (task, code) = index.add_documents(documents, None).await;
    snapshot!(code,@"202 Accepted");

    index.wait_task(task.uid()).await.succeeded();

    let (response, code) = index.get_task(task.uid()).await;
    snapshot!(code, @"200 OK");
    snapshot!(json_string!(response, { ".duration" => "[duration]", ".enqueuedAt" => "[date]", ".startedAt" => "[date]", ".finishedAt" => "[date]" }),
        @r###"
    {
      "uid": 1,
      "batchUid": 1,
      "indexUid": "test",
      "status": "succeeded",
      "type": "documentAdditionOrUpdate",
      "canceledBy": null,
      "details": {
        "receivedDocuments": 1,
        "indexedDocuments": 1
      },
      "error": null,
      "duration": "[duration]",
      "enqueuedAt": "[date]",
      "startedAt": "[date]",
      "finishedAt": "[date]"
    }
    "###);

    let (response, code) = index.get_document(1, None).await;
    snapshot!(code, @"200 OK");
    snapshot!(json_string!(response),
        @r###"
    {
      "doc_id": 1,
      "other": "bar"
    }
    "###);
}

#[actix_rt::test]
async fn add_no_documents() {
    let server = Server::new().await;
    let index = server.index("kefir");
    let (task, code) = index.add_documents(json!([]), None).await;
    snapshot!(code, @"202 Accepted");
    let task = server.wait_task(task.uid()).await;
    let task = task.succeeded();
    snapshot!(task, @r#"
    {
      "uid": "[uid]",
      "batchUid": "[batch_uid]",
      "indexUid": "kefir",
      "status": "succeeded",
      "type": "documentAdditionOrUpdate",
      "canceledBy": null,
      "details": {
        "receivedDocuments": 0,
        "indexedDocuments": 0
      },
      "error": null,
      "duration": "[duration]",
      "enqueuedAt": "[date]",
      "startedAt": "[date]",
      "finishedAt": "[date]"
    }
    "#);

    let (task, _code) = index.add_documents(json!([]), Some("kefkef")).await;
    let task = server.wait_task(task.uid()).await;
    let task = task.succeeded();
    snapshot!(task, @r#"
    {
      "uid": "[uid]",
      "batchUid": "[batch_uid]",
      "indexUid": "kefir",
      "status": "succeeded",
      "type": "documentAdditionOrUpdate",
      "canceledBy": null,
      "details": {
        "receivedDocuments": 0,
        "indexedDocuments": 0
      },
      "error": null,
      "duration": "[duration]",
      "enqueuedAt": "[date]",
      "startedAt": "[date]",
      "finishedAt": "[date]"
    }
    "#);

    let (task, _code) = index.add_documents(json!([{ "kefkef": 1 }]), None).await;
    let task = server.wait_task(task.uid()).await;
    let task = task.succeeded();
    snapshot!(task, @r#"
    {
      "uid": "[uid]",
      "batchUid": "[batch_uid]",
      "indexUid": "kefir",
      "status": "succeeded",
      "type": "documentAdditionOrUpdate",
      "canceledBy": null,
      "details": {
        "receivedDocuments": 1,
        "indexedDocuments": 1
      },
      "error": null,
      "duration": "[duration]",
      "enqueuedAt": "[date]",
      "startedAt": "[date]",
      "finishedAt": "[date]"
    }
    "#);
    let (documents, _status) = index.get_all_documents(GetAllDocumentsOptions::default()).await;
    snapshot!(documents, @r#"
    {
      "results": [
        {
          "kefkef": 1
        }
      ],
      "offset": 0,
      "limit": 20,
      "total": 1
    }
    "#);
}

#[actix_rt::test]
async fn add_larger_dataset() {
    let server = Server::new().await;
    let index = server.index("test");
    let update_id = index.load_test_set().await;
    let (response, code) = index.get_task(update_id).await;
    assert_eq!(code, 200);
    assert_eq!(response["status"], "succeeded");
    assert_eq!(response["type"], "documentAdditionOrUpdate");
    assert_eq!(response["details"]["indexedDocuments"], 77);
    assert_eq!(response["details"]["receivedDocuments"], 77);
    let (response, code) = index
        .get_all_documents(GetAllDocumentsOptions { limit: Some(1000), ..Default::default() })
        .await;
    assert_eq!(code, 200, "failed with `{}`", response);
    assert_eq!(response["results"].as_array().unwrap().len(), 77);

    // x-ndjson add large test
    let server = Server::new().await;
    let index = server.index("test");
    let update_id = index.load_test_set_ndjson().await;
    let (response, code) = index.get_task(update_id).await;
    assert_eq!(code, 200);
    assert_eq!(response["status"], "succeeded");
    assert_eq!(response["type"], "documentAdditionOrUpdate");
    assert_eq!(response["details"]["indexedDocuments"], 77);
    assert_eq!(response["details"]["receivedDocuments"], 77);
    let (response, code) = index
        .get_all_documents(GetAllDocumentsOptions { limit: Some(1000), ..Default::default() })
        .await;
    assert_eq!(code, 200, "failed with `{}`", response);
    assert_eq!(response["results"].as_array().unwrap().len(), 77);
}

#[actix_rt::test]
async fn error_add_documents_bad_document_id() {
    let server = Server::new().await;
    let index = server.index("test");
    index.create(Some("docid")).await;

    // unsupported characters

    let documents = json!([
        {
            "docid": "foo & bar",
            "content": "foobar"
        }
    ]);
    let (task, _status_code) = index.add_documents(documents, None).await;
    index.wait_task(task.uid()).await.failed();
    let (response, code) = index.get_task(task.uid()).await;
    snapshot!(code, @"200 OK");
    snapshot!(json_string!(response, { ".duration" => "[duration]", ".enqueuedAt" => "[date]", ".startedAt" => "[date]", ".finishedAt" => "[date]" }),
        @r###"
    {
      "uid": 1,
      "batchUid": 1,
      "indexUid": "test",
      "status": "failed",
      "type": "documentAdditionOrUpdate",
      "canceledBy": null,
      "details": {
        "receivedDocuments": 1,
        "indexedDocuments": 0
      },
      "error": {
        "message": "Document identifier `\"foo & bar\"` is invalid. A document identifier can be of type integer or string, only composed of alphanumeric characters (a-z A-Z 0-9), hyphens (-) and underscores (_), and can not be more than 511 bytes.",
        "code": "invalid_document_id",
        "type": "invalid_request",
        "link": "https://docs.meilisearch.com/errors#invalid_document_id"
      },
      "duration": "[duration]",
      "enqueuedAt": "[date]",
      "startedAt": "[date]",
      "finishedAt": "[date]"
    }
    "###);

    // More than 512 bytes
    let documents = json!([
        {
            "docid": "a".repeat(600),
            "content": "foobar"
        }
    ]);
    let (value, _code) = index.add_documents(documents, None).await;
    index.wait_task(value.uid()).await.failed();
    let (response, code) = index.get_task(value.uid()).await;
    snapshot!(code, @"200 OK");
    snapshot!(json_string!(response, { ".duration" => "[duration]", ".enqueuedAt" => "[date]", ".startedAt" => "[date]", ".finishedAt" => "[date]" }),
      @r###"
    {
      "uid": 2,
      "batchUid": 2,
      "indexUid": "test",
      "status": "failed",
      "type": "documentAdditionOrUpdate",
      "canceledBy": null,
      "details": {
        "receivedDocuments": 1,
        "indexedDocuments": 0
      },
      "error": {
        "message": "Document identifier `\"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\"` is invalid. A document identifier can be of type integer or string, only composed of alphanumeric characters (a-z A-Z 0-9), hyphens (-) and underscores (_), and can not be more than 511 bytes.",
        "code": "invalid_document_id",
        "type": "invalid_request",
        "link": "https://docs.meilisearch.com/errors#invalid_document_id"
      },
      "duration": "[duration]",
      "enqueuedAt": "[date]",
      "startedAt": "[date]",
      "finishedAt": "[date]"
    }
    "###);

    // Exactly 512 bytes
    let documents = json!([
        {
            "docid": "a".repeat(512),
            "content": "foobar"
        }
    ]);
    let (value, _code) = index.add_documents(documents, None).await;
    index.wait_task(value.uid()).await.failed();
    let (response, code) = index.get_task(value.uid()).await;
    snapshot!(code, @"200 OK");
    snapshot!(json_string!(response, { ".duration" => "[duration]", ".enqueuedAt" => "[date]", ".startedAt" => "[date]", ".finishedAt" => "[date]" }),
    @r###"
    {
      "uid": 3,
      "batchUid": 3,
      "indexUid": "test",
      "status": "failed",
      "type": "documentAdditionOrUpdate",
      "canceledBy": null,
      "details": {
        "receivedDocuments": 1,
        "indexedDocuments": 0
      },
      "error": {
        "message": "Document identifier `\"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\"` is invalid. A document identifier can be of type integer or string, only composed of alphanumeric characters (a-z A-Z 0-9), hyphens (-) and underscores (_), and can not be more than 511 bytes.",
        "code": "invalid_document_id",
        "type": "invalid_request",
        "link": "https://docs.meilisearch.com/errors#invalid_document_id"
      },
      "duration": "[duration]",
      "enqueuedAt": "[date]",
      "startedAt": "[date]",
      "finishedAt": "[date]"
    }
    "###);
}

#[actix_rt::test]
async fn error_add_documents_missing_document_id() {
    let server = Server::new().await;
    let index = server.index("test");
    index.create(Some("docid")).await;
    let documents = json!([
        {
            "id": "11",
            "content": "foobar"
        }
    ]);
    let (task, _status_code) = index.add_documents(documents, None).await;
    index.wait_task(task.uid()).await.failed();
    let (response, code) = index.get_task(task.uid()).await;
    snapshot!(code, @"200 OK");
    snapshot!(json_string!(response, { ".duration" => "[duration]", ".enqueuedAt" => "[date]", ".startedAt" => "[date]", ".finishedAt" => "[date]" }),
        @r###"
    {
      "uid": 1,
      "batchUid": 1,
      "indexUid": "test",
      "status": "failed",
      "type": "documentAdditionOrUpdate",
      "canceledBy": null,
      "details": {
        "receivedDocuments": 1,
        "indexedDocuments": 0
      },
      "error": {
        "message": "Document doesn't have a `docid` attribute: `{\"id\":\"11\",\"content\":\"foobar\"}`.",
        "code": "missing_document_id",
        "type": "invalid_request",
        "link": "https://docs.meilisearch.com/errors#missing_document_id"
      },
      "duration": "[duration]",
      "enqueuedAt": "[date]",
      "startedAt": "[date]",
      "finishedAt": "[date]"
    }
    "###);
}

#[actix_rt::test]
async fn error_document_field_limit_reached_in_one_document() {
    let server = Server::new().await;
    let index = server.index("test");

    index.create(Some("id")).await;

    let mut big_object = std::collections::HashMap::new();
    big_object.insert("id".to_owned(), "wow");
    for i in 0..(u16::MAX as usize + 1) {
        let key = i.to_string();
        big_object.insert(key, "I am a text!");
    }

    let documents = json!([big_object]);

    let (response, code) = index.update_documents(documents, Some("id")).await;
    snapshot!(code, @"202 Accepted");

    let response = index.wait_task(response.uid()).await.failed();
    snapshot!(code, @"202 Accepted");
    // Documents without a primary key are not accepted.
    snapshot!(response,
        @r###"
    {
      "uid": "[uid]",
      "batchUid": "[batch_uid]",
      "indexUid": "test",
      "status": "failed",
      "type": "documentAdditionOrUpdate",
      "canceledBy": null,
      "details": {
        "receivedDocuments": 1,
        "indexedDocuments": 0
      },
      "error": {
        "message": "A document cannot contain more than 65,535 fields.",
        "code": "max_fields_limit_exceeded",
        "type": "invalid_request",
        "link": "https://docs.meilisearch.com/errors#max_fields_limit_exceeded"
      },
      "duration": "[duration]",
      "enqueuedAt": "[date]",
      "startedAt": "[date]",
      "finishedAt": "[date]"
    }
    "###);
}

#[actix_rt::test]
async fn error_document_field_limit_reached_over_multiple_documents() {
    let server = Server::new().await;
    let index = server.index("test");

    index.create(Some("id")).await;

    let mut big_object = std::collections::HashMap::new();
    big_object.insert("id".to_owned(), "wow");
    for i in 0..(u16::MAX / 2) {
        let key = i.to_string();
        big_object.insert(key, "I am a text!");
    }

    let documents = json!([big_object]);

    let (response, code) = index.update_documents(documents, Some("id")).await;
    snapshot!(code, @"202 Accepted");

    let response = index.wait_task(response.uid()).await;
    snapshot!(code, @"202 Accepted");
    snapshot!(response,
        @r###"
    {
      "uid": "[uid]",
      "batchUid": "[batch_uid]",
      "indexUid": "test",
      "status": "succeeded",
      "type": "documentAdditionOrUpdate",
      "canceledBy": null,
      "details": {
        "receivedDocuments": 1,
        "indexedDocuments": 1
      },
      "error": null,
      "duration": "[duration]",
      "enqueuedAt": "[date]",
      "startedAt": "[date]",
      "finishedAt": "[date]"
    }
    "###);

    let mut big_object = std::collections::HashMap::new();
    big_object.insert("id".to_owned(), "waw");
    for i in (u16::MAX as usize / 2)..(u16::MAX as usize + 1) {
        let key = i.to_string();
        big_object.insert(key, "I am a text!");
    }

    let documents = json!([big_object]);

    let (response, code) = index.update_documents(documents, Some("id")).await;
    snapshot!(code, @"202 Accepted");

    let response = index.wait_task(response.uid()).await;
    snapshot!(code, @"202 Accepted");
    snapshot!(response,
        @r###"
    {
      "uid": "[uid]",
      "batchUid": "[batch_uid]",
      "indexUid": "test",
      "status": "failed",
      "type": "documentAdditionOrUpdate",
      "canceledBy": null,
      "details": {
        "receivedDocuments": 1,
        "indexedDocuments": 0
      },
      "error": {
        "message": "A document cannot contain more than 65,535 fields.",
        "code": "max_fields_limit_exceeded",
        "type": "invalid_request",
        "link": "https://docs.meilisearch.com/errors#max_fields_limit_exceeded"
      },
      "duration": "[duration]",
      "enqueuedAt": "[date]",
      "startedAt": "[date]",
      "finishedAt": "[date]"
    }
    "###);
}

#[actix_rt::test]
async fn error_document_field_limit_reached_in_one_nested_document() {
    let server = Server::new().await;
    let index = server.index("test");

    index.create(Some("id")).await;

    let mut nested = std::collections::HashMap::new();
    for i in 0..(u16::MAX as usize + 1) {
        let key = i.to_string();
        nested.insert(key, "I am a text!");
    }
    let mut big_object = std::collections::HashMap::new();
    big_object.insert("id".to_owned(), "wow");

    let documents = json!([big_object]);

    let (response, code) = index.update_documents(documents, Some("id")).await;
    snapshot!(code, @"202 Accepted");

    let response = index.wait_task(response.uid()).await;
    snapshot!(code, @"202 Accepted");
    // Documents without a primary key are not accepted.
    snapshot!(response,
        @r###"
    {
      "uid": "[uid]",
      "batchUid": "[batch_uid]",
      "indexUid": "test",
      "status": "succeeded",
      "type": "documentAdditionOrUpdate",
      "canceledBy": null,
      "details": {
        "receivedDocuments": 1,
        "indexedDocuments": 1
      },
      "error": null,
      "duration": "[duration]",
      "enqueuedAt": "[date]",
      "startedAt": "[date]",
      "finishedAt": "[date]"
    }
    "###);
}

#[actix_rt::test]
async fn error_document_field_limit_reached_over_multiple_documents_with_nested_fields() {
    let server = Server::new().await;
    let index = server.index("test");

    index.create(Some("id")).await;

    let mut nested = std::collections::HashMap::new();
    for i in 0..(u16::MAX / 2) {
        let key = i.to_string();
        nested.insert(key, "I am a text!");
    }
    let mut big_object = std::collections::HashMap::new();
    big_object.insert("id".to_owned(), "wow");

    let documents = json!([big_object]);

    let (response, code) = index.update_documents(documents, Some("id")).await;
    snapshot!(code, @"202 Accepted");

    let response = index.wait_task(response.uid()).await;
    snapshot!(code, @"202 Accepted");
    snapshot!(response,
        @r###"
    {
      "uid": "[uid]",
      "batchUid": "[batch_uid]",
      "indexUid": "test",
      "status": "succeeded",
      "type": "documentAdditionOrUpdate",
      "canceledBy": null,
      "details": {
        "receivedDocuments": 1,
        "indexedDocuments": 1
      },
      "error": null,
      "duration": "[duration]",
      "enqueuedAt": "[date]",
      "startedAt": "[date]",
      "finishedAt": "[date]"
    }
    "###);

    let mut nested = std::collections::HashMap::new();
    for i in 0..(u16::MAX / 2) {
        let key = i.to_string();
        nested.insert(key, "I am a text!");
    }
    let mut big_object = std::collections::HashMap::new();
    big_object.insert("id".to_owned(), "wow");

    let documents = json!([big_object]);

    let (response, code) = index.update_documents(documents, Some("id")).await;
    snapshot!(code, @"202 Accepted");

    let response = index.wait_task(response.uid()).await;
    snapshot!(code, @"202 Accepted");
    snapshot!(response,
        @r###"
    {
      "uid": "[uid]",
      "batchUid": "[batch_uid]",
      "indexUid": "test",
      "status": "succeeded",
      "type": "documentAdditionOrUpdate",
      "canceledBy": null,
      "details": {
        "receivedDocuments": 1,
        "indexedDocuments": 1
      },
      "error": null,
      "duration": "[duration]",
      "enqueuedAt": "[date]",
      "startedAt": "[date]",
      "finishedAt": "[date]"
    }
    "###);
}

#[actix_rt::test]
async fn add_documents_with_geo_field() {
    let server = Server::new().await;
    let index = server.index("doggo");
    index.update_settings(json!({"sortableAttributes": ["_geo"]})).await;

    let documents = json!([
        {
            "id": "1",
        },
        {
            "id": "2",
            "_geo": null,
        },
        {
            "id": "3",
            "_geo": { "lat": 1, "lng": 1 },
        },
        {
            "id": "4",
            "_geo": { "lat": "1", "lng": "1" },
        },
    ]);

    let (task, _status_code) = index.add_documents(documents, None).await;
    let response = index.wait_task(task.uid()).await;
    snapshot!(json_string!(response, { ".duration" => "[duration]", ".enqueuedAt" => "[date]", ".startedAt" => "[date]", ".finishedAt" => "[date]" }),
        @r###"
    {
      "uid": 1,
      "batchUid": 1,
      "indexUid": "doggo",
      "status": "succeeded",
      "type": "documentAdditionOrUpdate",
      "canceledBy": null,
      "details": {
        "receivedDocuments": 4,
        "indexedDocuments": 4
      },
      "error": null,
      "duration": "[duration]",
      "enqueuedAt": "[date]",
      "startedAt": "[date]",
      "finishedAt": "[date]"
    }
    "###);

    let (response, code) = index.get_all_documents(GetAllDocumentsOptions::default()).await;

    snapshot!(code, @"200 OK");
    snapshot!(json_string!(response, { ".duration" => "[duration]", ".enqueuedAt" => "[date]", ".startedAt" => "[date]", ".finishedAt" => "[date]" }),
    @r###"
    {
      "results": [
        {
          "id": "1"
        },
        {
          "id": "2",
          "_geo": null
        },
        {
          "id": "3",
          "_geo": {
            "lat": 1,
            "lng": 1
          }
        },
        {
          "id": "4",
          "_geo": {
            "lat": "1",
            "lng": "1"
          }
        }
      ],
      "offset": 0,
      "limit": 20,
      "total": 4
    }
    "###);

    let (response, code) = index
        .search_post(json!({"sort": ["_geoPoint(50.629973371633746,3.0569447399419567):desc"]}))
        .await;
    snapshot!(code, @"200 OK");
    // we are expecting docs 4 and 3 first as they have geo
    snapshot!(json_string!(response, { ".processingTimeMs" => "[time]" }),
    @r###"
    {
      "hits": [
        {
          "id": "4",
          "_geo": {
            "lat": "1",
            "lng": "1"
          },
          "_geoDistance": 5522018
        },
        {
          "id": "3",
          "_geo": {
            "lat": 1,
            "lng": 1
          },
          "_geoDistance": 5522018
        },
        {
          "id": "1"
        },
        {
          "id": "2",
          "_geo": null
        }
      ],
      "query": "",
      "processingTimeMs": "[time]",
      "limit": 20,
      "offset": 0,
      "estimatedTotalHits": 4
    }
    "###);
}

#[actix_rt::test]
async fn update_documents_with_geo_field() {
    let server = Server::new().await;
    let index = server.index("doggo");
    index.update_settings(json!({"sortableAttributes": ["_geo"]})).await;

    let documents = json!([
        {
            "id": "1",
        },
        {
            "id": "2",
            "_geo": null,
        },
        {
            "id": "3",
            "_geo": { "lat": 1, "lng": 1 },
        },
        {
            "id": "4",
            "_geo": { "lat": "1", "lng": "1" },
        },
    ]);

    let (task, _status_code) = index.add_documents(documents, None).await;
    let response = index.wait_task(task.uid()).await;
    snapshot!(json_string!(response, { ".duration" => "[duration]", ".enqueuedAt" => "[date]", ".startedAt" => "[date]", ".finishedAt" => "[date]" }),
        @r###"
    {
      "uid": 1,
      "batchUid": 1,
      "indexUid": "doggo",
      "status": "succeeded",
      "type": "documentAdditionOrUpdate",
      "canceledBy": null,
      "details": {
        "receivedDocuments": 4,
        "indexedDocuments": 4
      },
      "error": null,
      "duration": "[duration]",
      "enqueuedAt": "[date]",
      "startedAt": "[date]",
      "finishedAt": "[date]"
    }
    "###);

    let (response, code) = index
        .search_post(json!({"sort": ["_geoPoint(50.629973371633746,3.0569447399419567):desc"]}))
        .await;
    snapshot!(code, @"200 OK");
    // we are expecting docs 4 and 3 first as they have geo
    snapshot!(json_string!(response, { ".processingTimeMs" => "[time]" }),
    @r###"
    {
      "hits": [
        {
          "id": "4",
          "_geo": {
            "lat": "1",
            "lng": "1"
          },
          "_geoDistance": 5522018
        },
        {
          "id": "3",
          "_geo": {
            "lat": 1,
            "lng": 1
          },
          "_geoDistance": 5522018
        },
        {
          "id": "1"
        },
        {
          "id": "2",
          "_geo": null
        }
      ],
      "query": "",
      "processingTimeMs": "[time]",
      "limit": 20,
      "offset": 0,
      "estimatedTotalHits": 4
    }
    "###);

    let updated_documents = json!([{
      "id": "3",
      "doggo": "kefir",
    }]);
    let (task, _status_code) = index.update_documents(updated_documents, None).await;
    let response = index.wait_task(task.uid()).await;
    snapshot!(json_string!(response, { ".duration" => "[duration]", ".enqueuedAt" => "[date]", ".startedAt" => "[date]", ".finishedAt" => "[date]" }),
        @r###"
    {
      "uid": 2,
      "batchUid": 2,
      "indexUid": "doggo",
      "status": "succeeded",
      "type": "documentAdditionOrUpdate",
      "canceledBy": null,
      "details": {
        "receivedDocuments": 1,
        "indexedDocuments": 1
      },
      "error": null,
      "duration": "[duration]",
      "enqueuedAt": "[date]",
      "startedAt": "[date]",
      "finishedAt": "[date]"
    }
    "###);
    let (response, code) = index.get_all_documents(GetAllDocumentsOptions::default()).await;

    snapshot!(code, @"200 OK");
    snapshot!(json_string!(response, { ".duration" => "[duration]", ".enqueuedAt" => "[date]", ".startedAt" => "[date]", ".finishedAt" => "[date]" }),
    @r###"
    {
      "results": [
        {
          "id": "1"
        },
        {
          "id": "2",
          "_geo": null
        },
        {
          "id": "3",
          "_geo": {
            "lat": 1,
            "lng": 1
          },
          "doggo": "kefir"
        },
        {
          "id": "4",
          "_geo": {
            "lat": "1",
            "lng": "1"
          }
        }
      ],
      "offset": 0,
      "limit": 20,
      "total": 4
    }
    "###);

    let (response, code) = index
        .search_post(json!({"sort": ["_geoPoint(50.629973371633746,3.0569447399419567):desc"]}))
        .await;
    snapshot!(code, @"200 OK");
    // the search response should not have changed: we are expecting docs 4 and 3 first as they have geo
    snapshot!(json_string!(response, { ".processingTimeMs" => "[time]" }),
    @r###"
    {
      "hits": [
        {
          "id": "4",
          "_geo": {
            "lat": "1",
            "lng": "1"
          },
          "_geoDistance": 5522018
        },
        {
          "id": "3",
          "_geo": {
            "lat": 1,
            "lng": 1
          },
          "doggo": "kefir",
          "_geoDistance": 5522018
        },
        {
          "id": "1"
        },
        {
          "id": "2",
          "_geo": null
        }
      ],
      "query": "",
      "processingTimeMs": "[time]",
      "limit": 20,
      "offset": 0,
      "estimatedTotalHits": 4
    }
    "###);
}

#[actix_rt::test]
async fn add_documents_invalid_geo_field() {
    let server = Server::new().await;
    let index = server.index("test");
    index.create(Some("id")).await;
    index.update_settings(json!({"sortableAttributes": ["_geo"]})).await;

    // _geo is not an object
    let documents = json!([
        {
            "id": "11",
            "_geo": "foobar"
        }
    ]);

    let (task, _status_code) = index.add_documents(documents, None).await;
    index.wait_task(task.uid()).await.failed();
    let (response, code) = index.get_task(task.uid()).await;
    snapshot!(code, @"200 OK");
    snapshot!(json_string!(response, { ".duration" => "[duration]", ".enqueuedAt" => "[date]", ".startedAt" => "[date]", ".finishedAt" => "[date]" }),
        @r###"
    {
      "uid": 2,
      "batchUid": 2,
      "indexUid": "test",
      "status": "failed",
      "type": "documentAdditionOrUpdate",
      "canceledBy": null,
      "details": {
        "receivedDocuments": 1,
        "indexedDocuments": 0
      },
      "error": {
        "message": "Index `test`: The `_geo` field in the document with the id: `\"11\"` is not an object. Was expecting an object with the `_geo.lat` and `_geo.lng` fields but instead got `\"foobar\"`.",
        "code": "invalid_document_geo_field",
        "type": "invalid_request",
        "link": "https://docs.meilisearch.com/errors#invalid_document_geo_field"
      },
      "duration": "[duration]",
      "enqueuedAt": "[date]",
      "startedAt": "[date]",
      "finishedAt": "[date]"
    }
    "###);

    // _geo is an object but is missing both the lat and lng
    let documents = json!([
        {
            "id": "11",
            "_geo": {}
        }
    ]);

    let (task, _status_code) = index.add_documents(documents, None).await;
    index.wait_task(task.uid()).await.failed();
    let (response, code) = index.get_task(task.uid()).await;
    snapshot!(code, @"200 OK");
    snapshot!(json_string!(response, { ".duration" => "[duration]", ".enqueuedAt" => "[date]", ".startedAt" => "[date]", ".finishedAt" => "[date]" }),
        @r###"
    {
      "uid": 3,
      "batchUid": 3,
      "indexUid": "test",
      "status": "failed",
      "type": "documentAdditionOrUpdate",
      "canceledBy": null,
      "details": {
        "receivedDocuments": 1,
        "indexedDocuments": 0
      },
      "error": {
        "message": "Index `test`: Could not find latitude nor longitude in the document with the id: `\"11\"`. Was expecting `_geo.lat` and `_geo.lng` fields.",
        "code": "invalid_document_geo_field",
        "type": "invalid_request",
        "link": "https://docs.meilisearch.com/errors#invalid_document_geo_field"
      },
      "duration": "[duration]",
      "enqueuedAt": "[date]",
      "startedAt": "[date]",
      "finishedAt": "[date]"
    }
    "###);

    // _geo is an object but is missing both the lat and lng and contains an unexpected field
    let documents = json!([
        {
            "id": "11",
            "_geo": { "doggos": "are good" }
        }
    ]);

    let (task, _status_code) = index.add_documents(documents, None).await;
    index.wait_task(task.uid()).await.failed();
    let (response, code) = index.get_task(task.uid()).await;
    snapshot!(code, @"200 OK");
    snapshot!(json_string!(response, { ".duration" => "[duration]", ".enqueuedAt" => "[date]", ".startedAt" => "[date]", ".finishedAt" => "[date]" }),
        @r###"
    {
      "uid": 4,
      "batchUid": 4,
      "indexUid": "test",
      "status": "failed",
      "type": "documentAdditionOrUpdate",
      "canceledBy": null,
      "details": {
        "receivedDocuments": 1,
        "indexedDocuments": 0
      },
      "error": {
        "message": "Index `test`: Could not find latitude nor longitude in the document with the id: `\"11\"`. Was expecting `_geo.lat` and `_geo.lng` fields.",
        "code": "invalid_document_geo_field",
        "type": "invalid_request",
        "link": "https://docs.meilisearch.com/errors#invalid_document_geo_field"
      },
      "duration": "[duration]",
      "enqueuedAt": "[date]",
      "startedAt": "[date]",
      "finishedAt": "[date]"
    }
    "###);

    // _geo is an object but only contains the lat
    let documents = json!([
        {
            "id": "11",
            "_geo": { "lat": 12 }
        }
    ]);

    let (task, _status_code) = index.add_documents(documents, None).await;
    index.wait_task(task.uid()).await.failed();
    let (response, code) = index.get_task(task.uid()).await;
    snapshot!(code, @"200 OK");
    snapshot!(json_string!(response, { ".duration" => "[duration]", ".enqueuedAt" => "[date]", ".startedAt" => "[date]", ".finishedAt" => "[date]" }),
        @r###"
    {
      "uid": 5,
      "batchUid": 5,
      "indexUid": "test",
      "status": "failed",
      "type": "documentAdditionOrUpdate",
      "canceledBy": null,
      "details": {
        "receivedDocuments": 1,
        "indexedDocuments": 0
      },
      "error": {
        "message": "Index `test`: Could not find longitude in the document with the id: `\"11\"`. Was expecting a `_geo.lng` field.",
        "code": "invalid_document_geo_field",
        "type": "invalid_request",
        "link": "https://docs.meilisearch.com/errors#invalid_document_geo_field"
      },
      "duration": "[duration]",
      "enqueuedAt": "[date]",
      "startedAt": "[date]",
      "finishedAt": "[date]"
    }
    "###);

    // _geo is an object but only contains the lng
    let documents = json!([
        {
            "id": "11",
            "_geo": { "lng": 12 }
        }
    ]);

    let (task, _status_code) = index.add_documents(documents, None).await;
    index.wait_task(task.uid()).await.failed();
    let (response, code) = index.get_task(task.uid()).await;
    snapshot!(code, @"200 OK");
    snapshot!(json_string!(response, { ".duration" => "[duration]", ".enqueuedAt" => "[date]", ".startedAt" => "[date]", ".finishedAt" => "[date]" }),
        @r###"
    {
      "uid": 6,
      "batchUid": 6,
      "indexUid": "test",
      "status": "failed",
      "type": "documentAdditionOrUpdate",
      "canceledBy": null,
      "details": {
        "receivedDocuments": 1,
        "indexedDocuments": 0
      },
      "error": {
        "message": "Index `test`: Could not find latitude in the document with the id: `\"11\"`. Was expecting a `_geo.lat` field.",
        "code": "invalid_document_geo_field",
        "type": "invalid_request",
        "link": "https://docs.meilisearch.com/errors#invalid_document_geo_field"
      },
      "duration": "[duration]",
      "enqueuedAt": "[date]",
      "startedAt": "[date]",
      "finishedAt": "[date]"
    }
    "###);

    // _geo is an object but the lat has a wrong type
    let documents = json!([
        {
            "id": "11",
            "_geo": { "lat": true }
        }
    ]);

    let (task, _status_code) = index.add_documents(documents, None).await;
    index.wait_task(task.uid()).await.failed();
    let (response, code) = index.get_task(task.uid()).await;
    snapshot!(code, @"200 OK");
    snapshot!(json_string!(response, { ".duration" => "[duration]", ".enqueuedAt" => "[date]", ".startedAt" => "[date]", ".finishedAt" => "[date]" }),
        @r###"
    {
      "uid": 7,
      "batchUid": 7,
      "indexUid": "test",
      "status": "failed",
      "type": "documentAdditionOrUpdate",
      "canceledBy": null,
      "details": {
        "receivedDocuments": 1,
        "indexedDocuments": 0
      },
      "error": {
        "message": "Index `test`: Could not find longitude in the document with the id: `\"11\"`. Was expecting a `_geo.lng` field.",
        "code": "invalid_document_geo_field",
        "type": "invalid_request",
        "link": "https://docs.meilisearch.com/errors#invalid_document_geo_field"
      },
      "duration": "[duration]",
      "enqueuedAt": "[date]",
      "startedAt": "[date]",
      "finishedAt": "[date]"
    }
    "###);

    // _geo is an object but the lng has a wrong type
    let documents = json!([
        {
            "id": "11",
            "_geo": { "lng": true }
        }
    ]);

    let (task, _status_code) = index.add_documents(documents, None).await;
    index.wait_task(task.uid()).await.failed();
    let (response, code) = index.get_task(task.uid()).await;
    snapshot!(code, @"200 OK");
    snapshot!(json_string!(response, { ".duration" => "[duration]", ".enqueuedAt" => "[date]", ".startedAt" => "[date]", ".finishedAt" => "[date]" }),
        @r###"
    {
      "uid": 8,
      "batchUid": 8,
      "indexUid": "test",
      "status": "failed",
      "type": "documentAdditionOrUpdate",
      "canceledBy": null,
      "details": {
        "receivedDocuments": 1,
        "indexedDocuments": 0
      },
      "error": {
        "message": "Index `test`: Could not find latitude in the document with the id: `\"11\"`. Was expecting a `_geo.lat` field.",
        "code": "invalid_document_geo_field",
        "type": "invalid_request",
        "link": "https://docs.meilisearch.com/errors#invalid_document_geo_field"
      },
      "duration": "[duration]",
      "enqueuedAt": "[date]",
      "startedAt": "[date]",
      "finishedAt": "[date]"
    }
    "###);

    // _geo is an object but the lat and lng have a wrong type
    let documents = json!([
        {
            "id": "11",
            "_geo": { "lat": false, "lng": true }
        }
    ]);

    let (task, _status_code) = index.add_documents(documents, None).await;
    index.wait_task(task.uid()).await.failed();
    let (response, code) = index.get_task(task.uid()).await;
    snapshot!(code, @"200 OK");
    snapshot!(json_string!(response, { ".duration" => "[duration]", ".enqueuedAt" => "[date]", ".startedAt" => "[date]", ".finishedAt" => "[date]" }),
        @r###"
    {
      "uid": 9,
      "batchUid": 9,
      "indexUid": "test",
      "status": "failed",
      "type": "documentAdditionOrUpdate",
      "canceledBy": null,
      "details": {
        "receivedDocuments": 1,
        "indexedDocuments": 0
      },
      "error": {
        "message": "Index `test`: Could not parse latitude nor longitude in the document with the id: `\"11\"`. Was expecting finite numbers but instead got `false` and `true`.",
        "code": "invalid_document_geo_field",
        "type": "invalid_request",
        "link": "https://docs.meilisearch.com/errors#invalid_document_geo_field"
      },
      "duration": "[duration]",
      "enqueuedAt": "[date]",
      "startedAt": "[date]",
      "finishedAt": "[date]"
    }
    "###);

    // _geo is an object but the lat can't be parsed as a float
    let documents = json!([
        {
            "id": "11",
            "_geo": { "lat": "doggo" }
        }
    ]);

    let (task, _status_code) = index.add_documents(documents, None).await;
    index.wait_task(task.uid()).await.failed();
    let (response, code) = index.get_task(task.uid()).await;
    snapshot!(code, @"200 OK");
    snapshot!(json_string!(response, { ".duration" => "[duration]", ".enqueuedAt" => "[date]", ".startedAt" => "[date]", ".finishedAt" => "[date]" }),
        @r###"
    {
      "uid": 10,
      "batchUid": 10,
      "indexUid": "test",
      "status": "failed",
      "type": "documentAdditionOrUpdate",
      "canceledBy": null,
      "details": {
        "receivedDocuments": 1,
        "indexedDocuments": 0
      },
      "error": {
        "message": "Index `test`: Could not find longitude in the document with the id: `\"11\"`. Was expecting a `_geo.lng` field.",
        "code": "invalid_document_geo_field",
        "type": "invalid_request",
        "link": "https://docs.meilisearch.com/errors#invalid_document_geo_field"
      },
      "duration": "[duration]",
      "enqueuedAt": "[date]",
      "startedAt": "[date]",
      "finishedAt": "[date]"
    }
    "###);

    // _geo is an object but the lng can't be parsed as a float
    let documents = json!([
        {
            "id": "11",
            "_geo": { "lng": "doggo" }
        }
    ]);

    let (task, _status_code) = index.add_documents(documents, None).await;
    index.wait_task(task.uid()).await.failed();
    let (response, code) = index.get_task(task.uid()).await;
    snapshot!(code, @"200 OK");
    snapshot!(json_string!(response, { ".duration" => "[duration]", ".enqueuedAt" => "[date]", ".startedAt" => "[date]", ".finishedAt" => "[date]" }),
        @r###"
    {
      "uid": 11,
      "batchUid": 11,
      "indexUid": "test",
      "status": "failed",
      "type": "documentAdditionOrUpdate",
      "canceledBy": null,
      "details": {
        "receivedDocuments": 1,
        "indexedDocuments": 0
      },
      "error": {
        "message": "Index `test`: Could not find latitude in the document with the id: `\"11\"`. Was expecting a `_geo.lat` field.",
        "code": "invalid_document_geo_field",
        "type": "invalid_request",
        "link": "https://docs.meilisearch.com/errors#invalid_document_geo_field"
      },
      "duration": "[duration]",
      "enqueuedAt": "[date]",
      "startedAt": "[date]",
      "finishedAt": "[date]"
    }
    "###);

    // _geo is an object but the lat and lng can't be parsed as a float
    let documents = json!([
        {
            "id": "11",
            "_geo": { "lat": "doggo", "lng": "doggo" }
        }
    ]);

    let (task, _status_code) = index.add_documents(documents, None).await;
    index.wait_task(task.uid()).await.failed();
    let (response, code) = index.get_task(task.uid()).await;
    snapshot!(code, @"200 OK");
    snapshot!(json_string!(response, { ".duration" => "[duration]", ".enqueuedAt" => "[date]", ".startedAt" => "[date]", ".finishedAt" => "[date]" }),
        @r###"
    {
      "uid": 12,
      "batchUid": 12,
      "indexUid": "test",
      "status": "failed",
      "type": "documentAdditionOrUpdate",
      "canceledBy": null,
      "details": {
        "receivedDocuments": 1,
        "indexedDocuments": 0
      },
      "error": {
        "message": "Index `test`: Could not parse latitude nor longitude in the document with the id: `\"11\"`. Was expecting finite numbers but instead got `\"doggo\"` and `\"doggo\"`.",
        "code": "invalid_document_geo_field",
        "type": "invalid_request",
        "link": "https://docs.meilisearch.com/errors#invalid_document_geo_field"
      },
      "duration": "[duration]",
      "enqueuedAt": "[date]",
      "startedAt": "[date]",
      "finishedAt": "[date]"
    }
    "###);

    // _geo is a valid object but contains one extra unknown field
    let documents = json!([
        {
            "id": "11",
            "_geo": { "lat": 1, "lng": 2, "doggo": "are the best" }
        }
    ]);

    let (task, _status_code) = index.add_documents(documents, None).await;
    index.wait_task(task.uid()).await.failed();
    let (response, code) = index.get_task(task.uid()).await;
    snapshot!(code, @"200 OK");
    snapshot!(json_string!(response, { ".duration" => "[duration]", ".enqueuedAt" => "[date]", ".startedAt" => "[date]", ".finishedAt" => "[date]" }),
        @r###"
    {
      "uid": 13,
      "batchUid": 13,
      "indexUid": "test",
      "status": "failed",
      "type": "documentAdditionOrUpdate",
      "canceledBy": null,
      "details": {
        "receivedDocuments": 1,
        "indexedDocuments": 0
      },
      "error": {
        "message": "Index `test`: The `_geo` field in the document with the id: `\"11\"` contains the following unexpected fields: `{\"doggo\":\"are the best\"}`.",
        "code": "invalid_document_geo_field",
        "type": "invalid_request",
        "link": "https://docs.meilisearch.com/errors#invalid_document_geo_field"
      },
      "duration": "[duration]",
      "enqueuedAt": "[date]",
      "startedAt": "[date]",
      "finishedAt": "[date]"
    }
    "###);

    // The three next tests are related to #4333

    // _geo has a lat and lng but set to `null`
    let documents = json!([
        {
            "id": "12",
            "_geo": { "lng": null, "lat": 67}
        }
    ]);

    let (response, code) = index.add_documents(documents, None).await;
    snapshot!(code, @"202 Accepted");
    let response = index.wait_task(response.uid()).await.failed();
    snapshot!(json_string!(response, { ".duration" => "[duration]", ".enqueuedAt" => "[date]", ".startedAt" => "[date]", ".finishedAt" => "[date]" }),
        @r###"
    {
      "uid": 14,
      "batchUid": 14,
      "indexUid": "test",
      "status": "failed",
      "type": "documentAdditionOrUpdate",
      "canceledBy": null,
      "details": {
        "receivedDocuments": 1,
        "indexedDocuments": 0
      },
      "error": {
        "message": "Index `test`: Could not parse longitude in the document with the id: `\"12\"`. Was expecting a finite number but instead got `null`.",
        "code": "invalid_document_geo_field",
        "type": "invalid_request",
        "link": "https://docs.meilisearch.com/errors#invalid_document_geo_field"
      },
      "duration": "[duration]",
      "enqueuedAt": "[date]",
      "startedAt": "[date]",
      "finishedAt": "[date]"
    }
    "###);

    // _geo has a lat and lng but set to `null`
    let documents = json!([
        {
            "id": "12",
            "_geo": { "lng": 35, "lat": null }
        }
    ]);

    let (response, code) = index.add_documents(documents, None).await;
    snapshot!(code, @"202 Accepted");
    let response = index.wait_task(response.uid()).await.failed();
    snapshot!(json_string!(response, { ".duration" => "[duration]", ".enqueuedAt" => "[date]", ".startedAt" => "[date]", ".finishedAt" => "[date]" }),
        @r###"
    {
      "uid": 15,
      "batchUid": 15,
      "indexUid": "test",
      "status": "failed",
      "type": "documentAdditionOrUpdate",
      "canceledBy": null,
      "details": {
        "receivedDocuments": 1,
        "indexedDocuments": 0
      },
      "error": {
        "message": "Index `test`: Could not parse latitude in the document with the id: `\"12\"`. Was expecting a finite number but instead got `null`.",
        "code": "invalid_document_geo_field",
        "type": "invalid_request",
        "link": "https://docs.meilisearch.com/errors#invalid_document_geo_field"
      },
      "duration": "[duration]",
      "enqueuedAt": "[date]",
      "startedAt": "[date]",
      "finishedAt": "[date]"
    }
    "###);

    // _geo has a lat and lng but set to `null`
    let documents = json!([
        {
            "id": "13",
            "_geo": { "lng": null, "lat": null }
        }
    ]);

    let (response, code) = index.add_documents(documents, None).await;
    snapshot!(code, @"202 Accepted");
    let response = index.wait_task(response.uid()).await.failed();
    snapshot!(json_string!(response, { ".duration" => "[duration]", ".enqueuedAt" => "[date]", ".startedAt" => "[date]", ".finishedAt" => "[date]" }),
        @r###"
    {
      "uid": 16,
      "batchUid": 16,
      "indexUid": "test",
      "status": "failed",
      "type": "documentAdditionOrUpdate",
      "canceledBy": null,
      "details": {
        "receivedDocuments": 1,
        "indexedDocuments": 0
      },
      "error": {
        "message": "Index `test`: Could not parse latitude nor longitude in the document with the id: `\"13\"`. Was expecting finite numbers but instead got `null` and `null`.",
        "code": "invalid_document_geo_field",
        "type": "invalid_request",
        "link": "https://docs.meilisearch.com/errors#invalid_document_geo_field"
      },
      "duration": "[duration]",
      "enqueuedAt": "[date]",
      "startedAt": "[date]",
      "finishedAt": "[date]"
    }
    "###);
}

// Related to #4333
#[actix_rt::test]
async fn add_invalid_geo_and_then_settings() {
    let server = Server::new().await;
    let index = server.index("test");
    index.create(Some("id")).await;

    // _geo is not a correct object
    let documents = json!([
        {
            "id": "11",
            "_geo": { "lat": null, "lng": null },
        }
    ]);
    let (ret, code) = index.add_documents(documents, None).await;
    snapshot!(code, @"202 Accepted");
    let ret = index.wait_task(ret.uid()).await.succeeded();
    snapshot!(ret, @r###"
    {
      "uid": "[uid]",
      "batchUid": "[batch_uid]",
      "indexUid": "test",
      "status": "succeeded",
      "type": "documentAdditionOrUpdate",
      "canceledBy": null,
      "details": {
        "receivedDocuments": 1,
        "indexedDocuments": 1
      },
      "error": null,
      "duration": "[duration]",
      "enqueuedAt": "[date]",
      "startedAt": "[date]",
      "finishedAt": "[date]"
    }
    "###);

    let (ret, code) = index.update_settings(json!({ "sortableAttributes": ["_geo"] })).await;
    snapshot!(code, @"202 Accepted");
    let ret = index.wait_task(ret.uid()).await.failed();
    snapshot!(ret, @r###"
    {
      "uid": "[uid]",
      "batchUid": "[batch_uid]",
      "indexUid": "test",
      "status": "failed",
      "type": "settingsUpdate",
      "canceledBy": null,
      "details": {
        "sortableAttributes": [
          "_geo"
        ]
      },
      "error": {
        "message": "Index `test`: Could not parse latitude in the document with the id: `\"11\"`. Was expecting a finite number but instead got `null`.",
        "code": "invalid_document_geo_field",
        "type": "invalid_request",
        "link": "https://docs.meilisearch.com/errors#invalid_document_geo_field"
      },
      "duration": "[duration]",
      "enqueuedAt": "[date]",
      "startedAt": "[date]",
      "finishedAt": "[date]"
    }
    "###);
}

#[actix_rt::test]
async fn error_add_documents_payload_size() {
    let server = Server::new().await;
    let index = server.index("test");
    index.create(Some("id")).await;
    let document = json!(
        {
            "id": "11",
            "content": "Lorem ipsum dolor sit amet, consectetur adipiscing elit. Donec metus erat, consequat in blandit venenatis, ultricies eu ipsum. Etiam luctus elit et mollis ultrices. Nam turpis risus, dictum non eros in, eleifend feugiat elit. Morbi non dolor pulvinar, sagittis mi sed, ultricies lorem. Nulla ultricies sem metus. Donec at suscipit quam, sed elementum mi. Suspendisse potenti. Fusce pharetra turpis tortor, sed eleifend odio dapibus ut. Nulla facilisi. Suspendisse elementum, dui eget aliquet dignissim, ex tellus aliquam nisl, at eleifend nisl metus tempus diam. Mauris fermentum sollicitudin efficitur. Donec dignissim est vitae elit finibus faucibus"
        }
    );
    let documents: Vec<_> = (0..16000).map(|_| document.clone()).collect();
    let documents = json!(documents);
    let (response, code) = index.add_documents(documents, None).await;

    snapshot!(code, @"413 Payload Too Large");
    snapshot!(json_string!(response, { ".duration" => "[duration]", ".enqueuedAt" => "[date]", ".startedAt" => "[date]", ".finishedAt" => "[date]" }),
        @r###"
    {
      "message": "The provided payload reached the size limit. The maximum accepted payload size is 10 MiB.",
      "code": "payload_too_large",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#payload_too_large"
    }
    "###);
}

#[actix_rt::test]
async fn error_primary_key_inference() {
    let server = Server::new().await;
    let index = server.index("test");

    let documents = json!([
        {
            "title": "11",
            "desc": "foobar"
        }
    ]);

    let (task, _status_code) = index.add_documents(documents, None).await;
    index.wait_task(task.uid()).await.failed();
    let (response, code) = index.get_task(task.uid()).await;
    assert_eq!(code, 200);

    snapshot!(json_string!(response, { ".duration" => "[duration]", ".enqueuedAt" => "[date]", ".startedAt" => "[date]", ".finishedAt" => "[date]" }),
    @r###"
    {
      "uid": 0,
      "batchUid": 0,
      "indexUid": "test",
      "status": "failed",
      "type": "documentAdditionOrUpdate",
      "canceledBy": null,
      "details": {
        "receivedDocuments": 1,
        "indexedDocuments": 0
      },
      "error": {
        "message": "The primary key inference failed as the engine did not find any field ending with `id` in its name. Please specify the primary key manually using the `primaryKey` query parameter.",
        "code": "index_primary_key_no_candidate_found",
        "type": "invalid_request",
        "link": "https://docs.meilisearch.com/errors#index_primary_key_no_candidate_found"
      },
      "duration": "[duration]",
      "enqueuedAt": "[date]",
      "startedAt": "[date]",
      "finishedAt": "[date]"
    }
    "###);

    let documents = json!([
        {
            "primary_id": "12",
            "object_id": "42",
            "id": "124",
            "title": "11",
            "desc": "foobar"
        }
    ]);

    let (task, _status_code) = index.add_documents(documents, None).await;
    index.wait_task(task.uid()).await.failed();
    let (response, code) = index.get_task(task.uid()).await;
    assert_eq!(code, 200);

    snapshot!(json_string!(response, { ".duration" => "[duration]", ".enqueuedAt" => "[date]", ".startedAt" => "[date]", ".finishedAt" => "[date]" }),
    @r###"
    {
      "uid": 1,
      "batchUid": 1,
      "indexUid": "test",
      "status": "failed",
      "type": "documentAdditionOrUpdate",
      "canceledBy": null,
      "details": {
        "receivedDocuments": 1,
        "indexedDocuments": 0
      },
      "error": {
        "message": "The primary key inference failed as the engine found 3 fields ending with `id` in their names: 'id' and 'object_id'. Please specify the primary key manually using the `primaryKey` query parameter.",
        "code": "index_primary_key_multiple_candidates_found",
        "type": "invalid_request",
        "link": "https://docs.meilisearch.com/errors#index_primary_key_multiple_candidates_found"
      },
      "duration": "[duration]",
      "enqueuedAt": "[date]",
      "startedAt": "[date]",
      "finishedAt": "[date]"
    }
    "###);

    let documents = json!([
        {
            "primary_id": "12",
            "title": "11",
            "desc": "foobar"
        }
    ]);

    let (task, _status_code) = index.add_documents(documents, None).await;
    index.wait_task(task.uid()).await.succeeded();
    let (response, code) = index.get_task(task.uid()).await;
    assert_eq!(code, 200);

    snapshot!(json_string!(response, { ".duration" => "[duration]", ".enqueuedAt" => "[date]", ".startedAt" => "[date]", ".finishedAt" => "[date]" }),
    @r###"
    {
      "uid": 2,
      "batchUid": 2,
      "indexUid": "test",
      "status": "succeeded",
      "type": "documentAdditionOrUpdate",
      "canceledBy": null,
      "details": {
        "receivedDocuments": 1,
        "indexedDocuments": 1
      },
      "error": null,
      "duration": "[duration]",
      "enqueuedAt": "[date]",
      "startedAt": "[date]",
      "finishedAt": "[date]"
    }
    "###);
}

#[actix_rt::test]
async fn add_documents_with_primary_key_twice() {
    let server = Server::new().await;
    let index = server.index("test");

    let documents = json!([
        {
            "title": "11",
            "desc": "foobar"
        }
    ]);

    let (task, _status_code) = index.add_documents(documents.clone(), Some("title")).await;
    index.wait_task(task.uid()).await.succeeded();
    let (response, _code) = index.get_task(task.uid()).await;
    assert_eq!(response["status"], "succeeded");

    let (task, _status_code) = index.add_documents(documents, Some("title")).await;
    index.wait_task(task.uid()).await.succeeded();
    let (response, _code) = index.get_task(task.uid()).await;
    assert_eq!(response["status"], "succeeded");
}

#[actix_rt::test]
async fn batch_several_documents_addition() {
    let server = Server::new().await;
    let index = server.index("test");

    let mut documents: Vec<_> = (0..150usize)
        .map(|id| {
            json!(
                {
                    "id": id,
                    "title": "foo",
                    "desc": "bar"
                }
            )
        })
        .collect();

    documents[100] = json!({"title": "error", "desc": "error"});

    // enqueue batch of documents
    let mut waiter = Vec::new();
    for chunk in documents.chunks(30) {
        waiter.push(index.add_documents(json!(chunk), Some("id")));
    }

    // wait first batch of documents to finish
    futures::future::join_all(waiter).await;
    index.wait_task(4).await;

    // run a second completely failing batch
    documents[40] = json!({"title": "error", "desc": "error"});
    documents[70] = json!({"title": "error", "desc": "error"});
    documents[130] = json!({"title": "error", "desc": "error"});
    let mut waiter = Vec::new();
    for chunk in documents.chunks(30) {
        waiter.push(index.add_documents(json!(chunk), Some("id")));
    }
    // wait second batch of documents to finish
    futures::future::join_all(waiter).await;
    index.wait_task(9).await;

    let (response, _code) = index.filtered_tasks(&[], &["failed"], &[]).await;

    // Check if only the 6th task failed
    println!("{}", &response);
    assert_eq!(response["results"].as_array().unwrap().len(), 5);

    // Check if there are exactly 120 documents (150 - 30) in the index;
    let (response, code) = index
        .get_all_documents(GetAllDocumentsOptions { limit: Some(200), ..Default::default() })
        .await;
    assert_eq!(code, 200, "failed with `{}`", response);
    assert_eq!(response["results"].as_array().unwrap().len(), 120);
}

#[actix_rt::test]
async fn dry_register_file() {
    let temp = tempfile::tempdir().unwrap();

    let options =
        Opt { experimental_replication_parameters: true, ..default_settings(temp.path()) };
    let server = Server::new_with_options(options).await.unwrap();
    let index = server.index("tamo");

    let documents = r#"
        {
            "id": "12",
            "doggo": "kefir"
        }
    "#;

    let (response, code) = index
        .raw_add_documents(
            documents,
            vec![("Content-Type", "application/json"), ("DryRun", "true")],
            "",
        )
        .await;
    snapshot!(response, @r###"
    {
      "taskUid": 0,
      "indexUid": "tamo",
      "status": "enqueued",
      "type": "documentAdditionOrUpdate",
      "enqueuedAt": "[date]"
    }
    "###);
    snapshot!(code, @"202 Accepted");

    let (response, code) = index.get_task(response.uid()).await;
    snapshot!(response, @r###"
    {
      "message": "Task `0` not found.",
      "code": "task_not_found",
      "type": "invalid_request",
      "link": "https://docs.meilisearch.com/errors#task_not_found"
    }
    "###);
    snapshot!(code, @"404 Not Found");
}
