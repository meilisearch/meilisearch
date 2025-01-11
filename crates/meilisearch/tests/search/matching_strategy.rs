use meili_snap::snapshot;
use once_cell::sync::Lazy;

use crate::common::index::Index;
use crate::common::{Server, Value};
use crate::json;

async fn index_with_documents<'a>(server: &'a Server, documents: &Value) -> Index<'a> {
    let index = server.index("test");

    let (task, _status_code) = index.add_documents(documents.clone(), None).await;
    index.wait_task(task.uid()).await.succeeded();
    index
}

static SIMPLE_SEARCH_DOCUMENTS: Lazy<Value> = Lazy::new(|| {
    json!([
    {
        "title": "Shazam!",
        "id": "1",
    },
    {
        "title": "Captain Planet",
        "id": "2",
    },
    {
        "title": "Captain Marvel",
        "id": "3",
    },
    {
        "title": "a Captain Marvel ersatz",
        "id": "4"
    },
    {
        "title": "He's not part of the Marvel Cinematic Universe",
        "id": "5"
    },
    {
        "title": "a Shazam ersatz, but better than Captain Planet",
        "id": "6"
    },
    {
        "title": "Capitain CAAAAAVEEERNE!!!!",
        "id": "7"
    }
    ])
});

#[actix_rt::test]
async fn simple_search() {
    let server = Server::new().await;
    let index = index_with_documents(&server, &SIMPLE_SEARCH_DOCUMENTS).await;

    index
        .search(json!({"q": "Captain Marvel", "matchingStrategy": "last", "attributesToRetrieve": ["id"]}), |response, code| {
            snapshot!(code, @"200 OK");
            snapshot!(response["hits"], @r###"[{"id":"3"},{"id":"4"},{"id":"2"},{"id":"6"},{"id":"7"}]"###);
        })
        .await;

    index
        .search(json!({"q": "Captain Marvel", "matchingStrategy": "all", "attributesToRetrieve": ["id"]}), |response, code| {
            snapshot!(code, @"200 OK");
            snapshot!(response["hits"], @r###"[{"id":"3"},{"id":"4"}]"###);
        })
        .await;

    index
        .search(json!({"q": "Captain Marvel", "matchingStrategy": "frequency", "attributesToRetrieve": ["id"]}), |response, code| {
            snapshot!(code, @"200 OK");
            snapshot!(response["hits"], @r###"[{"id":"3"},{"id":"4"},{"id":"5"}]"###);
        })
        .await;
}

#[actix_rt::test]
async fn search_with_typo() {
    let server = Server::new().await;
    let index = index_with_documents(&server, &SIMPLE_SEARCH_DOCUMENTS).await;

    index
        .search(json!({"q": "Capitain Marvel", "matchingStrategy": "last", "attributesToRetrieve": ["id"]}), |response, code| {
            snapshot!(code, @"200 OK");
            snapshot!(response["hits"], @r###"[{"id":"3"},{"id":"4"},{"id":"7"},{"id":"2"},{"id":"6"}]"###);
        })
        .await;

    index
        .search(json!({"q": "Capitain Marvel", "matchingStrategy": "all", "attributesToRetrieve": ["id"]}), |response, code| {
            snapshot!(code, @"200 OK");
            snapshot!(response["hits"], @r###"[{"id":"3"},{"id":"4"}]"###);
        })
        .await;

    index
        .search(json!({"q": "Capitain Marvel", "matchingStrategy": "frequency", "attributesToRetrieve": ["id"]}), |response, code| {
            snapshot!(code, @"200 OK");
            snapshot!(response["hits"], @r###"[{"id":"3"},{"id":"4"},{"id":"5"}]"###);
        })
        .await;
}

#[actix_rt::test]
async fn search_with_unknown_word() {
    let server = Server::new().await;
    let index = index_with_documents(&server, &SIMPLE_SEARCH_DOCUMENTS).await;

    index
        .search(json!({"q": "Captain Supercopter Marvel", "matchingStrategy": "last", "attributesToRetrieve": ["id"]}), |response, code| {
            snapshot!(code, @"200 OK");
            snapshot!(response["hits"], @r###"[{"id":"2"},{"id":"3"},{"id":"4"},{"id":"6"},{"id":"7"}]"###);
        })
        .await;

    index
        .search(json!({"q": "Captain Supercopter Marvel", "matchingStrategy": "all", "attributesToRetrieve": ["id"]}), |response, code| {
            snapshot!(code, @"200 OK");
            snapshot!(response["hits"], @"[]");
        })
        .await;

    index
        .search(json!({"q": "Captain Supercopter Marvel", "matchingStrategy": "frequency", "attributesToRetrieve": ["id"]}), |response, code| {
            snapshot!(code, @"200 OK");
            snapshot!(response["hits"], @r###"[{"id":"3"},{"id":"4"},{"id":"5"}]"###);
        })
        .await;
}
