mod common;

use serde_json::json;

#[actix_rt::test]
async fn delete() {
    let mut server = common::Server::test_server().await;

    let (_response, status_code) = server.get_document(50).await;
    assert_eq!(status_code, 200);

    server.delete_document(50).await;

    let (_response, status_code) = server.get_document(50).await;
    assert_eq!(status_code, 404);
}

// Resolve the issue https://github.com/meilisearch/MeiliSearch/issues/493
#[actix_rt::test]
async fn delete_batch() {
    let mut server = common::Server::test_server().await;

    let doc_ids = vec!(50, 55, 60);
    for doc_id in &doc_ids {
        let (_response, status_code) = server.get_document(doc_id).await;
        assert_eq!(status_code, 200);
    }

    let body = serde_json::json!(&doc_ids);
    server.delete_multiple_documents(body).await;

    for doc_id in &doc_ids {
        let (_response, status_code) = server.get_document(doc_id).await;
        assert_eq!(status_code, 404);
    }
}

#[actix_rt::test]
async fn text_clear_all_placeholder_search() {
    let mut server = common::Server::with_uid("test");
    let body = json!({
        "uid": "test",
    });

    server.create_index(body).await;
    let settings = json!({
        "attributesForFaceting": ["genre"],
    });

    server.update_all_settings(settings).await;

    let documents = json!([
        { "id": 2,    "title": "Pride and Prejudice",                    "author": "Jane Austin",              "genre": "romance" },
        { "id": 456,  "title": "Le Petit Prince",                        "author": "Antoine de Saint-Exupéry", "genre": "adventure" },
        { "id": 1,    "title": "Alice In Wonderland",                    "author": "Lewis Carroll",            "genre": "fantasy" },
        { "id": 1344, "title": "The Hobbit",                             "author": "J. R. R. Tolkien",         "genre": "fantasy" },
        { "id": 4,    "title": "Harry Potter and the Half-Blood Prince", "author": "J. K. Rowling",            "genre": "fantasy" },
        { "id": 42,   "title": "The Hitchhiker's Guide to the Galaxy",   "author": "Douglas Adams" }
    ]);

    server.add_or_update_multiple_documents(documents).await;
    server.clear_all_documents().await;
    let (response, _) = server.search_post(json!({ "q": "", "facetsDistribution": ["genre"] })).await;
    assert_eq!(response["nbHits"], 0);
    let (response, _) = server.search_post(json!({ "q": "" })).await;
    assert_eq!(response["nbHits"], 0);
}
