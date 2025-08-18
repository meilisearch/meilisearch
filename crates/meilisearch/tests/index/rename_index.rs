use crate::common::{shared_does_not_exists_index, Server};
use crate::json;

#[actix_rt::test]
async fn rename_index_via_patch() {
    let server = Server::new_shared();
    let index = server.unique_index();

    // Create index first
    let (task, code) = index.create(None).await;
    assert_eq!(code, 202);
    server.wait_task(task.uid()).await.succeeded();

    // Rename via PATCH update endpoint
    let new_uid = format!("{}_renamed", index.uid);
    let body = json!({ "uid": &new_uid });
    let (task, code) = index.service.patch(format!("/indexes/{}", index.uid), body).await;

    assert_eq!(code, 202);
    let response = server.wait_task(task.uid()).await.succeeded();

    // Verify the rename succeeded
    assert_eq!(response["status"], "succeeded");
    assert_eq!(response["type"], "indexUpdate");
    assert_eq!(response["details"]["newIndexUid"], new_uid);

    // Check that old index doesn't exist
    let (_, code) = index.get().await;
    assert_eq!(code, 404);

    // Check that new index exists
    let (response, code) = server.service.get(format!("/indexes/{}", new_uid)).await;
    assert_eq!(code, 200);
    assert_eq!(response["uid"], new_uid);
}

#[actix_rt::test]
async fn rename_to_existing_index_via_patch() {
    let server = Server::new_shared();
    let index1 = server.unique_index();
    let index2 = server.unique_index();

    // Create both indexes
    let (task, code) = index1.create(None).await;
    assert_eq!(code, 202);
    server.wait_task(task.uid()).await.succeeded();

    let (task, code) = index2.create(None).await;
    assert_eq!(code, 202);
    server.wait_task(task.uid()).await.succeeded();

    // Try to rename index1 to index2's uid via PATCH (should fail)
    let body = json!({ "uid": index2.uid });
    let (task, code) = index1.service.patch(format!("/indexes/{}", index1.uid), body).await;

    assert_eq!(code, 202);
    let response = server.wait_task(task.uid()).await.failed();

    let expected_response = json!({
        "message": format!("Index `{}` already exists.", index2.uid),
        "code": "index_already_exists",
        "type": "invalid_request",
        "link": "https://docs.meilisearch.com/errors#index_already_exists"
    });

    assert_eq!(response["error"], expected_response);
}

#[actix_rt::test]
async fn rename_non_existent_index_via_patch() {
    let server = Server::new_shared();
    let index = shared_does_not_exists_index().await;

    // Try to rename non-existent index via PATCH
    let body = json!({ "uid": "new_name" });
    let (task, code) = index.service.patch(format!("/indexes/{}", index.uid), body).await;

    assert_eq!(code, 202);
    let response = server.wait_task(task.uid()).await.failed();

    let expected_response = json!({
        "message": format!("Index `{}` not found.", index.uid),
        "code": "index_not_found",
        "type": "invalid_request",
        "link": "https://docs.meilisearch.com/errors#index_not_found"
    });

    assert_eq!(response["error"], expected_response);
}

#[actix_rt::test]
async fn rename_with_invalid_uid_via_patch() {
    let server = Server::new_shared();
    let index = server.unique_index();

    // Create index first
    let (task, code) = index.create(None).await;
    assert_eq!(code, 202);
    server.wait_task(task.uid()).await.succeeded();

    // Try to rename with invalid uid via PATCH
    let body = json!({ "uid": "Invalid UID!" });
    let (_, code) = index.service.patch(format!("/indexes/{}", index.uid), body).await;

    assert_eq!(code, 400);
}

#[actix_rt::test]
async fn rename_index_with_documents_via_patch() {
    let server = Server::new_shared();
    let index = server.unique_index();

    // Create index and add documents
    let (task, code) = index.create(None).await;
    assert_eq!(code, 202);
    server.wait_task(task.uid()).await.succeeded();

    let documents = json!([
        { "id": 1, "title": "Movie 1" },
        { "id": 2, "title": "Movie 2" }
    ]);
    let (task, code) = index.add_documents(documents, None).await;
    assert_eq!(code, 202);
    server.wait_task(task.uid()).await.succeeded();

    // Rename the index via PATCH
    let new_uid = format!("{}_renamed", index.uid);
    let body = json!({ "uid": &new_uid });
    let (task, code) = index.service.patch(format!("/indexes/{}", index.uid), body).await;

    assert_eq!(code, 202);
    server.wait_task(task.uid()).await.succeeded();

    // Verify documents are accessible in renamed index
    let (response, code) = server.service.get(format!("/indexes/{}/documents", new_uid)).await;
    assert_eq!(code, 200);
    assert_eq!(response["results"].as_array().unwrap().len(), 2);
}

#[actix_rt::test]
async fn rename_index_and_update_primary_key_via_patch() {
    let server = Server::new_shared();
    let index = server.unique_index();

    // Create index without primary key
    let (task, code) = index.create(None).await;
    assert_eq!(code, 202);
    server.wait_task(task.uid()).await.succeeded();

    // Rename index and set primary key at the same time
    let new_uid = format!("{}_renamed", index.uid);
    let body = json!({
        "uid": &new_uid,
        "primaryKey": "id"
    });
    let (task, code) = index.service.patch(format!("/indexes/{}", index.uid), body).await;

    assert_eq!(code, 202);
    let response = server.wait_task(task.uid()).await.succeeded();

    // Verify the rename succeeded and primary key was set
    assert_eq!(response["status"], "succeeded");
    assert_eq!(response["type"], "indexUpdate");
    assert_eq!(response["details"]["newIndexUid"], new_uid);
    assert_eq!(response["details"]["primaryKey"], "id");

    // Check that old index doesn't exist
    let (_, code) = index.get().await;
    assert_eq!(code, 404);

    // Check that new index exists with correct primary key
    let (response, code) = server.service.get(format!("/indexes/{}", new_uid)).await;
    assert_eq!(code, 200);
    assert_eq!(response["uid"], new_uid);
    assert_eq!(response["primaryKey"], "id");
}

#[actix_rt::test]
async fn rename_index_and_verify_stats() {
    let server = Server::new_shared();
    let index = server.unique_index();

    // Create index and add documents
    let (task, code) = index.create(None).await;
    assert_eq!(code, 202);
    server.wait_task(task.uid()).await.succeeded();

    let documents = json!([
        { "id": 1, "title": "Movie 1", "genre": "Action" },
        { "id": 2, "title": "Movie 2", "genre": "Drama" },
        { "id": 3, "title": "Movie 3", "genre": "Comedy" }
    ]);
    let (task, code) = index.add_documents(documents, None).await;
    assert_eq!(code, 202);
    server.wait_task(task.uid()).await.succeeded();

    // Get stats before rename
    let (stats_before, code) = index.stats().await;
    assert_eq!(code, 200);
    assert_eq!(stats_before["numberOfDocuments"], 3);

    // Rename the index
    let new_uid = format!("{}_renamed", index.uid);
    let body = json!({ "uid": &new_uid });
    let (task, code) = index.service.patch(format!("/indexes/{}", index.uid), body).await;

    assert_eq!(code, 202);
    server.wait_task(task.uid()).await.succeeded();

    // Get stats after rename using the new uid
    let (stats_after, code) = server.service.get(format!("/indexes/{}/stats", new_uid)).await;
    assert_eq!(code, 200);
    assert_eq!(stats_after["numberOfDocuments"], 3);
    assert_eq!(stats_after["numberOfDocuments"], stats_before["numberOfDocuments"]);
}

#[actix_rt::test]
async fn rename_index_preserves_settings() {
    let server = Server::new_shared();
    let index = server.unique_index();

    // Create index
    let (task, code) = index.create(None).await;
    assert_eq!(code, 202);
    server.wait_task(task.uid()).await.succeeded();

    // Configure settings
    let settings = json!({
        "searchableAttributes": ["title", "description"],
        "filterableAttributes": ["genre", "year"],
        "sortableAttributes": ["year"],
        "rankingRules": [
            "words",
            "typo",
            "proximity",
            "attribute",
            "sort",
            "exactness"
        ],
        "stopWords": ["the", "a", "an"],
        "synonyms": {
            "movie": ["film", "picture"],
            "great": ["awesome", "excellent"]
        }
    });

    let (task, code) = index.update_settings(settings.clone()).await;
    assert_eq!(code, 202);
    server.wait_task(task.uid()).await.succeeded();

    // Rename the index
    let new_uid = format!("{}_renamed", index.uid);
    let body = json!({ "uid": &new_uid });
    let (task, code) = index.service.patch(format!("/indexes/{}", index.uid), body).await;

    assert_eq!(code, 202);
    server.wait_task(task.uid()).await.succeeded();

    // Verify settings are preserved
    let (settings_after, code) = server.service.get(format!("/indexes/{}/settings", new_uid)).await;
    assert_eq!(code, 200);

    assert_eq!(settings_after["searchableAttributes"], json!(["title", "description"]));
    assert_eq!(settings_after["filterableAttributes"], json!(["genre", "year"]));
    assert_eq!(settings_after["sortableAttributes"], json!(["year"]));

    // Check stopWords contains the same items (order may vary)
    let stop_words = settings_after["stopWords"].as_array().unwrap();
    assert_eq!(stop_words.len(), 3);
    assert!(stop_words.contains(&json!("the")));
    assert!(stop_words.contains(&json!("a")));
    assert!(stop_words.contains(&json!("an")));

    assert_eq!(settings_after["synonyms"]["movie"], json!(["film", "picture"]));
    assert_eq!(settings_after["synonyms"]["great"], json!(["awesome", "excellent"]));
}

#[actix_rt::test]
async fn rename_index_preserves_search_functionality() {
    let server = Server::new_shared();
    let index = server.unique_index();

    // Create index and add documents
    let (task, code) = index.create(None).await;
    assert_eq!(code, 202);
    server.wait_task(task.uid()).await.succeeded();

    let documents = json!([
        { "id": 1, "title": "The Matrix", "genre": "Sci-Fi", "year": 1999 },
        { "id": 2, "title": "Inception", "genre": "Sci-Fi", "year": 2010 },
        { "id": 3, "title": "The Dark Knight", "genre": "Action", "year": 2008 }
    ]);
    let (task, code) = index.add_documents(documents, None).await;
    assert_eq!(code, 202);
    server.wait_task(task.uid()).await.succeeded();

    // Make settings filterable
    let settings = json!({
        "filterableAttributes": ["genre", "year"],
        "sortableAttributes": ["year"]
    });
    let (task, code) = index.update_settings(settings).await;
    assert_eq!(code, 202);
    server.wait_task(task.uid()).await.succeeded();

    // Search before rename
    let search_params = json!({
        "q": "matrix",
        "filter": "genre = 'Sci-Fi'",
        "sort": ["year:asc"]
    });
    let (results_before, code) = index.search_post(search_params.clone()).await;
    assert_eq!(code, 200);
    assert_eq!(results_before["hits"].as_array().unwrap().len(), 1);
    assert_eq!(results_before["hits"][0]["title"], "The Matrix");

    // Rename the index
    let new_uid = format!("{}_renamed", index.uid);
    let body = json!({ "uid": &new_uid });
    let (task, code) = index.service.patch(format!("/indexes/{}", index.uid), body).await;

    assert_eq!(code, 202);
    server.wait_task(task.uid()).await.succeeded();

    // Search after rename
    let (results_after, code) =
        server.service.post(format!("/indexes/{}/search", new_uid), search_params).await;
    assert_eq!(code, 200);
    assert_eq!(results_after["hits"].as_array().unwrap().len(), 1);
    assert_eq!(results_after["hits"][0]["title"], "The Matrix");

    // Verify facet search also works
    let facet_search = json!({
        "facetQuery": "Sci",
        "facetName": "genre"
    });
    let (facet_results, code) =
        server.service.post(format!("/indexes/{}/facet-search", new_uid), facet_search).await;
    assert_eq!(code, 200);
    assert_eq!(facet_results["facetHits"].as_array().unwrap().len(), 1);
    assert_eq!(facet_results["facetHits"][0]["value"], "Sci-Fi");
}

#[actix_rt::test]
async fn rename_index_with_pending_tasks() {
    let server = Server::new_shared();
    let index = server.unique_index();

    // Create index
    let (task, code) = index.create(None).await;
    assert_eq!(code, 202);
    server.wait_task(task.uid()).await.succeeded();

    // Add initial documents
    let documents = json!([
        { "id": 1, "title": "Document 1" }
    ]);
    let (task, code) = index.add_documents(documents, None).await;
    assert_eq!(code, 202);
    server.wait_task(task.uid()).await.succeeded();

    // Start a rename
    let new_uid = format!("{}_renamed", index.uid);
    let body = json!({ "uid": &new_uid });
    let (rename_task, code) = index.service.patch(format!("/indexes/{}", index.uid), body).await;
    assert_eq!(code, 202);

    // Try to add documents to the old index while rename is pending
    let more_documents = json!([
        { "id": 2, "title": "Document 2" }
    ]);
    let (_, code) = index.add_documents(more_documents, None).await;
    assert_eq!(code, 202);

    // Wait for rename to complete
    server.wait_task(rename_task.uid()).await.succeeded();

    // Add documents to the new index
    let final_documents = json!([
        { "id": 3, "title": "Document 3" }
    ]);
    let (task, code) =
        server.service.post(format!("/indexes/{}/documents", new_uid), final_documents).await;
    assert_eq!(code, 202);
    server.wait_task(task.uid()).await.succeeded();

    // Verify all documents are accessible
    let (response, code) = server.service.get(format!("/indexes/{}/documents", new_uid)).await;
    assert_eq!(code, 200);
    let docs = response["results"].as_array().unwrap();
    assert!(!docs.is_empty()); // At least the initial document should be there
}
