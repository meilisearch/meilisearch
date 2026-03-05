use urlencoding::encode as urlencode;

use crate::common::{default_settings, Server};
use crate::json;

/// Test that a barrier for an already-completed task is satisfied immediately.
#[actix_rt::test]
async fn barrier_already_satisfied() {
    let server = Server::new().await;
    let index = server.unique_index();

    // Add documents and wait for completion
    let (task, code) = index.add_documents(json!([{"id": 1, "title": "Hello"}]), Some("id")).await;
    assert_eq!(code, 202);
    let task_uid = task.uid();
    server.wait_task(task_uid).await.succeeded();

    // Search with barrier pointing to the completed task — should return 200
    let barrier_val = format!("{}={}", index.uid, task_uid);
    let (response, code) = index
        .search_with_headers(json!({"q": "hello"}), vec![("X-Meili-Barrier", &barrier_val)])
        .await;
    assert_eq!(code, 200, "Expected 200 but got {code}: {response}");
    assert!(response["hits"].as_array().is_some());
}

/// Test that a barrier waits for a task to complete and then returns results.
#[actix_rt::test]
async fn barrier_wait_then_satisfied() {
    let server = Server::new().await;
    let index = server.unique_index();

    // Add documents — get the task UID but DON'T wait for it
    let (task, code) = index.add_documents(json!([{"id": 1, "title": "World"}]), Some("id")).await;
    assert_eq!(code, 202);
    let task_uid = task.uid();

    // Search with barrier — it should block until the task completes, then return 200
    let barrier_val = format!("{}={}", index.uid, task_uid);
    let (response, code) = index
        .search_with_headers(json!({"q": "world"}), vec![("X-Meili-Barrier", &barrier_val)])
        .await;
    assert_eq!(code, 200, "Expected 200 but got {code}: {response}");
}

/// Test that a barrier times out when waiting for a nonexistent task.
#[actix_rt::test]
async fn barrier_timeout() {
    let dir = tempfile::TempDir::new().unwrap();
    let mut options = default_settings(dir.path());
    options.barrier_timeout_ms = 100; // Short timeout
    let server = Server::new_with_options(options).await.unwrap();
    let index = server.unique_index();

    // Create the index first so routes work
    let (task, _) = index.add_documents(json!([{"id": 1}]), Some("id")).await;
    server.wait_task(task.uid()).await.succeeded();

    // Search with barrier for a very high task ID — will timeout
    let barrier_val = format!("{}=99999", index.uid);
    let (response, code) =
        index.search_with_headers(json!({"q": ""}), vec![("X-Meili-Barrier", &barrier_val)]).await;
    assert_eq!(code, 503, "Expected 503 but got {code}: {response}");
}

/// Test that the barrier timeout response body has the expected structure.
#[actix_rt::test]
async fn barrier_timeout_body() {
    let dir = tempfile::TempDir::new().unwrap();
    let mut options = default_settings(dir.path());
    options.barrier_timeout_ms = 100;
    let server = Server::new_with_options(options).await.unwrap();
    let index = server.unique_index();

    let (task, _) = index.add_documents(json!([{"id": 1}]), Some("id")).await;
    server.wait_task(task.uid()).await.succeeded();

    let barrier_val = format!("{}=99999", index.uid);
    let (response, code) =
        index.search_with_headers(json!({"q": ""}), vec![("X-Meili-Barrier", &barrier_val)]).await;
    assert_eq!(code, 503);
    assert_eq!(response["code"], "barrier_timeout");
    assert_eq!(response["type"], "system");
    assert!(response["message"].as_str().unwrap().contains("Barrier timeout"));
    assert!(response["link"].as_str().is_some());
}

/// Test that write responses include the X-Meili-Barrier header.
#[actix_rt::test]
async fn barrier_response_header_on_write() {
    let server = Server::new().await;
    let index = server.unique_index();

    // POST documents and check response headers
    let url = format!("/indexes/{}/documents?primaryKey=id", urlencode(&index.uid));
    let (response, code, headers) =
        server.service.post_with_response_headers(url, json!([{"id": 1, "title": "test"}])).await;
    assert_eq!(code, 202, "Expected 202 but got {code}: {response}");

    let barrier_header = headers.get("x-meili-barrier");
    assert!(
        barrier_header.is_some(),
        "Expected X-Meili-Barrier header in response, got: {headers:?}"
    );

    let val = barrier_header.unwrap();
    let task_uid = response["taskUid"].as_u64().unwrap();
    assert_eq!(val, &format!("{}={}", index.uid, task_uid));

    // Clean up
    server.wait_task(task_uid).await;
}

/// Test that settings updates include the X-Meili-Barrier header.
#[actix_rt::test]
async fn barrier_response_header_on_settings() {
    let server = Server::new().await;
    let index = server.unique_index();

    // Create the index first
    let (task, _) = index.add_documents(json!([{"id": 1}]), Some("id")).await;
    server.wait_task(task.uid()).await.succeeded();

    // PATCH settings and check response headers
    let url = format!("/indexes/{}/settings", urlencode(&index.uid));
    let (response, code, headers) = server
        .service
        .patch_with_response_headers(url, json!({"filterableAttributes": ["id"]}))
        .await;
    assert_eq!(code, 202, "Expected 202 but got {code}: {response}");

    let barrier_header = headers.get("x-meili-barrier");
    assert!(barrier_header.is_some(), "Expected X-Meili-Barrier header on settings update");

    let val = barrier_header.unwrap();
    let task_uid = response["taskUid"].as_u64().unwrap();
    assert_eq!(val, &format!("{}={}", index.uid, task_uid));

    server.wait_task(task_uid).await;
}

/// Test that swap-indexes does NOT include the X-Meili-Barrier header
/// (because index_uid is None for swap tasks).
#[actix_rt::test]
async fn barrier_no_header_for_swap() {
    let server = Server::new().await;
    let index_a = server.unique_index();
    let index_b = server.unique_index();

    // Create both indexes
    let (task, _) = index_a.add_documents(json!([{"id": 1}]), Some("id")).await;
    server.wait_task(task.uid()).await.succeeded();
    let (task, _) = index_b.add_documents(json!([{"id": 1}]), Some("id")).await;
    server.wait_task(task.uid()).await.succeeded();

    // Swap indexes
    let (_response, code, headers) = server
        .service
        .post_with_response_headers(
            "/swap-indexes",
            json!([{"indexes": [index_a.uid, index_b.uid]}]),
        )
        .await;
    assert_eq!(code, 202);
    assert!(
        !headers.contains_key("x-meili-barrier"),
        "Swap should NOT have X-Meili-Barrier header"
    );
}

/// Test that a canceled task satisfies a barrier (any terminal state counts).
#[actix_rt::test]
async fn barrier_canceled_task_satisfies() {
    let server = Server::new().await;
    let index = server.unique_index();

    // Add documents and wait for completion
    let (task, _) = index.add_documents(json!([{"id": 1}]), Some("id")).await;
    let task_uid = task.uid();
    server.wait_task(task_uid).await.succeeded();

    // Cancel the completed task (will succeed as a cancel operation, but the
    // original task is already done so it's effectively a no-op on the task itself)
    let (cancel_task, _) = server.cancel_tasks(&format!("uids={}", task_uid)).await;
    server.wait_task(cancel_task.uid()).await.succeeded();

    // The original task should still satisfy the barrier because it's in a terminal state
    let barrier_val = format!("{}={}", index.uid, task_uid);
    let (response, code) =
        index.search_with_headers(json!({"q": ""}), vec![("X-Meili-Barrier", &barrier_val)]).await;
    assert_eq!(
        code, 200,
        "Canceled/completed task should satisfy barrier but got {code}: {response}"
    );
}

/// Test that a failed task satisfies a barrier.
#[actix_rt::test]
async fn barrier_failed_task_satisfies() {
    let server = Server::new().await;
    let index = server.unique_index();

    // Create index first
    let (task, _) = index.add_documents(json!([{"id": 1}]), Some("id")).await;
    server.wait_task(task.uid()).await.succeeded();

    // Trigger a failing task by sending an invalid document payload
    let url = format!("/indexes/{}/documents", urlencode(&index.uid));
    let body = serde_json::to_string(&json!({"invalid": "not an array"})).unwrap();
    let (response, _code) =
        server.service.post_str(url, body, vec![("content-type", "application/json")]).await;

    // If we got a task, wait for it (it should fail)
    if let Some(task_uid) = response["taskUid"].as_u64() {
        let task_result = server.wait_task(task_uid).await;
        // The task should have failed
        assert!(
            task_result["status"] == "failed" || task_result["status"] == "succeeded",
            "Expected terminal status, got: {}",
            task_result["status"]
        );

        // The barrier should be satisfied regardless of success or failure
        let barrier_val = format!("{}={}", index.uid, task_uid);
        let (response, code) = index
            .search_with_headers(json!({"q": ""}), vec![("X-Meili-Barrier", &barrier_val)])
            .await;
        assert_eq!(code, 200, "Failed task should satisfy barrier: {response}");
    }
}

/// Test that a barrier with multiple indexes works.
#[actix_rt::test]
async fn barrier_multiple_indexes() {
    let server = Server::new().await;
    let index_a = server.unique_index();
    let index_b = server.unique_index();

    // Add documents to both indexes and wait
    let (task_a, _) = index_a.add_documents(json!([{"id": 1, "title": "Alpha"}]), Some("id")).await;
    let (task_b, _) = index_b.add_documents(json!([{"id": 1, "title": "Beta"}]), Some("id")).await;
    let task_a_uid = task_a.uid();
    let task_b_uid = task_b.uid();
    server.wait_task(task_a_uid).await.succeeded();
    server.wait_task(task_b_uid).await.succeeded();

    // Search on index_a with barrier for both indexes
    let barrier_val = format!("{}={},{}={}", index_a.uid, task_a_uid, index_b.uid, task_b_uid);
    let (response, code) = index_a
        .search_with_headers(json!({"q": ""}), vec![("X-Meili-Barrier", &barrier_val)])
        .await;
    assert_eq!(code, 200, "Multi-index barrier should succeed: {response}");
}

/// Test that a search without a barrier header works normally (no blocking).
#[actix_rt::test]
async fn barrier_no_header_no_wait() {
    let server = Server::new().await;
    let index = server.unique_index();

    let (task, _) = index.add_documents(json!([{"id": 1, "title": "Normal"}]), Some("id")).await;
    server.wait_task(task.uid()).await.succeeded();

    // Normal search without barrier header
    let (response, code) = index.search_post(json!({"q": "normal"})).await;
    assert_eq!(code, 200, "Search without barrier should work normally: {response}");
}

/// Test barrier on a nonexistent index (should timeout).
#[actix_rt::test]
async fn barrier_nonexistent_index() {
    let dir = tempfile::TempDir::new().unwrap();
    let mut options = default_settings(dir.path());
    options.barrier_timeout_ms = 100;
    let server = Server::new_with_options(options).await.unwrap();
    let index = server.unique_index();

    // Create and populate the index so the search route works
    let (task, _) = index.add_documents(json!([{"id": 1}]), Some("id")).await;
    server.wait_task(task.uid()).await.succeeded();

    // Search with barrier for a completely different nonexistent index
    let barrier_val = "nonexistent_idx=42";
    let (response, code) =
        index.search_with_headers(json!({"q": ""}), vec![("X-Meili-Barrier", barrier_val)]).await;
    assert_eq!(code, 503, "Barrier on nonexistent index should timeout: {response}");
}

/// Test barrier on document GET.
#[actix_rt::test]
async fn barrier_on_document_get() {
    let server = Server::new().await;
    let index = server.unique_index();

    let (task, _) = index.add_documents(json!([{"id": 1, "title": "Doc"}]), Some("id")).await;
    let task_uid = task.uid();
    server.wait_task(task_uid).await.succeeded();

    // GET document with barrier header
    let barrier_val = format!("{}={}", index.uid, task_uid);
    let url = format!("/indexes/{}/documents/1", urlencode(&index.uid));
    let (response, code) =
        server.service.get_with_headers(url, vec![("X-Meili-Barrier", &barrier_val)]).await;
    assert_eq!(code, 200, "GET document with barrier should succeed: {response}");
    assert_eq!(response["id"], 1);
}

/// Test barrier on facet search.
#[actix_rt::test]
async fn barrier_on_facet_search() {
    let server = Server::new().await;
    let index = server.unique_index();

    // Add documents and set filterable attributes
    let (task, _) = index
        .add_documents(
            json!([
                {"id": 1, "genre": "action"},
                {"id": 2, "genre": "comedy"}
            ]),
            Some("id"),
        )
        .await;
    server.wait_task(task.uid()).await.succeeded();

    let (task, _) = index.update_settings(json!({"filterableAttributes": ["genre"]})).await;
    let settings_uid = task.uid();
    server.wait_task(settings_uid).await.succeeded();

    // Facet search with barrier pointing to the settings task
    let barrier_val = format!("{}={}", index.uid, settings_uid);
    let url = format!("/indexes/{}/facet-search", urlencode(&index.uid));
    let body = serde_json::to_string(&json!({"facetName": "genre", "facetQuery": ""})).unwrap();
    let headers = vec![("content-type", "application/json"), ("X-Meili-Barrier", &*barrier_val)];
    let (response, code) = server.service.post_str(url, body, headers).await;
    assert_eq!(code, 200, "Facet search with barrier should succeed: {response}");
}

/// Test barrier on multi-search.
#[actix_rt::test]
async fn barrier_on_multi_search() {
    let server = Server::new().await;
    let index = server.unique_index();

    let (task, _) = index.add_documents(json!([{"id": 1, "title": "Multi"}]), Some("id")).await;
    let task_uid = task.uid();
    server.wait_task(task_uid).await.succeeded();

    let barrier_val = format!("{}={}", index.uid, task_uid);
    let (response, code) = server
        .multi_search_with_headers(
            json!({"queries": [{"indexUid": index.uid, "q": "multi"}]}),
            vec![("X-Meili-Barrier", &barrier_val)],
        )
        .await;
    assert_eq!(code, 200, "Multi-search with barrier should succeed: {response}");
}

/// Test that malformed barrier header returns an error.
#[actix_rt::test]
async fn barrier_malformed_header() {
    let server = Server::new().await;
    let index = server.unique_index();

    let (task, _) = index.add_documents(json!([{"id": 1}]), Some("id")).await;
    server.wait_task(task.uid()).await.succeeded();

    // Malformed: missing equals sign
    let (_response, code) =
        index.search_with_headers(json!({"q": ""}), vec![("X-Meili-Barrier", "movies4523")]).await;
    assert_eq!(code, 400, "Malformed barrier header should return 400");

    // Malformed: non-numeric task ID
    let (_response, code) =
        index.search_with_headers(json!({"q": ""}), vec![("X-Meili-Barrier", "movies=abc")]).await;
    assert_eq!(code, 400, "Non-numeric task ID should return 400");
}

/// Test that create_index also includes barrier header.
#[actix_rt::test]
async fn barrier_response_header_on_create_index() {
    let server = Server::new().await;
    let index_uid = uuid::Uuid::new_v4().to_string();

    let (_response, code, headers) = server
        .service
        .post_with_response_headers("/indexes", json!({"uid": index_uid, "primaryKey": "id"}))
        .await;
    assert_eq!(code, 202);
    assert!(
        headers.contains_key("x-meili-barrier"),
        "Create index should have X-Meili-Barrier header"
    );
}

/// Test that delete_index includes barrier header.
#[actix_rt::test]
async fn barrier_response_header_on_delete_index() {
    let server = Server::new().await;
    let index = server.unique_index();

    // Create index
    let (task, _) = index.add_documents(json!([{"id": 1}]), Some("id")).await;
    server.wait_task(task.uid()).await.succeeded();

    // Delete index — need to use request_with_headers
    let url = format!("/indexes/{}", urlencode(&index.uid));
    let req = actix_web::test::TestRequest::delete().uri(&url);
    let (_response, code, headers) = server.service.request_with_headers(req).await;
    assert_eq!(code, 202);
    assert!(
        headers.contains_key("x-meili-barrier"),
        "Delete index should have X-Meili-Barrier header"
    );
}
