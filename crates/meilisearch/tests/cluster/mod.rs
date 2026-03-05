use crate::common::{default_settings, Server};
use crate::json;

/// In standalone mode (no cluster config), writes work normally — no forwarding.
#[actix_rt::test]
async fn standalone_mode_no_forwarding() {
    let server = Server::new().await;
    let index = server.unique_index();

    // Standalone mode: writes should work directly
    let (task, code) = index.add_documents(json!([{"id": 1, "title": "Hello"}]), Some("id")).await;
    assert_eq!(code, 202, "Expected 202 but got {code}: {task}");

    server.wait_task(task.uid()).await.succeeded();

    // Search should work
    let (response, code) = index.search_post(json!({"q": "hello"})).await;
    assert_eq!(code, 200, "Expected 200 but got {code}: {response}");
}

/// A follower with no reachable leader should return 503.
#[actix_rt::test]
async fn follower_no_leader_returns_503() {
    let dir = tempfile::TempDir::new().unwrap();
    let mut options = default_settings(dir.path());
    options.cluster_role = Some("follower".to_string());
    // No cluster_peers set = no leader URL
    let server = Server::new_with_options(options).await.unwrap();
    let index = server.unique_index();

    // Write to a follower with no leader should return 503
    let (response, code) =
        index.add_documents(json!([{"id": 1, "title": "Hello"}]), Some("id")).await;
    assert_eq!(code, 503, "Expected 503 but got {code}: {response}");
    assert_eq!(response["code"], "cluster_no_leader");
}

/// A follower with an unreachable leader should return 503.
#[actix_rt::test]
async fn follower_unreachable_leader_returns_503() {
    let dir = tempfile::TempDir::new().unwrap();
    let mut options = default_settings(dir.path());
    options.cluster_role = Some("follower".to_string());
    // Point to a non-existent leader
    options.cluster_peers = Some("http://127.0.0.1:19999".to_string());
    let server = Server::new_with_options(options).await.unwrap();
    let index = server.unique_index();

    // Write to a follower with unreachable leader should return 503
    let (response, code) =
        index.add_documents(json!([{"id": 1, "title": "Hello"}]), Some("id")).await;
    assert_eq!(code, 503, "Expected 503 but got {code}: {response}");
    assert_eq!(response["code"], "cluster_leader_unreachable");
}

/// A follower serves search (reads) locally without forwarding.
#[actix_rt::test]
async fn follower_serves_reads_locally() {
    let dir = tempfile::TempDir::new().unwrap();
    let mut options = default_settings(dir.path());
    options.cluster_role = Some("follower".to_string());
    // No peers — reads should still work, only writes need the leader
    let server = Server::new_with_options(options).await.unwrap();

    // Health should work (it's a read endpoint)
    let (response, code) = server.service.get("/health").await;
    assert_eq!(code, 200, "Expected 200 but got {code}: {response}");

    // Version should work
    let (response, code) = server.service.get("/version").await;
    assert_eq!(code, 200, "Expected 200 but got {code}: {response}");
}

/// Settings update on a follower with no leader should return 503.
#[actix_rt::test]
async fn follower_forwards_settings_update() {
    let dir = tempfile::TempDir::new().unwrap();
    let mut options = default_settings(dir.path());
    options.cluster_role = Some("follower".to_string());
    // No leader configured
    let server = Server::new_with_options(options).await.unwrap();
    let index = server.unique_index();

    // Settings update should try to forward and get 503
    let (response, code) = index.update_settings(json!({"searchableAttributes": ["title"]})).await;
    assert_eq!(code, 503, "Expected 503 but got {code}: {response}");
    assert_eq!(response["code"], "cluster_no_leader");
}

/// Swap indexes on a follower with no leader should return 503.
#[actix_rt::test]
async fn follower_forwards_swap_indexes() {
    let dir = tempfile::TempDir::new().unwrap();
    let mut options = default_settings(dir.path());
    options.cluster_role = Some("follower".to_string());
    let server = Server::new_with_options(options).await.unwrap();

    let (response, code) =
        server.service.post("/swap-indexes", json!([{"indexes": ["a", "b"]}])).await;
    assert_eq!(code, 503, "Expected 503 but got {code}: {response}");
}

/// Leader mode works normally (no forwarding).
#[actix_rt::test]
async fn leader_mode_normal_operation() {
    let dir = tempfile::TempDir::new().unwrap();
    let mut options = default_settings(dir.path());
    options.cluster_role = Some("leader".to_string());
    options.node_id = Some("leader-1".to_string());
    let server = Server::new_with_options(options).await.unwrap();
    let index = server.unique_index();

    // Writes should work directly on the leader
    let (task, code) = index.add_documents(json!([{"id": 1, "title": "Hello"}]), Some("id")).await;
    assert_eq!(code, 202, "Expected 202 but got {code}: {task}");

    server.wait_task(task.uid()).await.succeeded();

    // Search should work
    let (response, code) = index.search_post(json!({"q": "hello"})).await;
    assert_eq!(code, 200, "Expected 200 but got {code}: {response}");
}

/// Delete operations on a follower with no leader should return 503.
#[actix_rt::test]
async fn follower_forwards_delete_operations() {
    let dir = tempfile::TempDir::new().unwrap();
    let mut options = default_settings(dir.path());
    options.cluster_role = Some("follower".to_string());
    let server = Server::new_with_options(options).await.unwrap();
    let index = server.unique_index();

    // Delete all documents
    let (response, code) = index.clear_all_documents().await;
    assert_eq!(code, 503, "Expected 503 for delete all but got {code}: {response}");

    // Delete index
    let (response, code) = index.delete().await;
    assert_eq!(code, 503, "Expected 503 for delete index but got {code}: {response}");
}

/// Dump creation on a follower with no leader should return 503.
#[actix_rt::test]
async fn follower_forwards_dump_creation() {
    let dir = tempfile::TempDir::new().unwrap();
    let mut options = default_settings(dir.path());
    options.cluster_role = Some("follower".to_string());
    let server = Server::new_with_options(options).await.unwrap();

    let (response, code) = server.service.post("/dumps", json!({})).await;
    assert_eq!(code, 503, "Expected 503 for dump creation but got {code}: {response}");
}
