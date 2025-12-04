//! Integration tests for MCP (Model Context Protocol) server
//!
//! Tests the JSON-RPC 2.0 protocol implementation at the /mcp endpoint.
//!
//! Note: Due to the test framework creating a new app instance per request,
//! session state doesn't persist between separate HTTP requests.
//! Tests that require initialization use batch requests to preserve session state.

use actix_web::http::StatusCode;

use crate::common::Server;
use crate::json;

/// Helper to create a server with MCP feature enabled
async fn server_with_mcp() -> Server {
    let server = Server::new().await;
    let (response, code) = server.set_features(json!({"mcp": true})).await;
    assert_eq!(code, StatusCode::OK, "Failed to enable MCP feature: {response}");
    server
}

/// Helper struct for MCP request building
struct McpRequest {
    id: u64,
    method: String,
    params: serde_json::Value,
}

impl McpRequest {
    fn initialize() -> Self {
        Self {
            id: 1,
            method: "initialize".to_string(),
            params: serde_json::json!({
                "protocolVersion": "2024-11-05",
                "clientInfo": {
                    "name": "meilisearch-test",
                    "version": "1.0.0"
                },
                "capabilities": {}
            }),
        }
    }

    fn tools_list(id: u64) -> Self {
        Self {
            id,
            method: "tools/list".to_string(),
            params: serde_json::json!({}),
        }
    }

    fn tools_call(id: u64, name: &str, arguments: serde_json::Value) -> Self {
        Self {
            id,
            method: "tools/call".to_string(),
            params: serde_json::json!({
                "name": name,
                "arguments": arguments
            }),
        }
    }

    fn custom(id: u64, method: &str, params: serde_json::Value) -> Self {
        Self {
            id,
            method: method.to_string(),
            params,
        }
    }

    fn to_json(&self) -> crate::common::Value {
        json!({
            "jsonrpc": "2.0",
            "id": self.id,
            "method": self.method,
            "params": self.params
        })
    }
}

/// Build a batch request from multiple MCP requests
fn batch(requests: Vec<McpRequest>) -> crate::common::Value {
    let arr: Vec<serde_json::Value> = requests.iter().map(|r| {
        serde_json::json!({
            "jsonrpc": "2.0",
            "id": r.id,
            "method": r.method,
            "params": r.params
        })
    }).collect();
    crate::common::Value(serde_json::Value::Array(arr))
}

// ============================================================================
// Protocol Tests
// ============================================================================

#[actix_rt::test]
async fn mcp_initialize_handshake() {
    let server = server_with_mcp().await;

    let (response, code) = server.service.post("/mcp", McpRequest::initialize().to_json()).await;

    assert_eq!(code, StatusCode::OK, "Response: {response}");
    assert_eq!(response["jsonrpc"], "2.0");
    assert_eq!(response["id"], 1);
    assert!(response["result"]["protocolVersion"].is_string());
    assert!(response["result"]["serverInfo"]["name"].is_string());
    assert!(response["result"]["serverInfo"]["version"].is_string());
    assert!(response["result"]["capabilities"]["tools"].is_object());
}

#[actix_rt::test]
async fn mcp_invalid_json_rpc() {
    let server = server_with_mcp().await;

    // Missing jsonrpc field
    let (response, code) = server.service.post("/mcp", json!({
        "id": 1,
        "method": "initialize"
    })).await;

    assert_eq!(code, StatusCode::BAD_REQUEST);
    assert!(response["error"].is_object());
}

#[actix_rt::test]
async fn mcp_unknown_method() {
    let server = server_with_mcp().await;

    // Use batch: initialize + unknown method
    let (response, code) = server.service.post("/mcp", batch(vec![
        McpRequest::initialize(),
        McpRequest::custom(2, "unknown/method", serde_json::json!({})),
    ])).await;

    assert_eq!(code, StatusCode::OK);
    let responses = response.as_array().expect("batch should return array");
    assert_eq!(responses.len(), 2);

    // Second response should be error
    assert!(responses[1]["error"].is_object());
    assert_eq!(responses[1]["error"]["code"], -32601); // Method not found
}

// ============================================================================
// Tools List Tests
// ============================================================================

#[actix_rt::test]
async fn mcp_tools_list_returns_all_tools() {
    let server = server_with_mcp().await;

    // Use batch: initialize + tools/list
    let (response, code) = server.service.post("/mcp", batch(vec![
        McpRequest::initialize(),
        McpRequest::tools_list(2),
    ])).await;

    assert_eq!(code, StatusCode::OK, "Response: {response}");
    let responses = response.as_array().expect("batch should return array");
    assert_eq!(responses.len(), 2);

    // Check tools list response
    let tools_response = &responses[1];
    assert_eq!(tools_response["jsonrpc"], "2.0");
    assert_eq!(tools_response["id"], 2);

    let tools = tools_response["result"]["tools"].as_array().expect("tools should be an array");
    assert_eq!(tools.len(), 3, "Should have 3 tools");

    // Verify tool names
    let tool_names: Vec<&str> = tools
        .iter()
        .filter_map(|t| t["name"].as_str())
        .collect();

    assert!(tool_names.contains(&"meilisearch_list_indexes"));
    assert!(tool_names.contains(&"meilisearch_get_index_info"));
    assert!(tool_names.contains(&"meilisearch_search"));
}

#[actix_rt::test]
async fn mcp_tools_have_valid_schemas() {
    let server = server_with_mcp().await;

    // Use batch: initialize + tools/list
    let (response, _) = server.service.post("/mcp", batch(vec![
        McpRequest::initialize(),
        McpRequest::tools_list(2),
    ])).await;

    let responses = response.as_array().unwrap();
    let tools = responses[1]["result"]["tools"].as_array().unwrap();

    for tool in tools {
        assert!(tool["name"].is_string(), "Tool must have a name");
        assert!(tool["description"].is_string(), "Tool must have a description");
        assert!(tool["inputSchema"].is_object(), "Tool must have an inputSchema");
        assert_eq!(tool["inputSchema"]["type"], "object", "Schema type must be object");
    }
}

// ============================================================================
// list_indexes Tool Tests
// ============================================================================

#[actix_rt::test]
async fn mcp_list_indexes_empty() {
    let server = server_with_mcp().await;

    // Use batch: initialize + list_indexes
    let (response, code) = server.service.post("/mcp", batch(vec![
        McpRequest::initialize(),
        McpRequest::tools_call(2, "meilisearch_list_indexes", serde_json::json!({})),
    ])).await;

    assert_eq!(code, StatusCode::OK, "Response: {response}");
    let responses = response.as_array().expect("batch should return array");

    // Parse the content from second response
    let content = &responses[1]["result"]["content"][0]["text"];
    let result: serde_json::Value = serde_json::from_str(content.as_str().unwrap()).unwrap();

    assert!(result["results"].is_array());
    assert_eq!(result["results"].as_array().unwrap().len(), 0);
    assert_eq!(result["total"], 0);
}

#[actix_rt::test]
async fn mcp_list_indexes_with_data() {
    let server = server_with_mcp().await;
    let index = server.unique_index();

    // Create an index with documents
    let (response, _) = index.add_documents(json!([{"id": 1, "title": "Test"}]), Some("id")).await;
    server.wait_task(response.uid()).await.succeeded();

    // Use batch: initialize + list_indexes
    let (response, code) = server.service.post("/mcp", batch(vec![
        McpRequest::initialize(),
        McpRequest::tools_call(2, "meilisearch_list_indexes", serde_json::json!({})),
    ])).await;

    assert_eq!(code, StatusCode::OK);
    let responses = response.as_array().unwrap();

    let content = &responses[1]["result"]["content"][0]["text"];
    let result: serde_json::Value = serde_json::from_str(content.as_str().unwrap()).unwrap();

    assert_eq!(result["total"], 1);
    let indexes = result["results"].as_array().unwrap();
    assert_eq!(indexes.len(), 1);
    assert_eq!(indexes[0]["numberOfDocuments"], 1);
}

// ============================================================================
// get_index_info Tool Tests
// ============================================================================

#[actix_rt::test]
async fn mcp_get_index_info_not_found() {
    let server = server_with_mcp().await;

    // Use batch: initialize + get_index_info
    let (response, code) = server.service.post("/mcp", batch(vec![
        McpRequest::initialize(),
        McpRequest::tools_call(2, "meilisearch_get_index_info", serde_json::json!({
            "indexUid": "non_existent_index"
        })),
    ])).await;

    assert_eq!(code, StatusCode::OK);
    let responses = response.as_array().unwrap();

    // Check it's an error response
    let content = &responses[1]["result"]["content"][0]["text"];
    let result: serde_json::Value = serde_json::from_str(content.as_str().unwrap()).unwrap();

    assert!(result["error"].is_object());
    assert_eq!(result["error"]["type"], "index_not_found");
}

#[actix_rt::test]
async fn mcp_get_index_info_success() {
    let server = server_with_mcp().await;
    let index = server.unique_index();

    // Create index with settings
    let (response, _) = index.add_documents(json!([{"id": 1, "title": "Test", "genre": "Action"}]), Some("id")).await;
    server.wait_task(response.uid()).await.succeeded();

    let (response, _) = index.update_settings(json!({
        "filterableAttributes": ["genre"],
        "sortableAttributes": ["title"]
    })).await;
    server.wait_task(response.uid()).await.succeeded();

    // Use batch: initialize + get_index_info
    let (response, code) = server.service.post("/mcp", batch(vec![
        McpRequest::initialize(),
        McpRequest::tools_call(2, "meilisearch_get_index_info", serde_json::json!({
            "indexUid": index.uid
        })),
    ])).await;

    assert_eq!(code, StatusCode::OK);
    let responses = response.as_array().unwrap();

    let content = &responses[1]["result"]["content"][0]["text"];
    let result: serde_json::Value = serde_json::from_str(content.as_str().unwrap()).unwrap();

    assert_eq!(result["uid"], index.uid);
    assert_eq!(result["numberOfDocuments"], 1);
    assert!(result["filterableAttributes"].is_array());
    assert!(result["sortableAttributes"].is_array());
}

// ============================================================================
// search Tool Tests
// ============================================================================

#[actix_rt::test]
async fn mcp_search_empty_index() {
    let server = server_with_mcp().await;
    let index = server.unique_index();

    // Create empty index
    let (response, _) = index.create(None).await;
    server.wait_task(response.uid()).await.succeeded();

    // Use batch: initialize + search
    let (response, code) = server.service.post("/mcp", batch(vec![
        McpRequest::initialize(),
        McpRequest::tools_call(2, "meilisearch_search", serde_json::json!({
            "indexUid": index.uid,
            "q": "test"
        })),
    ])).await;

    assert_eq!(code, StatusCode::OK);
    let responses = response.as_array().unwrap();

    let content = &responses[1]["result"]["content"][0]["text"];
    let result: serde_json::Value = serde_json::from_str(content.as_str().unwrap()).unwrap();

    assert!(result["hits"].is_array());
    assert_eq!(result["hits"].as_array().unwrap().len(), 0);
}

#[actix_rt::test]
async fn mcp_search_with_results() {
    let server = server_with_mcp().await;
    let index = server.unique_index();

    // Create index with documents
    let (response, _) = index.add_documents(json!([
        {"id": 1, "title": "The Matrix", "genre": "sci-fi"},
        {"id": 2, "title": "Inception", "genre": "sci-fi"},
        {"id": 3, "title": "The Godfather", "genre": "drama"}
    ]), Some("id")).await;
    server.wait_task(response.uid()).await.succeeded();

    // Use batch: initialize + search
    let (response, code) = server.service.post("/mcp", batch(vec![
        McpRequest::initialize(),
        McpRequest::tools_call(2, "meilisearch_search", serde_json::json!({
            "indexUid": index.uid,
            "q": "Matrix"
        })),
    ])).await;

    assert_eq!(code, StatusCode::OK);
    let responses = response.as_array().unwrap();

    let content = &responses[1]["result"]["content"][0]["text"];
    let result: serde_json::Value = serde_json::from_str(content.as_str().unwrap()).unwrap();

    let hits = result["hits"].as_array().unwrap();
    assert_eq!(hits.len(), 1);
    assert_eq!(hits[0]["title"], "The Matrix");
}

#[actix_rt::test]
async fn mcp_search_with_filter() {
    let server = server_with_mcp().await;
    let index = server.unique_index();

    // Create index with documents and filterable attributes
    let (response, _) = index.add_documents(json!([
        {"id": 1, "title": "The Matrix", "genre": "sci-fi"},
        {"id": 2, "title": "Inception", "genre": "sci-fi"},
        {"id": 3, "title": "The Godfather", "genre": "drama"}
    ]), Some("id")).await;
    server.wait_task(response.uid()).await.succeeded();

    let (response, _) = index.update_settings(json!({
        "filterableAttributes": ["genre"]
    })).await;
    server.wait_task(response.uid()).await.succeeded();

    // Use batch: initialize + search with filter
    let (response, code) = server.service.post("/mcp", batch(vec![
        McpRequest::initialize(),
        McpRequest::tools_call(2, "meilisearch_search", serde_json::json!({
            "indexUid": index.uid,
            "q": "",
            "filter": "genre = 'sci-fi'"
        })),
    ])).await;

    assert_eq!(code, StatusCode::OK);
    let responses = response.as_array().unwrap();

    let content = &responses[1]["result"]["content"][0]["text"];
    let result: serde_json::Value = serde_json::from_str(content.as_str().unwrap()).unwrap();

    let hits = result["hits"].as_array().unwrap();
    assert_eq!(hits.len(), 2);
}

#[actix_rt::test]
async fn mcp_search_index_not_found() {
    let server = server_with_mcp().await;

    // Use batch: initialize + search non-existent index
    let (response, code) = server.service.post("/mcp", batch(vec![
        McpRequest::initialize(),
        McpRequest::tools_call(2, "meilisearch_search", serde_json::json!({
            "indexUid": "non_existent_index",
            "q": "test"
        })),
    ])).await;

    assert_eq!(code, StatusCode::OK);
    let responses = response.as_array().unwrap();

    let content = &responses[1]["result"]["content"][0]["text"];
    let result: serde_json::Value = serde_json::from_str(content.as_str().unwrap()).unwrap();

    // Should be an error
    assert!(result["error"].is_object());
    assert_eq!(result["error"]["type"], "index_not_found");
}

// ============================================================================
// Error Handling Tests
// ============================================================================

#[actix_rt::test]
async fn mcp_tool_call_without_initialize() {
    let server = server_with_mcp().await;

    // Try to call tool without initializing (single request, not batch)
    let (response, code) = server.service.post(
        "/mcp",
        McpRequest::tools_call(1, "meilisearch_list_indexes", serde_json::json!({})).to_json()
    ).await;

    assert_eq!(code, StatusCode::OK);
    assert!(response["error"].is_object());
}

#[actix_rt::test]
async fn mcp_unknown_tool() {
    let server = server_with_mcp().await;

    // Use batch: initialize + unknown tool
    let (response, code) = server.service.post("/mcp", batch(vec![
        McpRequest::initialize(),
        McpRequest::tools_call(2, "unknown_tool", serde_json::json!({})),
    ])).await;

    assert_eq!(code, StatusCode::OK);
    let responses = response.as_array().unwrap();

    let content = &responses[1]["result"]["content"][0]["text"];
    let result: serde_json::Value = serde_json::from_str(content.as_str().unwrap()).unwrap();

    assert!(result["error"].is_object());
}

#[actix_rt::test]
async fn mcp_missing_required_parameter() {
    let server = server_with_mcp().await;

    // Use batch: initialize + get_index_info without required param
    let (response, code) = server.service.post("/mcp", batch(vec![
        McpRequest::initialize(),
        McpRequest::tools_call(2, "meilisearch_get_index_info", serde_json::json!({})),
    ])).await;

    assert_eq!(code, StatusCode::OK);
    let responses = response.as_array().unwrap();

    let content = &responses[1]["result"]["content"][0]["text"];
    let result: serde_json::Value = serde_json::from_str(content.as_str().unwrap()).unwrap();

    // Should be an error about missing parameter
    assert!(result["error"].is_object());
}

// ============================================================================
// Batch Request Tests
// ============================================================================

#[actix_rt::test]
async fn mcp_batch_requests() {
    let server = server_with_mcp().await;

    // Send batch of requests
    let (response, code) = server.service.post("/mcp", batch(vec![
        McpRequest::initialize(),
        McpRequest::tools_list(2),
    ])).await;

    assert_eq!(code, StatusCode::OK);
    assert!(response.as_array().is_some());

    let responses = response.as_array().unwrap();
    assert_eq!(responses.len(), 2);
}

// ============================================================================
// Feature Gate Tests
// ============================================================================

#[actix_rt::test]
async fn mcp_disabled_by_default() {
    // Don't use server_with_mcp() - use regular Server to test default state
    let server = Server::new().await;

    // MCP should be disabled by default
    let (response, code) = server.service.post("/mcp", McpRequest::initialize().to_json()).await;

    assert_eq!(code, StatusCode::BAD_REQUEST);
    assert!(response["error"]["message"].as_str().unwrap().contains("mcp"));
    assert!(response["error"]["message"].as_str().unwrap().contains("experimental feature"));
}
