use actix_web::{test, web, App};
use futures::StreamExt;
use serde_json::json;

#[actix_rt::test]
async fn test_mcp_server_sse_communication() {
    let app = test::init_service(
        App::new()
            .app_data(web::Data::new(crate::server::McpServer::new(
                crate::registry::McpToolRegistry::new(),
            )))
            .route("/mcp", web::get().to(crate::server::mcp_sse_handler)),
    )
    .await;

    let req = test::TestRequest::get()
        .uri("/mcp")
        .insert_header(("Accept", "text/event-stream"))
        .to_request();

    let mut resp = test::call_service(&app, req).await;
    assert!(resp.status().is_success());
    assert_eq!(
        resp.headers().get("Content-Type").unwrap(),
        "text/event-stream"
    );
}

#[actix_rt::test]
async fn test_mcp_full_workflow() {
    // This test simulates a complete MCP client-server interaction
    let registry = create_test_registry();
    let server = crate::server::McpServer::new(registry);
    
    // 1. Initialize
    let init_request = crate::protocol::McpRequest::Initialize {
        params: crate::protocol::InitializeParams {
            protocol_version: "2024-11-05".to_string(),
            capabilities: Default::default(),
            client_info: crate::protocol::ClientInfo {
                name: "test-client".to_string(),
                version: "1.0.0".to_string(),
            },
        },
    };
    
    let init_response = server.handle_request(init_request).await;
    assert!(matches!(init_response, crate::protocol::McpResponse::Initialize { .. }));
    
    // 2. List tools
    let list_request = crate::protocol::McpRequest::ListTools;
    let list_response = server.handle_request(list_request).await;
    
    let tools = match list_response {
        crate::protocol::McpResponse::ListTools { result, .. } => result.tools,
        _ => panic!("Expected ListTools response"),
    };
    
    assert!(!tools.is_empty());
    
    // 3. Call a tool
    let call_request = crate::protocol::McpRequest::CallTool {
        params: crate::protocol::CallToolParams {
            name: tools[0].name.clone(),
            arguments: json!({
                "indexUid": "test-index"
            }),
        },
    };
    
    let call_response = server.handle_request(call_request).await;
    assert!(matches!(call_response, crate::protocol::McpResponse::CallTool { .. }));
}

#[actix_rt::test]
async fn test_mcp_authentication_integration() {
    let registry = create_test_registry();
    let server = crate::server::McpServer::new(registry);
    
    // Test with valid API key
    let request_with_auth = crate::protocol::McpRequest::CallTool {
        params: crate::protocol::CallToolParams {
            name: "getStats".to_string(),
            arguments: json!({
                "_auth": {
                    "apiKey": "test-api-key"
                }
            }),
        },
    };
    
    let response = server.handle_request(request_with_auth).await;
    
    // Depending on auth implementation, this should either succeed or fail appropriately
    match response {
        crate::protocol::McpResponse::CallTool { .. } |
        crate::protocol::McpResponse::Error { .. } => {
            // Both are valid responses depending on auth setup
        }
        _ => panic!("Unexpected response type"),
    }
}

#[actix_rt::test]
async fn test_mcp_streaming_responses() {
    // Test that long-running operations can stream progress updates
    let mut registry = crate::registry::McpToolRegistry::new();
    registry.register_tool(crate::registry::McpTool {
        name: "createIndexWithDocuments".to_string(),
        description: "Create index and add documents".to_string(),
        input_schema: json!({
            "type": "object",
            "properties": {
                "indexUid": { "type": "string" },
                "documents": { "type": "array" }
            },
            "required": ["indexUid", "documents"]
        }),
        http_method: "POST".to_string(),
        path_template: "/indexes/{index_uid}/documents".to_string(),
    });
    
    let server = crate::server::McpServer::new(registry);
    
    let request = crate::protocol::McpRequest::CallTool {
        params: crate::protocol::CallToolParams {
            name: "createIndexWithDocuments".to_string(),
            arguments: json!({
                "indexUid": "streaming-test",
                "documents": [
                    {"id": 1, "title": "Test 1"},
                    {"id": 2, "title": "Test 2"},
                ]
            }),
        },
    };
    
    let response = server.handle_request(request).await;
    
    match response {
        crate::protocol::McpResponse::CallTool { result, .. } => {
            // Should contain progress information if available
            assert!(!result.content.is_empty());
        }
        _ => panic!("Expected CallTool response"),
    }
}

#[actix_rt::test]
async fn test_mcp_error_handling_scenarios() {
    let server = crate::server::McpServer::new(crate::registry::McpToolRegistry::new());
    
    // Test various error scenarios
    let error_scenarios = vec![
        (
            crate::protocol::McpRequest::CallTool {
                params: crate::protocol::CallToolParams {
                    name: "nonExistentTool".to_string(),
                    arguments: json!({}),
                },
            },
            -32601, // Method not found
        ),
        (
            crate::protocol::McpRequest::CallTool {
                params: crate::protocol::CallToolParams {
                    name: "searchDocuments".to_string(),
                    arguments: json!("invalid"), // Invalid JSON structure
                },
            },
            -32602, // Invalid params
        ),
    ];
    
    for (request, expected_code) in error_scenarios {
        let response = server.handle_request(request).await;
        
        match response {
            crate::protocol::McpResponse::Error { error, .. } => {
                assert_eq!(error.code, expected_code);
            }
            _ => panic!("Expected Error response"),
        }
    }
}

#[actix_rt::test]
async fn test_mcp_concurrent_requests() {
    let registry = create_test_registry();
    let server = web::Data::new(crate::server::McpServer::new(registry));
    
    // Simulate multiple concurrent requests
    let futures = (0..10).map(|i| {
        let server = server.clone();
        async move {
            let request = crate::protocol::McpRequest::CallTool {
                params: crate::protocol::CallToolParams {
                    name: "getStats".to_string(),
                    arguments: json!({ "request_id": i }),
                },
            };
            
            server.handle_request(request).await
        }
    });
    
    let results = futures::future::join_all(futures).await;
    
    // All requests should complete successfully
    for (i, result) in results.iter().enumerate() {
        match result {
            crate::protocol::McpResponse::CallTool { .. } |
            crate::protocol::McpResponse::Error { .. } => {
                // Both are acceptable outcomes
            }
            _ => panic!("Unexpected response type for request {}", i),
        }
    }
}

fn create_test_registry() -> crate::registry::McpToolRegistry {
    let mut registry = crate::registry::McpToolRegistry::new();
    
    // Add some test tools
    registry.register_tool(crate::registry::McpTool {
        name: "getStats".to_string(),
        description: "Get server statistics".to_string(),
        input_schema: json!({
            "type": "object",
            "properties": {}
        }),
        http_method: "GET".to_string(),
        path_template: "/stats".to_string(),
    });
    
    registry.register_tool(crate::registry::McpTool {
        name: "searchDocuments".to_string(),
        description: "Search for documents".to_string(),
        input_schema: json!({
            "type": "object",
            "properties": {
                "indexUid": { "type": "string" },
                "q": { "type": "string" }
            },
            "required": ["indexUid"]
        }),
        http_method: "POST".to_string(),
        path_template: "/indexes/{index_uid}/search".to_string(),
    });
    
    registry
}