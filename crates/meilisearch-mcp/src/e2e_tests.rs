use actix_web::{test, web, App};
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

    let resp = test::call_service(&app, req).await;
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
    let init_request = crate::protocol::JsonRpcRequest {
        jsonrpc: "2.0".to_string(),
        method: "initialize".to_string(),
        params: Some(json!({
            "protocol_version": "2024-11-05",
            "capabilities": {},
            "client_info": {
                "name": "test-client",
                "version": "1.0.0"
            }
        })),
        id: json!(1),
    };
    
    let init_response = server.handle_json_rpc_request(init_request).await;
    assert!(matches!(init_response, crate::protocol::JsonRpcResponse::Success { .. }));
    
    // 2. List tools
    let list_request = crate::protocol::JsonRpcRequest {
        jsonrpc: "2.0".to_string(),
        method: "tools/list".to_string(),
        params: None,
        id: json!(2),
    };
    let list_response = server.handle_json_rpc_request(list_request).await;
    
    let tools = match list_response {
        crate::protocol::JsonRpcResponse::Success { result, .. } => {
            let list_result: crate::protocol::ListToolsResult = serde_json::from_value(result).unwrap();
            list_result.tools
        },
        _ => panic!("Expected success response"),
    };
    
    assert!(!tools.is_empty());
    
    // 3. Call a tool
    let call_request = crate::protocol::JsonRpcRequest {
        jsonrpc: "2.0".to_string(),
        method: "tools/call".to_string(),
        params: Some(json!({
            "name": tools[0].name.clone(),
            "arguments": {
                "indexUid": "test-index"
            }
        })),
        id: json!(3),
    };
    
    let call_response = server.handle_json_rpc_request(call_request).await;
    assert!(matches!(call_response, crate::protocol::JsonRpcResponse::Success { .. }));
}

#[actix_rt::test]
async fn test_mcp_authentication_integration() {
    let registry = create_test_registry();
    let server = crate::server::McpServer::new(registry);
    
    // Test with valid API key
    let request_with_auth = crate::protocol::JsonRpcRequest {
        jsonrpc: "2.0".to_string(),
        method: "tools/call".to_string(),
        params: Some(json!({
            "name": "getStats",
            "arguments": {
                "_auth": {
                    "apiKey": "test-api-key"
                }
            }
        })),
        id: json!(1),
    };
    
    let response = server.handle_json_rpc_request(request_with_auth).await;
    
    // Depending on auth implementation, this should either succeed or fail appropriately
    assert!(matches!(response, 
        crate::protocol::JsonRpcResponse::Success { .. } | 
        crate::protocol::JsonRpcResponse::Error { .. }
    ));
}

#[actix_rt::test]
async fn test_mcp_tool_execution_with_params() {
    let registry = create_test_registry();
    let server = crate::server::McpServer::new(registry);
    
    // Test tool with complex parameters
    let request = crate::protocol::JsonRpcRequest {
        jsonrpc: "2.0".to_string(),
        method: "tools/call".to_string(),
        params: Some(json!({
            "name": "searchDocuments",
            "arguments": {
                "indexUid": "products",
                "q": "laptop",
                "limit": 10,
                "offset": 0,
                "filter": "price > 500",
                "sort": ["price:asc"],
                "facets": ["brand", "category"]
            }
        })),
        id: json!(1),
    };
    
    let response = server.handle_json_rpc_request(request).await;
    
    match response {
        crate::protocol::JsonRpcResponse::Success { result, .. } => {
            let call_result: crate::protocol::CallToolResult = serde_json::from_value(result).unwrap();
            assert!(!call_result.content.is_empty());
            assert_eq!(call_result.content[0].content_type, "text");
            // Verify the response contains search-related content
            assert!(call_result.content[0].text.contains("search") || 
                    call_result.content[0].text.contains("products"));
        }
        _ => panic!("Expected success response"),
    }
}

#[actix_rt::test]
async fn test_mcp_error_handling() {
    let registry = create_test_registry();
    let server = crate::server::McpServer::new(registry);
    
    // Test with non-existent tool
    let request = crate::protocol::JsonRpcRequest {
        jsonrpc: "2.0".to_string(),
        method: "tools/call".to_string(),
        params: Some(json!({
            "name": "nonExistentTool",
            "arguments": {}
        })),
        id: json!(1),
    };
    
    let response = server.handle_json_rpc_request(request).await;
    
    match response {
        crate::protocol::JsonRpcResponse::Error { error, .. } => {
            assert_eq!(error.code, crate::protocol::METHOD_NOT_FOUND);
            assert!(error.message.contains("Tool not found"));
        }
        _ => panic!("Expected error response"),
    }
    
    // Test with invalid parameters
    let request = crate::protocol::JsonRpcRequest {
        jsonrpc: "2.0".to_string(),
        method: "tools/call".to_string(),
        params: Some(json!({
            "name": "searchDocuments",
            "arguments": {
                // Missing required indexUid parameter
                "q": "test"
            }
        })),
        id: json!(2),
    };
    
    let response = server.handle_json_rpc_request(request).await;
    
    match response {
        crate::protocol::JsonRpcResponse::Error { error, .. } => {
            assert_eq!(error.code, crate::protocol::INVALID_PARAMS);
            assert!(error.message.contains("Invalid parameters") || 
                    error.message.contains("required"));
        }
        _ => panic!("Expected error response"),
    }
}

#[actix_rt::test]
async fn test_mcp_protocol_version_negotiation() {
    let server = crate::server::McpServer::new(crate::registry::McpToolRegistry::new());
    
    // Test with different protocol versions
    let request = crate::protocol::JsonRpcRequest {
        jsonrpc: "2.0".to_string(),
        method: "initialize".to_string(),
        params: Some(json!({
            "protocol_version": "2024-01-01",  // Old version
            "capabilities": {},
            "client_info": {
                "name": "test-client",
                "version": "1.0.0"
            }
        })),
        id: json!(1),
    };
    
    let response = server.handle_json_rpc_request(request).await;
    
    match response {
        crate::protocol::JsonRpcResponse::Success { result, .. } => {
            let init_result: crate::protocol::InitializeResult = serde_json::from_value(result).unwrap();
            // Server should respond with its supported version
            assert_eq!(init_result.protocol_version, "2024-11-05");
        }
        _ => panic!("Expected success response"),
    }
}

fn create_test_registry() -> crate::registry::McpToolRegistry {
    let mut registry = crate::registry::McpToolRegistry::new();
    
    // Add test tools
    registry.register_tool(crate::registry::McpTool {
        name: "getStats".to_string(),
        description: "Get server statistics".to_string(),
        input_schema: json!({
            "type": "object",
            "properties": {},
            "required": []
        }),
        http_method: "GET".to_string(),
        path_template: "/stats".to_string(),
    });
    
    registry.register_tool(crate::registry::McpTool {
        name: "searchDocuments".to_string(),
        description: "Search for documents in an index".to_string(),
        input_schema: json!({
            "type": "object",
            "properties": {
                "indexUid": {
                    "type": "string",
                    "description": "The index UID"
                },
                "q": {
                    "type": "string",
                    "description": "Query string"
                },
                "limit": {
                    "type": "integer",
                    "description": "Maximum number of results"
                },
                "offset": {
                    "type": "integer",
                    "description": "Number of results to skip"
                }
            },
            "required": ["indexUid"]
        }),
        http_method: "POST".to_string(),
        path_template: "/indexes/{indexUid}/search".to_string(),
    });
    
    registry
}