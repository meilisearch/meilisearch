use crate::protocol::*;
use crate::server::McpServer;
use crate::registry::McpToolRegistry;
use serde_json::json;
use tokio;

#[tokio::test]
async fn test_mcp_initialize_request() {
    let server = McpServer::new(McpToolRegistry::new());
    
    let request = JsonRpcRequest {
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
    
    let response = server.handle_json_rpc_request(request).await;
    
    match response {
        JsonRpcResponse::Success { result, .. } => {
            let init_result: InitializeResult = serde_json::from_value(result).unwrap();
            assert_eq!(init_result.protocol_version, "2024-11-05");
            assert_eq!(init_result.server_info.name, "meilisearch-mcp");
            assert!(init_result.capabilities.tools.list_changed);
        }
        _ => panic!("Expected success response"),
    }
}

#[tokio::test]
async fn test_mcp_list_tools_request() {
    let mut registry = McpToolRegistry::new();
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
    
    let server = McpServer::new(registry);
    let request = JsonRpcRequest {
        jsonrpc: "2.0".to_string(),
        method: "tools/list".to_string(),
        params: None,
        id: json!(2),
    };
    let response = server.handle_json_rpc_request(request).await;
    
    match response {
        JsonRpcResponse::Success { result, .. } => {
            let list_result: ListToolsResult = serde_json::from_value(result).unwrap();
            assert_eq!(list_result.tools.len(), 1);
            assert_eq!(list_result.tools[0].name, "searchDocuments");
            assert_eq!(list_result.tools[0].description, "Search for documents");
            assert!(list_result.tools[0].input_schema["type"] == "object");
        }
        _ => panic!("Expected success response"),
    }
}

#[tokio::test]
async fn test_mcp_call_tool_request_success() {
    let mut registry = McpToolRegistry::new();
    registry.register_tool(crate::registry::McpTool {
        name: "getStats".to_string(),
        description: "Get server statistics".to_string(),
        input_schema: json!({
            "type": "object",
            "properties": {},
        }),
        http_method: "GET".to_string(),
        path_template: "/stats".to_string(),
    });
    
    let server = McpServer::new(registry);
    let request = JsonRpcRequest {
        jsonrpc: "2.0".to_string(),
        method: "tools/call".to_string(),
        params: Some(json!({
            "name": "getStats",
            "arguments": {}
        })),
        id: json!(1),
    };
    
    let response = server.handle_json_rpc_request(request).await;
    
    match response {
        JsonRpcResponse::Success { result, .. } => {
            let call_result: CallToolResult = serde_json::from_value(result).unwrap();
            assert!(!call_result.content.is_empty());
            assert_eq!(call_result.content[0].content_type, "text");
            assert!(call_result.is_error.is_none() || !call_result.is_error.unwrap());
        }
        _ => panic!("Expected success response"),
    }
}

#[tokio::test]
async fn test_mcp_call_unknown_tool() {
    let server = McpServer::new(McpToolRegistry::new());
    let request = JsonRpcRequest {
        jsonrpc: "2.0".to_string(),
        method: "tools/call".to_string(),
        params: Some(json!({
            "name": "unknownTool",
            "arguments": {}
        })),
        id: json!(1),
    };
    
    let response = server.handle_json_rpc_request(request).await;
    
    match response {
        JsonRpcResponse::Error { error, .. } => {
            assert_eq!(error.code, crate::protocol::METHOD_NOT_FOUND);
            assert!(error.message.contains("Tool not found"));
        }
        _ => panic!("Expected error response"),
    }
}

#[tokio::test]
async fn test_mcp_call_tool_with_invalid_params() {
    let mut registry = McpToolRegistry::new();
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
    
    let server = McpServer::new(registry);
    let request = JsonRpcRequest {
        jsonrpc: "2.0".to_string(),
        method: "tools/call".to_string(),
        params: Some(json!({
            "name": "searchDocuments",
            "arguments": {} // Missing required indexUid
        })),
        id: json!(1),
    };
    
    let response = server.handle_json_rpc_request(request).await;
    
    match response {
        JsonRpcResponse::Error { error, .. } => {
            assert_eq!(error.code, crate::protocol::INVALID_PARAMS);
            assert!(error.message.contains("Invalid parameters"));
        }
        _ => panic!("Expected error response"),
    }
}

#[tokio::test]
async fn test_protocol_version_negotiation() {
    let server = McpServer::new(McpToolRegistry::new());
    
    let test_versions = vec![
        "2024-11-05",
        "2024-11-01", // Older version
        "2025-01-01", // Future version
    ];
    
    for version in test_versions {
        let request = JsonRpcRequest {
            jsonrpc: "2.0".to_string(),
            method: "initialize".to_string(),
            params: Some(json!({
                "protocol_version": version,
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
            JsonRpcResponse::Success { result, .. } => {
                let init_result: InitializeResult = serde_json::from_value(result).unwrap();
                // Server should always return its supported version
                assert_eq!(init_result.protocol_version, "2024-11-05");
            }
            _ => panic!("Expected success response"),
        }
    }
}

#[tokio::test]
async fn test_json_rpc_response_serialization() {
    let response = JsonRpcResponse::Success {
        jsonrpc: "2.0".to_string(),
        result: json!({
            "protocol_version": "2024-11-05",
            "capabilities": {
                "tools": {
                    "list_changed": true
                },
                "experimental": {}
            },
            "server_info": {
                "name": "meilisearch-mcp",
                "version": env!("CARGO_PKG_VERSION")
            }
        }),
        id: json!(1),
    };
    
    let serialized = serde_json::to_string(&response).unwrap();
    let deserialized: JsonRpcResponse = serde_json::from_str(&serialized).unwrap();
    
    match deserialized {
        JsonRpcResponse::Success { result, .. } => {
            assert_eq!(result["protocol_version"], "2024-11-05");
            assert_eq!(result["server_info"]["name"], "meilisearch-mcp");
        }
        _ => panic!("Deserialization failed"),
    }
}

#[tokio::test]
async fn test_tool_result_formatting() {
    let result = CallToolResult {
        content: vec![
            ToolContent {
                content_type: "text".to_string(),
                text: "Success: Index created".to_string(),
            },
        ],
        is_error: None,
    };
    
    let serialized = serde_json::to_string(&result).unwrap();
    assert!(serialized.contains("\"type\":\"text\""));
    assert!(serialized.contains("Success: Index created"));
    assert!(!serialized.contains("is_error"));
}

#[tokio::test]
async fn test_error_response_formatting() {
    let error_response = JsonRpcResponse::Error {
        jsonrpc: "2.0".to_string(),
        error: JsonRpcError {
            code: -32601,
            message: "Method not found".to_string(),
            data: Some(json!({ "method": "unknownMethod" })),
        },
        id: json!(1),
    };
    
    let serialized = serde_json::to_string(&error_response).unwrap();
    assert!(serialized.contains("\"code\":-32601"));
    assert!(serialized.contains("Method not found"));
    assert!(serialized.contains("unknownMethod"));
}