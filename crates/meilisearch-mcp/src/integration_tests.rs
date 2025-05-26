use crate::protocol::*;
use crate::server::McpServer;
use crate::registry::McpToolRegistry;
use serde_json::json;
use tokio;

#[tokio::test]
async fn test_mcp_initialize_request() {
    let server = McpServer::new(McpToolRegistry::new());
    
    let request = McpRequest::Initialize {
        params: InitializeParams {
            protocol_version: "2024-11-05".to_string(),
            capabilities: ClientCapabilities::default(),
            client_info: ClientInfo {
                name: "test-client".to_string(),
                version: "1.0.0".to_string(),
            },
        },
    };
    
    let response = server.handle_request(request).await;
    
    match response {
        McpResponse::Initialize { result, .. } => {
            assert_eq!(result.protocol_version, "2024-11-05");
            assert_eq!(result.server_info.name, "meilisearch-mcp");
            assert!(result.capabilities.tools.list_changed);
        }
        _ => panic!("Expected Initialize response"),
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
    let request = McpRequest::ListTools;
    let response = server.handle_request(request).await;
    
    match response {
        McpResponse::ListTools { result, .. } => {
            assert_eq!(result.tools.len(), 1);
            assert_eq!(result.tools[0].name, "searchDocuments");
            assert_eq!(result.tools[0].description, "Search for documents");
            assert!(result.tools[0].input_schema["type"] == "object");
        }
        _ => panic!("Expected ListTools response"),
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
    let request = McpRequest::CallTool {
        params: CallToolParams {
            name: "getStats".to_string(),
            arguments: json!({}),
        },
    };
    
    let response = server.handle_request(request).await;
    
    match response {
        McpResponse::CallTool { result, .. } => {
            assert!(!result.content.is_empty());
            assert_eq!(result.content[0].content_type, "text");
            assert!(result.is_error.is_none() || !result.is_error.unwrap());
        }
        _ => panic!("Expected CallTool response"),
    }
}

#[tokio::test]
async fn test_mcp_call_unknown_tool() {
    let server = McpServer::new(McpToolRegistry::new());
    let request = McpRequest::CallTool {
        params: CallToolParams {
            name: "unknownTool".to_string(),
            arguments: json!({}),
        },
    };
    
    let response = server.handle_request(request).await;
    
    match response {
        McpResponse::Error { error, .. } => {
            assert_eq!(error.code, -32601);
            assert!(error.message.contains("Tool not found"));
        }
        _ => panic!("Expected Error response"),
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
    let request = McpRequest::CallTool {
        params: CallToolParams {
            name: "searchDocuments".to_string(),
            arguments: json!({}), // Missing required indexUid
        },
    };
    
    let response = server.handle_request(request).await;
    
    match response {
        McpResponse::Error { error, .. } => {
            assert_eq!(error.code, -32602);
            assert!(error.message.contains("Invalid parameters"));
        }
        _ => panic!("Expected Error response"),
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
        let request = McpRequest::Initialize {
            params: InitializeParams {
                protocol_version: version.to_string(),
                capabilities: ClientCapabilities::default(),
                client_info: ClientInfo {
                    name: "test-client".to_string(),
                    version: "1.0.0".to_string(),
                },
            },
        };
        
        let response = server.handle_request(request).await;
        
        match response {
            McpResponse::Initialize { result, .. } => {
                // Server should always return its supported version
                assert_eq!(result.protocol_version, "2024-11-05");
            }
            _ => panic!("Expected Initialize response"),
        }
    }
}

#[tokio::test]
async fn test_mcp_response_serialization() {
    let response = McpResponse::Initialize {
        jsonrpc: "2.0".to_string(),
        result: InitializeResult {
            protocol_version: "2024-11-05".to_string(),
            capabilities: ServerCapabilities {
                tools: ToolsCapability {
                    list_changed: true,
                },
                experimental: json!({}),
            },
            server_info: ServerInfo {
                name: "meilisearch-mcp".to_string(),
                version: env!("CARGO_PKG_VERSION").to_string(),
            },
        },
    };
    
    let serialized = serde_json::to_string(&response).unwrap();
    let deserialized: McpResponse = serde_json::from_str(&serialized).unwrap();
    
    match deserialized {
        McpResponse::Initialize { result, .. } => {
            assert_eq!(result.protocol_version, "2024-11-05");
            assert_eq!(result.server_info.name, "meilisearch-mcp");
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
    let error_response = McpResponse::Error {
        jsonrpc: "2.0".to_string(),
        error: McpError {
            code: -32601,
            message: "Method not found".to_string(),
            data: Some(json!({ "method": "unknownMethod" })),
        },
    };
    
    let serialized = serde_json::to_string(&error_response).unwrap();
    assert!(serialized.contains("\"code\":-32601"));
    assert!(serialized.contains("Method not found"));
    assert!(serialized.contains("unknownMethod"));
}