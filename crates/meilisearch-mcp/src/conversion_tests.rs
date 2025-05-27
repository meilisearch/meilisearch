use crate::registry::{McpTool, McpToolRegistry};
use serde_json::json;
use utoipa::openapi::{OpenApi, PathItem};

#[test]
fn test_convert_simple_get_endpoint() {
    let tool = McpTool::from_openapi_path(
        "/indexes/{index_uid}",
        "GET",
        &create_mock_path_item_get(),
    );
    
    assert_eq!(tool.name, "getIndex");
    assert_eq!(tool.description, "Get information about an index");
    assert_eq!(tool.http_method, "GET");
    assert_eq!(tool.path_template, "/indexes/{index_uid}");
    
    let schema = &tool.input_schema;
    assert_eq!(schema["type"], "object");
    assert_eq!(schema["required"], json!(["indexUid"]));
    assert_eq!(schema["properties"]["indexUid"]["type"], "string");
}

#[test]
fn test_convert_search_endpoint_with_query_params() {
    let tool = McpTool::from_openapi_path(
        "/indexes/{index_uid}/search",
        "POST",
        &create_mock_search_path_item(),
    );
    
    assert_eq!(tool.name, "searchDocuments");
    assert_eq!(tool.description, "Search for documents in an index");
    assert_eq!(tool.http_method, "POST");
    
    let schema = &tool.input_schema;
    assert_eq!(schema["type"], "object");
    assert_eq!(schema["required"], json!(["indexUid"]));
    assert!(schema["properties"]["q"].is_object());
    assert!(schema["properties"]["limit"].is_object());
    assert!(schema["properties"]["offset"].is_object());
    assert!(schema["properties"]["filter"].is_object());
}

#[test]
fn test_convert_document_addition_endpoint() {
    let tool = McpTool::from_openapi_path(
        "/indexes/{index_uid}/documents",
        "POST",
        &create_mock_add_documents_path_item(),
    );
    
    assert_eq!(tool.name, "addDocuments");
    assert_eq!(tool.description, "Add or replace documents in an index");
    assert_eq!(tool.http_method, "POST");
    
    let schema = &tool.input_schema;
    assert_eq!(schema["type"], "object");
    assert_eq!(schema["required"], json!(["indexUid", "documents"]));
    assert_eq!(schema["properties"]["documents"]["type"], "array");
}

#[test]
fn test_registry_deduplication() {
    let mut registry = McpToolRegistry::new();
    
    let tool1 = McpTool {
        name: "searchDocuments".to_string(),
        description: "Search documents".to_string(),
        input_schema: json!({}),
        http_method: "POST".to_string(),
        path_template: "/indexes/{index_uid}/search".to_string(),
    };
    
    let tool2 = McpTool {
        name: "searchDocuments".to_string(),
        description: "Updated description".to_string(),
        input_schema: json!({"updated": true}),
        http_method: "POST".to_string(),
        path_template: "/indexes/{index_uid}/search".to_string(),
    };
    
    registry.register_tool(tool1);
    registry.register_tool(tool2);
    
    assert_eq!(registry.list_tools().len(), 1);
    assert_eq!(registry.get_tool("searchDocuments").unwrap().description, "Updated description");
}

#[test]
fn test_openapi_to_mcp_tool_conversion() {
    let openapi = create_mock_openapi();
    let registry = McpToolRegistry::from_openapi(&openapi);
    
    let tools = registry.list_tools();
    assert!(tools.len() > 0);
    
    let search_tool = registry.get_tool("searchDocuments");
    assert!(search_tool.is_some());
    
    let index_tool = registry.get_tool("getIndex");
    assert!(index_tool.is_some());
}

#[test]
fn test_tool_name_generation() {
    let test_cases = vec![
        ("/indexes", "GET", "getIndexes"),
        ("/indexes", "POST", "createIndex"),
        ("/indexes/{index_uid}", "GET", "getIndex"),
        ("/indexes/{index_uid}", "PUT", "updateIndex"),
        ("/indexes/{index_uid}", "DELETE", "deleteIndex"),
        ("/indexes/{index_uid}/documents", "GET", "getDocuments"),
        ("/indexes/{index_uid}/documents", "POST", "addDocuments"),
        ("/indexes/{index_uid}/documents", "DELETE", "deleteDocuments"),
        ("/indexes/{index_uid}/search", "POST", "searchDocuments"),
        ("/indexes/{index_uid}/settings", "GET", "getSettings"),
        ("/indexes/{index_uid}/settings", "PATCH", "updateSettings"),
        ("/tasks", "GET", "getTasks"),
        ("/tasks/{task_uid}", "GET", "getTask"),
        ("/keys", "GET", "getApiKeys"),
        ("/keys", "POST", "createApiKey"),
        ("/multi-search", "POST", "multiSearch"),
        ("/swap-indexes", "POST", "swapIndexes"),
    ];
    
    for (path, method, expected_name) in test_cases {
        let name = McpTool::generate_tool_name(path, method);
        assert_eq!(name, expected_name, "Path: {}, Method: {}", path, method);
    }
}

#[test]
fn test_parameter_extraction() {
    let tool = McpTool::from_openapi_path(
        "/indexes/{index_uid}/documents/{document_id}",
        "GET",
        &create_mock_get_document_path_item(),
    );
    
    let schema = &tool.input_schema;
    assert_eq!(schema["required"], json!(["indexUid", "documentId"]));
    assert_eq!(schema["properties"]["indexUid"]["type"], "string");
    assert_eq!(schema["properties"]["documentId"]["type"], "string");
}

fn create_mock_path_item_get() -> PathItem {
    serde_json::from_value(json!({
        "get": {
            "summary": "Get information about an index",
            "parameters": [
                {
                    "name": "index_uid",
                    "in": "path",
                    "required": true,
                    "schema": {
                        "type": "string"
                    }
                }
            ],
            "responses": {
                "200": {
                    "description": "Index information"
                }
            }
        }
    }))
    .unwrap()
}

fn create_mock_search_path_item() -> PathItem {
    serde_json::from_value(json!({
        "post": {
            "summary": "Search for documents in an index",
            "parameters": [
                {
                    "name": "index_uid",
                    "in": "path",
                    "required": true,
                    "schema": {
                        "type": "string"
                    }
                }
            ],
            "requestBody": {
                "content": {
                    "application/json": {
                        "schema": {
                            "type": "object",
                            "properties": {
                                "q": {
                                    "type": "string",
                                    "description": "Search query"
                                },
                                "limit": {
                                    "type": "integer",
                                    "default": 20
                                },
                                "offset": {
                                    "type": "integer",
                                    "default": 0
                                },
                                "filter": {
                                    "type": "string"
                                }
                            }
                        }
                    }
                }
            },
            "responses": {
                "200": {
                    "description": "Search results"
                }
            }
        }
    }))
    .unwrap()
}

fn create_mock_add_documents_path_item() -> PathItem {
    serde_json::from_value(json!({
        "post": {
            "summary": "Add or replace documents in an index",
            "parameters": [
                {
                    "name": "index_uid",
                    "in": "path",
                    "required": true,
                    "schema": {
                        "type": "string"
                    }
                }
            ],
            "requestBody": {
                "content": {
                    "application/json": {
                        "schema": {
                            "type": "array",
                            "items": {
                                "type": "object"
                            }
                        }
                    }
                }
            },
            "responses": {
                "202": {
                    "description": "Accepted"
                }
            }
        }
    }))
    .unwrap()
}

fn create_mock_get_document_path_item() -> PathItem {
    serde_json::from_value(json!({
        "get": {
            "summary": "Get a specific document",
            "parameters": [
                {
                    "name": "index_uid",
                    "in": "path",
                    "required": true,
                    "schema": {
                        "type": "string"
                    }
                },
                {
                    "name": "document_id",
                    "in": "path",
                    "required": true,
                    "schema": {
                        "type": "string"
                    }
                }
            ],
            "responses": {
                "200": {
                    "description": "Document found"
                }
            }
        }
    }))
    .unwrap()
}

fn create_mock_openapi() -> OpenApi {
    serde_json::from_value(json!({
        "openapi": "3.1.0",
        "info": {
            "title": "Meilisearch API",
            "version": "1.0.0"
        },
        "paths": {
            "/indexes": {
                "get": {
                    "summary": "List all indexes",
                    "responses": {
                        "200": {
                            "description": "List of indexes"
                        }
                    }
                },
                "post": {
                    "summary": "Create an index",
                    "responses": {
                        "202": {
                            "description": "Index created"
                        }
                    }
                }
            },
            "/indexes/{index_uid}": {
                "get": {
                    "summary": "Get information about an index",
                    "parameters": [
                        {
                            "name": "index_uid",
                            "in": "path",
                            "required": true,
                            "schema": {
                                "type": "string"
                            }
                        }
                    ],
                    "responses": {
                        "200": {
                            "description": "Index information"
                        }
                    }
                }
            },
            "/indexes/{index_uid}/search": {
                "post": {
                    "summary": "Search for documents in an index",
                    "parameters": [
                        {
                            "name": "index_uid",
                            "in": "path",
                            "required": true,
                            "schema": {
                                "type": "string"
                            }
                        }
                    ],
                    "responses": {
                        "200": {
                            "description": "Search results"
                        }
                    }
                }
            }
        }
    }))
    .unwrap()
}