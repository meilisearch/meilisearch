use crate::error::Error;
use crate::protocol::*;
use crate::registry::McpToolRegistry;
use actix_web::{web, HttpRequest, HttpResponse};
use async_stream::try_stream;
use futures::stream::{StreamExt, TryStreamExt};
use serde_json::{json, Value};
use std::sync::Arc;

pub struct McpServer {
    registry: Arc<McpToolRegistry>,
    meilisearch_client: Option<Arc<dyn MeilisearchClient>>,
}

#[async_trait::async_trait]
pub trait MeilisearchClient: Send + Sync {
    async fn call_endpoint(
        &self,
        method: &str,
        path: &str,
        body: Option<Value>,
        auth_header: Option<String>,
    ) -> Result<Value, Error>;
}

impl McpServer {
    pub fn new(registry: McpToolRegistry) -> Self {
        Self {
            registry: Arc::new(registry),
            meilisearch_client: None,
        }
    }

    pub fn with_client(mut self, client: Arc<dyn MeilisearchClient>) -> Self {
        self.meilisearch_client = Some(client);
        self
    }

    pub async fn handle_request(&self, request: McpRequest) -> McpResponse {
        match request {
            McpRequest::Initialize { params } => self.handle_initialize(params),
            McpRequest::ListTools => self.handle_list_tools(),
            McpRequest::CallTool { params } => self.handle_call_tool(params).await,
        }
    }

    fn handle_initialize(&self, _params: InitializeParams) -> McpResponse {
        McpResponse::Initialize {
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
        }
    }

    fn handle_list_tools(&self) -> McpResponse {
        let tools = self.registry.list_tools();
        
        McpResponse::ListTools {
            jsonrpc: "2.0".to_string(),
            result: ListToolsResult { tools },
        }
    }

    async fn handle_call_tool(&self, params: CallToolParams) -> McpResponse {
        // Get the tool definition
        let tool = match self.registry.get_tool(&params.name) {
            Some(tool) => tool,
            None => {
                return McpResponse::Error {
                    jsonrpc: "2.0".to_string(),
                    error: McpError {
                        code: -32601,
                        message: format!("Tool not found: {}", params.name),
                        data: None,
                    },
                };
            }
        };

        // Validate parameters
        if let Err(e) = self.validate_parameters(&params.arguments, &tool.input_schema) {
            return McpResponse::Error {
                jsonrpc: "2.0".to_string(),
                error: McpError {
                    code: -32602,
                    message: format!("Invalid parameters: {}", e),
                    data: Some(json!({ "schema": tool.input_schema })),
                },
            };
        }

        // Execute the tool
        match self.execute_tool(tool, params.arguments).await {
            Ok(result) => McpResponse::CallTool {
                jsonrpc: "2.0".to_string(),
                result: CallToolResult {
                    content: vec![ToolContent {
                        content_type: "text".to_string(),
                        text: result,
                    }],
                    is_error: None,
                },
            },
            Err(e) => McpResponse::Error {
                jsonrpc: "2.0".to_string(),
                error: McpError {
                    code: -32000,
                    message: format!("Tool execution failed: {}", e),
                    data: None,
                },
            },
        }
    }

    fn validate_parameters(&self, args: &Value, schema: &Value) -> Result<(), String> {
        // Check if args is an object
        if !args.is_object() {
            return Err("Arguments must be an object".to_string());
        }
        
        // Basic validation - check required fields
        if let (Some(args_obj), Some(schema_obj)) = (args.as_object(), schema.as_object()) {
            if let Some(required) = schema_obj.get("required").and_then(|r| r.as_array()) {
                for req_field in required {
                    if let Some(field_name) = req_field.as_str() {
                        if !args_obj.contains_key(field_name) {
                            return Err(format!("Missing required field: {}", field_name));
                        }
                    }
                }
            }
        }
        Ok(())
    }

    async fn execute_tool(
        &self,
        tool: &crate::registry::McpTool,
        mut arguments: Value,
    ) -> Result<String, Error> {
        // Extract authentication if provided
        let auth_header = arguments
            .as_object_mut()
            .and_then(|obj| obj.remove("_auth"))
            .and_then(|auth| {
                auth.get("apiKey")
                    .and_then(|k| k.as_str())
                    .map(|s| s.to_string())
            })
            .map(|key| format!("Bearer {}", key));

        // Build the actual path by replacing parameters
        let mut path = tool.path_template.clone();
        if let Some(args_obj) = arguments.as_object() {
            for (key, value) in args_obj {
                let param_pattern = format!("{{{}}}", camel_to_snake_case(key));
                if let Some(val_str) = value.as_str() {
                    path = path.replace(&param_pattern, val_str);
                }
            }
        }

        // Prepare request body for POST/PUT/PATCH methods
        let body = match tool.http_method.as_str() {
            "POST" | "PUT" | "PATCH" => {
                // Remove path parameters from body
                if let Some(args_obj) = arguments.as_object_mut() {
                    let mut body_obj = args_obj.clone();
                    // Remove any parameters that were used in the path
                    for (key, _) in args_obj.iter() {
                        let param_pattern = format!("{{{}}}", camel_to_snake_case(key));
                        if tool.path_template.contains(&param_pattern) {
                            body_obj.remove(key);
                        }
                    }
                    Some(Value::Object(body_obj))
                } else {
                    Some(arguments.clone())
                }
            }
            _ => None,
        };

        // Execute the request
        if let Some(client) = &self.meilisearch_client {
            match client.call_endpoint(&tool.http_method, &path, body, auth_header).await {
                Ok(response) => Ok(serde_json::to_string_pretty(&response)?),
                Err(e) => Err(e),
            }
        } else {
            // Mock response for testing
            Ok(json!({
                "status": "success",
                "message": format!("Executed {} {}", tool.http_method, path)
            })
            .to_string())
        }
    }
}

pub async fn mcp_sse_handler(
    _req: HttpRequest,
    _server: web::Data<McpServer>,
) -> Result<HttpResponse, actix_web::Error> {
    // For MCP SSE transport, we need to handle incoming messages via query parameters
    // The MCP inspector will send requests as query params on the SSE connection
    
    let stream = try_stream! {
        // Keep the connection alive
        loop {
            tokio::time::sleep(tokio::time::Duration::from_secs(30)).await;
            yield format!(": keepalive\n\n");
        }
    };

    Ok(HttpResponse::Ok()
        .content_type("text/event-stream")
        .insert_header(("Cache-Control", "no-cache"))
        .insert_header(("Connection", "keep-alive"))
        .insert_header(("X-Accel-Buffering", "no"))
        .streaming(stream.map(|result: Result<String, anyhow::Error>| {
            result.map(|s| actix_web::web::Bytes::from(s))
        }).map_err(|e| actix_web::error::ErrorInternalServerError(e))))
}


fn camel_to_snake_case(s: &str) -> String {
    let mut result = String::new();
    for (i, ch) in s.chars().enumerate() {
        if ch.is_uppercase() && i > 0 {
            result.push('_');
        }
        result.push(ch.to_lowercase().next().unwrap());
    }
    result
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_camel_to_snake_case() {
        assert_eq!(camel_to_snake_case("indexUid"), "index_uid");
        assert_eq!(camel_to_snake_case("documentId"), "document_id");
        assert_eq!(camel_to_snake_case("simple"), "simple");
    }

}