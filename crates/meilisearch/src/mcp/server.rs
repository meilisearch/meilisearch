//! MCP Server - HTTP/SSE transport implementation
//!
//! Implements the MCP Streamable HTTP transport specification (2025-03-26).
//! Provides a `/mcp` endpoint that handles:
//! - POST: JSON-RPC requests from clients
//! - GET: SSE stream for server-initiated messages

use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use actix_web::http::header;
use actix_web::web::{self, Data};
use actix_web::{HttpRequest, HttpResponse};
use index_scheduler::IndexScheduler;
use serde_json::{json, Value};
use uuid::Uuid;

use super::error::McpError;
use super::protocol::{
    Content, ErrorObject, InitializeParams, InitializeResult, JsonRpcError, JsonRpcRequest,
    JsonRpcResponse, ServerCapabilities, ServerInfo, ToolResult, ToolsCapability,
};
use super::tools;

/// Session state for MCP connections
#[derive(Debug, Clone)]
pub struct McpSession {
    pub initialized: bool,
}

impl Default for McpSession {
    fn default() -> Self {
        Self { initialized: false }
    }
}

/// Global session store
pub struct McpSessionStore {
    sessions: RwLock<HashMap<String, McpSession>>,
}

impl McpSessionStore {
    pub fn new() -> Self {
        Self {
            sessions: RwLock::new(HashMap::new()),
        }
    }

    pub fn get_or_create(&self, session_id: &str) -> McpSession {
        let sessions = self.sessions.read().unwrap();
        let session = sessions.get(session_id).cloned().unwrap_or_default();
        tracing::info!("MCP Session lookup for '{}': initialized={}, total_sessions={}",
            session_id, session.initialized, sessions.len());
        session
    }

    pub fn update(&self, session_id: &str, session: McpSession) {
        let mut sessions = self.sessions.write().unwrap();
        tracing::info!("MCP Storing session '{}': initialized={}", session_id, session.initialized);
        sessions.insert(session_id.to_string(), session);
    }

    pub fn create_session(&self) -> String {
        let session_id = Uuid::new_v4().to_string();
        let mut sessions = self.sessions.write().unwrap();
        sessions.insert(session_id.clone(), McpSession::default());
        session_id
    }
}

impl Default for McpSessionStore {
    fn default() -> Self {
        Self::new()
    }
}

/// Configure MCP routes
pub fn configure(cfg: &mut web::ServiceConfig) {
    cfg.service(
        web::resource("")
            .route(web::post().to(handle_mcp_post))
            .route(web::get().to(handle_mcp_get)),
    );
}

/// Extract or create session ID from request
fn get_or_create_session_id(req: &HttpRequest, session_store: &McpSessionStore) -> String {
    req.headers()
        .get("Mcp-Session-Id")
        .and_then(|h| h.to_str().ok())
        .map(String::from)
        .unwrap_or_else(|| session_store.create_session())
}

/// Handle POST requests (JSON-RPC messages from client)
pub async fn handle_mcp_post(
    req: HttpRequest,
    body: web::Json<Value>,
    index_scheduler: Data<IndexScheduler>,
    session_store: Data<McpSessionStore>,
) -> HttpResponse {
    let session_id = get_or_create_session_id(&req, &session_store);
    let mut session = session_store.get_or_create(&session_id);

    tracing::debug!("MCP POST request, session: {}", session_id);

    // Handle single request or batch
    let response = if body.is_array() {
        // Batch request
        let requests: Vec<JsonRpcRequest> = match serde_json::from_value(body.into_inner()) {
            Ok(r) => r,
            Err(e) => {
                return HttpResponse::BadRequest().json(json!({
                    "jsonrpc": "2.0",
                    "id": null,
                    "error": {
                        "code": -32700,
                        "message": format!("Parse error: {}", e)
                    }
                }));
            }
        };

        let mut responses = Vec::new();
        for request in requests {
            match handle_single_request(&request, &mut session, &index_scheduler).await {
                Ok(Some(resp)) => responses.push(resp),
                Ok(None) => {} // Notification, no response
                Err(e) => responses.push(create_error_response(request.id.clone(), e)),
            }
        }
        session_store.update(&session_id, session);

        if responses.is_empty() {
            return HttpResponse::Accepted()
                .insert_header(("Mcp-Session-Id", session_id))
                .finish();
        }
        serde_json::to_value(responses).unwrap_or(json!(null))
    } else {
        // Single request
        let request: JsonRpcRequest = match serde_json::from_value(body.into_inner()) {
            Ok(r) => r,
            Err(e) => {
                return HttpResponse::BadRequest().json(json!({
                    "jsonrpc": "2.0",
                    "id": null,
                    "error": {
                        "code": -32700,
                        "message": format!("Parse error: {}", e)
                    }
                }));
            }
        };

        match handle_single_request(&request, &mut session, &index_scheduler).await {
            Ok(Some(resp)) => {
                session_store.update(&session_id, session);
                resp
            }
            Ok(None) => {
                session_store.update(&session_id, session);
                return HttpResponse::Accepted()
                    .insert_header(("Mcp-Session-Id", session_id))
                    .finish();
            }
            Err(e) => {
                session_store.update(&session_id, session);
                create_error_response(request.id, e)
            }
        }
    };

    HttpResponse::Ok()
        .insert_header(("Mcp-Session-Id", session_id))
        .insert_header((header::CONTENT_TYPE, "application/json"))
        .json(response)
}

/// Handle GET requests (SSE stream for server-initiated messages)
pub async fn handle_mcp_get(
    req: HttpRequest,
    session_store: Data<McpSessionStore>,
) -> HttpResponse {
    let session_id = get_or_create_session_id(&req, &session_store);

    tracing::debug!("MCP GET request (SSE), session: {}", session_id);

    // For now, we don't have server-initiated messages, so return 405
    // In a full implementation, this would open an SSE stream
    HttpResponse::MethodNotAllowed()
        .insert_header(("Mcp-Session-Id", session_id))
        .json(json!({
            "error": "Server-initiated messages not yet supported. Use POST for requests."
        }))
}

/// Handle a single JSON-RPC request
async fn handle_single_request(
    request: &JsonRpcRequest,
    session: &mut McpSession,
    index_scheduler: &Data<IndexScheduler>,
) -> Result<Option<Value>, McpError> {
    tracing::debug!("Handling MCP method: {}", request.method);

    match request.method.as_str() {
        "initialize" => {
            let result = handle_initialize(request)?;
            session.initialized = true; // Mark initialized after successful handshake
            Ok(Some(result))
        }
        "notifications/initialized" => {
            // Client acknowledgment - session already initialized
            Ok(None) // Notifications don't need responses
        }
        "tools/list" => {
            let result = handle_tools_list(request)?;
            Ok(Some(result))
        }
        "tools/call" => {
            let result = handle_tools_call(request, session, index_scheduler).await?;
            Ok(Some(result))
        }
        _ => Err(McpError::MethodNotFound(request.method.clone())),
    }
}

/// Handle initialize request
fn handle_initialize(request: &JsonRpcRequest) -> Result<Value, McpError> {
    let _params: InitializeParams = serde_json::from_value(request.params.clone())
        .map_err(|e| McpError::InvalidRequest(format!("Invalid initialize params: {}", e)))?;

    let result = InitializeResult {
        protocol_version: "2024-11-05".to_string(),
        server_info: ServerInfo {
            name: "meilisearch-mcp".to_string(),
            version: env!("CARGO_PKG_VERSION").to_string(),
        },
        capabilities: ServerCapabilities {
            tools: Some(ToolsCapability { list_changed: false }),
        },
    };

    Ok(json!(JsonRpcResponse {
        jsonrpc: "2.0".to_string(),
        id: request.id.clone(),
        result: serde_json::to_value(result)?,
    }))
}

/// Handle tools/list request
fn handle_tools_list(request: &JsonRpcRequest) -> Result<Value, McpError> {
    let tools_schema = tools::get_tools_schema();

    let response = JsonRpcResponse {
        jsonrpc: "2.0".to_string(),
        id: request.id.clone(),
        result: json!({ "tools": tools_schema }),
    };

    Ok(serde_json::to_value(response)?)
}

/// Handle tools/call request
async fn handle_tools_call(
    request: &JsonRpcRequest,
    session: &McpSession,
    index_scheduler: &Data<IndexScheduler>,
) -> Result<Value, McpError> {
    if !session.initialized {
        return Err(McpError::InvalidRequest(
            "Server not initialized. Send initialize request first.".to_string(),
        ));
    }

    let params: Value = request.params.clone();
    let tool_name = params
        .get("name")
        .and_then(|n| n.as_str())
        .ok_or_else(|| McpError::MissingParameter("name".to_string()))?;

    let arguments = params.get("arguments").cloned().unwrap_or(json!({}));

    tracing::info!("Executing MCP tool: {} with args: {:?}", tool_name, arguments);

    // Get Arc from Data
    let scheduler: Arc<IndexScheduler> = index_scheduler.clone().into_inner();

    match tools::execute_tool(tool_name, arguments, scheduler).await {
        Ok(result) => {
            let tool_result = ToolResult {
                content: vec![Content::Text {
                    text: serde_json::to_string(&result)?,
                }],
                is_error: None,
            };

            let response = JsonRpcResponse {
                jsonrpc: "2.0".to_string(),
                id: request.id.clone(),
                result: serde_json::to_value(tool_result)?,
            };

            Ok(serde_json::to_value(response)?)
        }
        Err(e) => {
            tracing::error!("MCP tool execution error: {}", e);

            // Get available indexes for error context
            let scheduler: Arc<IndexScheduler> = index_scheduler.clone().into_inner();
            let available_indexes = scheduler.index_names().ok();

            let error_context = e.to_context(available_indexes);
            let error_text = json!({
                "error": {
                    "type": error_context.error_type,
                    "message": e.to_string(),
                    "code": error_context.code,
                    "context": error_context.context,
                }
            });

            let tool_result = ToolResult {
                content: vec![Content::Text {
                    text: serde_json::to_string(&error_text)?,
                }],
                is_error: Some(true),
            };

            let response = JsonRpcResponse {
                jsonrpc: "2.0".to_string(),
                id: request.id.clone(),
                result: serde_json::to_value(tool_result)?,
            };

            Ok(serde_json::to_value(response)?)
        }
    }
}

/// Create JSON-RPC error response
fn create_error_response(id: Value, error: McpError) -> Value {
    let error_obj = ErrorObject {
        code: error.to_jsonrpc_code(),
        message: error.to_string(),
        data: None,
    };

    json!(JsonRpcError {
        jsonrpc: "2.0".to_string(),
        id,
        error: error_obj,
    })
}
