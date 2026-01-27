//! MCP Server - HTTP/SSE transport implementation
//!
//! Implements the MCP Streamable HTTP transport specification (2025-03-26).
//! Provides a `/mcp` endpoint that handles:
//! - POST: JSON-RPC requests from clients
//! - GET: SSE stream for server-initiated messages

use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};

use actix_web::http::header::{self, AUTHORIZATION};
use actix_web::web::{self, Data};
use actix_web::{HttpRequest, HttpResponse};
use index_scheduler::IndexScheduler;
use meilisearch_auth::{AuthController, AuthFilter};
use meilisearch_types::keys::Action;
use serde_json::{json, Value};
use uuid::Uuid;

use super::error::McpError;
use super::protocol::{
    Content, ErrorObject, InitializeParams, InitializeResult, JsonRpcError, JsonRpcRequest,
    JsonRpcResponse, ServerCapabilities, ServerInfo, ToolResult, ToolsCapability,
};
use super::tools;

/// Session TTL - sessions expire after 1 hour of inactivity
const SESSION_TTL: Duration = Duration::from_secs(3600);

/// Maximum number of sessions before forced cleanup
const MAX_SESSIONS: usize = 10000;

/// Session state for MCP connections
#[derive(Debug, Clone)]
pub struct McpSession {
    pub initialized: bool,
    pub last_accessed: Instant,
}

impl Default for McpSession {
    fn default() -> Self {
        Self {
            initialized: false,
            last_accessed: Instant::now(),
        }
    }
}

/// Global session store with TTL-based cleanup
pub struct McpSessionStore {
    sessions: RwLock<HashMap<String, McpSession>>,
}

impl McpSessionStore {
    pub fn new() -> Self {
        Self {
            sessions: RwLock::new(HashMap::new()),
        }
    }

    /// Cleanup expired sessions
    fn cleanup_expired(&self, sessions: &mut HashMap<String, McpSession>) {
        let now = Instant::now();
        let before = sessions.len();
        sessions.retain(|_, session| now.duration_since(session.last_accessed) < SESSION_TTL);
        let removed = before - sessions.len();
        if removed > 0 {
            tracing::debug!("MCP session cleanup: removed {} expired sessions", removed);
        }
    }

    pub fn get_or_create(&self, session_id: &str) -> McpSession {
        let sessions = self.sessions.read().expect("MCP session store lock poisoned");
        let session = sessions.get(session_id).cloned().unwrap_or_default();
        tracing::trace!(
            "MCP session lookup: id={}, initialized={}, total={}",
            session_id,
            session.initialized,
            sessions.len()
        );
        session
    }

    pub fn update(&self, session_id: &str, mut session: McpSession) {
        let mut sessions = self.sessions.write().expect("MCP session store lock poisoned");

        // Cleanup if we have too many sessions
        if sessions.len() >= MAX_SESSIONS {
            self.cleanup_expired(&mut sessions);
        }

        session.last_accessed = Instant::now();
        tracing::trace!("MCP session update: id={}, initialized={}", session_id, session.initialized);
        sessions.insert(session_id.to_string(), session);
    }

    pub fn create_session(&self) -> String {
        let session_id = Uuid::new_v4().to_string();
        let mut sessions = self.sessions.write().expect("MCP session store lock poisoned");

        // Cleanup expired sessions periodically
        if sessions.len() >= MAX_SESSIONS {
            self.cleanup_expired(&mut sessions);
        }

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

/// Extract token from Authorization header
fn extract_token_from_request(req: &HttpRequest) -> Result<Option<&str>, McpError> {
    match req
        .headers()
        .get(AUTHORIZATION)
        .map(|type_token| type_token.to_str().unwrap_or_default().splitn(2, ' '))
    {
        Some(mut type_token) => match type_token.next() {
            Some("Bearer") => match type_token.next() {
                Some(token) => Ok(Some(token)),
                None => Err(McpError::MissingAuthorizationHeader),
            },
            _ => Err(McpError::MissingAuthorizationHeader),
        },
        None => Ok(None),
    }
}

/// Authenticate MCP request and return AuthFilter
fn authenticate_mcp_request(
    auth: &AuthController,
    token: Option<&str>,
) -> Result<AuthFilter, McpError> {
    // If no master key is configured, allow access without auth
    let master_key = auth.get_master_key();

    match (master_key, token) {
        // No master key configured - allow full access
        (None, _) => Ok(AuthFilter::default()),
        // Master key matches token - full access
        (Some(mk), Some(t)) if mk == t => Ok(AuthFilter::default()),
        // Token provided, try to authenticate as API key
        (Some(_), Some(token)) => {
            let key_uuid = auth
                .get_optional_uid_from_encoded_key(token.as_bytes())
                .map_err(|_| McpError::InvalidApiKey)?
                .ok_or(McpError::InvalidApiKey)?;

            auth.get_key_filters(key_uuid, None)
                .map_err(|_| McpError::InvalidApiKey)
        }
        // Master key set but no token provided
        (Some(_), None) => Err(McpError::MissingAuthorizationHeader),
    }
}

/// Check if a key is authorized for a specific action on an optional index
fn is_key_authorized_for_action(
    auth: &AuthController,
    token: Option<&str>,
    action: Action,
    index: Option<&str>,
) -> bool {
    let master_key = auth.get_master_key();

    match (master_key, token) {
        // No master key - allow all
        (None, _) => true,
        // Master key matches - allow all
        (Some(mk), Some(t)) if mk == t => true,
        // API key - check authorization
        (Some(_), Some(token)) => {
            if let Ok(Some(key_uuid)) = auth.get_optional_uid_from_encoded_key(token.as_bytes()) {
                auth.is_key_authorized(key_uuid, action, index).unwrap_or(false)
            } else {
                false
            }
        }
        // No token when master key is set
        (Some(_), None) => false,
    }
}

/// Handle POST requests (JSON-RPC messages from client)
pub async fn handle_mcp_post(
    req: HttpRequest,
    body: web::Json<Value>,
    index_scheduler: Data<IndexScheduler>,
    auth: Data<AuthController>,
    session_store: Data<McpSessionStore>,
) -> HttpResponse {
    // Check if MCP feature is enabled
    if let Err(e) = index_scheduler.features().check_mcp("using MCP") {
        return HttpResponse::BadRequest().json(json!({
            "jsonrpc": "2.0",
            "id": null,
            "error": {
                "code": -32600,
                "message": e.to_string()
            }
        }));
    }

    // Extract token from Authorization header
    let token = match extract_token_from_request(&req) {
        Ok(t) => t,
        Err(e) => {
            let error_context = e.to_context(None);
            return HttpResponse::Unauthorized().json(json!({
                "jsonrpc": "2.0",
                "id": null,
                "error": {
                    "code": -32600,
                    "message": e.to_string(),
                    "data": error_context
                }
            }));
        }
    };

    // Authenticate the request
    let auth_filter = match authenticate_mcp_request(&auth, token) {
        Ok(filter) => filter,
        Err(e) => {
            let error_context = e.to_context(None);
            return HttpResponse::Unauthorized().json(json!({
                "jsonrpc": "2.0",
                "id": null,
                "error": {
                    "code": -32600,
                    "message": e.to_string(),
                    "data": error_context
                }
            }));
        }
    };

    let session_id = get_or_create_session_id(&req, &session_store);
    let mut session = session_store.get_or_create(&session_id);

    tracing::trace!("MCP POST request: session={}", session_id);

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
            match handle_single_request(&request, &mut session, &index_scheduler, &auth, token, &auth_filter).await {
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

        match handle_single_request(&request, &mut session, &index_scheduler, &auth, token, &auth_filter).await {
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
    index_scheduler: Data<IndexScheduler>,
    session_store: Data<McpSessionStore>,
) -> HttpResponse {
    // Check if MCP feature is enabled
    if let Err(e) = index_scheduler.features().check_mcp("using MCP") {
        return HttpResponse::BadRequest().json(json!({
            "error": e.to_string()
        }));
    }

    let session_id = get_or_create_session_id(&req, &session_store);

    tracing::trace!("MCP GET request (SSE): session={}", session_id);

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
    auth: &Data<AuthController>,
    token: Option<&str>,
    auth_filter: &AuthFilter,
) -> Result<Option<Value>, McpError> {
    tracing::trace!("MCP method: {}", request.method);

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
            let result = handle_tools_list(request, auth, token)?;
            Ok(Some(result))
        }
        "tools/call" => {
            let result = handle_tools_call(request, session, index_scheduler, auth, token, auth_filter).await?;
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

/// Handle tools/list request - returns only tools the key can access
fn handle_tools_list(
    request: &JsonRpcRequest,
    auth: &Data<AuthController>,
    token: Option<&str>,
) -> Result<Value, McpError> {
    let all_tools = tools::get_tools_schema();

    // Filter tools based on API key permissions
    let authorized_tools: Vec<Value> = all_tools
        .into_iter()
        .filter(|tool| {
            let tool_name = tool["name"].as_str().unwrap_or("");
            match tool_name {
                "meilisearch_list_indexes" => {
                    is_key_authorized_for_action(auth, token, Action::IndexesGet, None)
                }
                "meilisearch_get_index_info" => {
                    // Requires both indexes.get and settings.get
                    is_key_authorized_for_action(auth, token, Action::IndexesGet, None)
                        && is_key_authorized_for_action(auth, token, Action::SettingsGet, None)
                }
                "meilisearch_search" => {
                    is_key_authorized_for_action(auth, token, Action::Search, None)
                }
                _ => false,
            }
        })
        .collect();

    let response = JsonRpcResponse {
        jsonrpc: "2.0".to_string(),
        id: request.id.clone(),
        result: json!({ "tools": authorized_tools }),
    };

    Ok(serde_json::to_value(response)?)
}

/// Handle tools/call request
async fn handle_tools_call(
    request: &JsonRpcRequest,
    session: &McpSession,
    index_scheduler: &Data<IndexScheduler>,
    auth: &Data<AuthController>,
    token: Option<&str>,
    auth_filter: &AuthFilter,
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

    // Check permission for the requested tool
    let (required_action, action_name) = match tool_name {
        "meilisearch_list_indexes" => (Action::IndexesGet, "indexes.get"),
        "meilisearch_get_index_info" => (Action::IndexesGet, "indexes.get"),
        "meilisearch_search" => (Action::Search, "search"),
        _ => {
            return Err(McpError::MethodNotFound(format!("Unknown tool: {}", tool_name)));
        }
    };

    if !is_key_authorized_for_action(auth, token, required_action, None) {
        return Err(McpError::Unauthorized {
            tool: tool_name.to_string(),
            action: action_name.to_string(),
        });
    }

    // For get_index_info, also check settings.get
    if tool_name == "meilisearch_get_index_info"
        && !is_key_authorized_for_action(auth, token, Action::SettingsGet, None)
    {
        return Err(McpError::Unauthorized {
            tool: tool_name.to_string(),
            action: "settings.get".to_string(),
        });
    }

    tracing::debug!("MCP tool call: {} args={:?}", tool_name, arguments);

    // Get Arc from Data
    let scheduler: Arc<IndexScheduler> = index_scheduler.clone().into_inner();

    match tools::execute_tool(tool_name, arguments, scheduler, auth_filter).await {
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
