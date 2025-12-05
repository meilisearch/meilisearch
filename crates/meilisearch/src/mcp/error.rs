//! Error types for MCP server

use serde::Serialize;
use serde_json::{json, Value};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum McpError {
    #[error("Index '{0}' not found")]
    IndexNotFound(String),

    #[error("Invalid parameter '{0}': {1}")]
    InvalidParameter(String, String),

    #[error("Missing required parameter '{0}'")]
    MissingParameter(String),

    #[error("Internal error: {0}")]
    InternalError(String),

    #[error("Method not found: {0}")]
    MethodNotFound(String),

    #[error("Invalid request: {0}")]
    InvalidRequest(String),

    #[error("Parse error: {0}")]
    ParseError(String),

    #[error("Missing Authorization header. Use 'Authorization: Bearer <api-key>'")]
    MissingAuthorizationHeader,

    #[error("Invalid API key")]
    InvalidApiKey,

    #[error("API key does not have '{action}' permission required for tool '{tool}'")]
    Unauthorized { tool: String, action: String },

    #[error("API key cannot access index '{index}'. Allowed indexes: {allowed:?}")]
    IndexUnauthorized { index: String, allowed: Vec<String> },
}

#[derive(Debug, Clone, Serialize)]
pub struct McpErrorContext {
    #[serde(rename = "type")]
    pub error_type: String,
    pub code: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub context: Option<Value>,
}

impl McpError {
    /// Convert to JSON-RPC error code
    pub fn to_jsonrpc_code(&self) -> i32 {
        match self {
            Self::ParseError(_) => -32700,
            Self::InvalidRequest(_) => -32600,
            Self::MethodNotFound(_) => -32601,
            _ => -32000, // Server error
        }
    }

    /// Generate error context with suggestions for recovery
    pub fn to_context(&self, available_indexes: Option<Vec<String>>) -> McpErrorContext {
        match self {
            Self::IndexNotFound(index_uid) => {
                let suggestion = available_indexes
                    .as_ref()
                    .and_then(|indexes| find_closest_match(index_uid, indexes));

                McpErrorContext {
                    error_type: "index_not_found".to_string(),
                    code: "index_not_found".to_string(),
                    context: Some(json!({
                        "index_uid": index_uid,
                        "available_indexes": available_indexes.unwrap_or_default(),
                        "suggestion": suggestion,
                    })),
                }
            }
            Self::InvalidParameter(param, reason) => McpErrorContext {
                error_type: "invalid_parameter_value".to_string(),
                code: "invalid_parameter_value".to_string(),
                context: Some(json!({
                    "parameter": param,
                    "reason": reason,
                })),
            },
            Self::MissingAuthorizationHeader => McpErrorContext {
                error_type: "authentication_required".to_string(),
                code: "missing_authorization_header".to_string(),
                context: Some(json!({
                    "fix": "Add 'Authorization: Bearer <api-key>' header to your request",
                    "docs": "https://www.meilisearch.com/docs/reference/api/keys"
                })),
            },
            Self::InvalidApiKey => McpErrorContext {
                error_type: "authentication_failed".to_string(),
                code: "invalid_api_key".to_string(),
                context: Some(json!({
                    "fix": "Check that your API key is valid and not expired",
                    "docs": "https://www.meilisearch.com/docs/reference/api/keys"
                })),
            },
            Self::Unauthorized { tool, action } => McpErrorContext {
                error_type: "unauthorized".to_string(),
                code: "insufficient_permissions".to_string(),
                context: Some(json!({
                    "tool": tool,
                    "required_action": action,
                    "fix": format!("Use an API key with '{}' permission", action),
                    "docs": "https://www.meilisearch.com/docs/reference/api/keys#actions"
                })),
            },
            Self::IndexUnauthorized { index, allowed } => McpErrorContext {
                error_type: "index_unauthorized".to_string(),
                code: "index_access_denied".to_string(),
                context: Some(json!({
                    "requested_index": index,
                    "allowed_indexes": allowed,
                    "fix": format!("Use an API key with access to '{}' or search from allowed indexes", index)
                })),
            },
            _ => McpErrorContext {
                error_type: "internal_error".to_string(),
                code: "internal_error".to_string(),
                context: None,
            },
        }
    }
}

/// Fuzzy match to find closest string match for suggestions
fn find_closest_match(target: &str, candidates: &[String]) -> Option<String> {
    if candidates.is_empty() {
        return None;
    }

    candidates
        .iter()
        .map(|candidate| (candidate, strsim::jaro_winkler(target, candidate)))
        .max_by(|(_, score_a), (_, score_b)| {
            score_a.partial_cmp(score_b).unwrap_or(std::cmp::Ordering::Equal)
        })
        .filter(|(_, score)| *score > 0.7)
        .map(|(candidate, _)| candidate.clone())
}

impl From<anyhow::Error> for McpError {
    fn from(err: anyhow::Error) -> Self {
        Self::InternalError(err.to_string())
    }
}

impl From<serde_json::Error> for McpError {
    fn from(err: serde_json::Error) -> Self {
        Self::ParseError(err.to_string())
    }
}
