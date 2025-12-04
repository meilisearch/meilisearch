//! Error types for MCP server

use serde::Serialize;
use serde_json::{json, Value};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum McpError {
    #[error("Index '{0}' not found")]
    IndexNotFound(String),

    #[error("Attribute '{0}' is not filterable. Filterable attributes: {1:?}")]
    AttributeNotFilterable(String, Vec<String>),

    #[error("Attribute '{0}' is not sortable. Sortable attributes: {1:?}")]
    AttributeNotSortable(String, Vec<String>),

    #[error("Embedder '{0}' not found. Available embedders: {1:?}")]
    EmbedderNotFound(String, Vec<String>),

    #[error("Invalid parameter '{0}': {1}")]
    InvalidParameter(String, String),

    #[error("Missing required parameter '{0}'")]
    MissingParameter(String),

    #[error("Search error: {0}")]
    SearchError(String),

    #[error("Internal error: {0}")]
    InternalError(String),

    #[error("Method not found: {0}")]
    MethodNotFound(String),

    #[error("Invalid request: {0}")]
    InvalidRequest(String),

    #[error("Parse error: {0}")]
    ParseError(String),
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
            Self::AttributeNotFilterable(attr, available) => McpErrorContext {
                error_type: "attribute_not_filterable".to_string(),
                code: "attribute_not_filterable".to_string(),
                context: Some(json!({
                    "attempted_attribute": attr,
                    "filterable_attributes": available,
                })),
            },
            Self::AttributeNotSortable(attr, available) => McpErrorContext {
                error_type: "attribute_not_sortable".to_string(),
                code: "attribute_not_sortable".to_string(),
                context: Some(json!({
                    "attempted_attribute": attr,
                    "sortable_attributes": available,
                })),
            },
            Self::EmbedderNotFound(embedder, available) => {
                let suggestion = find_closest_match(embedder, available);

                McpErrorContext {
                    error_type: "embedder_not_found".to_string(),
                    code: "embedder_not_found".to_string(),
                    context: Some(json!({
                        "requested_embedder": embedder,
                        "available_embedders": available,
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
