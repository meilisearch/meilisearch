//! MCP (Model Context Protocol) Server Implementation
//!
//! This module implements a MCP server that exposes Meilisearch search
//! capabilities to AI assistants via JSON-RPC 2.0 over HTTP (Streamable HTTP transport).
//!
//! ## Endpoint
//!
//! - `POST /mcp` - Send JSON-RPC requests
//! - `GET /mcp` - SSE stream for server-initiated messages (not yet implemented)
//!
//! ## Tools Provided
//!
//! 1. `meilisearch_list_indexes` - List available indexes
//! 2. `meilisearch_get_index_info` - Get index capabilities (filterable attrs, embedders, etc.)
//! 3. `meilisearch_search` - Perform search with full-text, semantic, or hybrid modes

mod error;
mod protocol;
mod server;
mod tools;

pub use error::McpError;
pub use server::{configure, McpSessionStore};
