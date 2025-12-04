//! MCP Tool Implementations
//!
//! This module implements the three core MCP tools for Meilisearch:
//! - list_indexes: List available indexes
//! - get_index_info: Get index metadata and capabilities
//! - search: Perform searches with various modes

use serde::Deserialize;
use serde_json::{json, Value};
use std::sync::Arc;

use index_scheduler::IndexScheduler;
use meilisearch_types::index_uid::IndexUid;
use uuid::Uuid;

use crate::mcp::error::McpError;
use crate::routes::indexes::search::search_kind;
use crate::search::{
    perform_search, RetrieveVectors,
    SearchParams as MeiliSearchParams, SearchQuery,
    DEFAULT_SEARCH_LIMIT, DEFAULT_SEARCH_OFFSET,
};

/// Get JSON Schema for all tools
pub fn get_tools_schema() -> Vec<Value> {
    vec![
        json!({
            "name": "meilisearch_list_indexes",
            "description": "List all available Meilisearch indexes with their basic information. Use this to discover which indexes you can search, their document counts, and primary keys.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "limit": {
                        "type": "integer",
                        "minimum": 1,
                        "maximum": 100,
                        "default": 20,
                        "description": "Maximum number of indexes to return."
                    },
                    "offset": {
                        "type": "integer",
                        "minimum": 0,
                        "default": 0,
                        "description": "Number of indexes to skip for pagination."
                    }
                },
                "additionalProperties": false
            }
        }),
        json!({
            "name": "meilisearch_get_index_info",
            "description": "Get detailed information about a specific index including filterable attributes, sortable attributes, searchable fields, and configured embedders for semantic search. Use this before constructing complex search queries to understand available options.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "index_uid": {
                        "type": "string",
                        "description": "Index identifier to inspect."
                    }
                },
                "required": ["index_uid"],
                "additionalProperties": false
            }
        }),
        json!({
            "name": "meilisearch_search",
            "description": "Search through a Meilisearch index using full-text, semantic, or hybrid search. Automatically handles keyword matching, typo tolerance, and AI-powered semantic understanding. Returns ranked results with highlighting and metadata.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "indexUid": {
                        "type": "string",
                        "description": "Target index identifier. Use list_indexes to discover available indices."
                    },
                    "q": {
                        "type": "string",
                        "description": "Search query string. Can be empty for semantic-only searches when using vector parameter."
                    },
                    "vector": {
                        "type": "array",
                        "items": {"type": "number"},
                        "description": "Query vector for semantic search. Dimensions must match the configured embedder."
                    },
                    "hybrid": {
                        "type": "object",
                        "properties": {
                            "embedder": {
                                "type": "string",
                                "description": "Name of the embedder to use for semantic search. Use get_index_info to discover available embedders."
                            },
                            "semanticRatio": {
                                "type": "number",
                                "minimum": 0.0,
                                "maximum": 1.0,
                                "default": 0.5,
                                "description": "Balance between keyword (0.0) and semantic (1.0) search. Default 0.5 provides balanced hybrid results."
                            }
                        },
                        "required": ["embedder"],
                        "description": "Enable hybrid search combining keyword and semantic results. Requires embedder configuration."
                    },
                    "filter": {
                        "type": "string",
                        "description": "Filter expression using Meilisearch syntax (e.g., 'genre = horror AND year > 2000'). Use get_index_info to see filterable attributes."
                    },
                    "sort": {
                        "type": "array",
                        "items": {"type": "string"},
                        "description": "Sort criteria (e.g., ['year:desc', 'title:asc']). Use get_index_info to see sortable attributes."
                    },
                    "limit": {
                        "type": "integer",
                        "minimum": 1,
                        "maximum": 1000,
                        "default": 20,
                        "description": "Maximum number of results to return."
                    },
                    "offset": {
                        "type": "integer",
                        "minimum": 0,
                        "default": 0,
                        "description": "Number of results to skip for pagination."
                    },
                    "attributesToRetrieve": {
                        "type": "array",
                        "items": {"type": "string"},
                        "description": "Specific document fields to return. Omit to return all fields."
                    },
                    "attributesToHighlight": {
                        "type": "array",
                        "items": {"type": "string"},
                        "description": "Fields to highlight matching terms in. Highlights are wrapped in <em> tags."
                    },
                    "showRankingScore": {
                        "type": "boolean",
                        "default": false,
                        "description": "Include ranking scores (0.0-1.0) showing relevance of each result."
                    },
                    "rankingScoreThreshold": {
                        "type": "number",
                        "minimum": 0.0,
                        "maximum": 1.0,
                        "description": "Exclude results below this ranking score. Useful for quality filtering."
                    }
                },
                "required": ["indexUid"],
                "additionalProperties": false
            }
        }),
    ]
}

/// Parameters for list_indexes tool
#[derive(Debug, Deserialize)]
pub struct ListIndexesParams {
    #[serde(default = "default_limit")]
    pub limit: usize,
    #[serde(default)]
    pub offset: usize,
}

fn default_limit() -> usize {
    20
}

/// Parameters for get_index_info tool
#[derive(Debug, Deserialize)]
pub struct GetIndexInfoParams {
    pub index_uid: String,
}

/// Parameters for MCP search tool (simplified from SearchQuery)
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct McpSearchParams {
    pub index_uid: String,
    #[serde(default)]
    pub q: Option<String>,
    #[serde(default)]
    pub vector: Option<Vec<f32>>,
    #[serde(default)]
    pub hybrid: Option<HybridParams>,
    #[serde(default)]
    pub filter: Option<String>,
    #[serde(default)]
    pub sort: Option<Vec<String>>,
    #[serde(default = "DEFAULT_SEARCH_LIMIT")]
    pub limit: usize,
    #[serde(default = "DEFAULT_SEARCH_OFFSET")]
    pub offset: usize,
    #[serde(default)]
    pub attributes_to_retrieve: Option<Vec<String>>,
    #[serde(default)]
    pub attributes_to_highlight: Option<Vec<String>>,
    #[serde(default)]
    pub show_ranking_score: bool,
    #[serde(default)]
    pub ranking_score_threshold: Option<f64>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct HybridParams {
    pub embedder: String,
    #[serde(default = "default_semantic_ratio")]
    pub semantic_ratio: f32,
}

fn default_semantic_ratio() -> f32 {
    0.5
}

/// Execute a tool call
pub async fn execute_tool(
    tool_name: &str,
    arguments: Value,
    index_scheduler: Arc<IndexScheduler>,
) -> Result<Value, McpError> {
    match tool_name {
        "meilisearch_list_indexes" => {
            let params: ListIndexesParams = serde_json::from_value(arguments)?;
            list_indexes(params, index_scheduler).await
        }
        "meilisearch_get_index_info" => {
            let params: GetIndexInfoParams = serde_json::from_value(arguments)?;
            get_index_info(params, index_scheduler).await
        }
        "meilisearch_search" => {
            let params: McpSearchParams = serde_json::from_value(arguments)?;
            search(params, index_scheduler).await
        }
        _ => Err(McpError::MethodNotFound(format!("Unknown tool: {}", tool_name))),
    }
}

/// List available indexes
async fn list_indexes(
    params: ListIndexesParams,
    index_scheduler: Arc<IndexScheduler>,
) -> Result<Value, McpError> {
    let indexes = index_scheduler
        .index_names()
        .map_err(|e| McpError::InternalError(format!("Failed to list indexes: {}", e)))?;

    let total = indexes.len();
    let indexes: Vec<_> = indexes
        .into_iter()
        .skip(params.offset)
        .take(params.limit)
        .collect();

    let mut results = Vec::new();
    for index_uid in indexes {
        if let Ok(index) = index_scheduler.index(&index_uid) {
            let rtxn = index
                .read_txn()
                .map_err(|e| McpError::InternalError(format!("Failed to read index: {}", e)))?;

            let primary_key = index.primary_key(&rtxn).ok().flatten().map(String::from);
            let number_of_documents = index.number_of_documents(&rtxn).unwrap_or(0);

            // Get creation/update times from index scheduler stats
            let stats = index_scheduler.index_stats(&index_uid).ok();

            results.push(json!({
                "uid": index_uid,
                "primaryKey": primary_key,
                "numberOfDocuments": number_of_documents,
                "createdAt": stats.as_ref().map(|s| s.inner_stats.created_at.to_string()),
                "updatedAt": stats.as_ref().map(|s| s.inner_stats.updated_at.to_string()),
            }));
        }
    }

    Ok(json!({
        "results": results,
        "offset": params.offset,
        "limit": params.limit,
        "total": total,
    }))
}

/// Get detailed index information
async fn get_index_info(
    params: GetIndexInfoParams,
    index_scheduler: Arc<IndexScheduler>,
) -> Result<Value, McpError> {
    let index_uid = IndexUid::try_from(params.index_uid.clone())
        .map_err(|_| McpError::InvalidParameter("index_uid".to_string(), "Invalid index UID format".to_string()))?;

    let index = index_scheduler
        .index(&index_uid.to_string())
        .map_err(|_| {
            McpError::IndexNotFound(params.index_uid)
        })?;

    let rtxn = index
        .read_txn()
        .map_err(|e| McpError::InternalError(format!("Failed to read index: {}", e)))?;

    // Get settings
    let primary_key = index.primary_key(&rtxn).ok().flatten().map(String::from);
    let number_of_documents = index.number_of_documents(&rtxn).unwrap_or(0);

    let searchable_attributes: Vec<String> = index
        .user_defined_searchable_fields(&rtxn)
        .ok()
        .flatten()
        .map(|attrs| attrs.into_iter().map(String::from).collect())
        .unwrap_or_else(|| vec!["*".to_string()]);

    // Get filterable attributes from rules - serialize each rule to get string representation
    let filterable_attributes: Vec<String> = index
        .filterable_attributes_rules(&rtxn)
        .ok()
        .map(|rules| {
            rules.into_iter()
                .map(|rule| serde_json::to_string(&rule).unwrap_or_default())
                .collect()
        })
        .unwrap_or_default();

    let sortable_attributes: Vec<String> = index
        .sortable_fields(&rtxn)
        .ok()
        .map(|attrs| attrs.into_iter().collect())
        .unwrap_or_default();

    // Get embedder info
    let mut embedders = serde_json::Map::new();
    if let Ok(embedding_configs) = index.embedding_configs().embedding_configs(&rtxn) {
        for config in embedding_configs {
            embedders.insert(
                config.name.clone(),
                json!({
                    "source": format!("{:?}", config.config.embedder_options),
                    "quantized": config.config.quantized(),
                }),
            );
        }
    }

    Ok(json!({
        "uid": index_uid.to_string(),
        "primaryKey": primary_key,
        "numberOfDocuments": number_of_documents,
        "searchableAttributes": searchable_attributes,
        "filterableAttributes": filterable_attributes,
        "sortableAttributes": sortable_attributes,
        "embedders": embedders,
    }))
}

/// Perform search
async fn search(
    params: McpSearchParams,
    index_scheduler: Arc<IndexScheduler>,
) -> Result<Value, McpError> {
    let index_uid_str = params.index_uid.clone();
    let index_uid = IndexUid::try_from(params.index_uid.clone())
        .map_err(|_| McpError::InvalidParameter("index_uid".to_string(), "Invalid index UID format".to_string()))?;

    // Get the index
    let index = index_scheduler
        .index(&index_uid.to_string())
        .map_err(|_| McpError::IndexNotFound(params.index_uid.clone()))?;

    // Convert MCP search params to Meilisearch SearchQuery
    let search_query = SearchQuery {
        q: params.q,
        vector: params.vector,
        limit: params.limit,
        offset: params.offset,
        filter: params.filter.map(|f| serde_json::Value::String(f)),
        sort: params.sort,
        show_ranking_score: params.show_ranking_score,
        ranking_score_threshold: params.ranking_score_threshold.and_then(|t| {
            use crate::search::RankingScoreThreshold;
            RankingScoreThreshold::try_from(t).ok()
        }),
        attributes_to_retrieve: params.attributes_to_retrieve.map(|attrs| {
            attrs.into_iter().collect()
        }),
        attributes_to_highlight: params.attributes_to_highlight.map(|attrs| {
            attrs.into_iter().collect()
        }),
        hybrid: params.hybrid.map(|h| {
            use crate::search::{HybridQuery, SemanticRatio};
            HybridQuery {
                embedder: h.embedder,
                semantic_ratio: SemanticRatio::try_from(h.semantic_ratio)
                    .unwrap_or_else(|_| SemanticRatio::default()),
            }
        }),
        ..Default::default()
    };

    // Determine search kind (keyword, semantic, or hybrid)
    let search_kind_result = search_kind(&search_query, &index_scheduler, index_uid_str.clone(), &index)
        .map_err(|e| McpError::InternalError(format!("Failed to determine search kind: {}", e)))?;

    let retrieve_vectors = RetrieveVectors::new(search_query.retrieve_vectors);
    let request_uid = Uuid::now_v7();
    let features = index_scheduler.features();

    // Clone values needed for spawn_blocking
    let search_params = MeiliSearchParams {
        index_uid: index_uid_str,
        query: search_query,
        search_kind: search_kind_result,
        retrieve_vectors,
        features,
        request_uid,
        include_metadata: false,
    };

    // Perform the search in a blocking task
    let search_result = tokio::task::spawn_blocking(move || {
        perform_search(search_params, &index)
    })
    .await
    .map_err(|e| McpError::InternalError(format!("Search task failed: {}", e)))?
    .map_err(|e| McpError::InternalError(format!("Search failed: {}", e)))?;

    // Return the search result (first element of tuple is the result, second is time budget)
    let (result, _time_budget) = search_result;

    Ok(serde_json::to_value(result)?)
}
