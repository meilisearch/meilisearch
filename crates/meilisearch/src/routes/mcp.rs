use std::collections::BTreeMap;
use std::io::Cursor;
use std::sync::LazyLock;
use std::{fmt, mem};

use actix_web::web::{self, Data};
use actix_web::{FromRequest, HttpRequest, HttpResponse};
use deserr::actix_web::{AwebJson, AwebQueryParameter};
use deserr::{Deserr, IntoValue, Value, ValuePointerRef};
use either::Either;
use index_scheduler::IndexScheduler;
use meilisearch_types::batch_view::BatchView;
use meilisearch_types::deserr::{DeserrError, DeserrJson, DeserrJsonError};
use meilisearch_types::error::deserr_codes::BadRequest;
use meilisearch_types::error::ResponseError;
use serde::Serialize;
use serde_json::Number;
use utoipa::openapi::schema::{AdditionalProperties, ArrayItems, Components, Ref, Schema};
use utoipa::openapi::{ObjectBuilder, OpenApi, RefOr};
use utoipa::{OpenApi as _, ToSchema};

use crate::analytics::Analytics;
use crate::extractors::authentication::GuardedData;
use crate::routes::MeilisearchApi;
use crate::search_queue::SearchQueue;

static MEILISEARCH_OPEN_API: LazyLock<OpenApi> = LazyLock::new(|| MeilisearchApi::openapi());

#[routes::routes(
    tag = "MCP connection",
    routes(
        "" => post(mcp)
    ),
    tags((
        name = "MCP",
        description = "Model Context Protocol (MCP) is an open protocol that enables seamless integration between LLM applications and external data sources and tools.",
    )),
)]
pub struct McpApi;

/// Stream batches changes
///
/// The `/batches/stream` route returns information about [asynchronous operations](https://docs.meilisearch.com/learn/advanced/asynchronous_operations.html) (indexing, document updates, settings changes, and so on).
///
/// Batches are sent throught an SSE stream any time their progress or status changes, i.e., enqueued, processing, succeeded, failed.
#[routes::path(
    security(),
    request_body = McpQuery,
    responses(
        (status = 200, description = "Stream of batches changes.", body = BatchView, content_type = "application/x-ndjson", example = json!(
            {
                "uid": 0,
                "details": {
                    "receivedDocuments": 1,
                    "indexedDocuments": 1
                },
                "progress": null,
                "stats": {
                    "totalNbTasks": 1,
                    "status": {
                        "succeeded": 1
                    },
                    "types": {
                        "documentAdditionOrUpdate": 1
                    },
                    "indexUids": {
                        "INDEX_NAME": 1
                    }
                },
                "duration": "PT0.364788S",
                "startedAt": "2024-12-10T15:48:49.672141Z",
                "finishedAt": "2024-12-10T15:48:50.036929Z",
                "batchStrategy": "batched all enqueued tasks"
            }
        ))
    )
)]
async fn mcp(
    request: HttpRequest,
    index_scheduler: Data<IndexScheduler>,
    search_queue: web::Data<SearchQueue>,
    personalization_service: web::Data<crate::personalization::PersonalizationService>,
    body: AwebJson<McpQuery, DeserrJsonError>,
    analytics: web::Data<Analytics>,
) -> Result<HttpResponse, ResponseError> {
    index_scheduler.features().check_mcp_route("calling the /mcp route")?;

    let body = body.into_inner();
    let McpQuery { jsonrpc, id, method, mut params } = dbg!(body);

    let response = match method.as_str() {
        method::SERVER_DISCOVERY => McpResponse {
            jsonrpc,
            id,
            result: Some(McpResult {
                result_type: RESULT_TYPE_COMPLETE,
                is_error: None,
                supported_versions: Some(SUPPORTED_VERSIONS),
                tools: None,
                resources: None,
                prompts: None,
                meta: Some(McpServerMeta {
                    // TODO what's my name and version?
                    server_info: ClientServerInfo {
                        name: "Meilisearch".to_string(),
                        version: "1.53.0".to_string(),
                    },
                }),
                capabilities: Some(McpCapabilities { tools: Some(BTreeMap::new()), resources: None }),
                content: None,
                structured_content: None,
                instructions: Some(
                    "Meilisearch is a prefix search engine that supports filtering, sorting, federated searching (mixing results from different indexes).\
                    Meilisearch support classic keyword search but may also support semantic search throught the use of the hybrid search parameter.
                    You can find more information about available embedders for a given index when describing an index.\
                    We recommend you to use the listIndexes, describeIndex, and searchInIndexes tools, in this order to fetch the right informations from the available indexes.".to_string()
                ),
                ttl_ms: 300_000, // 5min
                cache_scope: cache_scope::PRIVATE,
            }),
            error: None,
        },
        method::TOOLS_LIST => McpResponse {
            jsonrpc,
            id,
            result: Some(McpResult {
                result_type: RESULT_TYPE_COMPLETE,
                is_error: None,
                content: None,
                structured_content: None,
                tools: Some(list_tools()),
                resources: None,
                prompts: None,
                supported_versions: None,
                meta: None,
                capabilities: None,
                instructions: None,
                ttl_ms: 300_000, // TODO 86_400_000, // 24h
                cache_scope: cache_scope::PRIVATE,
            }),
            error: None,
        },
        method::RESOURCES_LIST => McpResponse {
            jsonrpc,
            id,
            result: Some(McpResult {
                result_type: RESULT_TYPE_COMPLETE,
                is_error: None,
                content: None,
                structured_content: None,
                tools: None,
                resources: Some(vec![]),
                prompts: None,
                supported_versions: None,
                meta: None,
                capabilities: None,
                instructions: None,
                ttl_ms: 300_000, // TODO 86_400_000, // 24h
                cache_scope: cache_scope::PRIVATE,
            }),
            error: None,
        },
        method::PROMPTS_LIST => McpResponse {
            jsonrpc,
            id,
            result: Some(McpResult {
                result_type: RESULT_TYPE_COMPLETE,
                is_error: None,
                content: None,
                structured_content: None,
                tools: None,
                resources: None,
                prompts: Some(vec![]),
                supported_versions: None,
                meta: None,
                capabilities: None,
                instructions: None,
                ttl_ms: 300_000, // TODO 86_400_000, // 24h
                cache_scope: cache_scope::PRIVATE,
            }),
            error: None,
        },
        method::TOOLS_CALL => match params.name.as_deref() {
            Some(tool_name::SEARCH_IN_INDEXES) => {
                // request
                // TODO it cannot fail, right? right!?
                let query = serde_json::to_vec(&params.arguments.unwrap_or_default()).unwrap();
                let mut payload = actix_web::dev::Payload::from(query);

                // // TODO don't unwrap
                let guarded_index_scheduler =
                    GuardedData::from_request(&request, &mut payload).await.unwrap();
                // TODO don't unwrap
                let params = AwebJson::from_request(&request, &mut payload).await.unwrap();

                let result = super::multi_search::multi_search_with_post(
                    guarded_index_scheduler,
                    search_queue,
                    personalization_service,
                    params,
                    request,
                    analytics,
                )
                .await;

                match result {
                    Ok(response) => {
                        let body = response.into_body();
                        // TODO do not unwrap
                        let bytes = actix_web::body::to_bytes(body).await.unwrap();
                        let text = String::from_utf8_lossy(&bytes).into_owned();
                        // TODO this blocks and would have been better to have a serde_json
                        //      RawValue to avoid allocating too much and simply pass through
                        let content = serde_json::from_reader(Cursor::new(bytes)).unwrap();
                        McpResponse {
                            jsonrpc,
                            id,
                            result: Some(McpResult {
                                result_type: RESULT_TYPE_COMPLETE,
                                is_error: None,
                                tools: None,
                                resources: None,
                                prompts: None,
                                content: Some(vec![McpTextContentOutput::from(text)]),
                                structured_content: Some(content),
                                supported_versions: None,
                                meta: None,
                                capabilities: None,
                                instructions: None,
                                ttl_ms: 0, // immediately stale
                                cache_scope: cache_scope::PRIVATE,
                            }),
                            error: None,
                        }
                    }
                    Err(response) => {
                        tracing::error!("{response:?}");
                        McpResponse {
                            jsonrpc,
                            id,
                            result: Some(McpResult {
                                result_type: RESULT_TYPE_COMPLETE,
                                is_error: None,
                                tools: None,
                                resources: None,
                                prompts: None,
                                structured_content: Some(serde_json::to_value(&response).unwrap()),
                                content: Some(vec![McpTextContentOutput::from(response.message)]),
                                supported_versions: None,
                                meta: None,
                                capabilities: None,
                                instructions: None,
                                ttl_ms: 0, // immediately stale
                                cache_scope: cache_scope::PRIVATE,
                            }),
                            error: None,
                        }
                    },
                }
            }
            Some(tool_name::FACET_SEARCH) => {
                // request
                // TODO it cannot fail, right? right!?
                let index_uid = match params.arguments.as_mut() {
                    Some(serde_json::Value::Object(object)) => {
                        // We remove the extra indexUid parameter to make sure the route accepts the payload
                        object.remove("indexUid").expect("missing indexUid parameter").as_str().unwrap().to_owned()
                    }
                    _ => panic!("Invalid arguments: expected Object found something else"),
                };

                let query = serde_json::to_vec(&params.arguments.unwrap_or_default()).unwrap();
                let mut payload = actix_web::dev::Payload::from(query);

                // TODO don't unwrap
                let guarded_index_scheduler =
                    GuardedData::from_request(&request, &mut payload).await.unwrap();
                // TODO don't unwrap
                let params = AwebJson::from_request(&request, &mut payload).await.unwrap();

                let result = super::indexes::facet_search::search(
                    guarded_index_scheduler,
                    search_queue,
                    index_uid.into(),
                    params,
                    request,
                    analytics,
                )
                .await;

                match result {
                    Ok(response) => {
                        let body = response.into_body();
                        // TODO do not unwrap
                        let bytes = actix_web::body::to_bytes(body).await.unwrap();
                        let text = String::from_utf8_lossy(&bytes).into_owned();
                        // TODO this blocks and would have been better to have a serde_json
                        //      RawValue to avoid allocating too much and simply pass through
                        let content = serde_json::from_reader(Cursor::new(bytes)).unwrap();
                        McpResponse {
                            jsonrpc,
                            id,
                            result: Some(McpResult {
                                result_type: RESULT_TYPE_COMPLETE,
                                is_error: None,
                                tools: None,
                                resources: None,
                                prompts: None,
                                content: Some(vec![McpTextContentOutput::from(text)]),
                                structured_content: Some(content),
                                supported_versions: None,
                                meta: None,
                                capabilities: None,
                                instructions: None,
                                ttl_ms: 0, // immediately stale
                                cache_scope: cache_scope::PRIVATE,
                            }),
                            error: None,
                        }
                    }
                    Err(response) => {
                        tracing::error!("{response:?}");
                        McpResponse {
                        jsonrpc,
                        id,
                        result: Some(McpResult {
                            result_type: RESULT_TYPE_COMPLETE,
                            is_error: None,
                            tools: None,
                            resources: None,
                            prompts: None,
                            structured_content: Some(serde_json::to_value(&response).unwrap()),
                            content: Some(vec![McpTextContentOutput::from(response.message)]),
                            supported_versions: None,
                            meta: None,
                            capabilities: None,
                            instructions: None,
                            ttl_ms: 0, // immediately stale
                            cache_scope: cache_scope::PRIVATE,
                        }),
                        error: None,
                    }
                    },
                }
            }
            Some(tool_name::LIST_INDEXES) => {
                #[derive(Serialize)]
                #[serde(rename_all = "camelCase")]
                struct Pagination {
                    #[serde(skip_serializing_if = "Option::is_none")]
                    offset: Option<u64>,
                    #[serde(skip_serializing_if = "Option::is_none")]
                    limit: Option<u64>,
                }

                let pagination = match params.arguments.as_mut() {
                    Some(serde_json::Value::Object(object)) => {
                        // We remove the extra indexUid parameter to make sure the route accepts the payload
                        // TODO better error message in case the time is invalid
                        let offset = object.remove("offset").and_then(|off| off.as_u64());
                        let limit = object.remove("limit").and_then(|limit| limit.as_u64());
                        Pagination { offset, limit }
                    }
                    _ => panic!("Invalid arguments: expected Object found something else"),
                };

                // // TODO don't unwrap
                let mut payload = actix_web::dev::Payload::None;
                let guarded_index_scheduler =
                    GuardedData::from_request(&request, &mut payload).await.unwrap();
                let query = serde_urlencoded::to_string(&pagination).unwrap();
                // TODO don't unwrap
                let paginate = AwebQueryParameter::from_query(&query).unwrap();

                let result = super::indexes::list_indexes(guarded_index_scheduler, paginate).await;

                match result {
                    Ok(response) => {
                        let body = response.into_body();
                        // TODO do not unwrap
                        let bytes = actix_web::body::to_bytes(body).await.unwrap();
                        let text = String::from_utf8_lossy(&bytes).into_owned();
                        // TODO this blocks and would have been better to have a serde_json
                        //      RawValue to avoid allocating too much and simply pass through
                        let content = serde_json::from_reader(Cursor::new(bytes)).unwrap();
                        McpResponse {
                            jsonrpc,
                            id,
                            result: Some(McpResult {
                                result_type: RESULT_TYPE_COMPLETE,
                                is_error: None,
                                tools: None,
                                resources: None,
                                prompts: None,
                                content: Some(vec![McpTextContentOutput::from(text)]),
                                structured_content: Some(content),
                                supported_versions: None,
                                meta: None,
                                capabilities: None,
                                instructions: None,
                                ttl_ms: 300_000, // 5min
                                cache_scope: cache_scope::PRIVATE,
                            }),
                            error: None,
                        }
                    }
                    Err(response) => {
                        tracing::error!("{response:?}");
                        McpResponse {
                            jsonrpc,
                            id,
                            result: Some(McpResult {
                                result_type: RESULT_TYPE_COMPLETE,
                                is_error: None,
                                tools: None,
                                resources: None,
                                prompts: None,
                                structured_content: Some(serde_json::to_value(&response).unwrap()),
                                content: Some(vec![McpTextContentOutput::from(response.message)]),
                                supported_versions: None,
                                meta: None,
                                capabilities: None,
                                instructions: None,
                                ttl_ms: 0, // immediately stale
                                cache_scope: cache_scope::PRIVATE,
                            }),
                            error: None,
                        }
                    },
                }
            }
            // TODO <https://modelcontextprotocol.io/specification/2026-07-28/server/tools#error-handling>
            Some(_unknown) => todo!("Unknown tool. What to do?"),
            None => todo!("no params name. what do to?"),
        },
        _otherwise => {
            todo!("unwkown method: {_otherwise}")
        }
    };

    eprintln!("{}", serde_json::to_string_pretty(&response).unwrap());

    Ok(HttpResponse::Ok().json(response))
}

pub mod method {
    pub const SERVER_DISCOVERY: &str = "server/discover";
    pub const TOOLS_CALL: &str = "tools/call";
    pub const TOOLS_LIST: &str = "tools/list";
    pub const RESOURCES_LIST: &str = "resources/list";
    pub const PROMPTS_LIST: &str = "prompts/list";
}

pub mod tool_name {
    pub const LIST_INDEXES: &str = "listIndexes";
    pub const DESCRIBE_INDEXES: &str = "describeIndexes";
    pub const SEARCH_IN_INDEXES: &str = "searchInIndexes";
    pub const FACET_SEARCH: &str = "facetSearch";
}

pub mod cache_scope {
    pub const PUBLIC: &str = "public";
    pub const PRIVATE: &str = "private";
}

fn list_tools() -> Vec<McpToolDefinition> {
    vec![
        {
            // search in indexes
            let route = "/multi-search";
            let paths = MEILISEARCH_OPEN_API.paths.paths.get(route).unwrap();
            let components = MEILISEARCH_OPEN_API.components.as_ref().unwrap();
            let operation = paths.post.as_ref().unwrap();
            let request_body = operation.request_body.as_ref().unwrap();
            let content = request_body.content.get("application/json").unwrap();
            let ref_or_schema = content.schema.clone().unwrap();
            let schema = clean_refs_from_schema(components, ref_or_schema).unwrap();

            McpToolDefinition {
                name: tool_name::SEARCH_IN_INDEXES.to_string(),
                title: operation.summary.clone().unwrap(),
                description: operation.description.clone().unwrap(),
                // TODO maybe add more information about how to do filtering and such?
                //      It is probably better to explain it in the OpenAPI description or examples maybe?
                input_schema: schema,
            }
        },
        {
            // facet search
            let route = "/indexes/{index_uid}/facet-search";
            let paths = MEILISEARCH_OPEN_API.paths.paths.get(route).unwrap();
            let components = MEILISEARCH_OPEN_API.components.as_ref().unwrap();
            let operation = paths.post.as_ref().unwrap();
            let request_body = operation.request_body.as_ref().unwrap();
            let content = request_body.content.get("application/json").unwrap();
            let ref_or_schema = content.schema.clone().unwrap();
            let mut schema = clean_refs_from_schema(components, ref_or_schema).unwrap();

            // We modify the schema's properties a bit to expose
            // the original-in-the-path index uid.
            if let Some(param) = operation
                .parameters
                .as_ref()
                .unwrap()
                .iter()
                .find(|param| param.name == "index_uid")
            {
                if let Schema::Object(object) = &mut schema {
                    let field_name = "indexUid";
                    let ref_or_schema = param.schema.clone().unwrap();
                    let mut schema = clean_refs_from_schema(components, ref_or_schema).unwrap();
                    if let Schema::Object(object) = &mut schema {
                        object.description = param.description.clone();
                    }
                    // Insert this new mandatory field at the begining
                    object.properties.insert_before(0, field_name.to_string(), RefOr::T(schema));
                    object.required.push(field_name.to_string());
                }
            }

            McpToolDefinition {
                name: tool_name::FACET_SEARCH.to_string(),
                title: operation.summary.clone().unwrap(),
                description: operation.description.clone().unwrap(),
                input_schema: schema,
            }
        },
        {
            // list indexes
            let route = "/indexes";
            let paths = MEILISEARCH_OPEN_API.paths.paths.get(route).unwrap();
            let components = MEILISEARCH_OPEN_API.components.as_ref().unwrap();
            let operation = paths.get.as_ref().unwrap();

            let mut properties = ObjectBuilder::new();
            for parameter in operation.parameters.as_ref().unwrap() {
                let ref_or_schema = parameter.schema.as_ref().unwrap().clone();
                let mut schema = clean_refs_from_schema(components, ref_or_schema).unwrap();
                if let Schema::Object(object) = &mut schema {
                    object.description = parameter.description.clone();
                }
                properties = properties.property(&parameter.name, schema);
            }

            let schema = Schema::from(properties);

            McpToolDefinition {
                name: tool_name::LIST_INDEXES.to_string(),
                title: operation.summary.clone().unwrap(),
                description: operation.description.clone().unwrap(),
                input_schema: schema,
            }
        },
    ]
}

#[routes::request]
#[derive(Debug, Clone)]
pub struct McpQuery {
    #[request(required)]
    jsonrpc: String,
    #[request(required)]
    id: RequestId,
    #[request(required)]
    method: String, // server/discover, tools/list, resources/list
    #[request(required)]
    params: ParamsWithMeta,
}

// TODO Note that I would have rather refused unknown fields
//      but online playgrounds provide more fields than expected
//      <https://mcpplaygroundonline.com>
#[routes::request(allow_unknown_fields)]
#[derive(Debug, Clone)]
pub struct ParamsWithMeta {
    // TODO Note that this field MUST be provided but most playground don't
    //      <https://mcpplaygroundonline.com>
    #[request(default, rename = "_meta")]
    meta: Option<McpClientMeta>,
    #[request(default)]
    name: Option<String>, // get_weather
    #[request(default)]
    arguments: Option<serde_json::Value>, // RawValue would have been better
}

#[routes::request(allow_unknown_fields)]
#[derive(Debug, Clone)]
pub struct McpClientMeta {
    #[request(required, rename = "io.modelcontextprotocol/protocolVersion")]
    protocol_version: String, // "2026-07-28"
    #[request(required, rename = "io.modelcontextprotocol/clientInfo")]
    client_info: ClientServerInfo,
    #[request(default, rename = "io.modelcontextprotocol/clientCapabilities")]
    client_capabilities: serde_json::Value,
}

#[routes::request]
#[derive(Debug, Clone, Serialize)]
pub struct ClientServerInfo {
    #[request(required)]
    name: String, // "ExampleClient"
    #[request(required)]
    version: String, // "1.0.0"
}

#[derive(Clone, Debug, Serialize)]
#[serde(transparent)]
pub struct RequestId {
    #[serde(serialize_with = "either::serde_untagged::serialize")]
    inner: Either<Number, String>,
}

impl Deserr<DeserrError<DeserrJson, BadRequest>> for RequestId {
    fn deserialize_from_value<V: IntoValue>(
        value: Value<V>,
        _location: ValuePointerRef,
    ) -> Result<Self, DeserrError<DeserrJson, BadRequest>> {
        let inner = match value {
            Value::Integer(x) => Either::Left(Number::from(x)),
            Value::NegativeInteger(x) => Either::Left(Number::from(x)),
            Value::Float(x) => Either::Left(Number::from_f64(x).unwrap()), // TODO don't unwrap
            Value::String(string) => Either::Right(string),
            _otherwise => todo!(),
        };

        Ok(RequestId { inner })
    }
}

impl utoipa::ToSchema for RequestId {
    fn name() -> std::borrow::Cow<'static, str> {
        std::borrow::Cow::Borrowed("RequestId")
    }
}
impl utoipa::PartialSchema for RequestId {
    fn schema() -> utoipa::openapi::RefOr<utoipa::openapi::schema::Schema> {
        utoipa::openapi::OneOfBuilder::new()
            .item(
                utoipa::openapi::ObjectBuilder::new()
                    .schema_type(utoipa::openapi::schema::Type::Integer),
            )
            .item(
                utoipa::openapi::ObjectBuilder::new()
                    .schema_type(utoipa::openapi::schema::Type::String),
            )
            .description(Some(
                "The request ID MUST NOT match the ID of any other request \
                the sender has issued and not yet received a response for",
            ))
            .into()
    }
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct McpResponse {
    jsonrpc: String,
    id: RequestId,
    #[serde(skip_serializing_if = "Option::is_none")]
    result: Option<McpResult>,
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<McpError>,
}

// manual impl: not sure why we need the Serialize derive
impl routes::RequestBody for RequestId {}

// TODO prefer using an enum, but utoipa is not cool with it
const RESULT_TYPE_COMPLETE: &str = "complete";
const SUPPORTED_VERSIONS: &[&str] = &["2026-07-28"];
// const RESULT_TYPE_INPUT_REQUIRED: &str = "input_required";

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct McpResult {
    result_type: &'static str, // "complete", "input_required"
    #[serde(skip_serializing_if = "Option::is_none")]
    is_error: Option<bool>,
    /// Protocol versions the server supports. The client should choose one of these for subsequent requests.
    #[serde(skip_serializing_if = "Option::is_none")]
    supported_versions: Option<&'static [&'static str]>,
    #[serde(skip_serializing_if = "Option::is_none")]
    tools: Option<Vec<McpToolDefinition>>,
    // Note that for know we will simply return an empty list of resources
    #[serde(skip_serializing_if = "Option::is_none")]
    resources: Option<Vec<()>>,
    // Note that for know we will simply return an empty list of prompt
    #[serde(skip_serializing_if = "Option::is_none")]
    prompts: Option<Vec<()>>,
    #[serde(rename = "_meta", skip_serializing_if = "Option::is_none")]
    meta: Option<McpServerMeta>,
    /// Capabilities the server supports (tools, resources, prompts, etc.).
    #[serde(skip_serializing_if = "Option::is_none")]
    capabilities: Option<McpCapabilities>,
    #[serde(skip_serializing_if = "Option::is_none")]
    content: Option<Vec<McpTextContentOutput>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    structured_content: Option<serde_json::Value>,
    /// Optional natural-language guidance for LLMs on how to use this server effectively.
    #[serde(skip_serializing_if = "Option::is_none")]
    instructions: Option<String>,
    // <https://modelcontextprotocol.io/specification/2026-07-28/server/utilities/caching#cacheable-model>
    ttl_ms: usize,             // 300000
    cache_scope: &'static str, // public | private
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct McpServerMeta {
    #[serde(rename = "io.modelcontextprotocol/serverInfo")]
    server_info: ClientServerInfo,
}

// Note that those fields are just a way to display the
// capabilities and must always stay empty or not shown at all.
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct McpCapabilities {
    #[serde(skip_serializing_if = "Option::is_none")]
    tools: Option<BTreeMap<(), ()>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    resources: Option<BTreeMap<(), ()>>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct McpTextContentOutput {
    r#type: &'static str, // text
    text: String,
}

impl From<String> for McpTextContentOutput {
    fn from(text: String) -> Self {
        McpTextContentOutput { r#type: "text", text }
    }
}

/// <https://modelcontextprotocol.io/specification/2026-07-28/server/tools#data-types>
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct McpToolDefinition {
    /// Unique identifier for the tool.
    name: String,
    /// Optional human-readable name of the tool for display purposes.
    title: String,
    /// Human-readable description of functionality.
    description: String,
    // icons (optional)
    /// JSON Schema defining expected parameters.
    input_schema: Schema,
    // outputSchema (optional)
    // annotations (optional)
}

impl fmt::Debug for McpToolDefinition {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("McpToolDefinition")
            .field("name", &self.name)
            .field("title", &self.title)
            .field("description", &self.description)
            .finish()
    }
}

#[derive(Debug, Serialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct McpError {
    code: u64,
    message: String,
    data: McpErrorData, // Can be any JSON value
}

#[derive(Debug, Serialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct McpErrorData {
    status_code: u16,
    error_name: String,
    error_type: String,
    error_link: String,
}

impl From<&ResponseError> for McpErrorData {
    fn from(response_error: &ResponseError) -> Self {
        McpErrorData {
            status_code: response_error.status_code().as_u16(),
            error_name: response_error.error_name().to_string(),
            error_type: response_error.error_type().to_string(),
            error_link: response_error.error_link().to_string(),
        }
    }
}

fn ref_to_schema<'a>(components: &'a Components, r#ref: &Ref) -> Option<&'a Schema> {
    let location = r#ref.ref_location.strip_prefix("#/components/schemas/")?;
    match components.schemas.get(location)? {
        RefOr::Ref(r#ref) => ref_to_schema(components, r#ref),
        RefOr::T(schema) => Some(schema),
    }
}

fn clean_refs_from_schema(components: &Components, schema: RefOr<Schema>) -> Option<Schema> {
    let mut schema = match schema {
        RefOr::Ref(r#ref) => ref_to_schema(components, &r#ref)?.clone(),
        RefOr::T(schema) => schema,
    };

    match schema {
        Schema::Array(ref mut array) => {
            array.items = match mem::replace(&mut array.items, ArrayItems::False) {
                ArrayItems::RefOrSchema(ref_or_schema) => {
                    let schema = clean_refs_from_schema(components, *ref_or_schema)?;
                    ArrayItems::RefOrSchema(Box::new(RefOr::T(schema)))
                }
                ArrayItems::False => ArrayItems::False,
            };
        }
        Schema::Object(ref mut object) => {
            object.properties = mem::take(&mut object.properties)
                .into_iter()
                .map(|(property, schema)| {
                    clean_refs_from_schema(components, schema)
                        .map(|schema| (property, RefOr::T(schema)))
                })
                .collect::<Option<_>>()?;

            object.additional_properties = match mem::take(&mut object.additional_properties) {
                Some(props) => match *props {
                    AdditionalProperties::RefOr(r#ref) => {
                        let schema = clean_refs_from_schema(&components, r#ref)?;
                        Some(Box::new(AdditionalProperties::RefOr(RefOr::T(schema))))
                    }
                    AdditionalProperties::FreeForm(yes) => {
                        Some(Box::new(AdditionalProperties::FreeForm(yes)))
                    }
                },
                None => None,
            };
        }
        Schema::OneOf(ref mut one_of) => {
            one_of.items = mem::take(&mut one_of.items)
                .into_iter()
                .map(|schema| clean_refs_from_schema(components, schema).map(RefOr::T))
                .collect::<Option<_>>()?;
        }
        Schema::AllOf(ref mut all_of) => {
            all_of.items = mem::take(&mut all_of.items)
                .into_iter()
                .map(|schema| clean_refs_from_schema(components, schema).map(RefOr::T))
                .collect::<Option<_>>()?;
        }
        Schema::AnyOf(ref mut any_of) => {
            any_of.items = mem::take(&mut any_of.items)
                .into_iter()
                .map(|schema| clean_refs_from_schema(components, schema).map(RefOr::T))
                .collect::<Option<_>>()?;
        }
        _ => return None,
    };

    Some(schema)
}
