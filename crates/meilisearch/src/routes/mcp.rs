use std::io::Cursor;
use std::mem;
use std::sync::LazyLock;

use actix_web::web::{self, Data};
use actix_web::{FromRequest, HttpRequest, HttpResponse};
use deserr::actix_web::AwebJson;
use index_scheduler::IndexScheduler;
use meilisearch_types::batch_view::BatchView;
use meilisearch_types::deserr::DeserrJsonError;
use meilisearch_types::error::ResponseError;
use serde::Serialize;
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

    let McpQuery { jsonrpc, id, method, params } = body.into_inner();
    let response = match method.as_str() {
        method::SERVER_DISCOVERY => {
            McpResponse {
                jsonrpc,
                id,
                result: Some(McpResult {
                    result_type: RESULT_TYPE_COMPLETE,
                    content: None,
                    tools: Some(vec![
                        {
                            // search in indexes
                            let route = "/multi-search";
                            let paths = MEILISEARCH_OPEN_API.paths.paths.get(route).unwrap();
                            let components = MEILISEARCH_OPEN_API.components.as_ref().unwrap();
                            let operation = paths.post.as_ref().unwrap();
                            let request_body = operation.request_body.as_ref().unwrap();
                            let content = request_body.content.get("application/json").unwrap();
                            let ref_or_schema = content.schema.clone().unwrap();
                            let schema = clean_refs_from_schema(components, ref_or_schema);

                            McpToolDefinition {
                                name: tool_name::SEARCH_IN_INDEXES.to_string(),
                                title: operation.summary.clone().unwrap(),
                                description: operation.description.clone().unwrap(),
                                input_schema: {
                                    // TODO maybe add more information about how to do filtering and such?
                                    //      It is probably better to explain it in the OpenAPI description or examples maybe?
                                    serde_json::json!({
                                        "type": "object",
                                        "properties": schema,
                                    })
                                },
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
                            let mut schema = clean_refs_from_schema(components, ref_or_schema);

                            // We modify the schema's properties a bit to expose
                            // the original-in-the-path index uid.
                            if let Some(param) = operation
                                .parameters
                                .as_ref()
                                .unwrap()
                                .iter()
                                .find(|param| param.name == "index_uid")
                            {
                                if let Some(Schema::Object(object)) = &mut schema {
                                    object.properties.insert(
                                        "indexUid".to_string(),
                                        param.schema.clone().unwrap(),
                                    );
                                }
                            }

                            McpToolDefinition {
                                name: tool_name::FACET_SEARCH.to_string(),
                                title: operation.summary.clone().unwrap(),
                                description: operation.description.clone().unwrap(),
                                input_schema: serde_json::json!({
                                    "type": "object",
                                    "properties": schema,
                                }),
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
                                let mut schema =
                                    clean_refs_from_schema(components, ref_or_schema).unwrap();
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
                                input_schema: serde_json::to_value(schema).unwrap(),
                            }
                        },
                    ]),
                }),
                error: None,
            }
        }
        method::TOOLS_CALL => match params.name.as_deref() {
            Some(tool_name::SEARCH_IN_INDEXES) => {
                // request
                // TODO it cannot fail, right? right!?
                let multi_search_query =
                    serde_json::to_vec(&params.arguments.unwrap_or_default()).unwrap();
                let mut payload = actix_web::dev::Payload::from(multi_search_query);

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
                        // TODO this blocks and would have been better to have a serde_json
                        //      RawValue to avoid allocating too much and simply pass through
                        let content = serde_json::from_reader(Cursor::new(bytes)).unwrap();
                        McpResponse {
                            jsonrpc,
                            id,
                            result: Some(McpResult {
                                result_type: RESULT_TYPE_COMPLETE,
                                tools: None,
                                content,
                            }),
                            error: None,
                        }
                    }
                    Err(response) => McpResponse {
                        jsonrpc,
                        id,
                        result: None,
                        error: Some(McpError {
                            code: 0,
                            message: response.message().to_string(),
                            data: McpErrorData::from(&response),
                        }),
                    },
                }
            }
            Some(tool_name::FACET_SEARCH) => {
                // request
                // TODO it cannot fail, right? right!?
                let index_uid = params.arguments.as_ref().unwrap()["indexUid"].to_string();
                let multi_search_query =
                    serde_json::to_vec(&params.arguments.unwrap_or_default()).unwrap();
                let mut payload = actix_web::dev::Payload::from(multi_search_query);

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
                        // TODO this blocks and would have been better to have a serde_json
                        //      RawValue to avoid allocating too much and simply pass through
                        let content = serde_json::from_reader(Cursor::new(bytes)).unwrap();
                        McpResponse {
                            jsonrpc,
                            id,
                            result: Some(McpResult {
                                result_type: RESULT_TYPE_COMPLETE,
                                tools: None,
                                content,
                            }),
                            error: None,
                        }
                    }
                    Err(response) => McpResponse {
                        jsonrpc,
                        id,
                        result: None,
                        error: Some(McpError {
                            code: 0,
                            message: response.message().to_string(),
                            data: McpErrorData::from(&response),
                        }),
                    },
                }
            }
            Some(_unknown) => todo!("Unknown tool. What to do?"),
            None => todo!("no params name. what do to?"),
        },
        _otherwise => {
            todo!("break and send an error")
        }
    };

    Ok(HttpResponse::Ok().json(response))
}

pub mod method {
    pub const SERVER_DISCOVERY: &str = "server/discover";
    pub const TOOLS_CALL: &str = "tools/call";
}

pub mod tool_name {
    pub const LIST_INDEXES: &str = "listIndexes";
    pub const DESCRIBE_INDEXES: &str = "describeIndexes";
    pub const SEARCH_IN_INDEXES: &str = "searchInIndexes";
    pub const FACET_SEARCH: &str = "facetSearch";
}

#[routes::request]
#[derive(Debug, Clone, PartialEq)]
pub struct McpQuery {
    #[request(required)]
    jsonrpc: String,
    #[request(required)]
    id: String, // RequestId: String | Number
    #[request(required)]
    method: String, // server/discover, tools/list, resources/list
    #[request(required)]
    params: ParamsWithMeta,
}

#[routes::request]
#[derive(Debug, Clone, PartialEq)]
pub struct ParamsWithMeta {
    #[request(required, rename = "_meta")]
    meta: Meta,
    #[request(default)]
    name: Option<String>, // get_weather
    #[request(default)]
    arguments: Option<serde_json::Value>, // RawValue would have been better
}

#[routes::request]
#[derive(Debug, Clone, PartialEq)]
pub struct Meta {
    #[request(required, rename = "io.modelcontextprotocol/protocolVersion")]
    protocol_version: String, // "2026-07-28"
    #[request(required, rename = "io.modelcontextprotocol/clientInfo")]
    client_info: ClientInfo,
    #[request(default, rename = "io.modelcontextprotocol/clientCapabilities")]
    client_capabilities: serde_json::Value,
}

#[routes::request]
#[derive(Debug, Clone, PartialEq)]
pub struct ClientInfo {
    #[request(required)]
    name: String, // "ExampleClient"
    #[request(required)]
    version: String, // "1.0.0"
}

#[derive(Debug, Serialize, ToSchema)]
pub struct McpResponse {
    jsonrpc: String,
    id: String, // RequestId: String | Number
    #[serde(skip_serializing_if = "Option::is_none")]
    result: Option<McpResult>,
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<McpError>,
}

// TODO prefer using an enum, but utoipa is not cool with it
const RESULT_TYPE_COMPLETE: &str = "complete";
// const RESULT_TYPE_INPUT_REQUIRED: &str = "input_required";

#[derive(Debug, Serialize, ToSchema)]
pub struct McpResult {
    result_type: &'static str, // "complete", "input_required"
    #[serde(skip_serializing_if = "Option::is_none")]
    tools: Option<Vec<McpToolDefinition>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    content: Option<serde_json::Value>,
}

/// <https://modelcontextprotocol.io/specification/2026-07-28/server/tools#data-types>
#[derive(Debug, Serialize, ToSchema)]
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
    input_schema: serde_json::Value,
    // outputSchema (optional)
    // annotations (optional)
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

// TODO that's the way to go
// #[derive(Debug, Serialize, ToSchema)]
// pub struct RequestId(Either<Number, String>);
