use std::collections::BTreeMap;
use std::io::Cursor;
use std::sync::LazyLock;
use std::{fmt, mem};

use actix_web::web::{self, Data};
use actix_web::{FromRequest, HttpRequest, HttpResponse};
use anyhow::Context as _;
use deserr::actix_web::{AwebJson, AwebQueryParameter};
use deserr::{Deserr, IntoValue, Value, ValuePointerRef};
use either::Either;
use index_scheduler::IndexScheduler;
use meilisearch_types::deserr::{DeserrError, DeserrJson, DeserrJsonError};
use meilisearch_types::error::deserr_codes::BadRequest;
use meilisearch_types::error::Code::BadParameter;
use meilisearch_types::error::ResponseError;
use meilisearch_types::index_uid::IndexUid;
use meilisearch_types::milli;
use serde::Serialize;
use serde_json::Number;
use utoipa::openapi::path::Operation;
use utoipa::openapi::schema::{AdditionalProperties, ArrayItems, Components, Ref, Schema};
use utoipa::openapi::{ObjectBuilder, OpenApi, RefOr};
use utoipa::{OpenApi as _, PartialSchema, ToSchema};

use crate::analytics::segment_analytics::extract_user_agents;
use crate::analytics::Analytics;
use crate::extractors::authentication::GuardedData;
use crate::routes::mcp_analytics::McpAggregator;
use crate::routes::MeilisearchApi;
use crate::search::elapsed;
use crate::search_queue::SearchQueue;

static MEILISEARCH_OPEN_API: LazyLock<OpenApi> = LazyLock::new(MeilisearchApi::openapi);

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

/// Model context protocol (MCP)
///
/// The `/mcp` route exposes [the MCP open protocol](https://modelcontextprotocol.io) that enables seamless integration between LLM
/// applications and external data sources and tools.
#[routes::path(
    security(),
    request_body = McpQuery,
    responses(
        (status = 200, description = "Stream of batches changes.", body = McpResponse, content_type = "application/json", example = json!(
            {
                "jsonrpc": "2.0",
                "id": 42,
                "result": {
                    "resultType": "complete",
                    "supportedVersions": ["2026-07-28"],
                    "capabilities": {
                        "tools": {}
                    },
                    "_meta": {
                        "io.modelcontextprotocol/serverInfo": {
                        "name": "Meilisearch",
                        "version": "1.52.0"
                        }
                    },
                    "instructions": "This is a Meilisearch instance that is capable of returning documents based on a search query.",
                    "ttlMs": 3_600_000,
                    "cacheScope": "public"
                }
            }
        )),
        (status = 401, description = "The authorization header is missing.", body = ResponseError, content_type = "application/json", example = json!(
            {
                "message": "The Authorization header is missing. It must use the bearer authorization method.",
                "code": "missing_authorization_header",
                "type": "auth",
                "link": "https://docs.meilisearch.com/errors#missing_authorization_header"
            }
        )),
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
    let start_time = time::OffsetDateTime::now_utc();

    let body = body.into_inner();
    tracing::debug!("MCP JSON-RPC body received: {:?}", body);
    let McpQuery { jsonrpc, id, method, params } = body;

    // Create analytics aggregator
    let user_agents = extract_user_agents(&request);
    let mut aggregate = match params
        .meta
        .and_then(|m| m.client_info.get("name").and_then(|s| s.as_str().map(ToOwned::to_owned)))
    {
        Some(client) => McpAggregator::from_client(client),
        None => McpAggregator::without_client(),
    };

    let response = match method.as_str() {
        method::SERVER_DISCOVERY => {
            McpResponse { jsonrpc, id, result: Some(McpResult::discover()), error: None }
        }
        method::TOOLS_LIST => match McpResult::list_tools() {
            Ok(list_tools) => McpResponse { jsonrpc, id, result: Some(list_tools), error: None },
            Err(err) => {
                return Ok(HttpResponse::Ok().json(McpResponse {
                    jsonrpc,
                    id,
                    result: None,
                    error: Some(McpError::internal_error_from_anyhow(err)),
                }))
            }
        },
        method::RESOURCES_LIST => {
            McpResponse { jsonrpc, id, result: Some(McpResult::empty_resources()), error: None }
        }
        method::PROMPTS_LIST => {
            McpResponse { jsonrpc, id, result: Some(McpResult::empty_prompts()), error: None }
        }
        method::TOOLS_CALL => match params.name.as_deref() {
            Some(SearchInIndexes::NAME) => {
                match SearchInIndexes::call(
                    request,
                    search_queue,
                    analytics.clone(),
                    personalization_service,
                    params.arguments,
                )
                .await
                {
                    Ok(result) => McpResponse { jsonrpc, id, result: Some(result), error: None },
                    Err(error) => McpResponse { jsonrpc, id, result: None, error: Some(error) },
                }
            }
            Some(FacetSearch::NAME) => {
                match FacetSearch::call(
                    request,
                    search_queue,
                    analytics.clone(),
                    personalization_service,
                    params.arguments,
                )
                .await
                {
                    Ok(result) => McpResponse { jsonrpc, id, result: Some(result), error: None },
                    Err(error) => McpResponse { jsonrpc, id, result: None, error: Some(error) },
                }
            }
            Some(ListIndexes::NAME) => {
                match ListIndexes::call(
                    request,
                    search_queue,
                    analytics.clone(),
                    personalization_service,
                    params.arguments,
                )
                .await
                {
                    Ok(result) => McpResponse { jsonrpc, id, result: Some(result), error: None },
                    Err(error) => McpResponse { jsonrpc, id, result: None, error: Some(error) },
                }
            }
            Some(DescribeIndex::NAME) => {
                match DescribeIndex::call(
                    request,
                    search_queue,
                    analytics.clone(),
                    personalization_service,
                    params.arguments,
                )
                .await
                {
                    Ok(result) => McpResponse { jsonrpc, id, result: Some(result), error: None },
                    Err(error) => McpResponse { jsonrpc, id, result: None, error: Some(error) },
                }
            }
            Some(unknown_tool_name) => McpResponse {
                jsonrpc,
                id,
                result: None,
                error: Some(McpError::unknown_tool(unknown_tool_name)),
            },
            None => McpResponse {
                jsonrpc,
                id,
                result: None,
                error: Some(McpError::invalid_params("missing tool name")),
            },
        },
        unknow_method_name => McpResponse {
            jsonrpc,
            id,
            result: None,
            error: Some(McpError::unknow_method(unknow_method_name)),
        },
    };

    // Record success in analytics after the stream is set up
    aggregate.succeed(elapsed(start_time));
    analytics.publish_with_user_agents(aggregate, user_agents);

    Ok(HttpResponse::Ok().json(response))
}

trait McpTool {
    /// The name of the tool, i.e., listIndexes, describeIndex.
    const NAME: &'static str;

    fn definition() -> anyhow::Result<McpToolDefinition>;

    async fn call(
        request: HttpRequest,
        search_queue: web::Data<SearchQueue>,
        analytics: web::Data<Analytics>,
        personalization_service: web::Data<crate::personalization::PersonalizationService>,
        arguments: Option<serde_json::Value>,
    ) -> Result<McpResult, McpError>;
}

enum ListIndexes {}

impl McpTool for ListIndexes {
    const NAME: &'static str = "listIndexes";

    fn definition() -> anyhow::Result<McpToolDefinition> {
        // list indexes
        let route = "/indexes";
        let paths = MEILISEARCH_OPEN_API.paths.paths.get(route).context("retrieving paths")?;
        let components = MEILISEARCH_OPEN_API
            .components
            .as_ref()
            .context("retrieving the Meilisearch components")?;
        let operation = paths.get.as_ref().context("retrieving the GET data")?;

        // We retrieve the offset and limit from the query parameters
        let mut properties = ObjectBuilder::new();
        for parameter in operation.parameters.as_ref().context("retrieving the parameters")? {
            let ref_or_schema = parameter.schema.as_ref().context("retrieving the schema")?.clone();
            let mut schema = clean_refs_from_schema(components, ref_or_schema)
                .context("cleaning the refs from the schema")?;
            if let Schema::Object(object) = &mut schema {
                object.description = parameter.description.clone();
            }
            properties = properties.property(&parameter.name, schema);
        }

        let schema = Schema::from(properties);

        Ok(McpToolDefinition {
            name: Self::NAME.to_string(),
            title: operation.summary.clone().context("Extracting the summary from the schema")?,
            description: operation
                .description
                .clone()
                .context("Extracting the description from the schema")?,
            input_schema: schema,
        })
    }

    async fn call(
        request: HttpRequest,
        _search_queue: web::Data<SearchQueue>,
        _analytics: web::Data<Analytics>,
        _personalization_service: web::Data<crate::personalization::PersonalizationService>,
        mut arguments: Option<serde_json::Value>,
    ) -> Result<McpResult, McpError> {
        #[derive(Serialize)]
        #[serde(rename_all = "camelCase")]
        struct Pagination {
            #[serde(skip_serializing_if = "Option::is_none")]
            offset: Option<u64>,
            #[serde(skip_serializing_if = "Option::is_none")]
            limit: Option<u64>,
        }

        let pagination = match arguments.as_mut() {
            Some(serde_json::Value::Object(object)) => {
                // We remove the extra indexUid parameter to make sure the route accepts the payload
                let offset = object.remove("offset").and_then(|off| off.as_u64());
                let limit = object.remove("limit").and_then(|limit| limit.as_u64());
                Pagination { offset, limit }
            }
            _ => {
                return Err(McpError::invalid_params("expected JSON Object"));
            }
        };

        let mut payload = actix_web::dev::Payload::None;
        let guarded_index_scheduler = GuardedData::from_request(&request, &mut payload)
            .await
            .map_err(McpError::internal_error)?;
        let query = serde_urlencoded::to_string(&pagination).map_err(McpError::internal_error)?;
        let paginate = AwebQueryParameter::from_query(&query).map_err(McpError::internal_error)?;

        let result = super::indexes::list_indexes(guarded_index_scheduler, paginate).await;

        match result {
            Ok(response) => {
                let body = response.into_body();
                let bytes = actix_web::body::to_bytes(body)
                    .await
                    .map_err(McpError::internal_error_from_box_dyn)?;
                let text = String::from_utf8_lossy(&bytes).into_owned();
                // Note: this blocks and would have been better to have a serde_json
                //       RawValue to avoid allocating too much and simply pass through
                let content = serde_json::from_reader(Cursor::new(bytes))
                    .map_err(McpError::internal_error)?;
                Ok(McpResult::from_content_text_and_ttl(content, text, ttl_ms::QUICKLY_STALE))
            }
            Err(response) => {
                tracing::error!("{response:?}");
                McpResult::from_response_error(response).map_err(McpError::internal_error)
            }
        }
    }
}

enum DescribeIndex {}

impl McpTool for DescribeIndex {
    const NAME: &'static str = "describeIndex";

    fn definition() -> anyhow::Result<McpToolDefinition> {
        let components = MEILISEARCH_OPEN_API
            .components
            .as_ref()
            .context("retrieving the Meilisearch components")?;
        let ref_or_schema = <DescribeIndexParams as PartialSchema>::schema();

        let schema = clean_refs_from_schema(components, ref_or_schema)
            .context("cleaning the refs from the schema")?;

        Ok(McpToolDefinition {
            name: Self::NAME.to_string(),
            title: "Describe an index".to_string(),
            description:
                "Describes an index to understand what's stored inside and what's its purpose."
                    .to_string(),
            input_schema: schema,
        })
    }

    async fn call(
        request: HttpRequest,
        search_queue: web::Data<SearchQueue>,
        analytics: web::Data<Analytics>,
        _personalization_service: web::Data<crate::personalization::PersonalizationService>,
        mut arguments: Option<serde_json::Value>,
    ) -> Result<McpResult, McpError> {
        let DescribeIndexParams { index_uid } = match arguments.take() {
            Some(value) => serde_json::from_value(value)
                .map_err(|err| McpError::invalid_params(err.to_string()))?,
            _ => return Err(McpError::invalid_params("expected arguments found none")),
        };

        let query = serde_json::to_vec(&serde_json::json!({ "limit": 5 }))
            .expect("The json macro to correctly serialize");
        let mut payload = actix_web::dev::Payload::from(query);

        let guarded_index_scheduler = GuardedData::from_request(&request, &mut payload)
            .await
            .map_err(McpError::internal_error)?;
        let params = AwebJson::from_request(&request, &mut payload)
            .await
            .map_err(McpError::internal_error)?;

        let result = super::indexes::documents::documents_by_query_post(
            guarded_index_scheduler,
            index_uid.into_inner().into(),
            params,
            search_queue,
            request,
            analytics,
        )
        .await;

        let sample_hits = match result {
            Ok(response) => {
                let body = response.into_body();
                let bytes = actix_web::body::to_bytes(body)
                    .await
                    .map_err(McpError::internal_error_from_box_dyn)?;
                // Note: This blocks and would have been better to have a serde_json
                //       RawValue to avoid allocating too much and simply pass through
                let mut content: serde_json::Map<String, serde_json::Value> =
                    serde_json::from_reader(Cursor::new(bytes))
                        .map_err(McpError::internal_error)?;
                content.remove("results")
            }
            Err(response) => {
                tracing::error!("{response:?}");
                return McpResult::from_response_error(response).map_err(McpError::internal_error);
            }
        };

        #[derive(Debug, Clone, Serialize)]
        #[serde(rename_all = "camelCase")]
        struct IndexDescription {
            #[serde(skip_serializing_if = "Option::is_none")]
            sample_hits: Option<serde_json::Value>,
        }

        let description = IndexDescription { sample_hits };
        let content = serde_json::to_value(&description).map_err(McpError::internal_error)?;
        let text = serde_json::to_string(&content).map_err(McpError::internal_error)?;

        Ok(McpResult::from_content_text_and_ttl(content, text, ttl_ms::QUICKLY_STALE))
    }
}

enum FacetSearch {}

impl McpTool for FacetSearch {
    const NAME: &'static str = "facetSearch";

    fn definition() -> anyhow::Result<McpToolDefinition> {
        let (mut schema, components, operation) =
            retrieve_schema("/indexes/{index_uid}/facet-search")
                .context("while extracting the /indexes/{index_uid}/facet-search OpenAPI schema")?;

        // We modify the schema's properties a bit to expose
        // the original-in-the-path index uid.
        let params = operation.parameters.as_ref().context("extracting operation parameters")?;
        if let Some(param) = params.iter().find(|param| param.name == "index_uid") {
            if let Schema::Object(object) = &mut schema {
                let field_name = "indexUid";
                let ref_or_schema =
                    param.schema.clone().context("extracting the indexUid schema")?;
                let mut schema = clean_refs_from_schema(components, ref_or_schema)
                    .context("cleaning refs from schema")?;
                if let Schema::Object(object) = &mut schema {
                    object.description = param.description.clone();
                }
                // Insert this new mandatory field at the beginning
                object.properties.insert_before(0, field_name.to_string(), RefOr::T(schema));
                object.required.push(field_name.to_string());
            }
        }

        Ok(McpToolDefinition {
            name: Self::NAME.to_string(),
            title: operation.summary.clone().context("Extracting the summary from the schema")?,
            description: operation
                .description
                .clone()
                .context("Extracting the description from the schema")?,
            input_schema: schema,
        })
    }

    async fn call(
        request: HttpRequest,
        search_queue: web::Data<SearchQueue>,
        analytics: web::Data<Analytics>,
        _personalization_service: web::Data<crate::personalization::PersonalizationService>,
        mut arguments: Option<serde_json::Value>,
    ) -> Result<McpResult, McpError> {
        let index_uid = match arguments.as_mut() {
            Some(serde_json::Value::Object(object)) => {
                // We remove the extra indexUid parameter to make sure the route accepts the payload
                let index_uid = match object.remove("indexUid") {
                    Some(uid) => uid,
                    None => return Err(McpError::invalid_params("missing indexUid")),
                };
                match index_uid.as_str() {
                    Some(s) => s.to_owned(),
                    None => {
                        return Err(McpError::invalid_params(
                            "expected the indexUid to be a string",
                        ))
                    }
                }
            }
            _ => return Err(McpError::invalid_params("expected JSON Object")),
        };

        let query =
            serde_json::to_vec(&arguments.unwrap_or_default()).map_err(McpError::internal_error)?;
        let mut payload = actix_web::dev::Payload::from(query);
        let guarded_index_scheduler = GuardedData::from_request(&request, &mut payload)
            .await
            .map_err(McpError::internal_error)?;
        let params = AwebJson::from_request(&request, &mut payload)
            .await
            .map_err(McpError::internal_error)?;

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
                let bytes = actix_web::body::to_bytes(body)
                    .await
                    .map_err(McpError::internal_error_from_box_dyn)?;
                let text = String::from_utf8_lossy(&bytes).into_owned();
                // Note: This blocks and would have been better to have a serde_json
                //       RawValue to avoid allocating too much and simply pass through
                let content = serde_json::from_reader(Cursor::new(bytes))
                    .map_err(McpError::internal_error)?;
                Ok(McpResult::from_content_text_and_ttl(content, text, ttl_ms::IMMEDIATELY_STALE))
            }
            Err(response) => {
                tracing::error!("{response:?}");
                McpResult::from_response_error(response).map_err(McpError::internal_error)
            }
        }
    }
}

enum SearchInIndexes {}

impl McpTool for SearchInIndexes {
    const NAME: &'static str = "searchInIndexes";

    fn definition() -> anyhow::Result<McpToolDefinition> {
        let (schema, _, operation) = retrieve_schema("/multi-search")
            .context("while extracting the /multi-search OpenAPI schema")?;

        Ok(McpToolDefinition {
            name: Self::NAME.to_string(),
            title: operation
                .summary
                .clone()
                .context("reading the summary of the /multi-search route")?,
            description: operation
                .description
                .clone()
                .context("reading the description of the /multi-search route")?,
            // TODO maybe add more information about how to do filtering and such?
            //      It is probably better to explain it in the OpenAPI description or examples maybe?
            input_schema: schema,
        })
    }

    async fn call(
        request: HttpRequest,
        search_queue: web::Data<SearchQueue>,
        analytics: web::Data<Analytics>,
        personalization_service: web::Data<crate::personalization::PersonalizationService>,
        arguments: Option<serde_json::Value>,
    ) -> Result<McpResult, McpError> {
        let query =
            serde_json::to_vec(&arguments.unwrap_or_default()).map_err(McpError::internal_error)?;

        let mut payload = actix_web::dev::Payload::from(query);
        let guarded_index_scheduler = GuardedData::from_request(&request, &mut payload)
            .await
            .map_err(McpError::internal_error)?;

        let params = AwebJson::from_request(&request, &mut payload)
            .await
            .map_err(McpError::internal_error)?;

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
                let bytes = actix_web::body::to_bytes(body)
                    .await
                    .map_err(McpError::internal_error_from_box_dyn)?;
                let text = String::from_utf8_lossy(&bytes).into_owned();
                // Note: This blocks and would have been better to have a serde_json
                //       RawValue to avoid allocating too much and simply pass through
                let content = serde_json::from_reader(Cursor::new(bytes))
                    .map_err(McpError::internal_error)?;
                Ok(McpResult::from_content_text_and_ttl(content, text, ttl_ms::IMMEDIATELY_STALE))
            }
            Err(response) => {
                tracing::error!("{response:?}");
                McpResult::from_response_error(response).map_err(McpError::internal_error)
            }
        }
    }
}

pub mod method {
    pub const SERVER_DISCOVERY: &str = "server/discover";
    pub const TOOLS_CALL: &str = "tools/call";
    pub const TOOLS_LIST: &str = "tools/list";
    pub const RESOURCES_LIST: &str = "resources/list";
    pub const PROMPTS_LIST: &str = "prompts/list";
}

pub mod cache_scope {
    pub const PRIVATE: &str = "private";
}

pub mod ttl_ms {
    pub const IMMEDIATELY_STALE: usize = 0;
    pub const QUICKLY_STALE: usize = 500_000; // 5 mins
    pub const STATIC_RESULT: usize = 86_400_000; // 24 hours
}

#[routes::request(db)]
#[derive(Debug, Clone)]
/// Describes an index
pub struct DescribeIndexParams {
    #[request(required)]
    index_uid: IndexUid,
}

#[routes::request]
#[derive(Debug, Clone)]
pub struct McpQuery {
    /// Defines the version of a JSON-RPC request.
    ///
    /// You can find more information about this field on [the JSON-RPC specification](https://www.jsonrpc.org/specification#request_object).
    #[request(required)]
    jsonrpc: String,
    /// Defines the id of a JSON-RPC request.
    ///
    /// You can find more information about this field on [the JSON-RPC specification](https://www.jsonrpc.org/specification#request_object).
    #[request(required)]
    id: RequestId,
    /// The method to call using the JSON-RPC format.
    ///
    /// You can find more information about this field on [the JSON-RPC specification](https://www.jsonrpc.org/specification#request_object).
    #[request(required)]
    method: String, // e.g., server/discover, tools/list, resources/list
    /// The parameters to call the method with following the JSON-RPC format.
    ///
    /// You can find more information about this field on [the JSON-RPC specification](https://www.jsonrpc.org/specification#request_object).
    #[request(required)]
    params: ParamsWithMeta,
}

// Note that I would have rather refused unknown fields
// but online playgrounds provide more fields than expected
// <https://mcpplaygroundonline.com>
#[derive(Debug, Clone)]
#[routes::request(allow_unknown_fields)]
pub struct ParamsWithMeta {
    /// The _meta property used by the MCP protocol.
    ///
    /// You can find more information about this field on [the MCP specification](https://modelcontextprotocol.io/specification/2026-07-28/basic/index#_meta).
    #[request(default, rename = "_meta")]
    meta: Option<McpClientMeta>,
    /// The tool name to call used by the MCP protocol.
    ///
    /// You can find more information about this field on [the MCP specification](https://modelcontextprotocol.io/specification/2026-07-28/server/tools#tool-names).
    #[request(default)]
    name: Option<String>, // e.g. get_weather
    /// The arguments to give to the tool from the MCP protocol.
    ///
    /// You can find more information about this field on [the MCP specification](https://modelcontextprotocol.io/specification/2026-07-28/schema#calltoolrequestparams).
    #[request(default)]
    arguments: Option<serde_json::Value>, // RawValue would have been better
}

#[derive(Debug, Clone)]
#[routes::request(allow_unknown_fields)]
pub struct McpClientMeta {
    #[request(required, rename = "io.modelcontextprotocol/protocolVersion")]
    _protocol_version: String, // "2026-07-28"
    #[request(required, rename = "io.modelcontextprotocol/clientInfo")]
    client_info: serde_json::Value, // { "name": "ExampleClient", "version": "1.0.0" }
    #[request(default, rename = "io.modelcontextprotocol/clientCapabilities")]
    _client_capabilities: serde_json::Value,
}

#[derive(Debug, Clone, Serialize)]
#[routes::request]
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
            Value::Float(x) => match Number::from_f64(x) {
                Some(f) => Either::Left(f),
                None => {
                    return Err(DeserrError::new(
                        format!("Invalid type: expected non-infinite nor NaN number found: {x}"),
                        BadParameter,
                    ))
                }
            },
            Value::String(string) => Either::Right(string),
            _otherwise => {
                return Err(DeserrError::new(
                    "Invalid type: expected integer or string".to_string(),
                    BadParameter,
                ))
            }
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

#[derive(Debug, Serialize, ToSchema)]
#[serde(rename_all = "camelCase")]
#[schema(rename_all = "camelCase")]
pub struct McpResponse {
    /// The JSON-RPC version.
    jsonrpc: String,
    /// The JSON-RPC request ID.
    id: RequestId,
    /// The JSON-RPC result.
    #[serde(skip_serializing_if = "Option::is_none")]
    result: Option<McpResult>,
    /// The JSON-RPC error.
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<McpError>,
}

// manual impl: not sure why we need the Serialize derive
impl routes::RequestBody for RequestId {}

// Note I would have rather prefered to use an enum, but utoipa is not cool with it
const RESULT_TYPE_COMPLETE: &str = "complete";
const SUPPORTED_VERSIONS: &[&str] = &["2026-07-28"];

#[derive(Debug, Serialize, ToSchema)]
#[serde(rename_all = "camelCase")]
#[schema(rename_all = "camelCase")]
pub struct McpResult {
    result_type: &'static str, // "complete", "input_required"
    #[serde(skip_serializing_if = "Option::is_none")]
    is_error: Option<bool>,
    /// Protocol versions the server supports. The client should choose one of these for subsequent requests.
    #[serde(skip_serializing_if = "Option::is_none")]
    supported_versions: Option<&'static [&'static str]>,
    #[serde(skip_serializing_if = "Option::is_none")]
    tools: Option<Vec<McpToolDefinition>>,
    // Note that for now we will simply return an empty list of resources
    #[serde(skip_serializing_if = "Option::is_none")]
    resources: Option<Vec<serde_json::Value>>,
    // Note that for now we will simply return an empty list of prompts
    #[serde(skip_serializing_if = "Option::is_none")]
    prompts: Option<Vec<serde_json::Value>>,
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
    ttl_ms: usize,             // 300_000
    cache_scope: &'static str, // public | private
}

impl McpResult {
    fn from_response_error(response: ResponseError) -> serde_json::Result<McpResult> {
        Ok(McpResult {
            result_type: RESULT_TYPE_COMPLETE,
            is_error: Some(true),
            tools: None,
            resources: None,
            prompts: None,
            structured_content: Some(serde_json::to_value(&response)?),
            content: Some(vec![McpTextContentOutput::from(response.message)]),
            supported_versions: None,
            meta: None,
            capabilities: None,
            instructions: None,
            ttl_ms: ttl_ms::IMMEDIATELY_STALE,
            cache_scope: cache_scope::PRIVATE,
        })
    }

    fn from_content_text_and_ttl(
        content: serde_json::Value,
        text: String,
        ttl_ms: usize,
    ) -> McpResult {
        McpResult {
            result_type: RESULT_TYPE_COMPLETE,
            is_error: None,
            tools: None,
            resources: None,
            prompts: None,
            structured_content: Some(content),
            content: Some(vec![McpTextContentOutput::from(text)]),
            supported_versions: None,
            meta: None,
            capabilities: None,
            instructions: None,
            ttl_ms,
            cache_scope: cache_scope::PRIVATE,
        }
    }

    fn discover() -> McpResult {
        let major = milli::constants::VERSION_MAJOR;
        let minor = milli::constants::VERSION_MINOR;
        let patch = milli::constants::VERSION_PATCH;

        McpResult {
            result_type: RESULT_TYPE_COMPLETE,
            is_error: None,
            supported_versions: Some(SUPPORTED_VERSIONS),
            tools: None,
            resources: None,
            prompts: None,
            meta: Some(McpServerMeta {
                server_info: ClientServerInfo {
                    name: "Meilisearch".to_string(),
                    version: format!("{major}.{minor}.{patch}"),
                },
            }),
            // Note that we MUST declare the tools as the server supports tool calling.
            // <https://modelcontextprotocol.io/specification/2026-07-28/server/tools#capabilities>
            capabilities: Some(McpCapabilities { tools: Some(BTreeMap::new()) }),
            content: None,
            structured_content: None,
            instructions: Some(
                "Meilisearch is a prefix search engine that supports filtering, sorting, federated searching (mixing results from different indexes).\
                Meilisearch support classic keyword search but may also support semantic search throught the use of the hybrid search parameter.
                You can find more information about available embedders for a given index when describing an index.\
                We recommend you to use the listIndexes, describeIndex, and searchInIndexes tools, in this order to fetch the right informations from the available indexes.".to_string()
            ),
            ttl_ms: ttl_ms::QUICKLY_STALE,
            cache_scope: cache_scope::PRIVATE,
        }
    }

    fn empty_prompts() -> McpResult {
        McpResult {
            result_type: RESULT_TYPE_COMPLETE,
            is_error: None,
            content: None,
            structured_content: None,
            tools: None,
            resources: None,
            prompts: Some(vec![]), // no prompts
            supported_versions: None,
            meta: None,
            capabilities: None,
            instructions: None,
            ttl_ms: ttl_ms::STATIC_RESULT,
            cache_scope: cache_scope::PRIVATE,
        }
    }

    fn empty_resources() -> McpResult {
        McpResult {
            result_type: RESULT_TYPE_COMPLETE,
            is_error: None,
            content: None,
            structured_content: None,
            tools: None,
            resources: Some(vec![]), // no resources
            prompts: None,
            supported_versions: None,
            meta: None,
            capabilities: None,
            instructions: None,
            ttl_ms: ttl_ms::STATIC_RESULT,
            cache_scope: cache_scope::PRIVATE,
        }
    }

    fn list_tools() -> anyhow::Result<McpResult> {
        let tools = vec![
            SearchInIndexes::definition()?,
            FacetSearch::definition()?,
            ListIndexes::definition()?,
            DescribeIndex::definition()?,
        ];

        Ok(McpResult {
            result_type: RESULT_TYPE_COMPLETE,
            is_error: None,
            content: None,
            structured_content: None,
            tools: Some(tools),
            resources: None,
            prompts: None,
            supported_versions: None,
            meta: None,
            capabilities: None,
            instructions: None,
            ttl_ms: ttl_ms::STATIC_RESULT,
            cache_scope: cache_scope::PRIVATE,
        })
    }
}

#[derive(Debug, Serialize, ToSchema)]
#[serde(rename_all = "camelCase")]
#[schema(rename_all = "camelCase")]
pub struct McpServerMeta {
    #[serde(rename = "io.modelcontextprotocol/serverInfo")]
    server_info: ClientServerInfo,
}

// Note that those fields are just a way to display the
// capabilities and must always stay empty or not shown at all.
#[derive(Debug, Serialize, ToSchema)]
#[serde(rename_all = "camelCase")]
#[schema(rename_all = "camelCase")]
pub struct McpCapabilities {
    #[serde(skip_serializing_if = "Option::is_none")]
    tools: Option<BTreeMap<(), ()>>,
}

#[derive(Debug, Serialize, ToSchema)]
#[serde(rename_all = "camelCase")]
#[schema(rename_all = "camelCase")]
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
#[derive(Serialize, ToSchema)]
#[serde(rename_all = "camelCase")]
#[schema(rename_all = "camelCase")]
pub struct McpToolDefinition {
    /// Unique identifier for the tool.
    name: String,
    /// Optional human-readable name of the tool for display purposes.
    title: String,
    /// Human-readable description of functionality.
    description: String,
    /// JSON Schema defining expected parameters.
    #[schema(value_type = serde_json::Value)]
    input_schema: Schema,
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
    code: i64,
    message: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    data: Option<McpErrorData>, // Can be any JSON value
}

impl McpError {
    fn unknown_tool(invalid_tool_name: &str) -> McpError {
        McpError { code: -32602, message: format!("Unknown tool: {invalid_tool_name}"), data: None }
    }

    fn unknow_method(unknow_method_name: &str) -> McpError {
        McpError {
            code: -32601,
            message: format!("Unknow method: {unknow_method_name}"),
            data: None,
        }
    }

    fn invalid_params(message: impl AsRef<str>) -> McpError {
        McpError {
            code: -32602,
            message: format!("Invalid params: {}", message.as_ref()),
            data: None,
        }
    }

    fn internal_error(error: impl std::error::Error) -> McpError {
        McpError { code: -32603, message: format!("Internal error: {error}"), data: None }
    }

    fn internal_error_from_anyhow(error: anyhow::Error) -> McpError {
        McpError { code: -32603, message: format!("Internal error: {error}"), data: None }
    }

    fn internal_error_from_box_dyn(error: Box<dyn std::error::Error + 'static>) -> McpError {
        McpError { code: -32603, message: format!("Internal error: {error}"), data: None }
    }
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

fn retrieve_schema(
    route: &str,
) -> anyhow::Result<(Schema, &'static Components, &'static Operation)> {
    let paths = MEILISEARCH_OPEN_API.paths.paths.get(route).context("retrieving paths")?;
    let components = MEILISEARCH_OPEN_API
        .components
        .as_ref()
        .context("retrieving the Meilisearch components")?;
    let operation = paths.post.as_ref().context("retrieving the POST data")?;
    let request_body = operation.request_body.as_ref().context("retrieving the request body")?;
    let content =
        request_body.content.get("application/json").context("retrieving the body content")?;
    let ref_or_schema = content.schema.clone().context("retrieving the schema")?;
    clean_refs_from_schema(components, ref_or_schema)
        .context("cleaning the refs from the schema")
        .map(|schema| (schema, components, operation))
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
                        let schema = clean_refs_from_schema(components, r#ref)?;
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
