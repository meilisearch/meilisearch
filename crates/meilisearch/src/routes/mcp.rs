use std::io::Cursor;

use actix_web::http::header;
use actix_web::web::{self, Data};
use actix_web::{FromRequest, HttpRequest, HttpResponse};
use deserr::actix_web::AwebJson;
use index_scheduler::IndexScheduler;
use meilisearch_auth::AuthController;
use meilisearch_types::batch_view::BatchView;
use meilisearch_types::deserr::DeserrJsonError;
use meilisearch_types::error::ResponseError;
use serde::Serialize;
use utoipa::ToSchema;

use crate::analytics::Analytics;
use crate::extractors::authentication::GuardedData;
use crate::search_queue::SearchQueue;

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
    req: HttpRequest,
    index_scheduler: Data<IndexScheduler>,
    auth_controller: Data<AuthController>,
    search_queue: web::Data<SearchQueue>,
    personalization_service: web::Data<crate::personalization::PersonalizationService>,
    body: AwebJson<McpQuery, DeserrJsonError>,
    analytics: web::Data<Analytics>,
) -> Result<HttpResponse, ResponseError> {
    index_scheduler.features().check_mcp_route("calling the /mcp route")?;

    let McpQuery { jsonrpc, id, method, params } = body.into_inner();

    let response = match method.as_str() {
        "server/discover" => todo!("list tools and resources"),
        // TODO get this from OpenApi
        "tools/call" => match params.name.as_deref() {
            Some("search_in_index") => {
                let index_uid = "test";
                let path =
                    format!("/indexes/{}/search", serde_urlencoded::to_string(index_uid).unwrap());
                let request =
                    actix_web::test::TestRequest::with_uri(&path).app_data(auth_controller);
                let request = match req.headers().get(header::AUTHORIZATION) {
                    Some(token) => request.insert_header((header::AUTHORIZATION, token)),
                    None => request,
                };

                let request = request.to_http_request();
                let mut payload = actix_web::dev::Payload::None;
                // TODO don't unwrap
                let guarded_index_scheduler =
                    GuardedData::from_request(&request, &mut payload).await.unwrap();
                // TODO don't unwrap
                let path = web::Path::from_request(&request, &mut payload).await.unwrap();
                // TODO don't unwrap
                let params = AwebJson::from_request(&request, &mut payload).await.unwrap();

                let result = super::indexes::search::search_with_post(
                    guarded_index_scheduler,
                    search_queue,
                    personalization_service,
                    path,
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
                        // TODO this blocks and would have prefered to have a serde_json RawValue
                        //      to avoid allocating too much and simply pass through.
                        let content = serde_json::from_reader(Cursor::new(bytes)).unwrap();
                        McpResponse {
                            jsonrpc,
                            id,
                            result: Some(McpResult { result_type: RESULT_TYPE_COMPLETE, content }),
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
    content: serde_json::Value,
}

#[derive(Debug, Serialize, ToSchema)]
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
