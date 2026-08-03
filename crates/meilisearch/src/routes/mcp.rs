use actix_web::http::header;
use actix_web::web::{self, Data};
use actix_web::{FromRequest, HttpRequest, Responder};
use actix_web_lab::sse::Sse;
use deserr::actix_web::AwebJson;
use index_scheduler::IndexScheduler;
use meilisearch_auth::AuthController;
use meilisearch_types::batch_view::BatchView;
use meilisearch_types::deserr::DeserrJsonError;
use meilisearch_types::error::ResponseError;

use serde::Serialize;
use tokio::runtime::Handle;
use utoipa::ToSchema;

use crate::analytics::Analytics;
use crate::search_queue::SearchQueue;

use crate::extractors::authentication::GuardedData;

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
) -> Result<impl Responder, ResponseError> {
    index_scheduler.features().check_mcp_route("calling the /mcp route")?;

    let McpQuery { jsonrpc, id, method, params } = body.into_inner();
    // TODO get this from OpenApi
    if method != "tools/call" || params.name.as_deref() != Some("search_in_index") {
        // TODO return an error
        todo!("break and send an error");
    }

    // {
    //   "jsonrpc": "2.0",
    //   "id": 2,
    //   "method": "tools/call",
    //   "params": {
    //     "name": "get_weather",
    //     "arguments": {
    //       "location": "New York"
    //     }
    //   }
    // }

    let index_uid = "test";
    let path = format!("/indexes/{}/search", serde_urlencoded::to_string(index_uid).unwrap());
    let request = actix_web::test::TestRequest::with_uri(&path).app_data(auth_controller);
    let request = match req.headers().get(header::AUTHORIZATION) {
        Some(token) => request.insert_header((header::AUTHORIZATION, token)),
        None => request,
    };

    let request = request.to_http_request();
    let mut payload = actix_web::dev::Payload::None;
    // TODO don't unwrap
    let guarded_index_scheduler = GuardedData::from_request(&request, &mut payload).await.unwrap();
    // TODO don't unwrap
    let path = web::Path::from_request(&request, &mut payload).await.unwrap();
    // TODO don't unwrap
    let params = AwebJson::from_request(&request, &mut payload).await.unwrap();

    let (tx, rx) = tokio::sync::mpsc::channel(10);
    // let _join_handle = Handle::current().spawn(async move {

    super::indexes::search::search_with_post(
        guarded_index_scheduler,
        search_queue,
        personalization_service,
        path,
        params,
        request,
        analytics,
    )
    .await;

    drop(tx);
    // let data = sse::Data::new_json(task).unwrap();
    // let _ = tx.send(Event::Data(data)).await;
    // });

    Ok(Sse::from_infallible_receiver(rx)
        .with_retry_duration(std::time::Duration::from_secs(10))
        .customize()
        .insert_header(("X-Accel-Buffering", "no")))
}

#[routes::request]
#[derive(Debug, Clone, PartialEq)]
pub struct McpQuery {
    #[request(required)]
    jsonrpc: String,
    #[request(required)]
    id: String,
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
    id: String,
    result: McpResult,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct McpResult {
    result_type: String, // "complete", "input_required"
    content: serde_json::Value,
    is_error: bool,
}
