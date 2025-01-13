use actix_web::web::Data;
use actix_web::{web, HttpRequest, HttpResponse};
use deserr::actix_web::AwebJson;
use deserr::Deserr;
use index_scheduler::IndexScheduler;
use meilisearch_types::deserr::DeserrJsonError;
use meilisearch_types::error::deserr_codes::InvalidSwapIndexes;
use meilisearch_types::error::ResponseError;
use meilisearch_types::index_uid::IndexUid;
use meilisearch_types::tasks::{IndexSwap, KindWithContent};
use serde::Serialize;
use utoipa::{OpenApi, ToSchema};

use super::{get_task_id, is_dry_run, SummarizedTaskView};
use crate::analytics::{Aggregate, Analytics};
use crate::error::MeilisearchHttpError;
use crate::extractors::authentication::policies::*;
use crate::extractors::authentication::{AuthenticationError, GuardedData};
use crate::extractors::sequential_extractor::SeqHandler;
use crate::Opt;

#[derive(OpenApi)]
#[openapi(paths(swap_indexes))]
pub struct SwapIndexesApi;

pub fn configure(cfg: &mut web::ServiceConfig) {
    cfg.service(web::resource("").route(web::post().to(SeqHandler(swap_indexes))));
}

#[derive(Deserr, Debug, Clone, PartialEq, Eq, ToSchema)]
#[deserr(error = DeserrJsonError, rename_all = camelCase, deny_unknown_fields)]
pub struct SwapIndexesPayload {
    /// Array of the two indexUids to be swapped
    #[deserr(error = DeserrJsonError<InvalidSwapIndexes>, missing_field_error = DeserrJsonError::missing_swap_indexes)]
    indexes: Vec<IndexUid>,
}

#[derive(Serialize)]
struct IndexSwappedAnalytics {
    swap_operation_number: usize,
}

impl Aggregate for IndexSwappedAnalytics {
    fn event_name(&self) -> &'static str {
        "Indexes Swapped"
    }

    fn aggregate(self: Box<Self>, new: Box<Self>) -> Box<Self> {
        Box::new(Self {
            swap_operation_number: self.swap_operation_number.max(new.swap_operation_number),
        })
    }

    fn into_event(self: Box<Self>) -> serde_json::Value {
        serde_json::to_value(*self).unwrap_or_default()
    }
}

/// Swap indexes
///
/// Swap the documents, settings, and task history of two or more indexes. You can only swap indexes in pairs. However, a single request can swap as many index pairs as you wish.
/// Swapping indexes is an atomic transaction: either all indexes are successfully swapped, or none are.
/// Swapping indexA and indexB will also replace every mention of indexA by indexB and vice-versa in the task history. enqueued tasks are left unmodified.
#[utoipa::path(
    post,
    path = "",
    tag = "Indexes",
    security(("Bearer" = ["search", "*"])),
    request_body = Vec<SwapIndexesPayload>,
    responses(
        (status = OK, description = "Task successfully enqueued", body = SummarizedTaskView, content_type = "application/json", example = json!(
            {
                "taskUid": 3,
                "indexUid": null,
                "status": "enqueued",
                "type": "indexSwap",
                "enqueuedAt": "2021-08-12T10:00:00.000000Z"
            }
        )),
        (status = 401, description = "The authorization header is missing", body = ResponseError, content_type = "application/json", example = json!(
            {
                "message": "The Authorization header is missing. It must use the bearer authorization method.",
                "code": "missing_authorization_header",
                "type": "auth",
                "link": "https://docs.meilisearch.com/errors#missing_authorization_header"
            }
        )),
    )
)]
pub async fn swap_indexes(
    index_scheduler: GuardedData<ActionPolicy<{ actions::INDEXES_SWAP }>, Data<IndexScheduler>>,
    params: AwebJson<Vec<SwapIndexesPayload>, DeserrJsonError>,
    req: HttpRequest,
    opt: web::Data<Opt>,
    analytics: web::Data<Analytics>,
) -> Result<HttpResponse, ResponseError> {
    let params = params.into_inner();
    analytics.publish(IndexSwappedAnalytics { swap_operation_number: params.len() }, &req);
    let filters = index_scheduler.filters();

    let mut swaps = vec![];
    for SwapIndexesPayload { indexes } in params.into_iter() {
        // TODO: switch to deserr
        let (lhs, rhs) = match indexes.as_slice() {
            [lhs, rhs] => (lhs, rhs),
            _ => {
                return Err(MeilisearchHttpError::SwapIndexPayloadWrongLength(indexes).into());
            }
        };
        if !filters.is_index_authorized(lhs) || !filters.is_index_authorized(rhs) {
            return Err(AuthenticationError::InvalidToken.into());
        }
        swaps.push(IndexSwap { indexes: (lhs.to_string(), rhs.to_string()) });
    }

    let task = KindWithContent::IndexSwap { swaps };
    let uid = get_task_id(&req, &opt)?;
    let dry_run = is_dry_run(&req, &opt)?;
    let task: SummarizedTaskView =
        tokio::task::spawn_blocking(move || index_scheduler.register(task, uid, dry_run))
            .await??
            .into();
    Ok(HttpResponse::Accepted().json(task))
}
