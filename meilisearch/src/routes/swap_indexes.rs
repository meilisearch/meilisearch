use actix_web::web::Data;
use actix_web::{web, HttpRequest, HttpResponse};
use deserr::DeserializeFromValue;
use index_scheduler::IndexScheduler;
use meilisearch_types::error::deserr_codes::InvalidSwapIndexes;
use meilisearch_types::error::{DeserrError, ResponseError};
use meilisearch_types::tasks::{IndexSwap, KindWithContent};
use serde_json::json;

use super::SummarizedTaskView;
use crate::analytics::Analytics;
use crate::error::MeilisearchHttpError;
use crate::extractors::authentication::policies::*;
use crate::extractors::authentication::{AuthenticationError, GuardedData};
use crate::extractors::json::ValidatedJson;
use crate::extractors::sequential_extractor::SeqHandler;

pub fn configure(cfg: &mut web::ServiceConfig) {
    cfg.service(web::resource("").route(web::post().to(SeqHandler(swap_indexes))));
}

#[derive(DeserializeFromValue, Debug, Clone, PartialEq, Eq)]
#[deserr(error = DeserrError, rename_all = camelCase, deny_unknown_fields)]
pub struct SwapIndexesPayload {
    #[deserr(error = DeserrError<InvalidSwapIndexes>)]
    indexes: Vec<String>,
}

pub async fn swap_indexes(
    index_scheduler: GuardedData<ActionPolicy<{ actions::INDEXES_SWAP }>, Data<IndexScheduler>>,
    params: ValidatedJson<Vec<SwapIndexesPayload>, DeserrError>,
    req: HttpRequest,
    analytics: web::Data<dyn Analytics>,
) -> Result<HttpResponse, ResponseError> {
    let params = params.into_inner();
    analytics.publish(
        "Indexes Swapped".to_string(),
        json!({
            "swap_operation_number": params.len(),
        }),
        Some(&req),
    );
    let search_rules = &index_scheduler.filters().search_rules;

    let mut swaps = vec![];
    for SwapIndexesPayload { indexes } in params.into_iter() {
        let (lhs, rhs) = match indexes.as_slice() {
            [lhs, rhs] => (lhs, rhs),
            _ => {
                return Err(MeilisearchHttpError::SwapIndexPayloadWrongLength(indexes).into());
            }
        };
        if !search_rules.is_index_authorized(lhs) || !search_rules.is_index_authorized(rhs) {
            return Err(AuthenticationError::InvalidToken.into());
        }
        swaps.push(IndexSwap { indexes: (lhs.clone(), rhs.clone()) });
    }

    let task = KindWithContent::IndexSwap { swaps };

    let task = index_scheduler.register(task)?;
    let task: SummarizedTaskView = task.into();
    Ok(HttpResponse::Accepted().json(task))
}
