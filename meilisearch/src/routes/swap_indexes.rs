use std::fmt;

use actix_web::web::Data;
use actix_web::{web, HttpRequest, HttpResponse};
use deserr::{DeserializeFromValue, IntoValue, ValuePointerRef};
use index_scheduler::IndexScheduler;
use meilisearch_types::error::{unwrap_any, Code, ErrorCode, ResponseError};
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
#[deserr(rename_all = camelCase, deny_unknown_fields)]
pub struct SwapIndexesPayload {
    indexes: Vec<String>,
}

pub async fn swap_indexes(
    index_scheduler: GuardedData<ActionPolicy<{ actions::INDEXES_SWAP }>, Data<IndexScheduler>>,
    params: ValidatedJson<Vec<SwapIndexesPayload>, SwapIndexesDeserrError>,
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

#[derive(Debug)]
pub struct SwapIndexesDeserrError {
    error: String,
    code: Code,
}

impl std::fmt::Display for SwapIndexesDeserrError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.error)
    }
}

impl std::error::Error for SwapIndexesDeserrError {}
impl ErrorCode for SwapIndexesDeserrError {
    fn error_code(&self) -> Code {
        self.code
    }
}

impl deserr::MergeWithError<SwapIndexesDeserrError> for SwapIndexesDeserrError {
    fn merge(
        _self_: Option<Self>,
        other: SwapIndexesDeserrError,
        _merge_location: ValuePointerRef,
    ) -> Result<Self, Self> {
        Err(other)
    }
}

impl deserr::DeserializeError for SwapIndexesDeserrError {
    fn error<V: IntoValue>(
        _self_: Option<Self>,
        error: deserr::ErrorKind<V>,
        location: ValuePointerRef,
    ) -> Result<Self, Self> {
        let error = unwrap_any(deserr::serde_json::JsonError::error(None, error, location)).0;

        let code = match location.last_field() {
            Some("indexes") => Code::InvalidSwapIndexes,
            _ => Code::BadRequest,
        };

        Err(SwapIndexesDeserrError { error, code })
    }
}
