use std::collections::HashSet;

use actix_web::web::Data;
use actix_web::{web, HttpResponse};
use index_scheduler::IndexScheduler;
use meilisearch_types::error::{Code, ResponseError};
use meilisearch_types::tasks::KindWithContent;
use serde::Deserialize;

use crate::extractors::authentication::policies::*;
use crate::extractors::authentication::GuardedData;
use crate::extractors::sequential_extractor::SeqHandler;
use crate::routes::tasks::TaskView;

pub fn configure(cfg: &mut web::ServiceConfig) {
    cfg.service(web::resource("").route(web::post().to(SeqHandler(indexes_swap))));
}

// TODO: Lo: revisit this struct once we have decided on what the payload should be
#[derive(Deserialize, Debug, Clone, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct IndexesSwapPayload {
    indexes: (String, String),
}

pub async fn indexes_swap(
    index_scheduler: GuardedData<ActionPolicy<{ actions::INDEXES_SWAP }>, Data<IndexScheduler>>,
    params: web::Json<Vec<IndexesSwapPayload>>,
) -> Result<HttpResponse, ResponseError> {
    let search_rules = &index_scheduler.filters().search_rules;

    // TODO: Lo: error when the params are empty
    // TODO: Lo: error when the same index appears more than once
    // TODO: Lo: error when not authorized to swap

    let mut swaps = vec![];
    let mut indexes_set = HashSet::<String>::default();
    for IndexesSwapPayload { indexes: (lhs, rhs) } in params.into_inner().into_iter() {
        if !search_rules.is_index_authorized(&lhs) || !search_rules.is_index_authorized(&lhs) {
            return Err(ResponseError::from_msg(
                "TODO: error message when we swap with an index were not allowed to access"
                    .to_owned(),
                Code::BadRequest,
            ));
        }
        swaps.push((lhs.clone(), rhs.clone()));
        // TODO: Lo: should this check be here or within the index scheduler?
        let is_unique_index_lhs = indexes_set.insert(lhs);
        if !is_unique_index_lhs {
            return Err(ResponseError::from_msg(
                "TODO: error message when same index is in more than one swap".to_owned(),
                Code::BadRequest,
            ));
        }
        let is_unique_index_rhs = indexes_set.insert(rhs);
        if !is_unique_index_rhs {
            return Err(ResponseError::from_msg(
                "TODO: error message when same index is in more than one swap".to_owned(),
                Code::BadRequest,
            ));
        }
    }

    let task = KindWithContent::IndexSwap { swaps };

    let task = index_scheduler.register(task)?;
    let task_view = TaskView::from_task(&task);

    Ok(HttpResponse::Accepted().json(task_view))
}
