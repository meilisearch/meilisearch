use std::collections::BTreeSet;

use actix_web::web::Data;
use actix_web::{web, HttpResponse};
use index_scheduler::IndexScheduler;
use meilisearch_types::error::ResponseError;
use meilisearch_types::tasks::{IndexSwap, KindWithContent};
use serde::Deserialize;

use super::SummarizedTaskView;
use crate::error::MeilisearchHttpError;
use crate::extractors::authentication::policies::*;
use crate::extractors::authentication::{AuthenticationError, GuardedData};
use crate::extractors::sequential_extractor::SeqHandler;
use crate::routes::tasks::TaskView;

pub fn configure(cfg: &mut web::ServiceConfig) {
    cfg.service(web::resource("").route(web::post().to(SeqHandler(swap_indexes))));
}
#[derive(Deserialize, Debug, Clone, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct SwapIndexesPayload {
    indexes: Vec<String>,
}

pub async fn swap_indexes(
    index_scheduler: GuardedData<ActionPolicy<{ actions::INDEXES_SWAP }>, Data<IndexScheduler>>,
    params: web::Json<Vec<SwapIndexesPayload>>,
) -> Result<HttpResponse, ResponseError> {
    let search_rules = &index_scheduler.filters().search_rules;

    let mut swaps = vec![];
    let mut indexes_set = BTreeSet::<String>::default();
    let mut unauthorized_indexes = BTreeSet::new();
    let mut duplicate_indexes = BTreeSet::new();
    for SwapIndexesPayload { indexes } in params.into_inner().into_iter() {
        let (lhs, rhs) = match indexes.as_slice() {
            [lhs, rhs] => (lhs, rhs),
            _ => {
                return Err(MeilisearchHttpError::SwapIndexPayloadWrongLength(indexes).into());
            }
        };
        if !search_rules.is_index_authorized(lhs) {
            unauthorized_indexes.insert(lhs.clone());
        }
        if !search_rules.is_index_authorized(rhs) {
            unauthorized_indexes.insert(rhs.clone());
        }

        swaps.push(IndexSwap { indexes: (lhs.clone(), rhs.clone()) });

        let is_unique_index_lhs = indexes_set.insert(lhs.clone());
        if !is_unique_index_lhs {
            duplicate_indexes.insert(lhs.clone());
        }
        let is_unique_index_rhs = indexes_set.insert(rhs.clone());
        if !is_unique_index_rhs {
            duplicate_indexes.insert(rhs.clone());
        }
    }
    if !duplicate_indexes.is_empty() {
        let duplicate_indexes: Vec<_> = duplicate_indexes.into_iter().collect();
        if let [index] = duplicate_indexes.as_slice() {
            return Err(MeilisearchHttpError::SwapDuplicateIndexFound(index.clone()).into());
        } else {
            return Err(MeilisearchHttpError::SwapDuplicateIndexesFound(duplicate_indexes).into());
        }
    }
    if !unauthorized_indexes.is_empty() {
        return Err(AuthenticationError::InvalidToken.into());
    }

    let task = KindWithContent::IndexSwap { swaps };

    let task = index_scheduler.register(task)?;
    let task: SummarizedTaskView = task.into();
    Ok(HttpResponse::Accepted().json(task))
}
