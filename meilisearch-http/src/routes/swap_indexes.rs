use std::collections::HashSet;

use actix_web::web::Data;
use actix_web::{web, HttpResponse};
use index_scheduler::IndexScheduler;
use meilisearch_types::error::ResponseError;
use meilisearch_types::tasks::KindWithContent;
use serde::Deserialize;

use self::errors::{DuplicateSwappedIndexError, IndexesNotFoundError};
use crate::extractors::authentication::policies::*;
use crate::extractors::authentication::GuardedData;
use crate::extractors::sequential_extractor::SeqHandler;
use crate::routes::tasks::TaskView;

pub fn configure(cfg: &mut web::ServiceConfig) {
    cfg.service(web::resource("").route(web::post().to(SeqHandler(swap_indexes))));
}

#[derive(Deserialize, Debug, Clone, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct SwapIndexesPayload {
    swap: (String, String),
}

pub async fn swap_indexes(
    index_scheduler: GuardedData<ActionPolicy<{ actions::INDEXES_SWAP }>, Data<IndexScheduler>>,
    params: web::Json<Vec<SwapIndexesPayload>>,
) -> Result<HttpResponse, ResponseError> {
    let search_rules = &index_scheduler.filters().search_rules;

    let mut swaps = vec![];
    let mut indexes_set = HashSet::<String>::default();
    let mut unknown_indexes = HashSet::new();
    let mut duplicate_indexes = HashSet::new();
    for SwapIndexesPayload { swap: (lhs, rhs) } in params.into_inner().into_iter() {
        if !search_rules.is_index_authorized(&lhs) {
            unknown_indexes.insert(lhs.clone());
        }
        if !search_rules.is_index_authorized(&rhs) {
            unknown_indexes.insert(rhs.clone());
        }

        swaps.push((lhs.clone(), rhs.clone()));

        let is_unique_index_lhs = indexes_set.insert(lhs.clone());
        if !is_unique_index_lhs {
            duplicate_indexes.insert(lhs);
        }
        let is_unique_index_rhs = indexes_set.insert(rhs.clone());
        if !is_unique_index_rhs {
            duplicate_indexes.insert(rhs);
        }
    }
    if !duplicate_indexes.is_empty() {
        return Err(DuplicateSwappedIndexError {
            indexes: duplicate_indexes.into_iter().collect(),
        }
        .into());
    }
    if !unknown_indexes.is_empty() {
        return Err(IndexesNotFoundError { indexes: unknown_indexes.into_iter().collect() }.into());
    }

    let task = KindWithContent::IndexSwap { swaps };

    let task = index_scheduler.register(task)?;
    let task_view = TaskView::from_task(&task);

    Ok(HttpResponse::Accepted().json(task_view))
}

pub mod errors {
    use std::fmt::Display;

    use meilisearch_types::error::{Code, ErrorCode};

    #[derive(Debug)]
    pub struct IndexesNotFoundError {
        pub indexes: Vec<String>,
    }
    impl Display for IndexesNotFoundError {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            if self.indexes.len() == 1 {
                write!(f, "Index `{}` not found,", self.indexes[0])?;
            } else {
                write!(f, "Indexes `{}`", self.indexes[0])?;
                for index in self.indexes.iter().skip(1) {
                    write!(f, ", `{}`", index)?;
                }
                write!(f, "not found.")?;
            }
            Ok(())
        }
    }
    impl std::error::Error for IndexesNotFoundError {}
    impl ErrorCode for IndexesNotFoundError {
        fn error_code(&self) -> Code {
            Code::IndexNotFound
        }
    }
    #[derive(Debug)]
    pub struct DuplicateSwappedIndexError {
        pub indexes: Vec<String>,
    }
    impl Display for DuplicateSwappedIndexError {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            if self.indexes.len() == 1 {
                write!(f, "Indexes must be declared only once during a swap. `{}` was specified several times.", self.indexes[0])?;
            } else {
                write!(
                    f,
                    "Indexes must be declared only once during a swap. `{}`",
                    self.indexes[0]
                )?;
                for index in self.indexes.iter().skip(1) {
                    write!(f, ", `{}`", index)?;
                }
                write!(f, "were specified several times.")?;
            }

            Ok(())
        }
    }
    impl std::error::Error for DuplicateSwappedIndexError {}
    impl ErrorCode for DuplicateSwappedIndexError {
        fn error_code(&self) -> Code {
            Code::DuplicateIndexFound
        }
    }
}
