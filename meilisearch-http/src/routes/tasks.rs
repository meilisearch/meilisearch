use actix_web::web::Data;
use actix_web::{web, HttpRequest, HttpResponse};
use index_scheduler::{IndexScheduler, TaskId};
use meilisearch_types::error::ResponseError;
use meilisearch_types::index_uid::IndexUid;
use meilisearch_types::star_or::StarOr;
use meilisearch_types::tasks::{Kind, Status};
use serde::Deserialize;
use serde_cs::vec::CS;
use serde_json::json;

use crate::analytics::Analytics;
use crate::extractors::authentication::{policies::*, GuardedData};
use crate::extractors::sequential_extractor::SeqHandler;

use super::fold_star_or;

const DEFAULT_LIMIT: fn() -> u32 = || 20;

pub fn configure(cfg: &mut web::ServiceConfig) {
    cfg.service(web::resource("").route(web::get().to(SeqHandler(get_tasks))))
        .service(web::resource("/{task_id}").route(web::get().to(SeqHandler(get_task))));
}

#[derive(Deserialize, Debug)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct TasksFilterQuery {
    #[serde(rename = "type")]
    type_: Option<CS<StarOr<Kind>>>,
    status: Option<CS<StarOr<Status>>>,
    index_uid: Option<CS<StarOr<IndexUid>>>,
    #[serde(default = "DEFAULT_LIMIT")]
    limit: u32,
    from: Option<TaskId>,
}

async fn get_tasks(
    index_scheduler: GuardedData<ActionPolicy<{ actions::TASKS_GET }>, Data<IndexScheduler>>,
    params: web::Query<TasksFilterQuery>,
    req: HttpRequest,
    analytics: web::Data<dyn Analytics>,
) -> Result<HttpResponse, ResponseError> {
    let TasksFilterQuery {
        type_,
        status,
        index_uid,
        limit,
        from,
    } = params.into_inner();

    let search_rules = &index_scheduler.filters().search_rules;

    // We first transform a potential indexUid=* into a "not specified indexUid filter"
    // for every one of the filters: type, status, and indexUid.
    let type_: Option<Vec<_>> = type_.and_then(fold_star_or);
    let status: Option<Vec<_>> = status.and_then(fold_star_or);
    let index_uid: Option<Vec<_>> = index_uid.and_then(fold_star_or);

    analytics.publish(
        "Tasks Seen".to_string(),
        json!({
            "filtered_by_index_uid": index_uid.as_ref().map_or(false, |v| !v.is_empty()),
            "filtered_by_type": type_.as_ref().map_or(false, |v| !v.is_empty()),
            "filtered_by_status": status.as_ref().map_or(false, |v| !v.is_empty()),
        }),
        Some(&req),
    );

    let mut filters = index_scheduler::Query::default();

    // Then we filter on potential indexes and make sure that the search filter
    // restrictions are also applied.
    match index_uid {
        Some(indexes) => {
            for name in indexes {
                if search_rules.is_index_authorized(&name) {
                    filters = filters.with_index(name.to_string());
                }
            }
        }
        None => {
            if !search_rules.is_index_authorized("*") {
                for (index, _policy) in search_rules.clone() {
                    filters = filters.with_index(index.to_string());
                }
            }
        }
    };

    if let Some(kinds) = type_ {
        for kind in kinds {
            filters = filters.with_kind(kind);
        }
    }

    if let Some(statuses) = status {
        for status in statuses {
            filters = filters.with_status(status);
        }
    }

    filters.from = from;
    // We +1 just to know if there is more after this "page" or not.
    let limit = limit.saturating_add(1);
    filters.limit = limit;

    let mut tasks_results: Vec<_> = index_scheduler.get_tasks(filters)?.into_iter().collect();

    // If we were able to fetch the number +1 tasks we asked
    // it means that there is more to come.
    let next = if tasks_results.len() == limit as usize {
        tasks_results.pop().map(|t| t.uid)
    } else {
        None
    };

    let from = tasks_results.first().map(|t| t.uid);

    // TODO: TAMO: define a structure to represent this type
    let tasks = json!({
        "results": tasks_results,
        "limit": limit.saturating_sub(1),
        "from": from,
        "next": next,
    });

    Ok(HttpResponse::Ok().json(tasks))
}

async fn get_task(
    index_scheduler: GuardedData<ActionPolicy<{ actions::TASKS_GET }>, Data<IndexScheduler>>,
    task_id: web::Path<TaskId>,
    req: HttpRequest,
    analytics: web::Data<dyn Analytics>,
) -> Result<HttpResponse, ResponseError> {
    let task_id = task_id.into_inner();

    analytics.publish(
        "Tasks Seen".to_string(),
        json!({ "per_task_uid": true }),
        Some(&req),
    );

    let search_rules = &index_scheduler.filters().search_rules;
    let mut filters = index_scheduler::Query::default();
    if !search_rules.is_index_authorized("*") {
        for (index, _policy) in search_rules.clone() {
            filters = filters.with_index(index);
        }
    }

    filters.uid = Some(vec![task_id]);

    if let Some(task) = index_scheduler.get_tasks(filters)?.first() {
        Ok(HttpResponse::Ok().json(task))
    } else {
        Err(index_scheduler::Error::TaskNotFound(task_id).into())
    }
}
