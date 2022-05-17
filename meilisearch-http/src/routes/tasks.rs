use actix_web::{web, HttpRequest, HttpResponse};
use meilisearch_error::ResponseError;
use meilisearch_lib::tasks::task::TaskId;
use meilisearch_lib::tasks::TaskFilter;
use meilisearch_lib::{IndexUid, MeiliSearch};
use serde::Deserialize;
use serde_cs::vec::CS;
use serde_json::json;

use crate::analytics::Analytics;
use crate::extractors::authentication::{policies::*, GuardedData};
use crate::extractors::sequential_extractor::SeqHandler;
use crate::task::{TaskListView, TaskStatus, TaskType, TaskView};

pub fn configure(cfg: &mut web::ServiceConfig) {
    cfg.service(web::resource("").route(web::get().to(SeqHandler(get_tasks))))
        .service(web::resource("/{task_id}").route(web::get().to(SeqHandler(get_task))));
}

#[derive(Deserialize, Debug)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct TasksFilter {
    #[serde(rename = "type")]
    type_: Option<CS<TaskType>>,
    status: Option<CS<TaskStatus>>,
    index_uid: Option<CS<IndexUid>>,
}

async fn get_tasks(
    meilisearch: GuardedData<ActionPolicy<{ actions::TASKS_GET }>, MeiliSearch>,
    params: web::Query<TasksFilter>,
    req: HttpRequest,
    analytics: web::Data<dyn Analytics>,
) -> Result<HttpResponse, ResponseError> {
    analytics.publish(
        "Tasks Seen".to_string(),
        json!({ "per_task_uid": false }),
        Some(&req),
    );

    let TasksFilter {
        type_,
        status,
        index_uid,
    } = params.into_inner();

    let search_rules = &meilisearch.filters().search_rules;
    let filters = match index_uid {
        Some(indexes) => {
            let mut filters = TaskFilter::default();
            for name in indexes.into_inner() {
                if search_rules.is_index_authorized(&name) {
                    filters.filter_index(name.to_string());
                }
            }
            Some(filters)
        }
        None => {
            if search_rules.is_index_authorized("*") {
                None
            } else {
                let mut filters = TaskFilter::default();
                for (index, _policy) in search_rules.clone() {
                    filters.filter_index(index);
                }
                Some(filters)
            }
        }
    };

    let tasks: TaskListView = meilisearch
        .list_tasks(filters, None, None)
        .await?
        .into_iter()
        .map(TaskView::from)
        .collect::<Vec<_>>()
        .into();

    Ok(HttpResponse::Ok().json(tasks))
}

async fn get_task(
    meilisearch: GuardedData<ActionPolicy<{ actions::TASKS_GET }>, MeiliSearch>,
    task_id: web::Path<TaskId>,
    req: HttpRequest,
    analytics: web::Data<dyn Analytics>,
) -> Result<HttpResponse, ResponseError> {
    analytics.publish(
        "Tasks Seen".to_string(),
        json!({ "per_task_uid": true }),
        Some(&req),
    );

    let search_rules = &meilisearch.filters().search_rules;
    let filters = if search_rules.is_index_authorized("*") {
        None
    } else {
        let mut filters = TaskFilter::default();
        for (index, _policy) in search_rules.clone() {
            filters.filter_index(index);
        }
        Some(filters)
    };

    let task: TaskView = meilisearch
        .get_task(task_id.into_inner(), filters)
        .await?
        .into();

    Ok(HttpResponse::Ok().json(task))
}
