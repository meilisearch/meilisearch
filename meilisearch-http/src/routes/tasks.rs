use actix_web::{web, HttpRequest, HttpResponse};
use meilisearch_error::ResponseError;
use meilisearch_lib::tasks::task::TaskId;
use meilisearch_lib::tasks::TaskFilter;
use meilisearch_lib::MeiliSearch;
use serde_json::json;

use crate::analytics::Analytics;
use crate::extractors::authentication::{policies::*, GuardedData};
use crate::extractors::sequential_extractor::SeqHandler;
use crate::task::{TaskListView, TaskView};

pub fn configure(cfg: &mut web::ServiceConfig) {
    cfg.service(web::resource("").route(web::get().to(SeqHandler(get_tasks))))
        .service(web::resource("/{task_id}").route(web::get().to(SeqHandler(get_task))));
}

async fn get_tasks(
    meilisearch: GuardedData<ActionPolicy<{ actions::TASKS_GET }>, MeiliSearch>,
    req: HttpRequest,
    analytics: web::Data<dyn Analytics>,
) -> Result<HttpResponse, ResponseError> {
    analytics.publish(
        "Tasks Seen".to_string(),
        json!({ "per_task_uid": false }),
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
