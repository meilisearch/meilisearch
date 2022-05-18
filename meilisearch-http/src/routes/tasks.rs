use actix_web::{web, HttpRequest, HttpResponse};
use meilisearch_error::ResponseError;
use meilisearch_lib::milli::update::IndexDocumentsMethod;
use meilisearch_lib::tasks::task::{DocumentDeletion, TaskContent, TaskEvent, TaskId};
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
pub struct TaskFilterQuery {
    #[serde(rename = "type")]
    type_: Option<CS<TaskType>>,
    status: Option<CS<TaskStatus>>,
    index_uid: Option<CS<IndexUid>>,
}

#[rustfmt::skip]
fn task_type_matches_content(type_: &TaskType, content: &TaskContent) -> bool {
    matches!((type_, content),
        (TaskType::IndexCreation, TaskContent::IndexCreation { .. })
        | (TaskType::IndexUpdate, TaskContent::IndexUpdate { .. })
        | (TaskType::IndexDeletion, TaskContent::IndexDeletion)
        | (TaskType::DocumentAddition, TaskContent::DocumentAddition {
              merge_strategy: IndexDocumentsMethod::ReplaceDocuments,
              ..
          })
        | (TaskType::DocumentPartial, TaskContent::DocumentAddition {
              merge_strategy: IndexDocumentsMethod::UpdateDocuments,
              ..
          })
        | (TaskType::DocumentDeletion, TaskContent::DocumentDeletion(DocumentDeletion::Ids(_)))
        | (TaskType::SettingsUpdate, TaskContent::SettingsUpdate { .. })
        | (TaskType::ClearAll, TaskContent::DocumentDeletion(DocumentDeletion::Clear))
    )
}

fn task_status_matches_events(status: &TaskStatus, events: &[TaskEvent]) -> bool {
    events.last().map_or(false, |event| {
        matches!(
            (status, event),
            (TaskStatus::Enqueued, TaskEvent::Created(_))
                | (
                    TaskStatus::Processing,
                    TaskEvent::Processing(_) | TaskEvent::Batched { .. }
                )
                | (TaskStatus::Succeeded, TaskEvent::Succeded { .. })
                | (TaskStatus::Failed, TaskEvent::Failed { .. }),
        )
    })
}

async fn get_tasks(
    meilisearch: GuardedData<ActionPolicy<{ actions::TASKS_GET }>, MeiliSearch>,
    params: web::Query<TaskFilterQuery>,
    req: HttpRequest,
    analytics: web::Data<dyn Analytics>,
) -> Result<HttpResponse, ResponseError> {
    analytics.publish(
        "Tasks Seen".to_string(),
        json!({ "per_task_uid": false }),
        Some(&req),
    );

    let TaskFilterQuery {
        type_,
        status,
        index_uid,
    } = params.into_inner();

    let search_rules = &meilisearch.filters().search_rules;

    // We first filter on potential indexes and make sure
    // that the search filter restrictions are also applied.
    let indexes_filters = match index_uid {
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

    // Then we complete the task filter with other potential status and types filters.
    let filters = match (type_, status) {
        (Some(CS(types)), Some(CS(statuses))) => {
            let mut filters = indexes_filters.unwrap_or_default();
            filters.filter_fn(move |task| {
                let matches_type = types
                    .iter()
                    .any(|t| task_type_matches_content(&t, &task.content));
                let matches_status = statuses
                    .iter()
                    .any(|s| task_status_matches_events(&s, &task.events));
                matches_type && matches_status
            });
            Some(filters)
        }
        (Some(CS(types)), None) => {
            let mut filters = indexes_filters.unwrap_or_default();
            filters.filter_fn(move |task| {
                types
                    .iter()
                    .any(|t| task_type_matches_content(&t, &task.content))
            });
            Some(filters)
        }
        (None, Some(CS(statuses))) => {
            let mut filters = indexes_filters.unwrap_or_default();
            filters.filter_fn(move |task| {
                statuses
                    .iter()
                    .any(|s| task_status_matches_events(&s, &task.events))
            });
            Some(filters)
        }
        (None, None) => indexes_filters,
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
