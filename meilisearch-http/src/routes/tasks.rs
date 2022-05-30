use actix_web::{web, HttpRequest, HttpResponse};
use meilisearch_error::ResponseError;
use meilisearch_lib::tasks::task::{TaskContent, TaskEvent, TaskId};
use meilisearch_lib::tasks::TaskFilter;
use meilisearch_lib::{IndexUid, MeiliSearch};
use serde::Deserialize;
use serde_cs::vec::CS;
use serde_json::json;
use std::str::FromStr;

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
    type_: Option<CS<StarOr<TaskType>>>,
    status: Option<CS<StarOr<TaskStatus>>>,
    index_uid: Option<CS<StarOr<IndexUid>>>,
}

/// A type that tries to match either a star (*) or
/// any other thing that implements `FromStr`.
#[derive(Debug)]
enum StarOr<T> {
    Star,
    Other(T),
}

impl<T: FromStr> FromStr for StarOr<T> {
    type Err = T::Err;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if s.trim() == "*" {
            Ok(StarOr::Star)
        } else {
            T::from_str(s).map(StarOr::Other)
        }
    }
}

/// Extracts the raw values from the `StarOr` types and
/// return None if a `StarOr::Star` is encountered.
fn fold_star_or<T>(content: Vec<StarOr<T>>) -> Option<Vec<T>> {
    content
        .into_iter()
        .fold(Some(Vec::new()), |acc, val| match (acc, val) {
            (None, _) | (_, StarOr::Star) => None,
            (Some(mut acc), StarOr::Other(uid)) => {
                acc.push(uid);
                Some(acc)
            }
        })
}

#[rustfmt::skip]
fn task_type_matches_content(type_: &TaskType, content: &TaskContent) -> bool {
    matches!((type_, content),
          (TaskType::IndexCreation, TaskContent::IndexCreation { .. })
        | (TaskType::IndexUpdate, TaskContent::IndexUpdate { .. })
        | (TaskType::IndexDeletion, TaskContent::IndexDeletion)
        | (TaskType::DocumentAdditionOrUpdate, TaskContent::DocumentAddition { .. })
        | (TaskType::DocumentDeletion, TaskContent::DocumentDeletion(_))
        | (TaskType::SettingsUpdate, TaskContent::SettingsUpdate { .. })
    )
}

#[rustfmt::skip]
fn task_status_matches_events(status: &TaskStatus, events: &[TaskEvent]) -> bool {
    events.last().map_or(false, |event| {
        matches!((status, event),
              (TaskStatus::Enqueued, TaskEvent::Created(_))
            | (TaskStatus::Processing, TaskEvent::Processing(_) | TaskEvent::Batched { .. })
            | (TaskStatus::Succeeded, TaskEvent::Succeeded { .. })
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

    // We first tranform a potential indexUid=* into a "not specified indexUid filter"
    // for every one of the filters: type, status, and indexUid.
    let type_ = type_.map(CS::into_inner).and_then(fold_star_or);
    let status = status.map(CS::into_inner).and_then(fold_star_or);
    let index_uid = index_uid.map(CS::into_inner).and_then(fold_star_or);

    // Then we filter on potential indexes and make sure that the search filter
    // restrictions are also applied.
    let indexes_filters = match index_uid {
        Some(indexes) => {
            let mut filters = TaskFilter::default();
            for name in indexes {
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
    let filters = if type_.is_some() || status.is_some() {
        let mut filters = indexes_filters.unwrap_or_default();
        filters.filter_fn(move |task| {
            let matches_type = match &type_ {
                Some(types) => types
                    .iter()
                    .any(|t| task_type_matches_content(t, &task.content)),
                None => true,
            };

            let matches_status = match &status {
                Some(statuses) => statuses
                    .iter()
                    .any(|t| task_status_matches_events(t, &task.events)),
                None => true,
            };

            matches_type && matches_status
        });
        Some(filters)
    } else {
        indexes_filters
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
