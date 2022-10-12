use actix_web::web::Data;
use actix_web::{web, HttpRequest, HttpResponse};
use index_scheduler::{IndexScheduler, TaskId};
use meilisearch_types::error::ResponseError;
use meilisearch_types::index_uid::IndexUid;
use meilisearch_types::settings::{Settings, Unchecked};
use meilisearch_types::star_or::StarOr;
use meilisearch_types::tasks::{serialize_duration, Details, Kind, Status, Task};
use serde::{Deserialize, Serialize};
use serde_cs::vec::CS;
use serde_json::json;
use time::{Duration, OffsetDateTime};

use crate::analytics::Analytics;
use crate::extractors::authentication::{policies::*, GuardedData};
use crate::extractors::sequential_extractor::SeqHandler;

use super::fold_star_or;

const DEFAULT_LIMIT: fn() -> u32 = || 20;

pub fn configure(cfg: &mut web::ServiceConfig) {
    cfg.service(web::resource("").route(web::get().to(SeqHandler(get_tasks))))
        .service(web::resource("/{task_id}").route(web::get().to(SeqHandler(get_task))));
}

#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct TaskView {
    pub uid: TaskId,
    #[serde(default)]
    pub index_uid: Option<String>,
    pub status: Status,
    #[serde(rename = "type")]
    pub kind: Kind,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub details: Option<DetailsView>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<ResponseError>,

    #[serde(
        serialize_with = "serialize_duration",
        skip_serializing_if = "Option::is_none",
        default
    )]
    pub duration: Option<Duration>,
    #[serde(with = "time::serde::rfc3339")]
    pub enqueued_at: OffsetDateTime,
    #[serde(
        with = "time::serde::rfc3339::option",
        skip_serializing_if = "Option::is_none",
        default
    )]
    pub started_at: Option<OffsetDateTime>,
    #[serde(
        with = "time::serde::rfc3339::option",
        skip_serializing_if = "Option::is_none",
        default
    )]
    pub finished_at: Option<OffsetDateTime>,
}

impl From<Task> for TaskView {
    fn from(task: Task) -> Self {
        TaskView {
            uid: task.uid,
            index_uid: task
                .indexes()
                .and_then(|vec| vec.first().map(|i| i.to_string())),
            status: task.status,
            kind: task.kind.as_kind(),
            details: task.details.map(DetailsView::from),
            error: task.error.clone(),
            duration: task
                .started_at
                .zip(task.finished_at)
                .map(|(start, end)| end - start),
            enqueued_at: task.enqueued_at,
            started_at: task.started_at,
            finished_at: task.finished_at,
        }
    }
}

#[derive(Default, Debug, PartialEq, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct DetailsView {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub received_documents: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub indexed_documents: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub primary_key: Option<Option<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub received_document_ids: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub deleted_documents: Option<Option<u64>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub matched_tasks: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub deleted_tasks: Option<Option<usize>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub original_query: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub dump_uid: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(flatten)]
    pub settings: Option<Settings<Unchecked>>,
}

impl From<Details> for DetailsView {
    fn from(details: Details) -> Self {
        match details.clone() {
            Details::DocumentAddition {
                received_documents,
                indexed_documents,
            } => DetailsView {
                received_documents: Some(received_documents),
                indexed_documents: Some(indexed_documents),
                ..DetailsView::default()
            },
            Details::Settings { settings } => DetailsView {
                settings: Some(settings),
                ..DetailsView::default()
            },
            Details::IndexInfo { primary_key } => DetailsView {
                primary_key: Some(primary_key),
                ..DetailsView::default()
            },
            Details::DocumentDeletion {
                received_document_ids,
                deleted_documents,
            } => DetailsView {
                received_document_ids: Some(received_document_ids),
                deleted_documents: Some(deleted_documents),
                ..DetailsView::default()
            },
            Details::ClearAll { deleted_documents } => DetailsView {
                deleted_documents: Some(deleted_documents),
                ..DetailsView::default()
            },
            Details::DeleteTasks {
                matched_tasks,
                deleted_tasks,
                original_query,
            } => DetailsView {
                matched_tasks: Some(matched_tasks),
                deleted_tasks: Some(deleted_tasks),
                original_query: Some(original_query),
                ..DetailsView::default()
            },
            Details::Dump { dump_uid } => DetailsView {
                dump_uid: Some(dump_uid),
                ..DetailsView::default()
            },
        }
    }
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

#[rustfmt::skip]
fn task_type_matches_content(type_: &TaskType, content: &TaskContent) -> bool {
    matches!((type_, content),
          (TaskType::IndexCreation, TaskContent::IndexCreation { .. })
        | (TaskType::IndexUpdate, TaskContent::IndexUpdate { .. })
        | (TaskType::IndexDeletion, TaskContent::IndexDeletion { .. })
        | (TaskType::DocumentAdditionOrUpdate, TaskContent::DocumentAddition { .. })
        | (TaskType::DocumentDeletion, TaskContent::DocumentDeletion{ .. })
        | (TaskType::SettingsUpdate, TaskContent::SettingsUpdate { .. })
        | (TaskType::DumpCreation, TaskContent::Dump { .. })
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
