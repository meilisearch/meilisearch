use std::str::FromStr;

use actix_web::web::Data;
use actix_web::{web, HttpRequest, HttpResponse};
use index_scheduler::{IndexScheduler, Query, TaskId};
use meilisearch_types::error::ResponseError;
use meilisearch_types::index_uid::IndexUid;
use meilisearch_types::settings::{Settings, Unchecked};
use meilisearch_types::star_or::StarOr;
use meilisearch_types::tasks::{
    serialize_duration, Details, IndexSwap, Kind, KindWithContent, Status, Task,
};
use serde::{Deserialize, Serialize};
use serde_cs::vec::CS;
use serde_json::json;
use time::{Duration, OffsetDateTime};
use tokio::task;

use self::date_deserializer::{deserialize_date, DeserializeDateOption};
use super::{fold_star_or, SummarizedTaskView};
use crate::analytics::Analytics;
use crate::extractors::authentication::policies::*;
use crate::extractors::authentication::GuardedData;
use crate::extractors::sequential_extractor::SeqHandler;

const DEFAULT_LIMIT: fn() -> u32 = || 20;

pub fn configure(cfg: &mut web::ServiceConfig) {
    cfg.service(
        web::resource("")
            .route(web::get().to(SeqHandler(get_tasks)))
            .route(web::delete().to(SeqHandler(delete_tasks))),
    )
    .service(web::resource("/cancel").route(web::post().to(SeqHandler(cancel_tasks))))
    .service(web::resource("/{task_id}").route(web::get().to(SeqHandler(get_task))));
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct TaskView {
    pub uid: TaskId,
    #[serde(default)]
    pub index_uid: Option<String>,
    pub status: Status,
    #[serde(rename = "type")]
    pub kind: Kind,
    pub canceled_by: Option<TaskId>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub details: Option<DetailsView>,
    pub error: Option<ResponseError>,
    #[serde(serialize_with = "serialize_duration", default)]
    pub duration: Option<Duration>,
    #[serde(with = "time::serde::rfc3339")]
    pub enqueued_at: OffsetDateTime,
    #[serde(with = "time::serde::rfc3339::option", default)]
    pub started_at: Option<OffsetDateTime>,
    #[serde(with = "time::serde::rfc3339::option", default)]
    pub finished_at: Option<OffsetDateTime>,
}

impl TaskView {
    pub fn from_task(task: &Task) -> TaskView {
        TaskView {
            uid: task.uid,
            index_uid: task.index_uid().map(ToOwned::to_owned),
            status: task.status,
            kind: task.kind.as_kind(),
            canceled_by: task.canceled_by,
            details: task.details.clone().map(DetailsView::from),
            error: task.error.clone(),
            duration: task.started_at.zip(task.finished_at).map(|(start, end)| end - start),
            enqueued_at: task.enqueued_at,
            started_at: task.started_at,
            finished_at: task.finished_at,
        }
    }
}

#[derive(Default, Debug, PartialEq, Eq, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct DetailsView {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub received_documents: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub indexed_documents: Option<Option<u64>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub primary_key: Option<Option<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub provided_ids: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub deleted_documents: Option<Option<u64>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub matched_tasks: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub canceled_tasks: Option<Option<u64>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub deleted_tasks: Option<Option<u64>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub original_filter: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub dump_uid: Option<Option<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(flatten)]
    pub settings: Option<Box<Settings<Unchecked>>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub swaps: Option<Vec<IndexSwap>>,
}

impl From<Details> for DetailsView {
    fn from(details: Details) -> Self {
        match details {
            Details::DocumentAdditionOrUpdate { received_documents, indexed_documents } => {
                DetailsView {
                    received_documents: Some(received_documents),
                    indexed_documents: Some(indexed_documents),
                    ..DetailsView::default()
                }
            }
            Details::SettingsUpdate { settings } => {
                DetailsView { settings: Some(settings), ..DetailsView::default() }
            }
            Details::IndexInfo { primary_key } => {
                DetailsView { primary_key: Some(primary_key), ..DetailsView::default() }
            }
            Details::DocumentDeletion {
                provided_ids: received_document_ids,
                deleted_documents,
            } => DetailsView {
                provided_ids: Some(received_document_ids),
                deleted_documents: Some(deleted_documents),
                ..DetailsView::default()
            },
            Details::ClearAll { deleted_documents } => {
                DetailsView { deleted_documents: Some(deleted_documents), ..DetailsView::default() }
            }
            Details::TaskCancelation { matched_tasks, canceled_tasks, original_filter } => {
                DetailsView {
                    matched_tasks: Some(matched_tasks),
                    canceled_tasks: Some(canceled_tasks),
                    original_filter: Some(original_filter),
                    ..DetailsView::default()
                }
            }
            Details::TaskDeletion { matched_tasks, deleted_tasks, original_filter } => {
                DetailsView {
                    matched_tasks: Some(matched_tasks),
                    deleted_tasks: Some(deleted_tasks),
                    original_filter: Some(original_filter),
                    ..DetailsView::default()
                }
            }
            Details::Dump { dump_uid } => {
                DetailsView { dump_uid: Some(dump_uid), ..DetailsView::default() }
            }
            Details::IndexSwap { swaps } => {
                DetailsView { swaps: Some(swaps), ..Default::default() }
            }
        }
    }
}

#[derive(Deserialize, Debug)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct TaskCommonQueryRaw {
    pub uids: Option<CS<String>>,
    pub canceled_by: Option<CS<String>>,
    pub types: Option<CS<StarOr<String>>>,
    pub statuses: Option<CS<StarOr<String>>>,
    pub index_uids: Option<CS<StarOr<String>>>,
}
impl TaskCommonQueryRaw {
    fn validate(self) -> Result<TaskCommonQuery, ResponseError> {
        let Self { uids, canceled_by, types, statuses, index_uids } = self;
        let uids = if let Some(uids) = uids {
            Some(
                uids.into_iter()
                    .map(|uid_string| {
                        uid_string.parse::<u32>().map_err(|_e| {
                            index_scheduler::Error::InvalidTaskUids { task_uid: uid_string }.into()
                        })
                    })
                    .collect::<Result<Vec<u32>, ResponseError>>()?,
            )
        } else {
            None
        };
        let canceled_by = if let Some(canceled_by) = canceled_by {
            Some(
                canceled_by
                    .into_iter()
                    .map(|canceled_by_string| {
                        canceled_by_string.parse::<u32>().map_err(|_e| {
                            index_scheduler::Error::InvalidTaskCanceledBy {
                                canceled_by: canceled_by_string,
                            }
                            .into()
                        })
                    })
                    .collect::<Result<Vec<u32>, ResponseError>>()?,
            )
        } else {
            None
        };

        let types = if let Some(types) = types.and_then(fold_star_or) as Option<Vec<String>> {
            Some(
                types
                    .into_iter()
                    .map(|type_string| {
                        Kind::from_str(&type_string).map_err(|_e| {
                            index_scheduler::Error::InvalidTaskTypes { type_: type_string }.into()
                        })
                    })
                    .collect::<Result<Vec<Kind>, ResponseError>>()?,
            )
        } else {
            None
        };
        let statuses = if let Some(statuses) =
            statuses.and_then(fold_star_or) as Option<Vec<String>>
        {
            Some(
                statuses
                    .into_iter()
                    .map(|status_string| {
                        Status::from_str(&status_string).map_err(|_e| {
                            index_scheduler::Error::InvalidTaskStatuses { status: status_string }
                                .into()
                        })
                    })
                    .collect::<Result<Vec<Status>, ResponseError>>()?,
            )
        } else {
            None
        };

        let index_uids =
            if let Some(index_uids) = index_uids.and_then(fold_star_or) as Option<Vec<String>> {
                Some(
                    index_uids
                        .into_iter()
                        .map(|index_uid_string| {
                            IndexUid::from_str(&index_uid_string)
                                .map(|index_uid| index_uid.to_string())
                                .map_err(|_e| {
                                    index_scheduler::Error::InvalidIndexUid {
                                        index_uid: index_uid_string,
                                    }
                                    .into()
                                })
                        })
                        .collect::<Result<Vec<String>, ResponseError>>()?,
                )
            } else {
                None
            };
        Ok(TaskCommonQuery { types, uids, canceled_by, statuses, index_uids })
    }
}

#[derive(Deserialize, Debug)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct TaskDateQueryRaw {
    pub after_enqueued_at: Option<String>,
    pub before_enqueued_at: Option<String>,
    pub after_started_at: Option<String>,
    pub before_started_at: Option<String>,
    pub after_finished_at: Option<String>,
    pub before_finished_at: Option<String>,
}
impl TaskDateQueryRaw {
    fn validate(self) -> Result<TaskDateQuery, ResponseError> {
        let Self {
            after_enqueued_at,
            before_enqueued_at,
            after_started_at,
            before_started_at,
            after_finished_at,
            before_finished_at,
        } = self;

        let mut query = TaskDateQuery {
            after_enqueued_at: None,
            before_enqueued_at: None,
            after_started_at: None,
            before_started_at: None,
            after_finished_at: None,
            before_finished_at: None,
        };

        for (field_name, string_value, before_or_after, dest) in [
            (
                "afterEnqueuedAt",
                after_enqueued_at,
                DeserializeDateOption::After,
                &mut query.after_enqueued_at,
            ),
            (
                "beforeEnqueuedAt",
                before_enqueued_at,
                DeserializeDateOption::Before,
                &mut query.before_enqueued_at,
            ),
            (
                "afterStartedAt",
                after_started_at,
                DeserializeDateOption::After,
                &mut query.after_started_at,
            ),
            (
                "beforeStartedAt",
                before_started_at,
                DeserializeDateOption::Before,
                &mut query.before_started_at,
            ),
            (
                "afterFinishedAt",
                after_finished_at,
                DeserializeDateOption::After,
                &mut query.after_finished_at,
            ),
            (
                "beforeFinishedAt",
                before_finished_at,
                DeserializeDateOption::Before,
                &mut query.before_finished_at,
            ),
        ] {
            if let Some(string_value) = string_value {
                *dest = Some(deserialize_date(field_name, &string_value, before_or_after)?);
            }
        }

        Ok(query)
    }
}

#[derive(Deserialize, Debug)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct TasksFilterQueryRaw {
    #[serde(flatten)]
    pub common: TaskCommonQueryRaw,
    #[serde(default = "DEFAULT_LIMIT")]
    pub limit: u32,
    pub from: Option<TaskId>,
    #[serde(flatten)]
    pub dates: TaskDateQueryRaw,
}

#[derive(Deserialize, Debug)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct TaskDeletionOrCancelationQueryRaw {
    #[serde(flatten)]
    pub common: TaskCommonQueryRaw,
    #[serde(flatten)]
    pub dates: TaskDateQueryRaw,
}

impl TasksFilterQueryRaw {
    fn validate(self) -> Result<TasksFilterQuery, ResponseError> {
        let Self { common, limit, from, dates } = self;
        let common = common.validate()?;
        let dates = dates.validate()?;

        Ok(TasksFilterQuery { common, limit, from, dates })
    }
}

impl TaskDeletionOrCancelationQueryRaw {
    fn validate(self) -> Result<TaskDeletionOrCancelationQuery, ResponseError> {
        let Self { common, dates } = self;
        let common = common.validate()?;
        let dates = dates.validate()?;

        Ok(TaskDeletionOrCancelationQuery { common, dates })
    }
}

#[derive(Serialize, Debug)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct TaskDateQuery {
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        serialize_with = "time::serde::rfc3339::option::serialize"
    )]
    after_enqueued_at: Option<OffsetDateTime>,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        serialize_with = "time::serde::rfc3339::option::serialize"
    )]
    before_enqueued_at: Option<OffsetDateTime>,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        serialize_with = "time::serde::rfc3339::option::serialize"
    )]
    after_started_at: Option<OffsetDateTime>,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        serialize_with = "time::serde::rfc3339::option::serialize"
    )]
    before_started_at: Option<OffsetDateTime>,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        serialize_with = "time::serde::rfc3339::option::serialize"
    )]
    after_finished_at: Option<OffsetDateTime>,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        serialize_with = "time::serde::rfc3339::option::serialize"
    )]
    before_finished_at: Option<OffsetDateTime>,
}

#[derive(Debug)]
pub struct TaskCommonQuery {
    types: Option<Vec<Kind>>,
    uids: Option<Vec<TaskId>>,
    canceled_by: Option<Vec<TaskId>>,
    statuses: Option<Vec<Status>>,
    index_uids: Option<Vec<String>>,
}

#[derive(Debug)]
pub struct TasksFilterQuery {
    limit: u32,
    from: Option<TaskId>,
    common: TaskCommonQuery,
    dates: TaskDateQuery,
}

#[derive(Debug)]
pub struct TaskDeletionOrCancelationQuery {
    common: TaskCommonQuery,
    dates: TaskDateQuery,
}

async fn cancel_tasks(
    index_scheduler: GuardedData<ActionPolicy<{ actions::TASKS_CANCEL }>, Data<IndexScheduler>>,
    params: web::Query<TaskDeletionOrCancelationQueryRaw>,
    req: HttpRequest,
    analytics: web::Data<dyn Analytics>,
) -> Result<HttpResponse, ResponseError> {
    let query = params.into_inner().validate()?;
    let TaskDeletionOrCancelationQuery {
        common: TaskCommonQuery { types, uids, canceled_by, statuses, index_uids },
        dates:
            TaskDateQuery {
                after_enqueued_at,
                before_enqueued_at,
                after_started_at,
                before_started_at,
                after_finished_at,
                before_finished_at,
            },
    } = query;

    analytics.publish(
        "Tasks Canceled".to_string(),
        json!({
            "filtered_by_uid": uids.is_some(),
            "filtered_by_index_uid": index_uids.is_some(),
            "filtered_by_type": types.is_some(),
            "filtered_by_status": statuses.is_some(),
            "filtered_by_canceled_by": canceled_by.is_some(),
            "filtered_by_before_enqueued_at": before_enqueued_at.is_some(),
            "filtered_by_after_enqueued_at": after_enqueued_at.is_some(),
            "filtered_by_before_started_at": before_started_at.is_some(),
            "filtered_by_after_started_at": after_started_at.is_some(),
            "filtered_by_before_finished_at": before_finished_at.is_some(),
            "filtered_by_after_finished_at": after_finished_at.is_some(),
        }),
        Some(&req),
    );

    let query = Query {
        limit: None,
        from: None,
        statuses,
        types,
        index_uids,
        uids,
        canceled_by,
        before_enqueued_at,
        after_enqueued_at,
        before_started_at,
        after_started_at,
        before_finished_at,
        after_finished_at,
    };

    if query.is_empty() {
        return Err(index_scheduler::Error::TaskCancelationWithEmptyQuery.into());
    }

    let tasks = index_scheduler.get_task_ids_from_authorized_indexes(
        &index_scheduler.read_txn()?,
        &query,
        &index_scheduler.filters().search_rules.authorized_indexes(),
    )?;
    let task_cancelation =
        KindWithContent::TaskCancelation { query: format!("?{}", req.query_string()), tasks };

    let task = task::spawn_blocking(move || index_scheduler.register(task_cancelation)).await??;
    let task: SummarizedTaskView = task.into();

    Ok(HttpResponse::Ok().json(task))
}

async fn delete_tasks(
    index_scheduler: GuardedData<ActionPolicy<{ actions::TASKS_DELETE }>, Data<IndexScheduler>>,
    params: web::Query<TaskDeletionOrCancelationQueryRaw>,
    req: HttpRequest,
    analytics: web::Data<dyn Analytics>,
) -> Result<HttpResponse, ResponseError> {
    let TaskDeletionOrCancelationQuery {
        common: TaskCommonQuery { types, uids, canceled_by, statuses, index_uids },
        dates:
            TaskDateQuery {
                after_enqueued_at,
                before_enqueued_at,
                after_started_at,
                before_started_at,
                after_finished_at,
                before_finished_at,
            },
    } = params.into_inner().validate()?;

    analytics.publish(
        "Tasks Deleted".to_string(),
        json!({
            "filtered_by_uid": uids.is_some(),
            "filtered_by_index_uid": index_uids.is_some(),
            "filtered_by_type": types.is_some(),
            "filtered_by_status": statuses.is_some(),
            "filtered_by_canceled_by": canceled_by.is_some(),
            "filtered_by_before_enqueued_at": before_enqueued_at.is_some(),
            "filtered_by_after_enqueued_at": after_enqueued_at.is_some(),
            "filtered_by_before_started_at": before_started_at.is_some(),
            "filtered_by_after_started_at": after_started_at.is_some(),
            "filtered_by_before_finished_at": before_finished_at.is_some(),
            "filtered_by_after_finished_at": after_finished_at.is_some(),
        }),
        Some(&req),
    );

    let query = Query {
        limit: None,
        from: None,
        statuses,
        types,
        index_uids,
        uids,
        canceled_by,
        after_enqueued_at,
        before_enqueued_at,
        after_started_at,
        before_started_at,
        after_finished_at,
        before_finished_at,
    };

    if query.is_empty() {
        return Err(index_scheduler::Error::TaskDeletionWithEmptyQuery.into());
    }

    let tasks = index_scheduler.get_task_ids_from_authorized_indexes(
        &index_scheduler.read_txn()?,
        &query,
        &index_scheduler.filters().search_rules.authorized_indexes(),
    )?;
    let task_deletion =
        KindWithContent::TaskDeletion { query: format!("?{}", req.query_string()), tasks };

    let task = task::spawn_blocking(move || index_scheduler.register(task_deletion)).await??;
    let task: SummarizedTaskView = task.into();

    Ok(HttpResponse::Ok().json(task))
}

#[derive(Debug, Serialize)]
pub struct AllTasks {
    results: Vec<TaskView>,
    limit: u32,
    from: Option<u32>,
    next: Option<u32>,
}

async fn get_tasks(
    index_scheduler: GuardedData<ActionPolicy<{ actions::TASKS_GET }>, Data<IndexScheduler>>,
    params: web::Query<TasksFilterQueryRaw>,
    req: HttpRequest,
    analytics: web::Data<dyn Analytics>,
) -> Result<HttpResponse, ResponseError> {
    analytics.get_tasks(&params, &req);

    let TasksFilterQuery {
        common: TaskCommonQuery { types, uids, canceled_by, statuses, index_uids },
        limit,
        from,
        dates:
            TaskDateQuery {
                after_enqueued_at,
                before_enqueued_at,
                after_started_at,
                before_started_at,
                after_finished_at,
                before_finished_at,
            },
    } = params.into_inner().validate()?;

    // We +1 just to know if there is more after this "page" or not.
    let limit = limit.saturating_add(1);

    let query = index_scheduler::Query {
        limit: Some(limit),
        from,
        statuses,
        types,
        index_uids,
        uids,
        canceled_by,
        before_enqueued_at,
        after_enqueued_at,
        before_started_at,
        after_started_at,
        before_finished_at,
        after_finished_at,
    };

    let mut tasks_results: Vec<TaskView> = index_scheduler
        .get_tasks_from_authorized_indexes(
            query,
            index_scheduler.filters().search_rules.authorized_indexes(),
        )?
        .into_iter()
        .map(|t| TaskView::from_task(&t))
        .collect();

    // If we were able to fetch the number +1 tasks we asked
    // it means that there is more to come.
    let next = if tasks_results.len() == limit as usize {
        tasks_results.pop().map(|t| t.uid)
    } else {
        None
    };

    let from = tasks_results.first().map(|t| t.uid);

    let tasks = AllTasks { results: tasks_results, limit: limit.saturating_sub(1), from, next };
    Ok(HttpResponse::Ok().json(tasks))
}

async fn get_task(
    index_scheduler: GuardedData<ActionPolicy<{ actions::TASKS_GET }>, Data<IndexScheduler>>,
    task_uid: web::Path<String>,
    req: HttpRequest,
    analytics: web::Data<dyn Analytics>,
) -> Result<HttpResponse, ResponseError> {
    let task_uid_string = task_uid.into_inner();

    let task_uid: TaskId = match task_uid_string.parse() {
        Ok(id) => id,
        Err(_e) => {
            return Err(index_scheduler::Error::InvalidTaskUids { task_uid: task_uid_string }.into())
        }
    };

    analytics.publish("Tasks Seen".to_string(), json!({ "per_task_uid": true }), Some(&req));

    let query = index_scheduler::Query { uids: Some(vec![task_uid]), ..Query::default() };

    if let Some(task) = index_scheduler
        .get_tasks_from_authorized_indexes(
            query,
            index_scheduler.filters().search_rules.authorized_indexes(),
        )?
        .first()
    {
        let task_view = TaskView::from_task(task);
        Ok(HttpResponse::Ok().json(task_view))
    } else {
        Err(index_scheduler::Error::TaskNotFound(task_uid).into())
    }
}

pub(crate) mod date_deserializer {
    use meilisearch_types::error::ResponseError;
    use time::format_description::well_known::Rfc3339;
    use time::macros::format_description;
    use time::{Date, Duration, OffsetDateTime, Time};

    pub enum DeserializeDateOption {
        Before,
        After,
    }

    pub fn deserialize_date(
        field_name: &str,
        value: &str,
        option: DeserializeDateOption,
    ) -> std::result::Result<OffsetDateTime, ResponseError> {
        // We can't parse using time's rfc3339 format, since then we won't know what part of the
        // datetime was not explicitly specified, and thus we won't be able to increment it to the
        // next step.
        if let Ok(datetime) = OffsetDateTime::parse(value, &Rfc3339) {
            // fully specified up to the second
            // we assume that the subseconds are 0 if not specified, and we don't increment to the next second
            Ok(datetime)
        } else if let Ok(datetime) = Date::parse(
            value,
            format_description!("[year repr:full base:calendar]-[month repr:numerical]-[day]"),
        ) {
            let datetime = datetime.with_time(Time::MIDNIGHT).assume_utc();
            // add one day since the time was not specified
            match option {
                DeserializeDateOption::Before => Ok(datetime),
                DeserializeDateOption::After => {
                    let datetime = datetime.checked_add(Duration::days(1)).unwrap_or(datetime);
                    Ok(datetime)
                }
            }
        } else {
            Err(index_scheduler::Error::InvalidTaskDate {
                field: field_name.to_string(),
                date: value.to_string(),
            }
            .into())
        }
    }
}

#[cfg(test)]
mod tests {
    use meili_snap::snapshot;

    use crate::routes::tasks::{TaskDeletionOrCancelationQueryRaw, TasksFilterQueryRaw};

    #[test]
    fn deserialize_task_filter_dates() {
        {
            let json = r#" { 
                "afterEnqueuedAt": "2021-12-03", 
                "beforeEnqueuedAt": "2021-12-03",
                "afterStartedAt": "2021-12-03", 
                "beforeStartedAt": "2021-12-03",
                "afterFinishedAt": "2021-12-03", 
                "beforeFinishedAt": "2021-12-03"
            } "#;
            let query = serde_json::from_str::<TaskDeletionOrCancelationQueryRaw>(json)
                .unwrap()
                .validate()
                .unwrap();
            snapshot!(format!("{:?}", query.dates.after_enqueued_at.unwrap()), @"2021-12-04 0:00:00.0 +00:00:00");
            snapshot!(format!("{:?}", query.dates.before_enqueued_at.unwrap()), @"2021-12-03 0:00:00.0 +00:00:00");
            snapshot!(format!("{:?}", query.dates.after_started_at.unwrap()), @"2021-12-04 0:00:00.0 +00:00:00");
            snapshot!(format!("{:?}", query.dates.before_started_at.unwrap()), @"2021-12-03 0:00:00.0 +00:00:00");
            snapshot!(format!("{:?}", query.dates.after_finished_at.unwrap()), @"2021-12-04 0:00:00.0 +00:00:00");
            snapshot!(format!("{:?}", query.dates.before_finished_at.unwrap()), @"2021-12-03 0:00:00.0 +00:00:00");
        }
        {
            let json = r#" { "afterEnqueuedAt": "2021-12-03T23:45:23Z", "beforeEnqueuedAt": "2021-12-03T23:45:23Z" } "#;
            let query = serde_json::from_str::<TaskDeletionOrCancelationQueryRaw>(json)
                .unwrap()
                .validate()
                .unwrap();
            snapshot!(format!("{:?}", query.dates.after_enqueued_at.unwrap()), @"2021-12-03 23:45:23.0 +00:00:00");
            snapshot!(format!("{:?}", query.dates.before_enqueued_at.unwrap()), @"2021-12-03 23:45:23.0 +00:00:00");
        }
        {
            let json = r#" { "afterEnqueuedAt": "1997-11-12T09:55:06-06:20" } "#;
            let query = serde_json::from_str::<TaskDeletionOrCancelationQueryRaw>(json)
                .unwrap()
                .validate()
                .unwrap();
            snapshot!(format!("{:?}", query.dates.after_enqueued_at.unwrap()), @"1997-11-12 9:55:06.0 -06:20:00");
        }
        {
            let json = r#" { "afterEnqueuedAt": "1997-11-12T09:55:06+00:00" } "#;
            let query = serde_json::from_str::<TaskDeletionOrCancelationQueryRaw>(json)
                .unwrap()
                .validate()
                .unwrap();
            snapshot!(format!("{:?}", query.dates.after_enqueued_at.unwrap()), @"1997-11-12 9:55:06.0 +00:00:00");
        }
        {
            let json = r#" { "afterEnqueuedAt": "1997-11-12T09:55:06.200000300Z" } "#;
            let query = serde_json::from_str::<TaskDeletionOrCancelationQueryRaw>(json)
                .unwrap()
                .validate()
                .unwrap();
            snapshot!(format!("{:?}", query.dates.after_enqueued_at.unwrap()), @"1997-11-12 9:55:06.2000003 +00:00:00");
        }
        {
            let json = r#" { "afterFinishedAt": "2021" } "#;
            let err = serde_json::from_str::<TaskDeletionOrCancelationQueryRaw>(json)
                .unwrap()
                .validate()
                .unwrap_err();
            snapshot!(format!("{err}"), @"Task `afterFinishedAt` `2021` is invalid. It should follow the YYYY-MM-DD or RFC 3339 date-time format.");
        }
        {
            let json = r#" { "beforeFinishedAt": "2021" } "#;
            let err = serde_json::from_str::<TaskDeletionOrCancelationQueryRaw>(json)
                .unwrap()
                .validate()
                .unwrap_err();
            snapshot!(format!("{err}"), @"Task `beforeFinishedAt` `2021` is invalid. It should follow the YYYY-MM-DD or RFC 3339 date-time format.");
        }
        {
            let json = r#" { "afterEnqueuedAt": "2021-12" } "#;
            let err = serde_json::from_str::<TaskDeletionOrCancelationQueryRaw>(json)
                .unwrap()
                .validate()
                .unwrap_err();
            snapshot!(format!("{err}"), @"Task `afterEnqueuedAt` `2021-12` is invalid. It should follow the YYYY-MM-DD or RFC 3339 date-time format.");
        }

        {
            let json = r#" { "beforeEnqueuedAt": "2021-12-03T23" } "#;
            let err = serde_json::from_str::<TaskDeletionOrCancelationQueryRaw>(json)
                .unwrap()
                .validate()
                .unwrap_err();
            snapshot!(format!("{err}"), @"Task `beforeEnqueuedAt` `2021-12-03T23` is invalid. It should follow the YYYY-MM-DD or RFC 3339 date-time format.");
        }
        {
            let json = r#" { "afterStartedAt": "2021-12-03T23:45" } "#;
            let err = serde_json::from_str::<TaskDeletionOrCancelationQueryRaw>(json)
                .unwrap()
                .validate()
                .unwrap_err();
            snapshot!(format!("{err}"), @"Task `afterStartedAt` `2021-12-03T23:45` is invalid. It should follow the YYYY-MM-DD or RFC 3339 date-time format.");

            let json = r#" { "beforeStartedAt": "2021-12-03T23:45" } "#;
            let err = serde_json::from_str::<TaskDeletionOrCancelationQueryRaw>(json)
                .unwrap()
                .validate()
                .unwrap_err();
            snapshot!(format!("{err}"), @"Task `beforeStartedAt` `2021-12-03T23:45` is invalid. It should follow the YYYY-MM-DD or RFC 3339 date-time format.");
        }
    }

    #[test]
    fn deserialize_task_filter_uids() {
        {
            let json = r#" { "uids": "78,1,12,73" } "#;
            let query = serde_json::from_str::<TaskDeletionOrCancelationQueryRaw>(json)
                .unwrap()
                .validate()
                .unwrap();
            snapshot!(format!("{:?}", query.common.uids.unwrap()), @"[78, 1, 12, 73]");
        }
        {
            let json = r#" { "uids": "1" } "#;
            let query = serde_json::from_str::<TaskDeletionOrCancelationQueryRaw>(json)
                .unwrap()
                .validate()
                .unwrap();
            snapshot!(format!("{:?}", query.common.uids.unwrap()), @"[1]");
        }
        {
            let json = r#" { "uids": "78,hello,world" } "#;
            let err = serde_json::from_str::<TaskDeletionOrCancelationQueryRaw>(json)
                .unwrap()
                .validate()
                .unwrap_err();
            snapshot!(format!("{err}"), @"Task uid `hello` is invalid. It should only contain numeric characters.");
        }
        {
            let json = r#" { "uids": "cat" } "#;
            let err = serde_json::from_str::<TaskDeletionOrCancelationQueryRaw>(json)
                .unwrap()
                .validate()
                .unwrap_err();
            snapshot!(format!("{err}"), @"Task uid `cat` is invalid. It should only contain numeric characters.");
        }
    }

    #[test]
    fn deserialize_task_filter_status() {
        {
            let json = r#" { "statuses": "succeeded,failed,enqueued,processing,canceled" } "#;
            let query = serde_json::from_str::<TaskDeletionOrCancelationQueryRaw>(json)
                .unwrap()
                .validate()
                .unwrap();
            snapshot!(format!("{:?}", query.common.statuses.unwrap()), @"[Succeeded, Failed, Enqueued, Processing, Canceled]");
        }
        {
            let json = r#" { "statuses": "enqueued" } "#;
            let query = serde_json::from_str::<TaskDeletionOrCancelationQueryRaw>(json)
                .unwrap()
                .validate()
                .unwrap();
            snapshot!(format!("{:?}", query.common.statuses.unwrap()), @"[Enqueued]");
        }
        {
            let json = r#" { "statuses": "finished" } "#;
            let err = serde_json::from_str::<TaskDeletionOrCancelationQueryRaw>(json)
                .unwrap()
                .validate()
                .unwrap_err();
            snapshot!(format!("{err}"), @"Task status `finished` is invalid. Available task statuses are `enqueued`, `processing`, `succeeded`, `failed`, `canceled`.");
        }
    }
    #[test]
    fn deserialize_task_filter_types() {
        {
            let json = r#" { "types": "documentAdditionOrUpdate,documentDeletion,settingsUpdate,indexCreation,indexDeletion,indexUpdate,indexSwap,taskCancelation,taskDeletion,dumpCreation,snapshotCreation" }"#;
            let query = serde_json::from_str::<TaskDeletionOrCancelationQueryRaw>(json)
                .unwrap()
                .validate()
                .unwrap();
            snapshot!(format!("{:?}", query.common.types.unwrap()), @"[DocumentAdditionOrUpdate, DocumentDeletion, SettingsUpdate, IndexCreation, IndexDeletion, IndexUpdate, IndexSwap, TaskCancelation, TaskDeletion, DumpCreation, SnapshotCreation]");
        }
        {
            let json = r#" { "types": "settingsUpdate" } "#;
            let query = serde_json::from_str::<TaskDeletionOrCancelationQueryRaw>(json)
                .unwrap()
                .validate()
                .unwrap();
            snapshot!(format!("{:?}", query.common.types.unwrap()), @"[SettingsUpdate]");
        }
        {
            let json = r#" { "types": "createIndex" } "#;
            let err = serde_json::from_str::<TaskDeletionOrCancelationQueryRaw>(json)
                .unwrap()
                .validate()
                .unwrap_err();
            snapshot!(format!("{err}"), @"Task type `createIndex` is invalid. Available task types are `documentAdditionOrUpdate`, `documentDeletion`, `settingsUpdate`, `indexCreation`, `indexDeletion`, `indexUpdate`, `indexSwap`, `taskCancelation`, `taskDeletion`, `dumpCreation`, `snapshotCreation`");
        }
    }
    #[test]
    fn deserialize_task_filter_index_uids() {
        {
            let json = r#" { "indexUids": "toto,tata-78" }"#;
            let query = serde_json::from_str::<TaskDeletionOrCancelationQueryRaw>(json)
                .unwrap()
                .validate()
                .unwrap();
            snapshot!(format!("{:?}", query.common.index_uids.unwrap()), @r###"["toto", "tata-78"]"###);
        }
        {
            let json = r#" { "indexUids": "index_a" } "#;
            let query = serde_json::from_str::<TaskDeletionOrCancelationQueryRaw>(json)
                .unwrap()
                .validate()
                .unwrap();
            snapshot!(format!("{:?}", query.common.index_uids.unwrap()), @r###"["index_a"]"###);
        }
        {
            let json = r#" { "indexUids": "1,hé" } "#;
            let err = serde_json::from_str::<TaskDeletionOrCancelationQueryRaw>(json)
                .unwrap()
                .validate()
                .unwrap_err();
            snapshot!(format!("{err}"), @"hé is not a valid index uid. Index uid can be an integer or a string containing only alphanumeric characters, hyphens (-) and underscores (_).");
        }
        {
            let json = r#" { "indexUids": "hé" } "#;
            let err = serde_json::from_str::<TaskDeletionOrCancelationQueryRaw>(json)
                .unwrap()
                .validate()
                .unwrap_err();
            snapshot!(format!("{err}"), @"hé is not a valid index uid. Index uid can be an integer or a string containing only alphanumeric characters, hyphens (-) and underscores (_).");
        }
    }

    #[test]
    fn deserialize_task_filter_general() {
        {
            let json = r#" { "from": 12, "limit": 15, "indexUids": "toto,tata-78", "statuses": "succeeded,enqueued", "afterEnqueuedAt": "2012-04-23", "uids": "1,2,3" }"#;
            let query =
                serde_json::from_str::<TasksFilterQueryRaw>(json).unwrap().validate().unwrap();
            snapshot!(format!("{:?}", query), @r###"TasksFilterQuery { limit: 15, from: Some(12), common: TaskCommonQuery { types: None, uids: Some([1, 2, 3]), canceled_by: None, statuses: Some([Succeeded, Enqueued]), index_uids: Some(["toto", "tata-78"]) }, dates: TaskDateQuery { after_enqueued_at: Some(2012-04-24 0:00:00.0 +00:00:00), before_enqueued_at: None, after_started_at: None, before_started_at: None, after_finished_at: None, before_finished_at: None } }"###);
        }
        {
            // Stars should translate to `None` in the query
            // Verify value of the default limit
            let json = r#" { "indexUids": "*", "statuses": "succeeded,*", "afterEnqueuedAt": "2012-04-23", "uids": "1,2,3" }"#;
            let query =
                serde_json::from_str::<TasksFilterQueryRaw>(json).unwrap().validate().unwrap();
            snapshot!(format!("{:?}", query), @"TasksFilterQuery { limit: 20, from: None, common: TaskCommonQuery { types: None, uids: Some([1, 2, 3]), canceled_by: None, statuses: None, index_uids: None }, dates: TaskDateQuery { after_enqueued_at: Some(2012-04-24 0:00:00.0 +00:00:00), before_enqueued_at: None, after_started_at: None, before_started_at: None, after_finished_at: None, before_finished_at: None } }");
        }
        {
            // Stars should also translate to `None` in task deletion/cancelation queries
            let json = r#" { "indexUids": "*", "statuses": "succeeded,*", "afterEnqueuedAt": "2012-04-23", "uids": "1,2,3" }"#;
            let query = serde_json::from_str::<TaskDeletionOrCancelationQueryRaw>(json)
                .unwrap()
                .validate()
                .unwrap();
            snapshot!(format!("{:?}", query), @"TaskDeletionOrCancelationQuery { common: TaskCommonQuery { types: None, uids: Some([1, 2, 3]), canceled_by: None, statuses: None, index_uids: None }, dates: TaskDateQuery { after_enqueued_at: Some(2012-04-24 0:00:00.0 +00:00:00), before_enqueued_at: None, after_started_at: None, before_started_at: None, after_finished_at: None, before_finished_at: None } }");
        }
        {
            // Stars in uids not allowed
            let json = r#" { "uids": "*" }"#;
            let err =
                serde_json::from_str::<TasksFilterQueryRaw>(json).unwrap().validate().unwrap_err();
            snapshot!(format!("{err}"), @"Task uid `*` is invalid. It should only contain numeric characters.");
        }
        {
            // From not allowed in task deletion/cancelation queries
            let json = r#" { "from": 12 }"#;
            let err = serde_json::from_str::<TaskDeletionOrCancelationQueryRaw>(json).unwrap_err();
            snapshot!(format!("{err}"), @"unknown field `from` at line 1 column 15");
        }
        {
            // Limit not allowed in task deletion/cancelation queries
            let json = r#" { "limit": 12 }"#;
            let err = serde_json::from_str::<TaskDeletionOrCancelationQueryRaw>(json).unwrap_err();
            snapshot!(format!("{err}"), @"unknown field `limit` at line 1 column 16");
        }
    }
}
