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

    #[serde(skip_serializing_if = "Option::is_none")]
    pub canceled_by: Option<TaskId>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub details: Option<DetailsView>,
    #[serde(skip_serializing_if = "Option::is_none")]
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
    pub indexed_documents: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub primary_key: Option<Option<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub matched_documents: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub deleted_documents: Option<Option<u64>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub matched_tasks: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub canceled_tasks: Option<Option<u64>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub deleted_tasks: Option<Option<u64>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub original_query: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub dump_uid: Option<String>,
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
                    indexed_documents,
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
                matched_documents: received_document_ids,
                deleted_documents,
            } => DetailsView {
                matched_documents: Some(received_document_ids),
                deleted_documents: Some(deleted_documents),
                ..DetailsView::default()
            },
            Details::ClearAll { deleted_documents } => {
                DetailsView { deleted_documents: Some(deleted_documents), ..DetailsView::default() }
            }
            Details::TaskCancelation { matched_tasks, canceled_tasks, original_query } => {
                DetailsView {
                    matched_tasks: Some(matched_tasks),
                    canceled_tasks: Some(canceled_tasks),
                    original_query: Some(original_query),
                    ..DetailsView::default()
                }
            }
            Details::TaskDeletion { matched_tasks, deleted_tasks, original_query } => DetailsView {
                matched_tasks: Some(matched_tasks),
                deleted_tasks: Some(deleted_tasks),
                original_query: Some(original_query),
                ..DetailsView::default()
            },
            Details::Dump { dump_uid } => {
                DetailsView { dump_uid: Some(dump_uid), ..DetailsView::default() }
            }
            Details::IndexSwap { swaps } => {
                DetailsView { swaps: Some(swaps), ..Default::default() }
            }
        }
    }
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct TaskDateQuery {
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        serialize_with = "time::serde::rfc3339::option::serialize",
        deserialize_with = "date_deserializer::after::deserialize"
    )]
    after_enqueued_at: Option<OffsetDateTime>,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        serialize_with = "time::serde::rfc3339::option::serialize",
        deserialize_with = "date_deserializer::before::deserialize"
    )]
    before_enqueued_at: Option<OffsetDateTime>,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        serialize_with = "time::serde::rfc3339::option::serialize",
        deserialize_with = "date_deserializer::after::deserialize"
    )]
    after_started_at: Option<OffsetDateTime>,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        serialize_with = "time::serde::rfc3339::option::serialize",
        deserialize_with = "date_deserializer::before::deserialize"
    )]
    before_started_at: Option<OffsetDateTime>,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        serialize_with = "time::serde::rfc3339::option::serialize",
        deserialize_with = "date_deserializer::after::deserialize"
    )]
    after_finished_at: Option<OffsetDateTime>,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        serialize_with = "time::serde::rfc3339::option::serialize",
        deserialize_with = "date_deserializer::before::deserialize"
    )]
    before_finished_at: Option<OffsetDateTime>,
}

#[derive(Deserialize, Debug)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct TasksFilterQuery {
    #[serde(rename = "type")]
    kind: Option<CS<StarOr<Kind>>>,
    uid: Option<CS<u32>>,
    status: Option<CS<StarOr<Status>>>,
    index_uid: Option<CS<StarOr<String>>>,
    #[serde(default = "DEFAULT_LIMIT")]
    limit: u32,
    from: Option<TaskId>,
    #[serde(flatten)]
    dates: TaskDateQuery,
}

#[derive(Deserialize, Debug)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct TaskDeletionQuery {
    #[serde(rename = "type")]
    kind: Option<CS<Kind>>,
    uid: Option<CS<u32>>,
    status: Option<CS<Status>>,
    index_uid: Option<CS<IndexUid>>,
    #[serde(flatten)]
    dates: TaskDateQuery,
}

#[derive(Deserialize, Debug)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct TaskCancelationQuery {
    #[serde(rename = "type")]
    type_: Option<CS<Kind>>,
    uid: Option<CS<u32>>,
    status: Option<CS<Status>>,
    index_uid: Option<CS<IndexUid>>,
    #[serde(flatten)]
    dates: TaskDateQuery,
}

async fn cancel_tasks(
    index_scheduler: GuardedData<ActionPolicy<{ actions::TASKS_CANCEL }>, Data<IndexScheduler>>,
    req: HttpRequest,
    params: web::Query<TaskCancelationQuery>,
) -> Result<HttpResponse, ResponseError> {
    let TaskCancelationQuery {
        type_,
        uid,
        status,
        index_uid,
        dates:
            TaskDateQuery {
                after_enqueued_at,
                before_enqueued_at,
                after_started_at,
                before_started_at,
                after_finished_at,
                before_finished_at,
            },
    } = params.into_inner();

    let kind: Option<Vec<_>> = type_.map(|x| x.into_iter().collect());
    let uid: Option<Vec<_>> = uid.map(|x| x.into_iter().collect());
    let status: Option<Vec<_>> = status.map(|x| x.into_iter().collect());
    let index_uid: Option<Vec<_>> =
        index_uid.map(|x| x.into_iter().map(|x| x.to_string()).collect());

    let query = Query {
        limit: None,
        from: None,
        status,
        kind,
        index_uid,
        uid,
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
        KindWithContent::TaskCancelation { query: req.query_string().to_string(), tasks };

    let task = task::spawn_blocking(move || index_scheduler.register(task_cancelation)).await??;
    let task: SummarizedTaskView = task.into();

    Ok(HttpResponse::Ok().json(task))
}

async fn delete_tasks(
    index_scheduler: GuardedData<ActionPolicy<{ actions::TASKS_DELETE }>, Data<IndexScheduler>>,
    req: HttpRequest,
    params: web::Query<TaskDeletionQuery>,
) -> Result<HttpResponse, ResponseError> {
    let TaskDeletionQuery {
        kind: type_,
        uid,
        status,
        index_uid,
        dates:
            TaskDateQuery {
                after_enqueued_at,
                before_enqueued_at,
                after_started_at,
                before_started_at,
                after_finished_at,
                before_finished_at,
            },
    } = params.into_inner();

    let kind: Option<Vec<_>> = type_.map(|x| x.into_iter().collect());
    let uid: Option<Vec<_>> = uid.map(|x| x.into_iter().collect());
    let status: Option<Vec<_>> = status.map(|x| x.into_iter().collect());
    let index_uid: Option<Vec<_>> =
        index_uid.map(|x| x.into_iter().map(|x| x.to_string()).collect());

    let query = Query {
        limit: None,
        from: None,
        status,
        kind,
        index_uid,
        uid,
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
        KindWithContent::TaskDeletion { query: req.query_string().to_string(), tasks };

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
    params: web::Query<TasksFilterQuery>,
    req: HttpRequest,
    analytics: web::Data<dyn Analytics>,
) -> Result<HttpResponse, ResponseError> {
    let TasksFilterQuery {
        kind,
        uid,
        status,
        index_uid,
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
    } = params.into_inner();

    // We first transform a potential indexUid=* into a "not specified indexUid filter"
    // for every one of the filters: type, status, and indexUid.
    let kind: Option<Vec<_>> = kind.and_then(fold_star_or);
    let uid: Option<Vec<_>> = uid.map(|x| x.into_iter().collect());
    let status: Option<Vec<_>> = status.and_then(fold_star_or);
    let index_uid: Option<Vec<_>> = index_uid.and_then(fold_star_or);

    analytics.publish(
        "Tasks Seen".to_string(),
        json!({
            "filtered_by_index_uid": index_uid.as_ref().map_or(false, |v| !v.is_empty()),
            "filtered_by_type": kind.as_ref().map_or(false, |v| !v.is_empty()),
            "filtered_by_status": status.as_ref().map_or(false, |v| !v.is_empty()),
        }),
        Some(&req),
    );

    // We +1 just to know if there is more after this "page" or not.
    let limit = limit.saturating_add(1);

    let query = index_scheduler::Query {
        limit: Some(limit),
        from,
        status,
        kind,
        index_uid,
        uid,
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
    task_id: web::Path<TaskId>,
    req: HttpRequest,
    analytics: web::Data<dyn Analytics>,
) -> Result<HttpResponse, ResponseError> {
    let task_id = task_id.into_inner();

    analytics.publish("Tasks Seen".to_string(), json!({ "per_task_uid": true }), Some(&req));

    let query = index_scheduler::Query { uid: Some(vec![task_id]), ..Query::default() };

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
        Err(index_scheduler::Error::TaskNotFound(task_id).into())
    }
}

pub(crate) mod date_deserializer {
    use time::format_description::well_known::Rfc3339;
    use time::macros::format_description;
    use time::{Date, Duration, OffsetDateTime, Time};

    enum DeserializeDateOption {
        Before,
        After,
    }

    fn deserialize_date<E: serde::de::Error>(
        value: &str,
        option: DeserializeDateOption,
    ) -> std::result::Result<OffsetDateTime, E> {
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
                    let datetime = datetime
                        .checked_add(Duration::days(1))
                        .ok_or_else(|| serde::de::Error::custom("date overflow"))?;
                    Ok(datetime)
                }
            }
        } else {
            Err(serde::de::Error::custom(
                "could not parse a date with the RFC3339 or YYYY-MM-DD format",
            ))
        }
    }

    /// Deserialize an upper bound datetime with RFC3339 or YYYY-MM-DD.
    pub(crate) mod before {
        use serde::Deserializer;
        use time::OffsetDateTime;

        use super::{deserialize_date, DeserializeDateOption};

        /// Deserialize an [`Option<OffsetDateTime>`] from its ISO 8601 representation.
        pub fn deserialize<'a, D: Deserializer<'a>>(
            deserializer: D,
        ) -> Result<Option<OffsetDateTime>, D::Error> {
            deserializer.deserialize_option(Visitor)
        }

        struct Visitor;

        #[derive(Debug)]
        struct DeserializeError;

        impl<'a> serde::de::Visitor<'a> for Visitor {
            type Value = Option<OffsetDateTime>;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str(
                    "an optional date written as a string with the RFC3339 or YYYY-MM-DD format",
                )
            }

            fn visit_str<E: serde::de::Error>(
                self,
                value: &str,
            ) -> Result<Option<OffsetDateTime>, E> {
                deserialize_date(value, DeserializeDateOption::Before).map(Some)
            }

            fn visit_some<D: Deserializer<'a>>(
                self,
                deserializer: D,
            ) -> Result<Option<OffsetDateTime>, D::Error> {
                deserializer.deserialize_str(Visitor)
            }

            fn visit_none<E: serde::de::Error>(self) -> Result<Option<OffsetDateTime>, E> {
                Ok(None)
            }

            fn visit_unit<E: serde::de::Error>(self) -> Result<Self::Value, E> {
                Ok(None)
            }
        }
    }
    /// Deserialize a lower bound datetime with RFC3339 or YYYY-MM-DD.
    ///
    /// If YYYY-MM-DD is used, the day is incremented by one.
    pub(crate) mod after {
        use serde::Deserializer;
        use time::OffsetDateTime;

        use super::{deserialize_date, DeserializeDateOption};

        /// Deserialize an [`Option<OffsetDateTime>`] from its ISO 8601 representation.
        pub fn deserialize<'a, D: Deserializer<'a>>(
            deserializer: D,
        ) -> Result<Option<OffsetDateTime>, D::Error> {
            deserializer.deserialize_option(Visitor)
        }

        struct Visitor;

        #[derive(Debug)]
        struct DeserializeError;

        impl<'a> serde::de::Visitor<'a> for Visitor {
            type Value = Option<OffsetDateTime>;
            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str(
                    "an optional date written as a string with the RFC3339 or YYYY-MM-DD format",
                )
            }

            fn visit_str<E: serde::de::Error>(
                self,
                value: &str,
            ) -> Result<Option<OffsetDateTime>, E> {
                deserialize_date(value, DeserializeDateOption::After).map(Some)
            }

            fn visit_some<D: Deserializer<'a>>(
                self,
                deserializer: D,
            ) -> Result<Option<OffsetDateTime>, D::Error> {
                deserializer.deserialize_str(Visitor)
            }

            fn visit_none<E: serde::de::Error>(self) -> Result<Option<OffsetDateTime>, E> {
                Ok(None)
            }

            fn visit_unit<E: serde::de::Error>(self) -> Result<Self::Value, E> {
                Ok(None)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use meili_snap::snapshot;

    use crate::routes::tasks::TaskDeletionQuery;

    #[test]
    fn deserialize_task_deletion_query_datetime() {
        {
            let json = r#" { 
                "afterEnqueuedAt": "2021-12-03", 
                "beforeEnqueuedAt": "2021-12-03",
                "afterStartedAt": "2021-12-03", 
                "beforeStartedAt": "2021-12-03",
                "afterFinishedAt": "2021-12-03", 
                "beforeFinishedAt": "2021-12-03"
            } "#;
            let query = serde_json::from_str::<TaskDeletionQuery>(json).unwrap();
            snapshot!(format!("{:?}", query.dates.after_enqueued_at.unwrap()), @"2021-12-04 0:00:00.0 +00:00:00");
            snapshot!(format!("{:?}", query.dates.before_enqueued_at.unwrap()), @"2021-12-03 0:00:00.0 +00:00:00");
            snapshot!(format!("{:?}", query.dates.after_started_at.unwrap()), @"2021-12-04 0:00:00.0 +00:00:00");
            snapshot!(format!("{:?}", query.dates.before_started_at.unwrap()), @"2021-12-03 0:00:00.0 +00:00:00");
            snapshot!(format!("{:?}", query.dates.after_finished_at.unwrap()), @"2021-12-04 0:00:00.0 +00:00:00");
            snapshot!(format!("{:?}", query.dates.before_finished_at.unwrap()), @"2021-12-03 0:00:00.0 +00:00:00");
        }
        {
            let json = r#" { "afterEnqueuedAt": "2021-12-03T23:45:23Z", "beforeEnqueuedAt": "2021-12-03T23:45:23Z" } "#;
            let query = serde_json::from_str::<TaskDeletionQuery>(json).unwrap();
            snapshot!(format!("{:?}", query.dates.after_enqueued_at.unwrap()), @"2021-12-03 23:45:23.0 +00:00:00");
            snapshot!(format!("{:?}", query.dates.before_enqueued_at.unwrap()), @"2021-12-03 23:45:23.0 +00:00:00");
        }
        {
            let json = r#" { "afterEnqueuedAt": "1997-11-12T09:55:06-06:20" } "#;
            let query = serde_json::from_str::<TaskDeletionQuery>(json).unwrap();
            snapshot!(format!("{:?}", query.dates.after_enqueued_at.unwrap()), @"1997-11-12 9:55:06.0 -06:20:00");
        }
        {
            let json = r#" { "afterEnqueuedAt": "1997-11-12T09:55:06+00:00" } "#;
            let query = serde_json::from_str::<TaskDeletionQuery>(json).unwrap();
            snapshot!(format!("{:?}", query.dates.after_enqueued_at.unwrap()), @"1997-11-12 9:55:06.0 +00:00:00");
        }
        {
            let json = r#" { "afterEnqueuedAt": "1997-11-12T09:55:06.200000300Z" } "#;
            let query = serde_json::from_str::<TaskDeletionQuery>(json).unwrap();
            snapshot!(format!("{:?}", query.dates.after_enqueued_at.unwrap()), @"1997-11-12 9:55:06.2000003 +00:00:00");
        }
        {
            let json = r#" { "afterEnqueuedAt": "2021" } "#;
            let err = serde_json::from_str::<TaskDeletionQuery>(json).unwrap_err();
            snapshot!(format!("{err}"), @"could not parse a date with the RFC3339 or YYYY-MM-DD format at line 1 column 30");
        }
        {
            let json = r#" { "afterEnqueuedAt": "2021-12" } "#;
            let err = serde_json::from_str::<TaskDeletionQuery>(json).unwrap_err();
            snapshot!(format!("{err}"), @"could not parse a date with the RFC3339 or YYYY-MM-DD format at line 1 column 33");
        }

        {
            let json = r#" { "afterEnqueuedAt": "2021-12-03T23" } "#;
            let err = serde_json::from_str::<TaskDeletionQuery>(json).unwrap_err();
            snapshot!(format!("{err}"), @"could not parse a date with the RFC3339 or YYYY-MM-DD format at line 1 column 39");
        }
        {
            let json = r#" { "afterEnqueuedAt": "2021-12-03T23:45" } "#;
            let err = serde_json::from_str::<TaskDeletionQuery>(json).unwrap_err();
            snapshot!(format!("{err}"), @"could not parse a date with the RFC3339 or YYYY-MM-DD format at line 1 column 42");
        }
    }
}
