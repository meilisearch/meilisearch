use std::num::ParseIntError;
use std::str::FromStr;

use actix_web::web::Data;
use actix_web::{web, HttpRequest, HttpResponse};
use deserr::DeserializeFromValue;
use index_scheduler::{IndexScheduler, Query, TaskId};
use meilisearch_types::error::deserr_codes::*;
use meilisearch_types::error::{DeserrError, ResponseError, TakeErrorMessage};
use meilisearch_types::index_uid::IndexUid;
use meilisearch_types::settings::{Settings, Unchecked};
use meilisearch_types::star_or::StarOr;
use meilisearch_types::tasks::{
    serialize_duration, Details, IndexSwap, Kind, KindWithContent, Status, Task,
};
use serde::{Deserialize, Serialize};
use serde_cs::vec::CS;
use serde_json::json;
use time::format_description::well_known::Rfc3339;
use time::macros::format_description;
use time::{Date, Duration, OffsetDateTime, Time};
use tokio::task;

use super::{fold_star_or, SummarizedTaskView};
use crate::analytics::Analytics;
use crate::extractors::authentication::policies::*;
use crate::extractors::authentication::GuardedData;
use crate::extractors::query_parameters::QueryParameter;
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

fn parse_option_cs<T: FromStr>(
    s: Option<CS<String>>,
) -> Result<Option<Vec<T>>, TakeErrorMessage<T::Err>> {
    if let Some(s) = s {
        s.into_iter()
            .map(|s| T::from_str(&s))
            .collect::<Result<Vec<T>, T::Err>>()
            .map_err(TakeErrorMessage)
            .map(Some)
    } else {
        Ok(None)
    }
}
fn parse_option_cs_star_or<T: FromStr>(
    s: Option<CS<StarOr<String>>>,
) -> Result<Option<Vec<T>>, TakeErrorMessage<T::Err>> {
    if let Some(s) = s.and_then(fold_star_or) as Option<Vec<String>> {
        s.into_iter()
            .map(|s| T::from_str(&s))
            .collect::<Result<Vec<T>, T::Err>>()
            .map_err(TakeErrorMessage)
            .map(Some)
    } else {
        Ok(None)
    }
}
fn parse_option_str<T: FromStr>(s: Option<String>) -> Result<Option<T>, TakeErrorMessage<T::Err>> {
    if let Some(s) = s {
        T::from_str(&s).map_err(TakeErrorMessage).map(Some)
    } else {
        Ok(None)
    }
}

fn parse_str<T: FromStr>(s: String) -> Result<T, TakeErrorMessage<T::Err>> {
    T::from_str(&s).map_err(TakeErrorMessage)
}

#[derive(Debug, DeserializeFromValue)]
#[deserr(error = DeserrError, rename_all = camelCase, deny_unknown_fields)]
pub struct TasksFilterQuery {
    #[deserr(error = DeserrError<InvalidTaskLimit>, default = DEFAULT_LIMIT(), from(String) = parse_str::<u32> -> TakeErrorMessage<ParseIntError>)]
    pub limit: u32,
    #[deserr(error = DeserrError<InvalidTaskFrom>, from(Option<String>) = parse_option_str::<TaskId> -> TakeErrorMessage<ParseIntError>)]
    pub from: Option<TaskId>,

    #[deserr(error = DeserrError<InvalidTaskUids>, from(Option<CS<String>>) = parse_option_cs::<u32> -> TakeErrorMessage<ParseIntError>)]
    pub uids: Option<Vec<u32>>,
    #[deserr(error = DeserrError<InvalidTaskCanceledBy>, from(Option<CS<String>>) = parse_option_cs::<u32> -> TakeErrorMessage<ParseIntError>)]
    pub canceled_by: Option<Vec<u32>>,
    #[deserr(error = DeserrError<InvalidTaskTypes>, default = None, from(Option<CS<StarOr<String>>>) = parse_option_cs_star_or::<Kind> -> TakeErrorMessage<ResponseError>)]
    pub types: Option<Vec<Kind>>,
    #[deserr(error = DeserrError<InvalidTaskStatuses>, default = None, from(Option<CS<StarOr<String>>>) = parse_option_cs_star_or::<Status> -> TakeErrorMessage<ResponseError>)]
    pub statuses: Option<Vec<Status>>,
    #[deserr(error = DeserrError<InvalidIndexUid>, default = None, from(Option<CS<StarOr<String>>>) = parse_option_cs_star_or::<IndexUid> -> TakeErrorMessage<ResponseError>)]
    pub index_uids: Option<Vec<IndexUid>>,

    #[deserr(error = DeserrError<InvalidTaskAfterEnqueuedAt>, default = None, from(Option<String>) = deserialize_date_after -> TakeErrorMessage<InvalidTaskDateError>)]
    pub after_enqueued_at: Option<OffsetDateTime>,
    #[deserr(error = DeserrError<InvalidTaskBeforeEnqueuedAt>, default = None, from(Option<String>) = deserialize_date_before -> TakeErrorMessage<InvalidTaskDateError>)]
    pub before_enqueued_at: Option<OffsetDateTime>,
    #[deserr(error = DeserrError<InvalidTaskAfterStartedAt>, default = None, from(Option<String>) = deserialize_date_after -> TakeErrorMessage<InvalidTaskDateError>)]
    pub after_started_at: Option<OffsetDateTime>,
    #[deserr(error = DeserrError<InvalidTaskBeforeStartedAt>, default = None, from(Option<String>) = deserialize_date_before -> TakeErrorMessage<InvalidTaskDateError>)]
    pub before_started_at: Option<OffsetDateTime>,
    #[deserr(error = DeserrError<InvalidTaskAfterFinishedAt>, default = None, from(Option<String>) = deserialize_date_after -> TakeErrorMessage<InvalidTaskDateError>)]
    pub after_finished_at: Option<OffsetDateTime>,
    #[deserr(error = DeserrError<InvalidTaskBeforeFinishedAt>, default = None, from(Option<String>) = deserialize_date_before -> TakeErrorMessage<InvalidTaskDateError>)]
    pub before_finished_at: Option<OffsetDateTime>,
}

#[derive(Deserialize, Debug, DeserializeFromValue)]
#[deserr(error = DeserrError, rename_all = camelCase, deny_unknown_fields)]
pub struct TaskDeletionOrCancelationQuery {
    #[deserr(error = DeserrError<InvalidTaskUids>, from(Option<CS<String>>) = parse_option_cs::<u32> -> TakeErrorMessage<ParseIntError>)]
    pub uids: Option<Vec<u32>>,
    #[deserr(error = DeserrError<InvalidTaskCanceledBy>, from(Option<CS<String>>) = parse_option_cs::<u32> -> TakeErrorMessage<ParseIntError>)]
    pub canceled_by: Option<Vec<u32>>,
    #[deserr(error = DeserrError<InvalidTaskTypes>, default = None, from(Option<CS<StarOr<String>>>) = parse_option_cs_star_or::<Kind> -> TakeErrorMessage<ResponseError>)]
    pub types: Option<Vec<Kind>>,
    #[deserr(error = DeserrError<InvalidTaskStatuses>, default = None, from(Option<CS<StarOr<String>>>) = parse_option_cs_star_or::<Status> -> TakeErrorMessage<ResponseError>)]
    pub statuses: Option<Vec<Status>>,
    #[deserr(error = DeserrError<InvalidIndexUid>, default = None, from(Option<CS<StarOr<String>>>) = parse_option_cs_star_or::<IndexUid> -> TakeErrorMessage<ResponseError>)]
    pub index_uids: Option<Vec<IndexUid>>,

    #[deserr(error = DeserrError<InvalidTaskAfterEnqueuedAt>, default = None, from(Option<String>) = deserialize_date_after -> TakeErrorMessage<InvalidTaskDateError>)]
    pub after_enqueued_at: Option<OffsetDateTime>,
    #[deserr(error = DeserrError<InvalidTaskBeforeEnqueuedAt>, default = None, from(Option<String>) = deserialize_date_before -> TakeErrorMessage<InvalidTaskDateError>)]
    pub before_enqueued_at: Option<OffsetDateTime>,
    #[deserr(error = DeserrError<InvalidTaskAfterStartedAt>, default = None, from(Option<String>) = deserialize_date_after -> TakeErrorMessage<InvalidTaskDateError>)]
    pub after_started_at: Option<OffsetDateTime>,
    #[deserr(error = DeserrError<InvalidTaskBeforeStartedAt>, default = None, from(Option<String>) = deserialize_date_before -> TakeErrorMessage<InvalidTaskDateError>)]
    pub before_started_at: Option<OffsetDateTime>,
    #[deserr(error = DeserrError<InvalidTaskAfterFinishedAt>, default = None, from(Option<String>) = deserialize_date_after -> TakeErrorMessage<InvalidTaskDateError>)]
    pub after_finished_at: Option<OffsetDateTime>,
    #[deserr(error = DeserrError<InvalidTaskBeforeFinishedAt>, default = None, from(Option<String>) = deserialize_date_before -> TakeErrorMessage<InvalidTaskDateError>)]
    pub before_finished_at: Option<OffsetDateTime>,
}

async fn cancel_tasks(
    index_scheduler: GuardedData<ActionPolicy<{ actions::TASKS_CANCEL }>, Data<IndexScheduler>>,
    params: QueryParameter<TaskDeletionOrCancelationQuery, DeserrError>,
    req: HttpRequest,
    analytics: web::Data<dyn Analytics>,
) -> Result<HttpResponse, ResponseError> {
    let TaskDeletionOrCancelationQuery {
        types,
        uids,
        canceled_by,
        statuses,
        index_uids,
        after_enqueued_at,
        before_enqueued_at,
        after_started_at,
        before_started_at,
        after_finished_at,
        before_finished_at,
    } = params.into_inner();

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
        index_uids: index_uids.map(|xs| xs.into_iter().map(|s| s.to_string()).collect()),
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
    params: QueryParameter<TaskDeletionOrCancelationQuery, DeserrError>,
    req: HttpRequest,
    analytics: web::Data<dyn Analytics>,
) -> Result<HttpResponse, ResponseError> {
    let TaskDeletionOrCancelationQuery {
        types,
        uids,
        canceled_by,
        statuses,
        index_uids,

        after_enqueued_at,
        before_enqueued_at,
        after_started_at,
        before_started_at,
        after_finished_at,
        before_finished_at,
    } = params.into_inner();

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
        index_uids: index_uids.map(|xs| xs.into_iter().map(|s| s.to_string()).collect()),
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
    params: QueryParameter<TasksFilterQuery, DeserrError>,
    req: HttpRequest,
    analytics: web::Data<dyn Analytics>,
) -> Result<HttpResponse, ResponseError> {
    let params = params.into_inner();
    analytics.get_tasks(&params, &req);

    let TasksFilterQuery {
        types,
        uids,
        canceled_by,
        statuses,
        index_uids,
        limit,
        from,
        after_enqueued_at,
        before_enqueued_at,
        after_started_at,
        before_started_at,
        after_finished_at,
        before_finished_at,
    } = params;

    // We +1 just to know if there is more after this "page" or not.
    let limit = limit.saturating_add(1);

    let query = index_scheduler::Query {
        limit: Some(limit),
        from,
        statuses,
        types,
        index_uids: index_uids.map(|xs| xs.into_iter().map(|s| s.to_string()).collect()),
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

pub enum DeserializeDateOption {
    Before,
    After,
}

pub fn deserialize_date(
    value: &str,
    option: DeserializeDateOption,
) -> std::result::Result<OffsetDateTime, TakeErrorMessage<InvalidTaskDateError>> {
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
        Err(TakeErrorMessage(InvalidTaskDateError(value.to_owned())))
    }
}

pub fn deserialize_date_before(
    value: Option<String>,
) -> std::result::Result<Option<OffsetDateTime>, TakeErrorMessage<InvalidTaskDateError>> {
    if let Some(value) = value {
        let date = deserialize_date(&value, DeserializeDateOption::Before)?;
        Ok(Some(date))
    } else {
        Ok(None)
    }
}
pub fn deserialize_date_after(
    value: Option<String>,
) -> std::result::Result<Option<OffsetDateTime>, TakeErrorMessage<InvalidTaskDateError>> {
    if let Some(value) = value {
        let date = deserialize_date(&value, DeserializeDateOption::After)?;
        Ok(Some(date))
    } else {
        Ok(None)
    }
}

#[derive(Debug)]
pub struct InvalidTaskDateError(String);
impl std::fmt::Display for InvalidTaskDateError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "`{}` is an invalid date-time. It should follow the YYYY-MM-DD or RFC 3339 date-time format.", self.0)
    }
}
impl std::error::Error for InvalidTaskDateError {}

#[cfg(test)]
mod tests {
    use deserr::DeserializeFromValue;
    use meili_snap::snapshot;
    use meilisearch_types::error::DeserrError;

    use crate::extractors::query_parameters::QueryParameter;
    use crate::routes::tasks::{TaskDeletionOrCancelationQuery, TasksFilterQuery};

    fn deserr_query_params<T>(j: &str) -> Result<T, actix_web::Error>
    where
        T: DeserializeFromValue<DeserrError>,
    {
        QueryParameter::<T, DeserrError>::from_query(j).map(|p| p.0)
    }

    #[test]
    fn deserialize_task_filter_dates() {
        {
            let params = "afterEnqueuedAt=2021-12-03&beforeEnqueuedAt=2021-12-03&afterStartedAt=2021-12-03&beforeStartedAt=2021-12-03&afterFinishedAt=2021-12-03&beforeFinishedAt=2021-12-03";
            let query = deserr_query_params::<TaskDeletionOrCancelationQuery>(params).unwrap();

            snapshot!(format!("{:?}", query.after_enqueued_at.unwrap()), @"2021-12-04 0:00:00.0 +00:00:00");
            snapshot!(format!("{:?}", query.before_enqueued_at.unwrap()), @"2021-12-03 0:00:00.0 +00:00:00");
            snapshot!(format!("{:?}", query.after_started_at.unwrap()), @"2021-12-04 0:00:00.0 +00:00:00");
            snapshot!(format!("{:?}", query.before_started_at.unwrap()), @"2021-12-03 0:00:00.0 +00:00:00");
            snapshot!(format!("{:?}", query.after_finished_at.unwrap()), @"2021-12-04 0:00:00.0 +00:00:00");
            snapshot!(format!("{:?}", query.before_finished_at.unwrap()), @"2021-12-03 0:00:00.0 +00:00:00");
        }
        {
            let params =
                "afterEnqueuedAt=2021-12-03T23:45:23Z&beforeEnqueuedAt=2021-12-03T23:45:23Z";
            let query = deserr_query_params::<TaskDeletionOrCancelationQuery>(params).unwrap();
            snapshot!(format!("{:?}", query.after_enqueued_at.unwrap()), @"2021-12-03 23:45:23.0 +00:00:00");
            snapshot!(format!("{:?}", query.before_enqueued_at.unwrap()), @"2021-12-03 23:45:23.0 +00:00:00");
        }
        {
            let params = "afterEnqueuedAt=1997-11-12T09:55:06-06:20";
            let query = deserr_query_params::<TaskDeletionOrCancelationQuery>(params).unwrap();
            snapshot!(format!("{:?}", query.after_enqueued_at.unwrap()), @"1997-11-12 9:55:06.0 -06:20:00");
        }
        {
            let params = "afterEnqueuedAt=1997-11-12T09:55:06%2B00:00";
            let query = deserr_query_params::<TaskDeletionOrCancelationQuery>(params).unwrap();
            snapshot!(format!("{:?}", query.after_enqueued_at.unwrap()), @"1997-11-12 9:55:06.0 +00:00:00");
        }
        {
            let params = "afterEnqueuedAt=1997-11-12T09:55:06.200000300Z";
            let query = deserr_query_params::<TaskDeletionOrCancelationQuery>(params).unwrap();
            snapshot!(format!("{:?}", query.after_enqueued_at.unwrap()), @"1997-11-12 9:55:06.2000003 +00:00:00");
        }
        {
            let params = "afterFinishedAt=2021";
            let err = deserr_query_params::<TaskDeletionOrCancelationQuery>(params).unwrap_err();
            snapshot!(format!("{err}"), @"`2021` is an invalid date-time. It should follow the YYYY-MM-DD or RFC 3339 date-time format. at `.afterFinishedAt`.");
        }
        {
            let params = "beforeFinishedAt=2021";
            let err = deserr_query_params::<TaskDeletionOrCancelationQuery>(params).unwrap_err();
            snapshot!(format!("{err}"), @"`2021` is an invalid date-time. It should follow the YYYY-MM-DD or RFC 3339 date-time format. at `.beforeFinishedAt`.");
        }
        {
            let params = "afterEnqueuedAt=2021-12";
            let err = deserr_query_params::<TaskDeletionOrCancelationQuery>(params).unwrap_err();
            snapshot!(format!("{err}"), @"`2021-12` is an invalid date-time. It should follow the YYYY-MM-DD or RFC 3339 date-time format. at `.afterEnqueuedAt`.");
        }

        {
            let params = "beforeEnqueuedAt=2021-12-03T23";
            let err = deserr_query_params::<TaskDeletionOrCancelationQuery>(params).unwrap_err();
            snapshot!(format!("{err}"), @"`2021-12-03T23` is an invalid date-time. It should follow the YYYY-MM-DD or RFC 3339 date-time format. at `.beforeEnqueuedAt`.");
        }
        {
            let params = "afterStartedAt=2021-12-03T23:45";
            let err = deserr_query_params::<TaskDeletionOrCancelationQuery>(params).unwrap_err();
            snapshot!(format!("{err}"), @"`2021-12-03T23:45` is an invalid date-time. It should follow the YYYY-MM-DD or RFC 3339 date-time format. at `.afterStartedAt`.");
        }
        {
            let params = "beforeStartedAt=2021-12-03T23:45";
            let err = deserr_query_params::<TaskDeletionOrCancelationQuery>(params).unwrap_err();
            snapshot!(format!("{err}"), @"`2021-12-03T23:45` is an invalid date-time. It should follow the YYYY-MM-DD or RFC 3339 date-time format. at `.beforeStartedAt`.");
        }
    }

    #[test]
    fn deserialize_task_filter_uids() {
        {
            let params = "uids=78,1,12,73";
            let query = deserr_query_params::<TaskDeletionOrCancelationQuery>(params).unwrap();
            snapshot!(format!("{:?}", query.uids.unwrap()), @"[78, 1, 12, 73]");
        }
        {
            let params = "uids=1";
            let query = deserr_query_params::<TaskDeletionOrCancelationQuery>(params).unwrap();
            snapshot!(format!("{:?}", query.uids.unwrap()), @"[1]");
        }
        {
            let params = "uids=78,hello,world";
            let err = deserr_query_params::<TaskDeletionOrCancelationQuery>(params).unwrap_err();
            snapshot!(format!("{err}"), @"invalid digit found in string at `.uids`.");
        }
        {
            let params = "uids=cat";
            let err = deserr_query_params::<TaskDeletionOrCancelationQuery>(params).unwrap_err();
            snapshot!(format!("{err}"), @"invalid digit found in string at `.uids`.");
        }
    }

    #[test]
    fn deserialize_task_filter_status() {
        {
            let params = "statuses=succeeded,failed,enqueued,processing,canceled";
            let query = deserr_query_params::<TaskDeletionOrCancelationQuery>(params).unwrap();
            snapshot!(format!("{:?}", query.statuses.unwrap()), @"[Succeeded, Failed, Enqueued, Processing, Canceled]");
        }
        {
            let params = "statuses=enqueued";
            let query = deserr_query_params::<TaskDeletionOrCancelationQuery>(params).unwrap();
            snapshot!(format!("{:?}", query.statuses.unwrap()), @"[Enqueued]");
        }
        {
            let params = "statuses=finished";
            let err = deserr_query_params::<TaskDeletionOrCancelationQuery>(params).unwrap_err();
            snapshot!(format!("{err}"), @"`finished` is not a status. Available status are `enqueued`, `processing`, `succeeded`, `failed`, `canceled`. at `.statuses`.");
        }
    }
    #[test]
    fn deserialize_task_filter_types() {
        {
            let params = "types=documentAdditionOrUpdate,documentDeletion,settingsUpdate,indexCreation,indexDeletion,indexUpdate,indexSwap,taskCancelation,taskDeletion,dumpCreation,snapshotCreation";
            let query = deserr_query_params::<TaskDeletionOrCancelationQuery>(params).unwrap();
            snapshot!(format!("{:?}", query.types.unwrap()), @"[DocumentAdditionOrUpdate, DocumentDeletion, SettingsUpdate, IndexCreation, IndexDeletion, IndexUpdate, IndexSwap, TaskCancelation, TaskDeletion, DumpCreation, SnapshotCreation]");
        }
        {
            let params = "types=settingsUpdate";
            let query = deserr_query_params::<TaskDeletionOrCancelationQuery>(params).unwrap();
            snapshot!(format!("{:?}", query.types.unwrap()), @"[SettingsUpdate]");
        }
        {
            let params = "types=createIndex";
            let err = deserr_query_params::<TaskDeletionOrCancelationQuery>(params).unwrap_err();
            snapshot!(format!("{err}"), @"`createIndex` is not a type. Available types are `documentAdditionOrUpdate`, `documentDeletion`, `settingsUpdate`, `indexCreation`, `indexDeletion`, `indexUpdate`, `indexSwap`, `taskCancelation`, `taskDeletion`, `dumpCreation`, `snapshotCreation`. at `.types`.");
        }
    }
    #[test]
    fn deserialize_task_filter_index_uids() {
        {
            let params = "indexUids=toto,tata-78";
            let query = deserr_query_params::<TaskDeletionOrCancelationQuery>(params).unwrap();
            snapshot!(format!("{:?}", query.index_uids.unwrap()), @r###"[IndexUid("toto"), IndexUid("tata-78")]"###);
        }
        {
            let params = "indexUids=index_a";
            let query = deserr_query_params::<TaskDeletionOrCancelationQuery>(params).unwrap();
            snapshot!(format!("{:?}", query.index_uids.unwrap()), @r###"[IndexUid("index_a")]"###);
        }
        {
            let params = "indexUids=1,hé";
            let err = deserr_query_params::<TaskDeletionOrCancelationQuery>(params).unwrap_err();
            snapshot!(format!("{err}"), @"`hé` is not a valid index uid. Index uid can be an integer or a string containing only alphanumeric characters, hyphens (-) and underscores (_). at `.indexUids`.");
        }
        {
            let params = "indexUids=hé";
            let err = deserr_query_params::<TaskDeletionOrCancelationQuery>(params).unwrap_err();
            snapshot!(format!("{err}"), @"`hé` is not a valid index uid. Index uid can be an integer or a string containing only alphanumeric characters, hyphens (-) and underscores (_). at `.indexUids`.");
        }
    }

    #[test]
    fn deserialize_task_filter_general() {
        {
            let params = "from=12&limit=15&indexUids=toto,tata-78&statuses=succeeded,enqueued&afterEnqueuedAt=2012-04-23&uids=1,2,3";
            let query = deserr_query_params::<TasksFilterQuery>(params).unwrap();
            snapshot!(format!("{:?}", query), @r###"TasksFilterQuery { limit: 15, from: Some(12), uids: Some([1, 2, 3]), canceled_by: None, types: None, statuses: Some([Succeeded, Enqueued]), index_uids: Some([IndexUid("toto"), IndexUid("tata-78")]), after_enqueued_at: Some(2012-04-24 0:00:00.0 +00:00:00), before_enqueued_at: None, after_started_at: None, before_started_at: None, after_finished_at: None, before_finished_at: None }"###);
        }
        {
            // Stars should translate to `None` in the query
            // Verify value of the default limit
            let params = "indexUids=*&statuses=succeeded,*&afterEnqueuedAt=2012-04-23&uids=1,2,3";
            let query = deserr_query_params::<TasksFilterQuery>(params).unwrap();
            snapshot!(format!("{:?}", query), @"TasksFilterQuery { limit: 20, from: None, uids: Some([1, 2, 3]), canceled_by: None, types: None, statuses: None, index_uids: None, after_enqueued_at: Some(2012-04-24 0:00:00.0 +00:00:00), before_enqueued_at: None, after_started_at: None, before_started_at: None, after_finished_at: None, before_finished_at: None }");
        }
        {
            // Stars should also translate to `None` in task deletion/cancelation queries
            let params = "indexUids=*&statuses=succeeded,*&afterEnqueuedAt=2012-04-23&uids=1,2,3";
            let query = deserr_query_params::<TaskDeletionOrCancelationQuery>(params).unwrap();
            snapshot!(format!("{:?}", query), @"TaskDeletionOrCancelationQuery { uids: Some([1, 2, 3]), canceled_by: None, types: None, statuses: None, index_uids: None, after_enqueued_at: Some(2012-04-24 0:00:00.0 +00:00:00), before_enqueued_at: None, after_started_at: None, before_started_at: None, after_finished_at: None, before_finished_at: None }");
        }
        {
            // Stars in uids not allowed
            let params = "uids=*";
            let err = deserr_query_params::<TasksFilterQuery>(params).unwrap_err();
            snapshot!(format!("{err}"), @"invalid digit found in string at `.uids`.");
        }
        {
            // From not allowed in task deletion/cancelation queries
            let params = "from=12";
            let err = deserr_query_params::<TaskDeletionOrCancelationQuery>(params).unwrap_err();
            snapshot!(format!("{err}"), @"Json deserialize error: unknown field `from`, expected one of `uids`, `canceledBy`, `types`, `statuses`, `indexUids`, `afterEnqueuedAt`, `beforeEnqueuedAt`, `afterStartedAt`, `beforeStartedAt`, `afterFinishedAt`, `beforeFinishedAt` at ``.");
        }
        {
            // Limit not allowed in task deletion/cancelation queries
            let params = "limit=12";
            let err = deserr_query_params::<TaskDeletionOrCancelationQuery>(params).unwrap_err();
            snapshot!(format!("{err}"), @"Json deserialize error: unknown field `limit`, expected one of `uids`, `canceledBy`, `types`, `statuses`, `indexUids`, `afterEnqueuedAt`, `beforeEnqueuedAt`, `afterStartedAt`, `beforeStartedAt`, `afterFinishedAt`, `beforeFinishedAt` at ``.");
        }
    }
}
