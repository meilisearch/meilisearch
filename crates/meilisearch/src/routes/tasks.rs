use std::io::ErrorKind;

use actix_web::web::Data;
use actix_web::{web, HttpRequest, HttpResponse};
use deserr::actix_web::AwebQueryParameter;
use deserr::Deserr;
use index_scheduler::{IndexScheduler, Query, TaskId};
use meilisearch_types::batches::BatchId;
use meilisearch_types::deserr::query_params::Param;
use meilisearch_types::deserr::DeserrQueryParamError;
use meilisearch_types::error::deserr_codes::*;
use meilisearch_types::error::{InvalidTaskDateError, ResponseError};
use meilisearch_types::index_uid::IndexUid;
use meilisearch_types::star_or::{OptionStarOr, OptionStarOrList};
use meilisearch_types::task_view::TaskView;
use meilisearch_types::tasks::{Kind, KindWithContent, Status};
use serde::{Deserialize, Serialize};
use time::format_description::well_known::Rfc3339;
use time::macros::format_description;
use time::{Date, Duration, OffsetDateTime, Time};
use tokio::io::AsyncReadExt;
use tokio::task;
use utoipa::{IntoParams, OpenApi, ToSchema};

use super::{get_task_id, is_dry_run, SummarizedTaskView, PAGINATION_DEFAULT_LIMIT};
use crate::analytics::{Aggregate, AggregateMethod, Analytics};
use crate::extractors::authentication::policies::*;
use crate::extractors::authentication::GuardedData;
use crate::extractors::sequential_extractor::SeqHandler;
use crate::{aggregate_methods, Opt};

#[derive(OpenApi)]
#[openapi(
    paths(get_tasks, delete_tasks, cancel_tasks, get_task),
    tags((
        name = "Tasks",
        description = "The tasks route gives information about the progress of the [asynchronous operations](https://docs.meilisearch.com/learn/advanced/asynchronous_operations.html).",
        external_docs(url = "https://www.meilisearch.com/docs/reference/api/tasks"),
    )),
)]
pub struct TaskApi;

pub fn configure(cfg: &mut web::ServiceConfig) {
    cfg.service(
        web::resource("")
            .route(web::get().to(SeqHandler(get_tasks)))
            .route(web::delete().to(SeqHandler(delete_tasks))),
    )
    .service(web::resource("/cancel").route(web::post().to(SeqHandler(cancel_tasks))))
    .service(web::resource("/{task_id}").route(web::get().to(SeqHandler(get_task))))
    .service(
        web::resource("/{task_id}/documents")
            .route(web::get().to(SeqHandler(get_task_documents_file))),
    );
}

#[derive(Debug, Deserr, IntoParams)]
#[deserr(error = DeserrQueryParamError, rename_all = camelCase, deny_unknown_fields)]
#[into_params(rename_all = "camelCase", parameter_in = Query)]
pub struct TasksFilterQuery {
    /// Maximum number of results to return.
    #[deserr(default = Param(PAGINATION_DEFAULT_LIMIT as u32), error = DeserrQueryParamError<InvalidTaskLimit>)]
    #[param(required = false, value_type = u32, example = 12, default = json!(PAGINATION_DEFAULT_LIMIT))]
    pub limit: Param<u32>,
    /// Fetch the next set of results from the given uid.
    #[deserr(default, error = DeserrQueryParamError<InvalidTaskFrom>)]
    #[param(required = false, value_type = Option<u32>, example = 12421)]
    pub from: Option<Param<TaskId>>,
    /// The order you want to retrieve the objects.
    #[deserr(default, error = DeserrQueryParamError<InvalidTaskReverse>)]
    #[param(required = false, value_type = Option<bool>, example = true)]
    pub reverse: Option<Param<bool>>,

    /// Permits to filter tasks by their batch uid. By default, when the `batchUids` query parameter is not set, all task uids are returned. It's possible to specify several batch uids by separating them with the `,` character.
    #[deserr(default, error = DeserrQueryParamError<InvalidBatchUids>)]
    #[param(required = false, value_type = Option<u32>, example = 12421)]
    pub batch_uids: OptionStarOrList<BatchId>,

    /// Permits to filter tasks by their uid. By default, when the uids query parameter is not set, all task uids are returned. It's possible to specify several uids by separating them with the `,` character.
    #[deserr(default, error = DeserrQueryParamError<InvalidTaskUids>)]
    #[param(required = false, value_type = Option<Vec<u32>>, example = json!([231, 423, 598]))]
    pub uids: OptionStarOrList<u32>,
    /// Permits to filter tasks using the uid of the task that canceled them. It's possible to specify several task uids by separating them with the `,` character.
    #[deserr(default, error = DeserrQueryParamError<InvalidTaskCanceledBy>)]
    #[param(required = false, value_type = Option<Vec<u32>>, example = json!([374]))]
    pub canceled_by: OptionStarOrList<u32>,
    /// Permits to filter tasks by their related type. By default, when `types` query parameter is not set, all task types are returned. It's possible to specify several types by separating them with the `,` character.
    #[deserr(default, error = DeserrQueryParamError<InvalidTaskTypes>)]
    #[param(required = false, value_type = Option<Vec<String>>, example = json!([Kind::DocumentAdditionOrUpdate]))]
    pub types: OptionStarOrList<Kind>,
    /// Permits to filter tasks by their status. By default, when `statuses` query parameter is not set, all task statuses are returned. It's possible to specify several statuses by separating them with the `,` character.
    #[deserr(default, error = DeserrQueryParamError<InvalidTaskStatuses>)]
    #[param(required = false, value_type = Option<Vec<Status>>, example = json!([Status::Succeeded, Status::Failed, Status::Canceled, Status::Enqueued, Status::Processing]))]
    pub statuses: OptionStarOrList<Status>,
    /// Permits to filter tasks by their related index. By default, when `indexUids` query parameter is not set, the tasks of all the indexes are returned. It is possible to specify several indexes by separating them with the `,` character.
    #[deserr(default, error = DeserrQueryParamError<InvalidIndexUid>)]
    #[param(required = false, value_type = Option<Vec<String>>, example = json!(["movies", "theater"]))]
    pub index_uids: OptionStarOrList<IndexUid>,

    /// Permits to filter tasks based on their enqueuedAt time. Matches tasks enqueued after the given date. Supports RFC 3339 date format.
    #[deserr(default, error = DeserrQueryParamError<InvalidTaskAfterEnqueuedAt>, try_from(OptionStarOr<String>) = deserialize_date_after -> InvalidTaskDateError)]
    #[param(required = false, value_type = Option<String>, example = "2024-08-08T16:37:09.971Z")]
    pub after_enqueued_at: OptionStarOr<OffsetDateTime>,
    /// Permits to filter tasks based on their enqueuedAt time. Matches tasks enqueued before the given date. Supports RFC 3339 date format.
    #[deserr(default, error = DeserrQueryParamError<InvalidTaskBeforeEnqueuedAt>, try_from(OptionStarOr<String>) = deserialize_date_before -> InvalidTaskDateError)]
    #[param(required = false, value_type = Option<String>, example = "2024-08-08T16:37:09.971Z")]
    pub before_enqueued_at: OptionStarOr<OffsetDateTime>,
    /// Permits to filter tasks based on their startedAt time. Matches tasks started after the given date. Supports RFC 3339 date format.
    #[deserr(default, error = DeserrQueryParamError<InvalidTaskAfterStartedAt>, try_from(OptionStarOr<String>) = deserialize_date_after -> InvalidTaskDateError)]
    #[param(required = false, value_type = Option<String>, example = "2024-08-08T16:37:09.971Z")]
    pub after_started_at: OptionStarOr<OffsetDateTime>,
    /// Permits to filter tasks based on their startedAt time. Matches tasks started before the given date. Supports RFC 3339 date format.
    #[deserr(default, error = DeserrQueryParamError<InvalidTaskBeforeStartedAt>, try_from(OptionStarOr<String>) = deserialize_date_before -> InvalidTaskDateError)]
    #[param(required = false, value_type = Option<String>, example = "2024-08-08T16:37:09.971Z")]
    pub before_started_at: OptionStarOr<OffsetDateTime>,
    /// Permits to filter tasks based on their finishedAt time. Matches tasks finished after the given date. Supports RFC 3339 date format.
    #[deserr(default, error = DeserrQueryParamError<InvalidTaskAfterFinishedAt>, try_from(OptionStarOr<String>) = deserialize_date_after -> InvalidTaskDateError)]
    #[param(required = false, value_type = Option<String>, example = "2024-08-08T16:37:09.971Z")]
    pub after_finished_at: OptionStarOr<OffsetDateTime>,
    /// Permits to filter tasks based on their finishedAt time. Matches tasks finished before the given date. Supports RFC 3339 date format.
    #[deserr(default, error = DeserrQueryParamError<InvalidTaskBeforeFinishedAt>, try_from(OptionStarOr<String>) = deserialize_date_before -> InvalidTaskDateError)]
    #[param(required = false, value_type = Option<String>, example = "2024-08-08T16:37:09.971Z")]
    pub before_finished_at: OptionStarOr<OffsetDateTime>,
}

impl TasksFilterQuery {
    pub(crate) fn into_query(self) -> Query {
        Query {
            limit: Some(self.limit.0),
            from: self.from.as_deref().copied(),
            reverse: self.reverse.as_deref().copied(),
            batch_uids: self.batch_uids.merge_star_and_none(),
            statuses: self.statuses.merge_star_and_none(),
            types: self.types.merge_star_and_none(),
            index_uids: self.index_uids.map(|x| x.to_string()).merge_star_and_none(),
            uids: self.uids.merge_star_and_none(),
            canceled_by: self.canceled_by.merge_star_and_none(),
            before_enqueued_at: self.before_enqueued_at.merge_star_and_none(),
            after_enqueued_at: self.after_enqueued_at.merge_star_and_none(),
            before_started_at: self.before_started_at.merge_star_and_none(),
            after_started_at: self.after_started_at.merge_star_and_none(),
            before_finished_at: self.before_finished_at.merge_star_and_none(),
            after_finished_at: self.after_finished_at.merge_star_and_none(),
        }
    }
}

impl TaskDeletionOrCancelationQuery {
    pub fn is_empty(&self) -> bool {
        matches!(
            self,
            TaskDeletionOrCancelationQuery {
                uids: OptionStarOrList::None,
                batch_uids: OptionStarOrList::None,
                canceled_by: OptionStarOrList::None,
                types: OptionStarOrList::None,
                statuses: OptionStarOrList::None,
                index_uids: OptionStarOrList::None,
                after_enqueued_at: OptionStarOr::None,
                before_enqueued_at: OptionStarOr::None,
                after_started_at: OptionStarOr::None,
                before_started_at: OptionStarOr::None,
                after_finished_at: OptionStarOr::None,
                before_finished_at: OptionStarOr::None
            }
        )
    }
}

#[derive(Debug, Deserr, IntoParams)]
#[deserr(error = DeserrQueryParamError, rename_all = camelCase, deny_unknown_fields)]
#[into_params(rename_all = "camelCase", parameter_in = Query)]
pub struct TaskDeletionOrCancelationQuery {
    /// Permits to filter tasks by their uid. By default, when the `uids` query parameter is not set, all task uids are returned. It's possible to specify several uids by separating them with the `,` character.
    #[deserr(default, error = DeserrQueryParamError<InvalidTaskUids>)]
    #[param(required = false, value_type = Option<Vec<u32>>, example = json!([231, 423, 598]))]
    pub uids: OptionStarOrList<u32>,
    /// Lets you filter tasks by their `batchUid`.
    #[deserr(default, error = DeserrQueryParamError<InvalidBatchUids>)]
    #[param(required = false, value_type = Option<Vec<u32>>, example = json!([231, 423, 598]))]
    pub batch_uids: OptionStarOrList<BatchId>,
    /// Permits to filter tasks using the uid of the task that canceled them. It's possible to specify several task uids by separating them with the `,` character.
    #[deserr(default, error = DeserrQueryParamError<InvalidTaskCanceledBy>)]
    #[param(required = false, value_type = Option<Vec<u32>>, example = json!([374]))]
    pub canceled_by: OptionStarOrList<u32>,
    /// Permits to filter tasks by their related type. By default, when `types` query parameter is not set, all task types are returned. It's possible to specify several types by separating them with the `,` character.
    #[deserr(default, error = DeserrQueryParamError<InvalidTaskTypes>)]
    #[param(required = false, value_type = Option<Vec<Kind>>, example = json!([Kind::DocumentDeletion]))]
    pub types: OptionStarOrList<Kind>,
    /// Permits to filter tasks by their status. By default, when `statuses` query parameter is not set, all task statuses are returned. It's possible to specify several statuses by separating them with the `,` character.
    #[deserr(default, error = DeserrQueryParamError<InvalidTaskStatuses>)]
    #[param(required = false, value_type = Option<Vec<Status>>, example = json!([Status::Succeeded, Status::Failed, Status::Canceled]))]
    pub statuses: OptionStarOrList<Status>,
    /// Permits to filter tasks by their related index. By default, when `indexUids` query parameter is not set, the tasks of all the indexes are returned. It is possible to specify several indexes by separating them with the `,` character.
    #[deserr(default, error = DeserrQueryParamError<InvalidIndexUid>)]
    #[param(required = false, value_type = Option<Vec<String>>, example = json!(["movies", "theater"]))]
    pub index_uids: OptionStarOrList<IndexUid>,

    /// Permits to filter tasks based on their enqueuedAt time. Matches tasks enqueued after the given date. Supports RFC 3339 date format.
    #[deserr(default, error = DeserrQueryParamError<InvalidTaskAfterEnqueuedAt>, try_from(OptionStarOr<String>) = deserialize_date_after -> InvalidTaskDateError)]
    #[param(required = false, value_type = Option<String>, example = "2024-08-08T16:37:09.971Z")]
    pub after_enqueued_at: OptionStarOr<OffsetDateTime>,
    /// Permits to filter tasks based on their enqueuedAt time. Matches tasks enqueued before the given date. Supports RFC 3339 date format.
    #[deserr(default, error = DeserrQueryParamError<InvalidTaskBeforeEnqueuedAt>, try_from(OptionStarOr<String>) = deserialize_date_before -> InvalidTaskDateError)]
    #[param(required = false, value_type = Option<String>, example = "2024-08-08T16:37:09.971Z")]
    pub before_enqueued_at: OptionStarOr<OffsetDateTime>,
    /// Permits to filter tasks based on their startedAt time. Matches tasks started after the given date. Supports RFC 3339 date format.
    #[deserr(default, error = DeserrQueryParamError<InvalidTaskAfterStartedAt>, try_from(OptionStarOr<String>) = deserialize_date_after -> InvalidTaskDateError)]
    #[param(required = false, value_type = Option<String>, example = "2024-08-08T16:37:09.971Z")]
    pub after_started_at: OptionStarOr<OffsetDateTime>,
    /// Permits to filter tasks based on their startedAt time. Matches tasks started before the given date. Supports RFC 3339 date format.
    #[deserr(default, error = DeserrQueryParamError<InvalidTaskBeforeStartedAt>, try_from(OptionStarOr<String>) = deserialize_date_before -> InvalidTaskDateError)]
    #[param(required = false, value_type = Option<String>, example = "2024-08-08T16:37:09.971Z")]
    pub before_started_at: OptionStarOr<OffsetDateTime>,
    /// Permits to filter tasks based on their finishedAt time. Matches tasks finished after the given date. Supports RFC 3339 date format.
    #[deserr(default, error = DeserrQueryParamError<InvalidTaskAfterFinishedAt>, try_from(OptionStarOr<String>) = deserialize_date_after -> InvalidTaskDateError)]
    #[param(required = false, value_type = Option<String>, example = "2024-08-08T16:37:09.971Z")]
    pub after_finished_at: OptionStarOr<OffsetDateTime>,
    /// Permits to filter tasks based on their finishedAt time. Matches tasks finished before the given date. Supports RFC 3339 date format.
    #[deserr(default, error = DeserrQueryParamError<InvalidTaskBeforeFinishedAt>, try_from(OptionStarOr<String>) = deserialize_date_before -> InvalidTaskDateError)]
    #[param(required = false, value_type = Option<String>, example = "2024-08-08T16:37:09.971Z")]
    pub before_finished_at: OptionStarOr<OffsetDateTime>,
}

impl TaskDeletionOrCancelationQuery {
    fn into_query(self) -> Query {
        Query {
            limit: None,
            from: None,
            reverse: None,
            batch_uids: self.batch_uids.merge_star_and_none(),
            statuses: self.statuses.merge_star_and_none(),
            types: self.types.merge_star_and_none(),
            index_uids: self.index_uids.map(|x| x.to_string()).merge_star_and_none(),
            uids: self.uids.merge_star_and_none(),
            canceled_by: self.canceled_by.merge_star_and_none(),
            before_enqueued_at: self.before_enqueued_at.merge_star_and_none(),
            after_enqueued_at: self.after_enqueued_at.merge_star_and_none(),
            before_started_at: self.before_started_at.merge_star_and_none(),
            after_started_at: self.after_started_at.merge_star_and_none(),
            before_finished_at: self.before_finished_at.merge_star_and_none(),
            after_finished_at: self.after_finished_at.merge_star_and_none(),
        }
    }
}

aggregate_methods!(
    CancelTasks => "Tasks Canceled",
    DeleteTasks => "Tasks Deleted",
);

#[derive(Serialize)]
struct TaskFilterAnalytics<Method: AggregateMethod> {
    filtered_by_uid: bool,
    filtered_by_index_uid: bool,
    filtered_by_type: bool,
    filtered_by_status: bool,
    filtered_by_canceled_by: bool,
    filtered_by_before_enqueued_at: bool,
    filtered_by_after_enqueued_at: bool,
    filtered_by_before_started_at: bool,
    filtered_by_after_started_at: bool,
    filtered_by_before_finished_at: bool,
    filtered_by_after_finished_at: bool,

    #[serde(skip)]
    marker: std::marker::PhantomData<Method>,
}

impl<Method: AggregateMethod + 'static> Aggregate for TaskFilterAnalytics<Method> {
    fn event_name(&self) -> &'static str {
        Method::event_name()
    }

    fn aggregate(self: Box<Self>, new: Box<Self>) -> Box<Self> {
        Box::new(Self {
            filtered_by_uid: self.filtered_by_uid | new.filtered_by_uid,
            filtered_by_index_uid: self.filtered_by_index_uid | new.filtered_by_index_uid,
            filtered_by_type: self.filtered_by_type | new.filtered_by_type,
            filtered_by_status: self.filtered_by_status | new.filtered_by_status,
            filtered_by_canceled_by: self.filtered_by_canceled_by | new.filtered_by_canceled_by,
            filtered_by_before_enqueued_at: self.filtered_by_before_enqueued_at
                | new.filtered_by_before_enqueued_at,
            filtered_by_after_enqueued_at: self.filtered_by_after_enqueued_at
                | new.filtered_by_after_enqueued_at,
            filtered_by_before_started_at: self.filtered_by_before_started_at
                | new.filtered_by_before_started_at,
            filtered_by_after_started_at: self.filtered_by_after_started_at
                | new.filtered_by_after_started_at,
            filtered_by_before_finished_at: self.filtered_by_before_finished_at
                | new.filtered_by_before_finished_at,
            filtered_by_after_finished_at: self.filtered_by_after_finished_at
                | new.filtered_by_after_finished_at,

            marker: std::marker::PhantomData,
        })
    }

    fn into_event(self: Box<Self>) -> serde_json::Value {
        serde_json::to_value(*self).unwrap_or_default()
    }
}

/// Cancel tasks
///
/// Cancel enqueued and/or processing [tasks](https://www.meilisearch.com/docs/learn/async/asynchronous_operations)
#[utoipa::path(
    post,
    path = "/cancel",
    tag = "Tasks",
    security(("Bearer" = ["tasks.cancel", "tasks.*", "*"])),
    params(TaskDeletionOrCancelationQuery),
    responses(
        (status = 200, description = "Task successfully enqueued", body = SummarizedTaskView, content_type = "application/json", example = json!(
            {
                "taskUid": 147,
                "indexUid": null,
                "status": "enqueued",
                "type": "taskCancelation",
                "enqueuedAt": "2024-08-08T17:05:55.791772Z"
            }
        )),
        (status = 400, description = "A filter is missing", body = ResponseError, content_type = "application/json", example = json!(
            {
                "message": "Query parameters to filter the tasks to cancel are missing. Available query parameters are: `uids`, `indexUids`, `statuses`, `types`, `canceledBy`, `beforeEnqueuedAt`, `afterEnqueuedAt`, `beforeStartedAt`, `afterStartedAt`, `beforeFinishedAt`, `afterFinishedAt`.",
                "code": "missing_task_filters",
                "type": "invalid_request",
                "link": "https://docs.meilisearch.com/errors#missing_task_filters"
            }
        )),
        (status = 401, description = "The authorization header is missing", body = ResponseError, content_type = "application/json", example = json!(
            {
                "message": "The Authorization header is missing. It must use the bearer authorization method.",
                "code": "missing_authorization_header",
                "type": "auth",
                "link": "https://docs.meilisearch.com/errors#missing_authorization_header"
            }
        )),
        (status = 404, description = "The task uid does not exist", body = ResponseError, content_type = "application/json", example = json!(
            {
                "message": "Task :taskUid not found.",
                "code": "task_not_found",
                "type": "invalid_request",
                "link": "https://docs.meilisearch.com/errors/#task_not_found"
            }
        ))
    )
)]
async fn cancel_tasks(
    index_scheduler: GuardedData<ActionPolicy<{ actions::TASKS_CANCEL }>, Data<IndexScheduler>>,
    params: AwebQueryParameter<TaskDeletionOrCancelationQuery, DeserrQueryParamError>,
    req: HttpRequest,
    opt: web::Data<Opt>,
    analytics: web::Data<Analytics>,
) -> Result<HttpResponse, ResponseError> {
    let params = params.into_inner();

    if params.is_empty() {
        return Err(index_scheduler::Error::TaskCancelationWithEmptyQuery.into());
    }

    analytics.publish(
        TaskFilterAnalytics::<CancelTasks> {
            filtered_by_uid: params.uids.is_some(),
            filtered_by_index_uid: params.index_uids.is_some(),
            filtered_by_type: params.types.is_some(),
            filtered_by_status: params.statuses.is_some(),
            filtered_by_canceled_by: params.canceled_by.is_some(),
            filtered_by_before_enqueued_at: params.before_enqueued_at.is_some(),
            filtered_by_after_enqueued_at: params.after_enqueued_at.is_some(),
            filtered_by_before_started_at: params.before_started_at.is_some(),
            filtered_by_after_started_at: params.after_started_at.is_some(),
            filtered_by_before_finished_at: params.before_finished_at.is_some(),
            filtered_by_after_finished_at: params.after_finished_at.is_some(),

            marker: std::marker::PhantomData,
        },
        &req,
    );

    let query = params.into_query();

    let (tasks, _) =
        index_scheduler.get_task_ids_from_authorized_indexes(&query, index_scheduler.filters())?;
    let task_cancelation =
        KindWithContent::TaskCancelation { query: format!("?{}", req.query_string()), tasks };

    let uid = get_task_id(&req, &opt)?;
    let dry_run = is_dry_run(&req, &opt)?;
    let task =
        task::spawn_blocking(move || index_scheduler.register(task_cancelation, uid, dry_run))
            .await??;
    let task: SummarizedTaskView = task.into();

    Ok(HttpResponse::Ok().json(task))
}

/// Delete tasks
///
/// Delete [tasks](https://docs.meilisearch.com/learn/advanced/asynchronous_operations.html) on filter
#[utoipa::path(
    delete,
    path = "",
    tag = "Tasks",
    security(("Bearer" = ["tasks.delete", "tasks.*", "*"])),
    params(TaskDeletionOrCancelationQuery),
    responses(
        (status = 200, description = "Task successfully enqueued", body = SummarizedTaskView, content_type = "application/json", example = json!(
            {
                "taskUid": 147,
                "indexUid": null,
                "status": "enqueued",
                "type": "taskDeletion",
                "enqueuedAt": "2024-08-08T17:05:55.791772Z"
            }
        )),
        (status = 400, description = "A filter is missing", body = ResponseError, content_type = "application/json", example = json!(
            {
                "message": "Query parameters to filter the tasks to delete are missing. Available query parameters are: `uids`, `indexUids`, `statuses`, `types`, `canceledBy`, `beforeEnqueuedAt`, `afterEnqueuedAt`, `beforeStartedAt`, `afterStartedAt`, `beforeFinishedAt`, `afterFinishedAt`.",
                "code": "missing_task_filters",
                "type": "invalid_request",
                "link": "https://docs.meilisearch.com/errors#missing_task_filters"
            }
        )),
        (status = 401, description = "The authorization header is missing", body = ResponseError, content_type = "application/json", example = json!(
            {
                "message": "The Authorization header is missing. It must use the bearer authorization method.",
                "code": "missing_authorization_header",
                "type": "auth",
                "link": "https://docs.meilisearch.com/errors#missing_authorization_header"
            }
        )),
        (status = 404, description = "The task uid does not exist", body = ResponseError, content_type = "application/json", example = json!(
            {
                "message": "Task :taskUid not found.",
                "code": "task_not_found",
                "type": "invalid_request",
                "link": "https://docs.meilisearch.com/errors/#task_not_found"
            }
        ))
    )
)]
async fn delete_tasks(
    index_scheduler: GuardedData<ActionPolicy<{ actions::TASKS_DELETE }>, Data<IndexScheduler>>,
    params: AwebQueryParameter<TaskDeletionOrCancelationQuery, DeserrQueryParamError>,
    req: HttpRequest,
    opt: web::Data<Opt>,
    analytics: web::Data<Analytics>,
) -> Result<HttpResponse, ResponseError> {
    let params = params.into_inner();

    if params.is_empty() {
        return Err(index_scheduler::Error::TaskDeletionWithEmptyQuery.into());
    }

    analytics.publish(
        TaskFilterAnalytics::<DeleteTasks> {
            filtered_by_uid: params.uids.is_some(),
            filtered_by_index_uid: params.index_uids.is_some(),
            filtered_by_type: params.types.is_some(),
            filtered_by_status: params.statuses.is_some(),
            filtered_by_canceled_by: params.canceled_by.is_some(),
            filtered_by_before_enqueued_at: params.before_enqueued_at.is_some(),
            filtered_by_after_enqueued_at: params.after_enqueued_at.is_some(),
            filtered_by_before_started_at: params.before_started_at.is_some(),
            filtered_by_after_started_at: params.after_started_at.is_some(),
            filtered_by_before_finished_at: params.before_finished_at.is_some(),
            filtered_by_after_finished_at: params.after_finished_at.is_some(),

            marker: std::marker::PhantomData,
        },
        &req,
    );

    let query = params.into_query();

    let (tasks, _) =
        index_scheduler.get_task_ids_from_authorized_indexes(&query, index_scheduler.filters())?;
    let task_deletion =
        KindWithContent::TaskDeletion { query: format!("?{}", req.query_string()), tasks };

    let uid = get_task_id(&req, &opt)?;
    let dry_run = is_dry_run(&req, &opt)?;
    let task = task::spawn_blocking(move || index_scheduler.register(task_deletion, uid, dry_run))
        .await??;
    let task: SummarizedTaskView = task.into();

    Ok(HttpResponse::Ok().json(task))
}

#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub struct AllTasks {
    /// The list of tasks that matched the filter.
    pub results: Vec<TaskView>,
    /// Total number of browsable results using offset/limit parameters for the given resource.
    pub total: u64,
    /// Limit given for the query. If limit is not provided as a query parameter, this parameter displays the default limit value.
    pub limit: u32,
    /// The first task uid returned.
    pub from: Option<u32>,
    /// Represents the value to send in from to fetch the next slice of the results. The first item for the next slice starts at this exact number. When the returned value is null, it means that all the data have been browsed in the given order.
    pub next: Option<u32>,
}

/// Get all tasks
///
/// Get all [tasks](https://docs.meilisearch.com/learn/advanced/asynchronous_operations.html)
#[utoipa::path(
    get,
    path = "",
    tag = "Tasks",
    security(("Bearer" = ["tasks.get", "tasks.*", "*"])),
    params(TasksFilterQuery),
    responses(
        (status = 200, description = "Get all tasks", body = AllTasks, content_type = "application/json", example = json!(
            {
                "results": [
                    {
                        "uid": 144,
                        "indexUid": "mieli",
                        "status": "succeeded",
                        "type": "indexCreation",
                        "canceledBy": null,
                        "details": null,
                        "error": null,
                        "duration": "PT0.009330S",
                        "enqueuedAt": "2024-08-08T09:01:13.348471Z",
                        "startedAt": "2024-08-08T09:01:13.349442Z",
                        "finishedAt": "2024-08-08T09:01:13.358772Z"
                    }
              ],
              "total": 1,
              "limit": 1,
              "from": 144,
              "next": null
            }
        )),
        (status = 401, description = "The authorization header is missing", body = ResponseError, content_type = "application/json", example = json!(
            {
                "message": "The Authorization header is missing. It must use the bearer authorization method.",
                "code": "missing_authorization_header",
                "type": "auth",
                "link": "https://docs.meilisearch.com/errors#missing_authorization_header"
            }
        )),
    )
)]
async fn get_tasks(
    index_scheduler: GuardedData<ActionPolicy<{ actions::TASKS_GET }>, Data<IndexScheduler>>,
    params: AwebQueryParameter<TasksFilterQuery, DeserrQueryParamError>,
) -> Result<HttpResponse, ResponseError> {
    let mut params = params.into_inner();
    // We +1 just to know if there is more after this "page" or not.
    params.limit.0 = params.limit.0.saturating_add(1);
    let limit = params.limit.0;
    let query = params.into_query();

    let filters = index_scheduler.filters();
    let (tasks, total) = index_scheduler.get_tasks_from_authorized_indexes(&query, filters)?;
    let mut results: Vec<_> = tasks.iter().map(TaskView::from_task).collect();

    // If we were able to fetch the number +1 tasks we asked
    // it means that there is more to come.
    let next = if results.len() == limit as usize { results.pop().map(|t| t.uid) } else { None };

    let from = results.first().map(|t| t.uid);
    let tasks = AllTasks { results, limit: limit.saturating_sub(1), total, from, next };

    Ok(HttpResponse::Ok().json(tasks))
}

/// Get a task
///
/// Get a [task](https://www.meilisearch.com/docs/learn/async/asynchronous_operations)
#[utoipa::path(
    get,
    path = "/{taskUid}",
    tag = "Tasks",
    security(("Bearer" = ["tasks.get", "tasks.*", "*"])),
    params(("taskUid", format = UInt32, example = "0", description = "The task identifier", nullable = false)),
    responses(
        (status = 200, description = "Task successfully retrieved", body = TaskView, content_type = "application/json", example = json!(
            {
                "uid": 1,
                "indexUid": "movies",
                "status": "succeeded",
                "type": "indexCreation",
                "canceledBy": null,
                "details": null,
                "error": null,
                "duration": "PT1S",
                "enqueuedAt": "2021-01-01T09:39:00.000000Z",
                "startedAt": "2021-01-01T09:39:01.000000Z",
                "finishedAt": "2021-01-01T09:39:02.000000Z"
            }
        )),
        (status = 401, description = "The authorization header is missing", body = ResponseError, content_type = "application/json", example = json!(
            {
                "message": "The Authorization header is missing. It must use the bearer authorization method.",
                "code": "missing_authorization_header",
                "type": "auth",
                "link": "https://docs.meilisearch.com/errors#missing_authorization_header"
            }
        )),
        (status = 404, description = "The task uid does not exist", body = ResponseError, content_type = "application/json", example = json!(
            {
                "message": "Task :taskUid not found.",
                "code": "task_not_found",
                "type": "invalid_request",
                "link": "https://docs.meilisearch.com/errors/#task_not_found"
            }
        ))
    )
)]
async fn get_task(
    index_scheduler: GuardedData<ActionPolicy<{ actions::TASKS_GET }>, Data<IndexScheduler>>,
    task_uid: web::Path<String>,
) -> Result<HttpResponse, ResponseError> {
    let task_uid_string = task_uid.into_inner();

    let task_uid: TaskId = match task_uid_string.parse() {
        Ok(id) => id,
        Err(_e) => {
            return Err(index_scheduler::Error::InvalidTaskUid { task_uid: task_uid_string }.into())
        }
    };

    let query = index_scheduler::Query { uids: Some(vec![task_uid]), ..Query::default() };
    let filters = index_scheduler.filters();
    let (tasks, _) = index_scheduler.get_tasks_from_authorized_indexes(&query, filters)?;

    if let Some(task) = tasks.first() {
        let task_view = TaskView::from_task(task);
        Ok(HttpResponse::Ok().json(task_view))
    } else {
        Err(index_scheduler::Error::TaskNotFound(task_uid).into())
    }
}

/// Get a task's documents.
///
/// Get a [task's documents file](https://www.meilisearch.com/docs/learn/async/asynchronous_operations).
#[utoipa::path(
    get,
    path = "/{taskUid}/documents",
    tag = "Tasks",
    security(("Bearer" = ["tasks.get", "tasks.*", "*"])),
    params(("taskUid", format = UInt32, example = "0", description = "The task identifier", nullable = false)),
    responses(
        (status = 200, description = "The content of the task update", body = serde_json::Value, content_type = "application/x-ndjson"),
        (status = 401, description = "The authorization header is missing", body = ResponseError, content_type = "application/json", example = json!(
            {
                "message": "The Authorization header is missing. It must use the bearer authorization method.",
                "code": "missing_authorization_header",
                "type": "auth",
                "link": "https://docs.meilisearch.com/errors#missing_authorization_header"
            }
        )),
        (status = 404, description = "The task uid does not exist", body = ResponseError, content_type = "application/json", example = json!(
            {
                "message": "Task :taskUid not found.",
                "code": "task_not_found",
                "type": "invalid_request",
                "link": "https://docs.meilisearch.com/errors/#task_not_found"
            }
        ))
    )
)]
async fn get_task_documents_file(
    index_scheduler: GuardedData<ActionPolicy<{ actions::TASKS_GET }>, Data<IndexScheduler>>,
    task_uid: web::Path<String>,
) -> Result<HttpResponse, ResponseError> {
    index_scheduler.features().check_get_task_documents_route()?;
    let task_uid_string = task_uid.into_inner();

    let task_uid: TaskId = match task_uid_string.parse() {
        Ok(id) => id,
        Err(_e) => {
            return Err(index_scheduler::Error::InvalidTaskUid { task_uid: task_uid_string }.into())
        }
    };

    let query = index_scheduler::Query { uids: Some(vec![task_uid]), ..Query::default() };
    let filters = index_scheduler.filters();
    let (tasks, _) = index_scheduler.get_tasks_from_authorized_indexes(&query, filters)?;

    if let Some(task) = tasks.first() {
        match task.content_uuid() {
            Some(uuid) => {
                let mut tfile = match index_scheduler.queue.update_file(uuid) {
                    Ok(file) => tokio::fs::File::from_std(file),
                    Err(file_store::Error::IoError(e)) if e.kind() == ErrorKind::NotFound => {
                        return Err(index_scheduler::Error::TaskFileNotFound(task_uid).into())
                    }
                    Err(e) => return Err(e.into()),
                };
                // Yes, that's awful to put everything in memory when we could have streamed it from
                // disk but it's really (really) complex to do with the current state of async Rust.
                let mut content = String::new();
                tfile.read_to_string(&mut content).await?;
                Ok(HttpResponse::Ok().content_type("application/x-ndjson").body(content))
            }
            None => Err(index_scheduler::Error::TaskFileNotFound(task_uid).into()),
        }
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
) -> std::result::Result<OffsetDateTime, InvalidTaskDateError> {
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
        Err(InvalidTaskDateError(value.to_owned()))
    }
}

pub fn deserialize_date_after(
    value: OptionStarOr<String>,
) -> std::result::Result<OptionStarOr<OffsetDateTime>, InvalidTaskDateError> {
    value.try_map(|x| deserialize_date(&x, DeserializeDateOption::After))
}
pub fn deserialize_date_before(
    value: OptionStarOr<String>,
) -> std::result::Result<OptionStarOr<OffsetDateTime>, InvalidTaskDateError> {
    value.try_map(|x| deserialize_date(&x, DeserializeDateOption::Before))
}
