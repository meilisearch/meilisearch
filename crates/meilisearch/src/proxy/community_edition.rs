use actix_web::HttpRequest;
use index_scheduler::IndexScheduler;
use meilisearch_types::network::{Network, Remote};
use meilisearch_types::tasks::network::{DbTaskNetwork, TaskNetwork};
use meilisearch_types::tasks::Task;

use crate::error::MeilisearchHttpError;
use crate::proxy::Body;

pub fn task_network_and_check_leader_and_version(
    _req: &HttpRequest,
    _network: &Network,
) -> Result<Option<TaskNetwork>, MeilisearchHttpError> {
    Ok(None)
}

pub async fn proxy<T, F>(
    _index_scheduler: &IndexScheduler,
    _index_uid: Option<&str>,
    _req: &HttpRequest,
    _task_network: DbTaskNetwork,
    _network: Network,
    _body: Body<T, F>,
    task: &Task,
) -> Result<Task, MeilisearchHttpError>
where
    T: serde::Serialize,
    F: FnMut(&str, &Remote, &mut T),
{
    Ok(task.clone())
}
