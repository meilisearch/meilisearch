use actix_web::web::{self, Data};
use actix_web::HttpResponse;
use deserr::actix_web::AwebQueryParameter;
use deserr::Deserr;
use index_scheduler::IndexScheduler;
use meilisearch_types::deserr::query_params::Param;
use meilisearch_types::deserr::DeserrQueryParamError;
use meilisearch_types::error::deserr_codes::{InvalidIndexLimit, InvalidIndexOffset};
use meilisearch_types::error::{Code, ResponseError};
use meilisearch_types::index_uid::IndexUid;
use meilisearch_types::keys::actions;
use serde::{Deserialize, Serialize};
use serde_json::json;
use tracing::debug;
use utoipa::{IntoParams, ToSchema};

use super::Pagination;
use crate::extractors::authentication::policies::ActionPolicy;
use crate::extractors::authentication::GuardedData;
use crate::routes::PAGINATION_DEFAULT_LIMIT;

pub mod chat_completions;
mod config;
mod errors;
pub mod settings;
mod utils;

/// The function name to report search progress.
/// This function is used to report on what meilisearch is
/// doing which must be used on the frontend to report progress.
const MEILI_SEARCH_PROGRESS_NAME: &str = "_meiliSearchProgress";
/// The function name to append a conversation message in the user conversation.
/// This function is used to append a conversation message in the user conversation.
/// This must be used on the frontend to keep context of what happened on the
/// Meilisearch-side and keep good context for follow up questions.
const MEILI_APPEND_CONVERSATION_MESSAGE_NAME: &str = "_meiliAppendConversationMessage";
/// The function name to report sources to the frontend.
/// This function is used to report sources to the frontend.
/// The call id is associated to the one used by the search progress function.
const MEILI_SEARCH_SOURCES_NAME: &str = "_meiliSearchSources";
/// The *internal* function name to provide to the LLM to search in indexes.
/// This function must not leak to the user as the LLM will call it and the
/// main goal of Meilisearch is to provide an answer to these calls.
const MEILI_SEARCH_IN_INDEX_FUNCTION_NAME: &str = "_meiliSearchInIndex";

#[derive(Deserialize)]
pub struct ChatsParam {
    workspace_uid: String,
}

pub fn configure(cfg: &mut web::ServiceConfig) {
    cfg.service(web::resource("").route(web::get().to(list_workspaces))).service(
        web::scope("/{workspace_uid}")
            .service(
                web::resource("")
                    .route(web::get().to(get_chat))
                    .route(web::delete().to(delete_chat)),
            )
            .service(web::scope("/chat/completions").configure(chat_completions::configure))
            .service(web::scope("/settings").configure(settings::configure)),
    );
}

pub async fn get_chat(
    index_scheduler: GuardedData<ActionPolicy<{ actions::CHATS_GET }>, Data<IndexScheduler>>,
    workspace_uid: web::Path<String>,
) -> Result<HttpResponse, ResponseError> {
    index_scheduler.features().check_chat_completions("displaying a chat")?;

    let workspace_uid = IndexUid::try_from(workspace_uid.into_inner())?;
    if index_scheduler.chat_workspace_exists(&workspace_uid)? {
        Ok(HttpResponse::Ok().json(json!({ "uid": workspace_uid })))
    } else {
        Err(ResponseError::from_msg(format!("chat {workspace_uid} not found"), Code::ChatNotFound))
    }
}

pub async fn delete_chat(
    index_scheduler: GuardedData<ActionPolicy<{ actions::CHATS_DELETE }>, Data<IndexScheduler>>,
    workspace_uid: web::Path<String>,
) -> Result<HttpResponse, ResponseError> {
    index_scheduler.features().check_chat_completions("deleting a chat")?;

    let workspace_uid = workspace_uid.into_inner();
    if index_scheduler.delete_chat_settings(&workspace_uid)? {
        Ok(HttpResponse::NoContent().finish())
    } else {
        Err(ResponseError::from_msg(format!("chat {workspace_uid} not found"), Code::ChatNotFound))
    }
}

#[derive(Deserr, Debug, Clone, Copy, IntoParams)]
#[deserr(error = DeserrQueryParamError, rename_all = camelCase, deny_unknown_fields)]
#[into_params(rename_all = "camelCase", parameter_in = Query)]
pub struct ListChats {
    /// The number of chat workspaces to skip before starting to retrieve anything
    #[param(value_type = Option<usize>, default, example = 100)]
    #[deserr(default, error = DeserrQueryParamError<InvalidIndexOffset>)]
    pub offset: Param<usize>,
    /// The number of chat workspaces to retrieve
    #[param(value_type = Option<usize>, default = 20, example = 1)]
    #[deserr(default = Param(PAGINATION_DEFAULT_LIMIT), error = DeserrQueryParamError<InvalidIndexLimit>)]
    pub limit: Param<usize>,
}

impl ListChats {
    fn as_pagination(self) -> Pagination {
        Pagination { offset: self.offset.0, limit: self.limit.0 }
    }
}

#[derive(Debug, Serialize, Clone, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct ChatWorkspaceView {
    /// Unique identifier for the index
    pub uid: String,
}

pub async fn list_workspaces(
    index_scheduler: GuardedData<ActionPolicy<{ actions::CHATS_GET }>, Data<IndexScheduler>>,
    paginate: AwebQueryParameter<ListChats, DeserrQueryParamError>,
) -> Result<HttpResponse, ResponseError> {
    index_scheduler.features().check_chat_completions("listing the chats")?;

    debug!(parameters = ?paginate, "List chat workspaces");
    let (total, workspaces) =
        index_scheduler.paginated_chat_workspace_uids(*paginate.offset, *paginate.limit)?;
    let workspaces =
        workspaces.into_iter().map(|uid| ChatWorkspaceView { uid }).collect::<Vec<_>>();
    let ret = paginate.as_pagination().format_with(total, workspaces);

    debug!(returns = ?ret, "List chat workspaces");
    Ok(HttpResponse::Ok().json(ret))
}
