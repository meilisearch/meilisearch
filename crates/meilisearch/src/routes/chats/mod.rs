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
use utoipa::{IntoParams, OpenApi, ToSchema};

use super::{Pagination, PaginationView};
use crate::extractors::authentication::policies::ActionPolicy;
use crate::extractors::authentication::GuardedData;
use crate::routes::PAGINATION_DEFAULT_LIMIT;

mod chat_completion_analytics;
pub mod chat_completions;
mod config;
mod errors;
pub mod settings;
mod utils;

#[derive(OpenApi)]
#[openapi(
    nest(
        (path = "/{workspace_uid}/chat/completions", api = chat_completions::ChatCompletionsApi),
        (path = "/{workspace_uid}/settings", api = settings::ChatSettingsApi),
    ),
    paths(list_workspaces, get_chat, delete_chat),
    tags((
        name = "Chats",
        description = "The `/chats` route allows you to manage chat workspaces and interact with LLM-powered chat completions that can search your Meilisearch indexes.",
    )),
)]
pub struct ChatApi;

/// The function name to report search progress.
/// This function is used to report on what meilisearch is
/// doing which must be used on the frontend to report progress.
const MEILI_SEARCH_PROGRESS_NAME: &str = "_meiliSearchProgress";
/// The function name to append a conversation message in the user
/// conversation. This function is used to append a conversation message in
/// the user conversation.
/// This must be used on the frontend to keep context of what happened on the
/// Meilisearch-side and keep good context for follow up questions.
const MEILI_APPEND_CONVERSATION_MESSAGE_NAME: &str = "_meiliAppendConversationMessage";
/// The function name to report sources to the frontend.
/// This function is used to report sources to the frontend.
/// The call id is associated to the one used by the search progress
/// function.
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

/// Get a chat workspace
///
/// Get information about a specific chat workspace by its unique identifier.
#[utoipa::path(
    get,
    path = "/{workspace_uid}",
    tag = "Chats",
    security(("Bearer" = ["chats.get", "*"])),
    params(("workspace_uid" = String, Path, description = "The unique identifier of the chat workspace")),
    responses(
        (status = 200, description = "The chat workspace", content_type = "application/json"),
        (status = 404, description = "Chat workspace not found", body = ResponseError, content_type = "application/json"),
    )
)]
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

/// Delete a chat workspace
///
/// Delete a chat workspace and all its associated settings.
#[utoipa::path(
    delete,
    path = "/{workspace_uid}",
    tag = "Chats",
    security(("Bearer" = ["chats.delete", "*"])),
    params(("workspace_uid" = String, Path, description = "The unique identifier of the chat workspace")),
    responses(
        (status = 204, description = "The chat workspace has been deleted"),
        (status = 404, description = "Chat workspace not found", body = ResponseError, content_type = "application/json"),
    )
)]
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
    /// The number of chat workspaces to skip before starting to retrieve
    /// anything
    #[param(required = false, value_type = Option<usize>, default, example = 100)]
    #[deserr(default, error = DeserrQueryParamError<InvalidIndexOffset>)]
    pub offset: Param<usize>,
    /// The number of chat workspaces to retrieve
    #[param(required = false, value_type = Option<usize>, default = 20, example = 1)]
    #[deserr(default = Param(PAGINATION_DEFAULT_LIMIT), error = DeserrQueryParamError<InvalidIndexLimit>)]
    pub limit: Param<usize>,
}

impl ListChats {
    fn as_pagination(self) -> Pagination {
        Pagination { offset: self.offset.0, limit: self.limit.0 }
    }
}

/// A chat workspace containing conversation data
#[derive(Debug, Serialize, Clone, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct ChatWorkspaceView {
    /// Unique identifier for the chat workspace
    pub uid: String,
}

/// List chat workspaces
///
/// List all chat workspaces with pagination.
#[utoipa::path(
    get,
    path = "",
    tag = "Chats",
    security(("Bearer" = ["chats.get", "*"])),
    params(ListChats),
    responses(
        (status = 200, description = "Paginated list of chat workspaces", body = PaginationView<ChatWorkspaceView>, content_type = "application/json"),
    )
)]
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
