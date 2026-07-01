use actix_web::web::{self, Data};
use actix_web::HttpResponse;
use deserr::actix_web::AwebJson;
use index_scheduler::IndexScheduler;
use meilisearch_types::deserr::DeserrJsonError;
use meilisearch_types::error::deserr_codes::*;
use meilisearch_types::error::{Code, ResponseError};
use meilisearch_types::features::{
    ChatCompletionPrompts as DbChatCompletionPrompts, ChatCompletionSettings,
    ChatCompletionSource as DbChatCompletionSource, DEFAULT_CHAT_SEARCH_DESCRIPTION_PROMPT,
    DEFAULT_CHAT_SEARCH_FILTER_PARAM_PROMPT, DEFAULT_CHAT_SEARCH_INDEX_UID_PARAM_PROMPT,
    DEFAULT_CHAT_SEARCH_Q_PARAM_PROMPT, DEFAULT_CHAT_SYSTEM_PROMPT,
};
use meilisearch_types::keys::actions;
use meilisearch_types::milli::update::Setting;

use super::ChatsParam;
use crate::extractors::authentication::policies::ActionPolicy;
use crate::extractors::authentication::GuardedData;

/// Get settings of a chat workspace
#[routes::path(
    security(("Bearer" = ["chats.settings.get", "*"])),
    params(
        ("workspace_uid" = String, Path, example = "my-workspace", description = "The unique identifier of the chat workspace.", nullable = false),
    ),
    responses(
        (status = 404, description = "Chat not found.", body = ResponseError, content_type = "application/json", example = json!(
            {
              "message": "Chat :workspaceUid not found.",
              "code": "chat_not_found",
              "type": "invalid_request",
              "link": "https://docs.meilisearch.com/errors#chat_not_found"
            }
        )),
        (status = 401, description = "The authorization header is missing.", body = ResponseError, content_type = "application/json", example = json!(
            {
                "message": "The Authorization header is missing. It must use the bearer authorization method.",
                "code": "missing_authorization_header",
                "type": "auth",
                "link": "https://docs.meilisearch.com/errors#missing_authorization_header"
            }
        )),
        (status = 200, description = "Chat settings retrieved.", content_type = "application/json", example = json!(
            {
                "source": "openAi",
                "baseUrl": null,
                "apiKey": "$LLM_API_KEY",
                "prompts": {
                    "system": "My super system prompt",
                    "searchDescription": "My super search tool description",
                    "searchQParam": "My awesome q search parameter description",
                    "searchIndexUidParam": "My incredible index uid param description",
                    "searchFilterParam": "My filter parameter description"
                }
            }
        )),
    ),
)]
pub async fn get_settings(
    index_scheduler: GuardedData<
        ActionPolicy<{ actions::CHATS_SETTINGS_GET }>,
        Data<IndexScheduler>,
    >,
    chats_param: web::Path<ChatsParam>,
) -> Result<HttpResponse, ResponseError> {
    index_scheduler.features().check_chat_completions("using the /chats/settings route")?;

    let ChatsParam { workspace_uid } = chats_param.into_inner();

    let mut settings = match index_scheduler.chat_settings(&workspace_uid)? {
        Some(settings) => settings,
        None => {
            return Err(ResponseError::from_msg(
                format!("Chat `{workspace_uid}` not found"),
                Code::ChatNotFound,
            ))
        }
    };
    settings.hide_secrets();
    Ok(HttpResponse::Ok().json(settings))
}

/// Update settings of a chat workspace
#[routes::path(
    security(("Bearer" = ["chats.settings.update", "*"])),
    request_body = ChatWorkspaceSettings,
    params(
        ("workspace_uid" = String, Path, example = "my-workspace", description = "The unique identifier of the chat workspace.", nullable = false),
    ),
    responses(
        (status = 404, description = "Chat not found.", body = ResponseError, content_type = "application/json", example = json!(
            {
              "message": "Chat :workspaceUid not found.",
              "code": "chat_not_found",
              "type": "invalid_request",
              "link": "https://docs.meilisearch.com/errors#chat_not_found"
            }
        )),
        (status = 401, description = "The authorization header is missing.", body = ResponseError, content_type = "application/json", example = json!(
            {
                "message": "The Authorization header is missing. It must use the bearer authorization method.",
                "code": "missing_authorization_header",
                "type": "auth",
                "link": "https://docs.meilisearch.com/errors#missing_authorization_header"
            }
        )),
        (status = 200, description = "Chat settings retrieved.", content_type = "application/json", example = json!(
            {
                "source": "openAi",
                "baseUrl": null,
                "apiKey": "$LLM_API_KEY",
                "prompts": {
                    "system": "My super system prompt",
                    "searchDescription": "My super search tool description",
                    "searchQParam": "My awesome q search parameter description",
                    "searchIndexUidParam": "My incredible index uid param description"
                }
            }
        )),
    ),
)]
pub async fn patch_settings(
    index_scheduler: GuardedData<
        ActionPolicy<{ actions::CHATS_SETTINGS_UPDATE }>,
        Data<IndexScheduler>,
    >,
    chats_param: web::Path<ChatsParam>,
    new: AwebJson<ChatWorkspaceSettings, DeserrJsonError>,
) -> Result<HttpResponse, ResponseError> {
    let new = new.into_inner();

    index_scheduler.features().check_chat_completions("using the /chats/settings route")?;
    let ChatsParam { workspace_uid } = chats_param.into_inner();

    let old_settings = index_scheduler.chat_settings(&workspace_uid)?.unwrap_or_default();

    let prompts = match new.prompts {
        Setting::Set(new_prompts) => DbChatCompletionPrompts {
            system: match new_prompts.system {
                Setting::Set(new_system) => new_system,
                Setting::Reset => DEFAULT_CHAT_SYSTEM_PROMPT.to_string(),
                Setting::NotSet => old_settings.prompts.system,
            },
            search_description: match new_prompts.search_description {
                Setting::Set(new_description) => new_description,
                Setting::Reset => DEFAULT_CHAT_SEARCH_DESCRIPTION_PROMPT.to_string(),
                Setting::NotSet => old_settings.prompts.search_description,
            },
            search_q_param: match new_prompts.search_q_param {
                Setting::Set(new_description) => new_description,
                Setting::Reset => DEFAULT_CHAT_SEARCH_Q_PARAM_PROMPT.to_string(),
                Setting::NotSet => old_settings.prompts.search_q_param,
            },
            search_filter_param: match new_prompts.search_filter_param {
                Setting::Set(new_description) => new_description,
                Setting::Reset => DEFAULT_CHAT_SEARCH_FILTER_PARAM_PROMPT.to_string(),
                Setting::NotSet => old_settings.prompts.search_filter_param,
            },
            search_index_uid_param: match new_prompts.search_index_uid_param {
                Setting::Set(new_description) => new_description,
                Setting::Reset => DEFAULT_CHAT_SEARCH_INDEX_UID_PARAM_PROMPT.to_string(),
                Setting::NotSet => old_settings.prompts.search_index_uid_param,
            },
        },
        Setting::Reset => DbChatCompletionPrompts::default(),
        Setting::NotSet => old_settings.prompts,
    };

    let mut settings = ChatCompletionSettings {
        source: match new.source {
            Setting::Set(new_source) => new_source.into(),
            Setting::Reset => DbChatCompletionSource::default(),
            Setting::NotSet => old_settings.source,
        },
        org_id: match new.org_id {
            Setting::Set(new_org_id) => Some(new_org_id),
            Setting::Reset => None,
            Setting::NotSet => old_settings.org_id,
        },
        project_id: match new.project_id {
            Setting::Set(new_project_id) => Some(new_project_id),
            Setting::Reset => None,
            Setting::NotSet => old_settings.project_id,
        },
        api_version: match new.api_version {
            Setting::Set(new_api_version) => Some(new_api_version),
            Setting::Reset => None,
            Setting::NotSet => old_settings.api_version,
        },
        deployment_id: match new.deployment_id {
            Setting::Set(new_deployment_id) => Some(new_deployment_id),
            Setting::Reset => None,
            Setting::NotSet => old_settings.deployment_id,
        },
        base_url: match new.base_url {
            Setting::Set(new_base_url) => Some(new_base_url),
            Setting::Reset => None,
            Setting::NotSet => old_settings.base_url,
        },
        api_key: match new.api_key {
            Setting::Set(new_api_key) => Some(new_api_key),
            Setting::Reset => None,
            Setting::NotSet => old_settings.api_key,
        },
        prompts,
    };

    // TODO send analytics
    // analytics.publish(
    //     PatchNetworkAnalytics {
    //         network_size: merged_remotes.len(),
    //         network_has_self: merged_self.is_some(),
    //     },
    //     &req,
    // );

    settings.validate()?;
    index_scheduler.put_chat_settings(&workspace_uid, &settings)?;

    settings.hide_secrets();

    Ok(HttpResponse::Ok().json(settings))
}

/// Reset the settings of a chat workspace
#[routes::path(
    security(("Bearer" = ["chats.settings.update", "*"])),
    params(
        ("workspace_uid" = String, Path, example = "my-workspace", description = "The unique identifier of the chat workspace.", nullable = false),
    ),
    responses(
        (status = 404, description = "Chat not found.", body = ResponseError, content_type = "application/json", example = json!(
            {
              "message": "Chat :workspaceUid not found.",
              "code": "chat_not_found",
              "type": "invalid_request",
              "link": "https://docs.meilisearch.com/errors#chat_not_found"
            }
        )),
        (status = 401, description = "The authorization header is missing.", body = ResponseError, content_type = "application/json", example = json!(
            {
                "message": "The Authorization header is missing. It must use the bearer authorization method.",
                "code": "missing_authorization_header",
                "type": "auth",
                "link": "https://docs.meilisearch.com/errors#missing_authorization_header"
            }
        )),
        (status = 200, description = "Chat settings retrieved.", content_type = "application/json", example = json!(
            {
                "source": "openAi",
                "baseUrl": null,
                "apiKey": "$LLM_API_KEY",
                "prompts": {
                    "system": "default system prompt",
                    "searchDescription": "default search tool description",
                    "searchQParam": "default q search parameter description",
                    "searchIndexUidParam": "default index uid param description"
                }
            }
        )),
    ),
)]
pub async fn reset_settings(
    index_scheduler: GuardedData<
        ActionPolicy<{ actions::CHATS_SETTINGS_UPDATE }>,
        Data<IndexScheduler>,
    >,
    chats_param: web::Path<ChatsParam>,
) -> Result<HttpResponse, ResponseError> {
    index_scheduler.features().check_chat_completions("using the /chats/settings route")?;

    let ChatsParam { workspace_uid } = chats_param.into_inner();
    if index_scheduler.chat_settings(&workspace_uid)?.is_some() {
        let settings = Default::default();
        index_scheduler.put_chat_settings(&workspace_uid, &settings)?;
        Ok(HttpResponse::Ok().json(settings))
    } else {
        Err(ResponseError::from_msg(
            format!("Chat `{workspace_uid}` not found"),
            Code::ChatNotFound,
        ))
    }
}

/// Settings for a chat workspace
#[routes::request]
#[derive(Debug, Clone)]
pub struct ChatWorkspaceSettings {
    /// LLM provider to use for chat completions
    #[request(default, schema_type = Option<ChatCompletionSource>)]
    pub source: Setting<ChatCompletionSource>,
    /// Organization ID for the LLM provider
    #[request(default, error = DeserrJsonError<InvalidChatCompletionOrgId>, schema_type = Option<String>, example = json!("dcba4321..."))]
    pub org_id: Setting<String>,
    /// Project ID for the LLM provider
    #[request(default, error = DeserrJsonError<InvalidChatCompletionProjectId>, schema_type = Option<String>, example = json!("4321dcba..."))]
    pub project_id: Setting<String>,
    /// API version for the LLM provider
    #[request(default, error = DeserrJsonError<InvalidChatCompletionApiVersion>, schema_type = Option<String>, example = json!("2024-02-01"))]
    pub api_version: Setting<String>,
    /// Deployment ID for Azure OpenAI
    #[request(default, error = DeserrJsonError<InvalidChatCompletionDeploymentId>, schema_type = Option<String>, example = json!("1234abcd..."))]
    pub deployment_id: Setting<String>,
    /// Base URL for the LLM API
    #[request(default, error = DeserrJsonError<InvalidChatCompletionBaseApi>, schema_type = Option<String>, example = json!("https://api.mistral.ai/v1"))]
    pub base_url: Setting<String>,
    /// API key for authentication with the LLM provider
    #[request(default, error = DeserrJsonError<InvalidChatCompletionApiKey>, schema_type = Option<String>, example = json!("abcd1234..."))]
    pub api_key: Setting<String>,
    /// Custom prompts for chat completions
    #[request(default, inline, schema_type = Option<ChatPrompts>)]
    pub prompts: Setting<ChatPrompts>,
}

/// LLM provider for chat completions
#[routes::request]
#[derive(Default, Debug, Clone, Copy)]
pub enum ChatCompletionSource {
    /// OpenAI API
    #[default]
    OpenAi,
    /// Mistral AI API
    Mistral,
    /// Azure OpenAI Service
    AzureOpenAi,
    /// vLLM compatible API
    VLlm,
}

impl From<ChatCompletionSource> for DbChatCompletionSource {
    fn from(source: ChatCompletionSource) -> Self {
        use ChatCompletionSource::*;
        match source {
            OpenAi => DbChatCompletionSource::OpenAi,
            Mistral => DbChatCompletionSource::Mistral,
            AzureOpenAi => DbChatCompletionSource::AzureOpenAi,
            VLlm => DbChatCompletionSource::VLlm,
        }
    }
}

/// Custom prompts for chat completions
#[routes::request]
#[derive(Debug, Clone)]
pub struct ChatPrompts {
    /// System prompt for the LLM
    #[request(default, schema_type = Option<String>, example = json!("You are a helpful assistant..."), error = DeserrJsonError<InvalidChatCompletionSystemPrompt>)]
    pub system: Setting<String>,
    /// Description of the search function for the LLM
    #[request(default, schema_type = Option<String>, example = json!("This is the search function..."), error = DeserrJsonError<InvalidChatCompletionSearchDescriptionPrompt>)]
    pub search_description: Setting<String>,
    /// Description of the query parameter for search
    #[request(default, schema_type = Option<String>, example = json!("This is query parameter..."), error = DeserrJsonError<InvalidChatCompletionSearchQueryParamPrompt>)]
    pub search_q_param: Setting<String>,
    /// Description of the filter parameter for search
    #[request(default, schema_type = Option<String>, example = json!("This is filter parameter..."), error = DeserrJsonError<InvalidChatCompletionSearchFilterParamPrompt>)]
    pub search_filter_param: Setting<String>,
    #[request(default, schema_type = Option<String>, example = json!("This is index you want to search in..."), error = DeserrJsonError<InvalidChatCompletionSearchIndexUidParamPrompt>)]
    pub search_index_uid_param: Setting<String>,
}
