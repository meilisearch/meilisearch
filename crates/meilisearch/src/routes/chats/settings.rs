use actix_web::web::{self, Data};
use actix_web::HttpResponse;
use deserr::Deserr;
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
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

use super::ChatsParam;
use crate::extractors::authentication::policies::ActionPolicy;
use crate::extractors::authentication::GuardedData;
use crate::extractors::sequential_extractor::SeqHandler;

pub fn configure(cfg: &mut web::ServiceConfig) {
    cfg.service(
        web::resource("")
            .route(web::get().to(SeqHandler(get_settings)))
            .route(web::patch().to(SeqHandler(patch_settings)))
            .route(web::delete().to(SeqHandler(reset_settings))),
    );
}

async fn get_settings(
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

async fn patch_settings(
    index_scheduler: GuardedData<
        ActionPolicy<{ actions::CHATS_SETTINGS_UPDATE }>,
        Data<IndexScheduler>,
    >,
    chats_param: web::Path<ChatsParam>,
    web::Json(new): web::Json<ChatWorkspaceSettings>,
) -> Result<HttpResponse, ResponseError> {
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

async fn reset_settings(
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
#[derive(Debug, Clone, Deserialize, Deserr, ToSchema)]
#[deserr(error = DeserrJsonError, rename_all = camelCase, deny_unknown_fields)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
#[schema(rename_all = "camelCase")]
pub struct ChatWorkspaceSettings {
    /// LLM provider to use for chat completions
    #[serde(default)]
    #[deserr(default)]
    #[schema(value_type = Option<ChatCompletionSource>)]
    pub source: Setting<ChatCompletionSource>,
    /// Organization ID for the LLM provider
    #[serde(default)]
    #[deserr(default, error = DeserrJsonError<InvalidChatCompletionOrgId>)]
    #[schema(value_type = Option<String>, example = json!("dcba4321..."))]
    pub org_id: Setting<String>,
    /// Project ID for the LLM provider
    #[serde(default)]
    #[deserr(default, error = DeserrJsonError<InvalidChatCompletionProjectId>)]
    #[schema(value_type = Option<String>, example = json!("4321dcba..."))]
    pub project_id: Setting<String>,
    /// API version for the LLM provider
    #[serde(default)]
    #[deserr(default, error = DeserrJsonError<InvalidChatCompletionApiVersion>)]
    #[schema(value_type = Option<String>, example = json!("2024-02-01"))]
    pub api_version: Setting<String>,
    /// Deployment ID for Azure OpenAI
    #[serde(default)]
    #[deserr(default, error = DeserrJsonError<InvalidChatCompletionDeploymentId>)]
    #[schema(value_type = Option<String>, example = json!("1234abcd..."))]
    pub deployment_id: Setting<String>,
    /// Base URL for the LLM API
    #[serde(default)]
    #[deserr(default, error = DeserrJsonError<InvalidChatCompletionBaseApi>)]
    #[schema(value_type = Option<String>, example = json!("https://api.mistral.ai/v1"))]
    pub base_url: Setting<String>,
    /// API key for authentication with the LLM provider
    #[serde(default)]
    #[deserr(default, error = DeserrJsonError<InvalidChatCompletionApiKey>)]
    #[schema(value_type = Option<String>, example = json!("abcd1234..."))]
    pub api_key: Setting<String>,
    /// Custom prompts for chat completions
    #[serde(default)]
    #[deserr(default)]
    #[schema(inline, value_type = Option<ChatPrompts>)]
    pub prompts: Setting<ChatPrompts>,
}

/// LLM provider for chat completions
#[derive(Default, Debug, Clone, Copy, Serialize, Deserialize, Deserr, ToSchema)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
#[deserr(error = DeserrJsonError, rename_all = camelCase, deny_unknown_fields)]
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
#[derive(Debug, Clone, Deserialize, Deserr, ToSchema)]
#[deserr(error = DeserrJsonError, rename_all = camelCase, deny_unknown_fields)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
#[schema(rename_all = "camelCase")]
pub struct ChatPrompts {
    /// System prompt for the LLM
    #[serde(default)]
    #[deserr(default, error = DeserrJsonError<InvalidChatCompletionSystemPrompt>)]
    #[schema(value_type = Option<String>, example = json!("You are a helpful assistant..."))]
    pub system: Setting<String>,
    /// Description of the search function for the LLM
    #[serde(default)]
    #[deserr(default, error = DeserrJsonError<InvalidChatCompletionSearchDescriptionPrompt>)]
    #[schema(value_type = Option<String>, example = json!("This is the search function..."))]
    pub search_description: Setting<String>,
    /// Description of the query parameter for search
    #[serde(default)]
    #[deserr(default, error = DeserrJsonError<InvalidChatCompletionSearchQueryParamPrompt>)]
    #[schema(value_type = Option<String>, example = json!("This is query parameter..."))]
    pub search_q_param: Setting<String>,
    /// Description of the filter parameter for search
    #[serde(default)]
    #[deserr(default, error = DeserrJsonError<InvalidChatCompletionSearchFilterParamPrompt>)]
    #[schema(value_type = Option<String>, example = json!("This is filter parameter..."))]
    pub search_filter_param: Setting<String>,
    #[serde(default)]
    #[deserr(default, error = DeserrJsonError<InvalidChatCompletionSearchIndexUidParamPrompt>)]
    #[schema(value_type = Option<String>, example = json!("This is index you want to search in..."))]
    pub search_index_uid_param: Setting<String>,
}
