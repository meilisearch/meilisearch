use actix_web::web::{self, Data};
use actix_web::HttpResponse;
use index_scheduler::IndexScheduler;
use meilisearch_types::error::ResponseError;
use meilisearch_types::keys::actions;
use meilisearch_types::milli::update::Setting;
use serde::{Deserialize, Serialize};

use crate::extractors::authentication::policies::ActionPolicy;
use crate::extractors::authentication::GuardedData;
use crate::extractors::sequential_extractor::SeqHandler;

use super::ChatsParam;

pub fn configure(cfg: &mut web::ServiceConfig) {
    cfg.service(
        web::resource("")
            .route(web::get().to(get_settings))
            .route(web::patch().to(SeqHandler(patch_settings)))
            .route(web::delete().to(SeqHandler(delete_settings))),
    );
}

async fn get_settings(
    index_scheduler: GuardedData<
        ActionPolicy<{ actions::CHATS_SETTINGS_GET }>,
        Data<IndexScheduler>,
    >,
    chats_param: web::Path<ChatsParam>,
) -> Result<HttpResponse, ResponseError> {
    let ChatsParam { workspace_uid } = chats_param.into_inner();

    // TODO do a spawn_blocking here ???
    let rtxn = index_scheduler.read_txn()?;
    let mut settings = match index_scheduler.chat_settings(&rtxn, &workspace_uid)? {
        Some(value) => serde_json::from_value(value).unwrap(),
        None => GlobalChatSettings::default(),
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
    web::Json(new): web::Json<GlobalChatSettings>,
) -> Result<HttpResponse, ResponseError> {
    let ChatsParam { workspace_uid } = chats_param.into_inner();

    // TODO do a spawn_blocking here
    let mut wtxn = index_scheduler.write_txn()?;
    let old = match index_scheduler.chat_settings(&mut wtxn, &workspace_uid)? {
        Some(value) => serde_json::from_value(value).unwrap(),
        None => GlobalChatSettings::default(),
    };

    let settings = GlobalChatSettings {
        source: new.source.or(old.source),
        base_api: new.base_api.clone().or(old.base_api),
        api_key: new.api_key.clone().or(old.api_key),
        prompts: match (new.prompts, old.prompts) {
            (Setting::NotSet, set) | (set, Setting::NotSet) => set,
            (Setting::Set(_) | Setting::Reset, Setting::Reset) => Setting::Reset,
            (Setting::Reset, Setting::Set(set)) => Setting::Set(set),
            // If both are set we must merge the prompts settings
            (Setting::Set(new), Setting::Set(old)) => Setting::Set(ChatPrompts {
                system: new.system.or(old.system),
                search_description: new.search_description.or(old.search_description),
                search_q_param: new.search_q_param.or(old.search_q_param),
                search_index_uid_param: new.search_index_uid_param.or(old.search_index_uid_param),
                pre_query: new.pre_query.or(old.pre_query),
            }),
        },
    };

    let value = serde_json::to_value(settings).unwrap();
    index_scheduler.put_chat_settings(&mut wtxn, &workspace_uid, &value)?;
    wtxn.commit()?;

    Ok(HttpResponse::Ok().finish())
}

async fn delete_settings(
    index_scheduler: GuardedData<
        ActionPolicy<{ actions::CHATS_SETTINGS_DELETE }>,
        Data<IndexScheduler>,
    >,
    chats_param: web::Path<ChatsParam>,
) -> Result<HttpResponse, ResponseError> {
    let ChatsParam { workspace_uid } = chats_param.into_inner();

    // TODO do a spawn_blocking here
    let mut wtxn = index_scheduler.write_txn()?;
    if index_scheduler.delete_chat_settings(&mut wtxn, &workspace_uid)? {
        wtxn.commit()?;
        Ok(HttpResponse::Ok().finish())
    } else {
        Ok(HttpResponse::NotFound().finish())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub enum ChatSource {
    OpenAi,
}

// TODO Implement Deserr on that.
// TODO Declare DbGlobalChatSettings (alias it).
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct GlobalChatSettings {
    #[serde(default, skip_serializing_if = "Setting::is_not_set")]
    pub source: Setting<ChatSource>,
    #[serde(default, skip_serializing_if = "Setting::is_not_set")]
    pub base_api: Setting<String>,
    #[serde(default, skip_serializing_if = "Setting::is_not_set")]
    pub api_key: Setting<String>,
    #[serde(default, skip_serializing_if = "Setting::is_not_set")]
    pub prompts: Setting<ChatPrompts>,
}

impl GlobalChatSettings {
    pub fn hide_secrets(&mut self) {
        match &mut self.api_key {
            Setting::Set(key) => Self::hide_secret(key),
            Setting::Reset => (),
            Setting::NotSet => (),
        }
    }

    fn hide_secret(secret: &mut String) {
        match secret.len() {
            x if x < 10 => {
                secret.replace_range(.., "XXX...");
            }
            x if x < 20 => {
                secret.replace_range(2.., "XXXX...");
            }
            x if x < 30 => {
                secret.replace_range(3.., "XXXXX...");
            }
            _x => {
                secret.replace_range(5.., "XXXXXX...");
            }
        }
    }
}

// TODO Implement Deserr on that.
// TODO Declare DbChatPrompts (alias it).
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct ChatPrompts {
    #[serde(default, skip_serializing_if = "Setting::is_not_set")]
    pub system: Setting<String>,
    #[serde(default, skip_serializing_if = "Setting::is_not_set")]
    pub search_description: Setting<String>,
    #[serde(default, skip_serializing_if = "Setting::is_not_set")]
    pub search_q_param: Setting<String>,
    #[serde(default, skip_serializing_if = "Setting::is_not_set")]
    pub search_index_uid_param: Setting<String>,
    #[serde(default, skip_serializing_if = "Setting::is_not_set")]
    pub pre_query: Setting<String>,
}

const DEFAULT_SYSTEM_MESSAGE: &str = "You are a highly capable research assistant with access to powerful search tools. IMPORTANT INSTRUCTIONS:\
    1. When answering questions, you MUST make multiple tool calls (at least 2-3) to gather comprehensive information.\
    2. Use different search queries for each tool call - vary keywords, rephrase questions, and explore different semantic angles to ensure broad coverage.\
    3. Always explicitly announce BEFORE making each tool call by saying: \"I'll search for [specific information] now.\"\
    4. Combine information from ALL tool calls to provide complete, nuanced answers rather than relying on a single source.\
    5. For complex topics, break down your research into multiple targeted queries rather than using a single generic search.";

/// The default description of the searchInIndex tool provided to OpenAI.
const DEFAULT_SEARCH_IN_INDEX_TOOL_DESCRIPTION: &str =
    "Search the database for relevant JSON documents using an optional query.";
/// The default description of the searchInIndex `q` parameter tool provided to OpenAI.
const DEFAULT_SEARCH_IN_INDEX_Q_PARAMETER_TOOL_DESCRIPTION: &str =
    "The search query string used to find relevant documents in the index. \
This should contain keywords or phrases that best represent what the user is looking for. \
More specific queries will yield more precise results.";
/// The default description of the searchInIndex `index` parameter tool provided to OpenAI.
const DEFAULT_SEARCH_IN_INDEX_INDEX_PARAMETER_TOOL_DESCRIPTION: &str =
"The name of the index to search within. An index is a collection of documents organized for search. \
Selecting the right index ensures the most relevant results for the user query";

impl Default for GlobalChatSettings {
    fn default() -> Self {
        GlobalChatSettings {
            source: Setting::NotSet,
            base_api: Setting::NotSet,
            api_key: Setting::NotSet,
            prompts: Setting::Set(ChatPrompts::default()),
        }
    }
}

impl Default for ChatPrompts {
    fn default() -> Self {
        ChatPrompts {
            system: Setting::Set(DEFAULT_SYSTEM_MESSAGE.to_string()),
            search_description: Setting::Set(DEFAULT_SEARCH_IN_INDEX_TOOL_DESCRIPTION.to_string()),
            search_q_param: Setting::Set(
                DEFAULT_SEARCH_IN_INDEX_Q_PARAMETER_TOOL_DESCRIPTION.to_string(),
            ),
            search_index_uid_param: Setting::Set(
                DEFAULT_SEARCH_IN_INDEX_INDEX_PARAMETER_TOOL_DESCRIPTION.to_string(),
            ),
            pre_query: Setting::Set(Default::default()),
        }
    }
}
