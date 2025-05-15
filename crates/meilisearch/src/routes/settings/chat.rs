use std::collections::BTreeMap;

use actix_web::web::{self, Data};
use actix_web::HttpResponse;
use index_scheduler::IndexScheduler;
use meilisearch_types::error::ResponseError;
use meilisearch_types::keys::actions;
use serde::{Deserialize, Serialize};

use crate::extractors::authentication::policies::ActionPolicy;
use crate::extractors::authentication::GuardedData;
use crate::extractors::sequential_extractor::SeqHandler;

pub fn configure(cfg: &mut web::ServiceConfig) {
    cfg.service(
        web::resource("")
            .route(web::get().to(get_settings))
            .route(web::patch().to(SeqHandler(patch_settings))),
    );
}

async fn get_settings(
    index_scheduler: GuardedData<
        ActionPolicy<{ actions::CHAT_SETTINGS_GET }>,
        Data<IndexScheduler>,
    >,
) -> Result<HttpResponse, ResponseError> {
    let settings = match index_scheduler.chat_settings()? {
        Some(value) => serde_json::from_value(value).unwrap(),
        None => ChatSettings::default(),
    };
    Ok(HttpResponse::Ok().json(settings))
}

async fn patch_settings(
    index_scheduler: GuardedData<
        ActionPolicy<{ actions::CHAT_SETTINGS_UPDATE }>,
        Data<IndexScheduler>,
    >,
    web::Json(chat_settings): web::Json<ChatSettings>,
) -> Result<HttpResponse, ResponseError> {
    let chat_settings = serde_json::to_value(chat_settings).unwrap();
    index_scheduler.put_chat_settings(&chat_settings)?;
    Ok(HttpResponse::Ok().finish())
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct ChatSettings {
    pub source: String,
    pub endpoint: Option<String>,
    pub api_key: Option<String>,
    pub prompts: ChatPrompts,
    pub indexes: BTreeMap<String, ChatIndexSettings>,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct ChatPrompts {
    pub system: String,
    pub search_description: String,
    pub search_q_param: String,
    pub search_index_uid_param: String,
    pub pre_query: String,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct ChatIndexSettings {
    pub description: String,
    pub document_template: String,
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

impl Default for ChatSettings {
    fn default() -> Self {
        ChatSettings {
            source: "openai".to_string(),
            endpoint: None,
            api_key: None,
            prompts: ChatPrompts {
                system: DEFAULT_SYSTEM_MESSAGE.to_string(),
                search_description: DEFAULT_SEARCH_IN_INDEX_TOOL_DESCRIPTION.to_string(),
                search_q_param: DEFAULT_SEARCH_IN_INDEX_Q_PARAMETER_TOOL_DESCRIPTION.to_string(),
                search_index_uid_param: DEFAULT_SEARCH_IN_INDEX_INDEX_PARAMETER_TOOL_DESCRIPTION
                    .to_string(),
                pre_query: "".to_string(),
            },
            indexes: BTreeMap::new(),
        }
    }
}
