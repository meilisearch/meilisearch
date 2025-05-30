use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

pub const DEFAULT_CHAT_SYSTEM_PROMPT: &str = "You are a highly capable research assistant with access to powerful search tools. IMPORTANT INSTRUCTIONS:1. When answering questions, you MUST make multiple tool calls (at least 2-3) to gather comprehensive information.2. Use different search queries for each tool call - vary keywords, rephrase questions, and explore different semantic angles to ensure broad coverage.3. Always explicitly announce BEFORE making each tool call by saying: \"I'll search for [specific information] now.\"4. Combine information from ALL tool calls to provide complete, nuanced answers rather than relying on a single source.5. For complex topics, break down your research into multiple targeted queries rather than using a single generic search.";
pub const DEFAULT_CHAT_SEARCH_DESCRIPTION_PROMPT: &str =
    "Search the database for relevant JSON documents using an optional query.";
pub const DEFAULT_CHAT_SEARCH_Q_PARAM_PROMPT: &str = "The search query string used to find relevant documents in the index. This should contain keywords or phrases that best represent what the user is looking for. More specific queries will yield more precise results.";
pub const DEFAULT_CHAT_SEARCH_INDEX_UID_PARAM_PROMPT: &str = "The name of the index to search within. An index is a collection of documents organized for search. Selecting the right index ensures the most relevant results for the user query. You can access to two indexes: movies, steam. The movies index contains movies with overviews. The steam index contains steam games from the Steam platform with their prices";
pub const DEFAULT_CHAT_PRE_QUERY_PROMPT: &str = "";

#[derive(Serialize, Deserialize, Debug, Clone, Copy, Default, PartialEq, Eq)]
#[serde(rename_all = "camelCase", default)]
pub struct RuntimeTogglableFeatures {
    pub metrics: bool,
    pub logs_route: bool,
    pub edit_documents_by_function: bool,
    pub contains_filter: bool,
    pub network: bool,
    pub get_task_documents_route: bool,
    pub composite_embedders: bool,
}

#[derive(Default, Debug, Clone, Copy)]
pub struct InstanceTogglableFeatures {
    pub metrics: bool,
    pub logs_route: bool,
    pub contains_filter: bool,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct Remote {
    pub url: String,
    #[serde(default)]
    pub search_api_key: Option<String>,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, Default)]
#[serde(rename_all = "camelCase")]
pub struct Network {
    #[serde(default, rename = "self")]
    pub local: Option<String>,
    #[serde(default)]
    pub remotes: BTreeMap<String, Remote>,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, Default)]
#[serde(rename_all = "camelCase")]
pub struct ChatCompletionSettings {
    pub source: ChatCompletionSource,
    #[serde(default)]
    pub base_api: Option<String>,
    #[serde(default)]
    pub api_key: Option<String>,
    #[serde(default)]
    pub prompts: ChatCompletionPrompts,
}

impl ChatCompletionSettings {
    pub fn hide_secrets(&mut self) {
        if let Some(api_key) = &mut self.api_key {
            Self::hide_secret(api_key);
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

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, Default)]
#[serde(rename_all = "camelCase")]
pub enum ChatCompletionSource {
    #[default]
    OpenAi,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct ChatCompletionPrompts {
    pub system: String,
    pub search_description: String,
    pub search_q_param: String,
    pub search_index_uid_param: String,
    pub pre_query: String,
}

impl Default for ChatCompletionPrompts {
    fn default() -> Self {
        Self {
            system: DEFAULT_CHAT_SYSTEM_PROMPT.to_string(),
            search_description: DEFAULT_CHAT_SEARCH_DESCRIPTION_PROMPT.to_string(),
            search_q_param: DEFAULT_CHAT_SEARCH_Q_PARAM_PROMPT.to_string(),
            search_index_uid_param: DEFAULT_CHAT_SEARCH_INDEX_UID_PARAM_PROMPT.to_string(),
            pre_query: DEFAULT_CHAT_PRE_QUERY_PROMPT.to_string(),
        }
    }
}
