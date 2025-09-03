use serde::{Deserialize, Serialize};

use crate::error::{Code, ResponseError};

pub const DEFAULT_CHAT_SYSTEM_PROMPT: &str = "You are a highly capable research assistant with access to powerful search tools. IMPORTANT INSTRUCTIONS:1. When answering questions, you MUST make multiple tool calls (at least 2-3) to gather comprehensive information.2. Use different search queries for each tool call - vary keywords, rephrase questions, and explore different semantic angles to ensure broad coverage.3. Always explicitly announce BEFORE making each tool call by saying: \"I'll search for [specific information] now.\"4. Combine information from ALL tool calls to provide complete, nuanced answers rather than relying on a single source.5. For complex topics, break down your research into multiple targeted queries rather than using a single generic search. Meilisearch doesn't use the colon (:) syntax to filter but rather the equal (=) one. Separate filters from query and keep the q parameter empty if needed. Same for the filter parameter: keep it empty if need be. If you need to find documents that CONTAINS keywords simply put the keywords in the q parameter do no use a filter for this purpose. Whenever you get an error, read the error message and fix your error. ";
pub const DEFAULT_CHAT_SEARCH_DESCRIPTION_PROMPT: &str =
    "Query: 'best story about Rust before 2018' with year: 2018, 2020, 2021\nlabel: analysis, golang, javascript\ntype: story, link\nvote: 300, 298, 278\n: {\"q\": \"\", \"filter\": \"category = Rust AND type = story AND year < 2018 AND vote > 100\"}\nQuery: 'A black or green car that can go fast with red brakes' with maxspeed_kmh: 200, 150, 130\ncolor: black, grey, red, green\nbrand: Toyota, Renault, Jeep, Ferrari\n: {\"q\": \"red brakes\", \"filter\": \"maxspeed_kmh > 150 AND color IN ['black', green]\"}\nQuery: 'Superman movie released in 2018 or after' with year: 2018, 2020, 2021\ngenres: Drama, Comedy, Adventure, Fiction\n: {\"q\":\"Superman\",\"filter\":\"genres IN [Adventure, Fiction] AND year >= 2018\"}";
pub const DEFAULT_CHAT_SEARCH_Q_PARAM_PROMPT: &str = "The search query string used to find relevant documents in the index. This should contain keywords or phrases that best represent what the user is looking for. More specific queries will yield more precise results.";
pub const DEFAULT_CHAT_SEARCH_FILTER_PARAM_PROMPT: &str = "The search filter string used to find relevant documents in the index. It supports parentheses, `=`, `!=`, `>=`, `>`, `<=`, `<`, `IN`, `NOT IN`, `TO`, `EXISTS`, `NOT EXISTS`, `IS NULL`, `IS NOT NULL`, `IS EMPTY`, `IS NOT EMPTY`, `_geoRadius`, or `_geoBoundingBox`. Here is an example: \"price > 100 AND category = 'electronics'\". The following is a list of fields that can be filtered on: ";
pub const DEFAULT_CHAT_SEARCH_INDEX_UID_PARAM_PROMPT: &str = "The name of the index to search within. An index is a collection of documents organized for search. Selecting the right index ensures the most relevant results for the user query.";

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
    pub chat_completions: bool,
    pub multimodal: bool,
    pub vector_store_setting: bool,
}

#[derive(Default, Debug, Clone, Copy)]
pub struct InstanceTogglableFeatures {
    pub metrics: bool,
    pub logs_route: bool,
    pub contains_filter: bool,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, Default)]
#[serde(rename_all = "camelCase")]
pub struct ChatCompletionSettings {
    pub source: ChatCompletionSource,
    #[serde(default)]
    pub org_id: Option<String>,
    #[serde(default)]
    pub project_id: Option<String>,
    #[serde(default)]
    pub api_version: Option<String>,
    #[serde(default)]
    pub deployment_id: Option<String>,
    #[serde(default)]
    pub base_url: Option<String>,
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

    pub fn validate(&self) -> Result<(), ResponseError> {
        use ChatCompletionSource::*;
        match self {
            Self { source: AzureOpenAi, base_url, deployment_id, api_version, .. } if base_url.is_none() || deployment_id.is_none() || api_version.is_none() => Err(ResponseError::from_msg(
                "azureOpenAi requires setting a valid `baseUrl`, `deploymentId`, and `apiVersion`".to_string(),
                Code::BadRequest,
            )),
            Self { source: VLlm, base_url, .. } if base_url.is_none() => Err(ResponseError::from_msg(
                "vLlm requires setting a valid `baseUrl`".to_string(),
                Code::BadRequest,
            )),
            _otherwise => Ok(()),
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone, Copy, PartialEq, Eq, Default)]
#[serde(rename_all = "camelCase")]
pub enum ChatCompletionSource {
    #[default]
    OpenAi,
    AzureOpenAi,
    Mistral,
    VLlm,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SystemRole {
    System,
    Developer,
}

impl ChatCompletionSource {
    pub fn system_role(&self, model: &str) -> SystemRole {
        use ChatCompletionSource::*;
        use SystemRole::*;
        match self {
            OpenAi if Self::old_openai_model(model) => System,
            OpenAi => Developer,
            AzureOpenAi if Self::old_openai_model(model) => System,
            AzureOpenAi => Developer,
            Mistral => System,
            VLlm => System,
        }
    }

    /// Returns true if the model is an old OpenAI model.
    ///
    /// Old OpenAI models use the system role while new ones use the developer role.
    fn old_openai_model(model: &str) -> bool {
        ["gpt-3.5", "gpt-4", "gpt-4.1", "gpt-4.5", "gpt-4o", "chatgpt-4o"].iter().any(|old| {
            model.starts_with(old)
                && model.chars().nth(old.chars().count()).is_none_or(|last| last == '-')
        })
    }

    pub fn base_url(&self) -> Option<&'static str> {
        use ChatCompletionSource::*;
        match self {
            OpenAi => Some("https://api.openai.com/v1/"),
            Mistral => Some("https://api.mistral.ai/v1/"),
            AzureOpenAi | VLlm => None,
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct ChatCompletionPrompts {
    #[serde(default)]
    pub system: String,
    #[serde(default)]
    pub search_description: String,
    #[serde(default)]
    pub search_q_param: String,
    #[serde(default = "default_search_filter_param")]
    pub search_filter_param: String,
    #[serde(default)]
    pub search_index_uid_param: String,
}

/// This function is used for when the search_filter_param is
/// not provided and this can happen when the database is in v1.15.
fn default_search_filter_param() -> String {
    DEFAULT_CHAT_SEARCH_FILTER_PARAM_PROMPT.to_string()
}

impl Default for ChatCompletionPrompts {
    fn default() -> Self {
        Self {
            system: DEFAULT_CHAT_SYSTEM_PROMPT.to_string(),
            search_description: DEFAULT_CHAT_SEARCH_DESCRIPTION_PROMPT.to_string(),
            search_q_param: DEFAULT_CHAT_SEARCH_Q_PARAM_PROMPT.to_string(),
            search_filter_param: DEFAULT_CHAT_SEARCH_FILTER_PARAM_PROMPT.to_string(),
            search_index_uid_param: DEFAULT_CHAT_SEARCH_INDEX_UID_PARAM_PROMPT.to_string(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const ALL_OPENAI_MODELS_OLDINESS: &[(&str, bool)] = &[
        ("gpt-4-0613", true),
        ("gpt-4", true),
        ("gpt-3.5-turbo", true),
        ("gpt-4o-audio-preview-2025-06-03", true),
        ("gpt-4.1-nano", true),
        ("gpt-4o-realtime-preview-2025-06-03", true),
        ("gpt-3.5-turbo-instruct", true),
        ("gpt-3.5-turbo-instruct-0914", true),
        ("gpt-4-1106-preview", true),
        ("gpt-3.5-turbo-1106", true),
        ("gpt-4-0125-preview", true),
        ("gpt-4-turbo-preview", true),
        ("gpt-3.5-turbo-0125", true),
        ("gpt-4-turbo", true),
        ("gpt-4-turbo-2024-04-09", true),
        ("gpt-4o", true),
        ("gpt-4o-2024-05-13", true),
        ("gpt-4o-mini-2024-07-18", true),
        ("gpt-4o-mini", true),
        ("gpt-4o-2024-08-06", true),
        ("chatgpt-4o-latest", true),
        ("gpt-4o-realtime-preview-2024-10-01", true),
        ("gpt-4o-audio-preview-2024-10-01", true),
        ("gpt-4o-audio-preview", true),
        ("gpt-4o-realtime-preview", true),
        ("gpt-4o-realtime-preview-2024-12-17", true),
        ("gpt-4o-audio-preview-2024-12-17", true),
        ("gpt-4o-mini-realtime-preview-2024-12-17", true),
        ("gpt-4o-mini-audio-preview-2024-12-17", true),
        ("gpt-4o-mini-realtime-preview", true),
        ("gpt-4o-mini-audio-preview", true),
        ("gpt-4o-2024-11-20", true),
        ("gpt-4.5-preview", true),
        ("gpt-4.5-preview-2025-02-27", true),
        ("gpt-4o-search-preview-2025-03-11", true),
        ("gpt-4o-search-preview", true),
        ("gpt-4o-mini-search-preview-2025-03-11", true),
        ("gpt-4o-mini-search-preview", true),
        ("gpt-4o-transcribe", true),
        ("gpt-4o-mini-transcribe", true),
        ("gpt-4o-mini-tts", true),
        ("gpt-4.1-2025-04-14", true),
        ("gpt-4.1", true),
        ("gpt-4.1-mini-2025-04-14", true),
        ("gpt-4.1-mini", true),
        ("gpt-4.1-nano-2025-04-14", true),
        ("gpt-3.5-turbo-16k", true),
        //
        // new models
        ("o1-preview-2024-09-12", false),
        ("o1-preview", false),
        ("o1-mini-2024-09-12", false),
        ("o1-mini", false),
        ("o1-2024-12-17", false),
        ("o1", false),
        ("o3-mini", false),
        ("o3-mini-2025-01-31", false),
        ("o1-pro-2025-03-19", false),
        ("o1-pro", false),
        ("o3-2025-04-16", false),
        ("o4-mini-2025-04-16", false),
        ("o3", false),
        ("o4-mini", false),
    ];

    #[test]
    fn old_openai_models() {
        for (name, is_old) in ALL_OPENAI_MODELS_OLDINESS.iter().copied() {
            assert_eq!(
                ChatCompletionSource::old_openai_model(name),
                is_old,
                "Model {name} is not considered old"
            );
        }
    }
}
