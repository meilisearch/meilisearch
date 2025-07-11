use async_openai::config::{AzureConfig, OpenAIConfig};
use meilisearch_types::features::ChatCompletionSettings as DbChatSettings;
use reqwest::header::HeaderMap;
use secrecy::SecretString;

#[derive(Debug, Clone)]
pub enum Config {
    OpenAiCompatible(OpenAIConfig),
    AzureOpenAiCompatible(AzureConfig),
}

impl Config {
    pub fn new(chat_settings: &DbChatSettings) -> Self {
        use meilisearch_types::features::ChatCompletionSource::*;
        match chat_settings.source {
            OpenAi | Mistral | VLlm => {
                let mut config = OpenAIConfig::default();
                if let Some(org_id) = chat_settings.org_id.as_ref() {
                    config = config.with_org_id(org_id);
                }
                if let Some(project_id) = chat_settings.project_id.as_ref() {
                    config = config.with_project_id(project_id);
                }
                if let Some(api_key) = chat_settings.api_key.as_ref() {
                    config = config.with_api_key(api_key);
                }
                let base_url = chat_settings.base_url.as_deref();
                if let Some(base_url) = chat_settings.source.base_url().or(base_url) {
                    config = config.with_api_base(base_url);
                }
                Self::OpenAiCompatible(config)
            }
            AzureOpenAi => {
                let mut config = AzureConfig::default();
                if let Some(version) = chat_settings.api_version.as_ref() {
                    config = config.with_api_version(version);
                }
                if let Some(deployment_id) = chat_settings.deployment_id.as_ref() {
                    config = config.with_deployment_id(deployment_id);
                }
                if let Some(api_key) = chat_settings.api_key.as_ref() {
                    config = config.with_api_key(api_key);
                }
                if let Some(base_url) = chat_settings.base_url.as_ref() {
                    config = config.with_api_base(base_url);
                }
                Self::AzureOpenAiCompatible(config)
            }
        }
    }
}

impl async_openai::config::Config for Config {
    fn headers(&self) -> HeaderMap {
        match self {
            Config::OpenAiCompatible(config) => config.headers(),
            Config::AzureOpenAiCompatible(config) => config.headers(),
        }
    }

    fn url(&self, path: &str) -> String {
        match self {
            Config::OpenAiCompatible(config) => config.url(path),
            Config::AzureOpenAiCompatible(config) => config.url(path),
        }
    }

    fn query(&self) -> Vec<(&str, &str)> {
        match self {
            Config::OpenAiCompatible(config) => config.query(),
            Config::AzureOpenAiCompatible(config) => config.query(),
        }
    }

    fn api_base(&self) -> &str {
        match self {
            Config::OpenAiCompatible(config) => config.api_base(),
            Config::AzureOpenAiCompatible(config) => config.api_base(),
        }
    }

    fn api_key(&self) -> &SecretString {
        match self {
            Config::OpenAiCompatible(config) => config.api_key(),
            Config::AzureOpenAiCompatible(config) => config.api_key(),
        }
    }
}
