use std::collections::HashMap;

use derive_builder::Builder;
use serde::{Deserialize, Serialize};

use crate::error::OpenAIError;

use super::{FunctionName, FunctionObject, ResponseFormat};

#[derive(Clone, Serialize, Debug, Deserialize, PartialEq, Default)]
pub struct AssistantToolCodeInterpreterResources {
    ///A list of [file](https://platform.openai.com/docs/api-reference/files) IDs made available to the `code_interpreter`` tool. There can be a maximum of 20 files associated with the tool.
    pub file_ids: Vec<String>, // maxItems: 20
}

#[derive(Clone, Serialize, Debug, Deserialize, PartialEq, Default)]
pub struct AssistantToolFileSearchResources {
    /// The ID of the [vector store](https://platform.openai.com/docs/api-reference/vector-stores/object) attached to this assistant. There can be a maximum of 1 vector store attached to the assistant.
    pub vector_store_ids: Vec<String>,
}

#[derive(Clone, Serialize, Debug, Deserialize, PartialEq)]
pub struct AssistantToolResources {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub code_interpreter: Option<AssistantToolCodeInterpreterResources>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub file_search: Option<AssistantToolFileSearchResources>,
}

#[derive(Clone, Serialize, Debug, Deserialize, PartialEq)]
pub struct CreateAssistantToolResources {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub code_interpreter: Option<AssistantToolCodeInterpreterResources>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub file_search: Option<CreateAssistantToolFileSearchResources>,
}

#[derive(Clone, Serialize, Debug, Deserialize, PartialEq, Default)]
pub struct CreateAssistantToolFileSearchResources {
    ///  The [vector store](https://platform.openai.com/docs/api-reference/vector-stores/object) attached to this assistant. There can be a maximum of 1 vector store attached to the assistant.
    pub vector_store_ids: Option<Vec<String>>,
    /// A helper to create a [vector store](https://platform.openai.com/docs/api-reference/vector-stores/object) with file_ids and attach it to this assistant. There can be a maximum of 1 vector store attached to the assistant.
    pub vector_stores: Option<Vec<AssistantVectorStore>>,
}

#[derive(Clone, Serialize, Debug, Deserialize, PartialEq, Default)]
pub struct AssistantVectorStore {
    /// A list of [file](https://platform.openai.com/docs/api-reference/files) IDs to add to the vector store. There can be a maximum of 10000 files in a vector store.
    pub file_ids: Vec<String>,

    /// The chunking strategy used to chunk the file(s). If not set, will use the `auto` strategy.
    pub chunking_strategy: Option<AssistantVectorStoreChunkingStrategy>,

    /// Set of 16 key-value pairs that can be attached to a vector store. This can be useful for storing additional information about the vector store in a structured format. Keys can be a maximum of 64 characters long and values can be a maxium of 512 characters long.
    pub metadata: Option<HashMap<String, String>>,
}

#[derive(Clone, Serialize, Debug, Deserialize, PartialEq, Default)]
#[serde(tag = "type")]
pub enum AssistantVectorStoreChunkingStrategy {
    /// The default strategy. This strategy currently uses a `max_chunk_size_tokens` of `800` and `chunk_overlap_tokens` of `400`.
    #[default]
    #[serde(rename = "auto")]
    Auto,
    #[serde(rename = "static")]
    Static { r#static: StaticChunkingStrategy },
}

/// Static Chunking Strategy
#[derive(Clone, Serialize, Debug, Deserialize, PartialEq, Default)]
pub struct StaticChunkingStrategy {
    /// The maximum number of tokens in each chunk. The default value is `800`. The minimum value is `100` and the maximum value is `4096`.
    pub max_chunk_size_tokens: u16,
    /// The number of tokens that overlap between chunks. The default value is `400`.
    ///
    /// Note that the overlap must not exceed half of `max_chunk_size_tokens`.
    pub chunk_overlap_tokens: u16,
}

/// Represents an `assistant` that can call the model and use tools.
#[derive(Clone, Serialize, Debug, Deserialize, PartialEq)]
pub struct AssistantObject {
    /// The identifier, which can be referenced in API endpoints.
    pub id: String,
    /// The object type, which is always `assistant`.
    pub object: String,
    /// The Unix timestamp (in seconds) for when the assistant was created.
    pub created_at: i32,
    /// The name of the assistant. The maximum length is 256 characters.
    pub name: Option<String>,
    /// The description of the assistant. The maximum length is 512 characters.
    pub description: Option<String>,
    /// ID of the model to use. You can use the [List models](https://platform.openai.com/docs/api-reference/models/list) API to see all of your available models, or see our [Model overview](https://platform.openai.com/docs/models) for descriptions of them.
    pub model: String,
    /// The system instructions that the assistant uses. The maximum length is 256,000 characters.
    pub instructions: Option<String>,
    /// A list of tool enabled on the assistant. There can be a maximum of 128 tools per assistant. Tools can be of types `code_interpreter`, `file_search`, or `function`.
    #[serde(default)]
    pub tools: Vec<AssistantTools>,
    /// A set of resources that are used by the assistant's tools. The resources are specific to the type of tool. For example, the `code_interpreter` tool requires a list of file IDs, while the `file_search` tool requires a list of vector store IDs.
    pub tool_resources: Option<AssistantToolResources>,
    /// Set of 16 key-value pairs that can be attached to an object. This can be useful for storing additional information about the object in a structured format. Keys can be a maximum of 64 characters long and values can be a maximum of 512 characters long.
    pub metadata: Option<HashMap<String, String>>,
    /// What sampling temperature to use, between 0 and 2. Higher values like 0.8 will make the output more random, while lower values like 0.2 will make it more focused and deterministic.
    pub temperature: Option<f32>,
    /// An alternative to sampling with temperature, called nucleus sampling, where the model considers the results of the tokens with top_p probability mass. So 0.1 means only the tokens comprising the top 10% probability mass are considered.
    /// We generally recommend altering this or temperature but not both.
    pub top_p: Option<f32>,

    pub response_format: Option<AssistantsApiResponseFormatOption>,
}

/// Specifies the format that the model must output. Compatible with [GPT-4o](https://platform.openai.com/docs/models/gpt-4o), [GPT-4 Turbo](https://platform.openai.com/docs/models/gpt-4-turbo-and-gpt-4), and all GPT-3.5 Turbo models since `gpt-3.5-turbo-1106`.
///
/// Setting to `{ "type": "json_schema", "json_schema": {...} }` enables Structured Outputs which guarantees the model will match your supplied JSON schema. Learn more in the [Structured Outputs guide](https://platform.openai.com/docs/guides/structured-outputs).
///
/// Setting to `{ "type": "json_object" }` enables JSON mode, which guarantees the message the model generates is valid JSON.
///
/// **Important:** when using JSON mode, you **must** also instruct the model to produce JSON yourself via a system or user message. Without this, the model may generate an unending stream of whitespace until the generation reaches the token limit, resulting in a long-running and seemingly "stuck" request. Also note that the message content may be partially cut off if `finish_reason="length"`, which indicates the generation exceeded `max_tokens` or the conversation exceeded the max context length.
#[derive(Clone, Serialize, Debug, Deserialize, PartialEq, Default)]
pub enum AssistantsApiResponseFormatOption {
    #[default]
    #[serde(rename = "auto")]
    Auto,
    #[serde(untagged)]
    Format(ResponseFormat),
}

/// Retrieval tool
#[derive(Clone, Serialize, Debug, Default, Deserialize, PartialEq)]
pub struct AssistantToolsFileSearch {
    /// Overrides for the file search tool.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub file_search: Option<AssistantToolsFileSearchOverrides>,
}

#[derive(Clone, Serialize, Debug, Deserialize, PartialEq)]
pub struct AssistantToolsFileSearchOverrides {
    ///  The maximum number of results the file search tool should output. The default is 20 for gpt-4* models and 5 for gpt-3.5-turbo. This number should be between 1 and 50 inclusive.
    ///
    //// Note that the file search tool may output fewer than `max_num_results` results. See the [file search tool documentation](https://platform.openai.com/docs/assistants/tools/file-search/customizing-file-search-settings) for more information.
    pub max_num_results: Option<u8>,
    pub ranking_options: Option<FileSearchRankingOptions>,
}

#[derive(Clone, Serialize, Debug, Deserialize, PartialEq)]
pub enum FileSearchRanker {
    #[serde(rename = "auto")]
    Auto,
    #[serde(rename = "default_2024_08_21")]
    Default2024_08_21,
}

/// The ranking options for the file search. If not specified, the file search tool will use the `auto` ranker and a score_threshold of 0.
///
/// See the [file search tool documentation](https://platform.openai.com/docs/assistants/tools/file-search#customizing-file-search-settings) for more information.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct FileSearchRankingOptions {
    /// The ranker to use for the file search. If not specified will use the `auto` ranker.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ranker: Option<FileSearchRanker>,

    /// The score threshold for the file search. All values must be a floating point number between 0 and 1.
    pub score_threshold: f32,
}

/// Function tool
#[derive(Clone, Serialize, Debug, Default, Deserialize, PartialEq)]
pub struct AssistantToolsFunction {
    pub function: FunctionObject,
}

#[derive(Clone, Serialize, Debug, Deserialize, PartialEq)]
#[serde(tag = "type")]
#[serde(rename_all = "snake_case")]
pub enum AssistantTools {
    CodeInterpreter,
    FileSearch(AssistantToolsFileSearch),
    Function(AssistantToolsFunction),
}

#[derive(Clone, Serialize, Default, Debug, Deserialize, Builder, PartialEq)]
#[builder(name = "CreateAssistantRequestArgs")]
#[builder(pattern = "mutable")]
#[builder(setter(into, strip_option), default)]
#[builder(derive(Debug))]
#[builder(build_fn(error = "OpenAIError"))]
pub struct CreateAssistantRequest {
    /// ID of the model to use. You can use the [List models](https://platform.openai.com/docs/api-reference/models/list) API to see all of your available models, or see our [Model overview](https://platform.openai.com/docs/models/overview) for descriptions of them.
    pub model: String,

    /// The name of the assistant. The maximum length is 256 characters.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,

    /// The description of the assistant. The maximum length is 512 characters.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,

    /// The system instructions that the assistant uses. The maximum length is 256,000 characters.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub instructions: Option<String>,

    /// A list of tool enabled on the assistant. There can be a maximum of 128 tools per assistant. Tools can be of types `code_interpreter`, `file_search`, or `function`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tools: Option<Vec<AssistantTools>>,

    /// A set of resources that are used by the assistant's tools. The resources are specific to the type of tool. For example, the `code_interpreter` tool requires a list of file IDs, while the `file_search` tool requires a list of vector store IDs.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tool_resources: Option<CreateAssistantToolResources>,

    /// Set of 16 key-value pairs that can be attached to an object. This can be useful for storing additional information about the object in a structured format. Keys can be a maximum of 64 characters long and values can be a maximum of 512 characters long.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub metadata: Option<HashMap<String, String>>,

    /// What sampling temperature to use, between 0 and 2. Higher values like 0.8 will make the output more random, while lower values like 0.2 will make it more focused and deterministic.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub temperature: Option<f32>,

    /// An alternative to sampling with temperature, called nucleus sampling, where the model considers the results of the tokens with top_p probability mass. So 0.1 means only the tokens comprising the top 10% probability mass are considered.
    ///
    /// We generally recommend altering this or temperature but not both.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub top_p: Option<f32>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub response_format: Option<AssistantsApiResponseFormatOption>,
}

#[derive(Clone, Serialize, Default, Debug, Deserialize, Builder, PartialEq)]
#[builder(name = "ModifyAssistantRequestArgs")]
#[builder(pattern = "mutable")]
#[builder(setter(into, strip_option), default)]
#[builder(derive(Debug))]
#[builder(build_fn(error = "OpenAIError"))]
pub struct ModifyAssistantRequest {
    /// ID of the model to use. You can use the [List models](https://platform.openai.com/docs/api-reference/models/list) API to see all of your available models, or see our [Model overview](https://platform.openai.com/docs/models/overview) for descriptions of them.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub model: Option<String>,

    /// The name of the assistant. The maximum length is 256 characters.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,

    /// The description of the assistant. The maximum length is 512 characters.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,

    /// The system instructions that the assistant uses. The maximum length is 256,000 characters.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub instructions: Option<String>,

    /// A list of tool enabled on the assistant. There can be a maximum of 128 tools per assistant. Tools can be of types `code_interpreter`, `file_search`, or `function`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tools: Option<Vec<AssistantTools>>,

    /// A set of resources that are used by the assistant's tools. The resources are specific to the type of tool. For example, the `code_interpreter` tool requires a list of file IDs, while the `file_search` tool requires a list of vector store IDs.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tool_resources: Option<AssistantToolResources>,
    /// Set of 16 key-value pairs that can be attached to an object. This can be useful for storing additional information about the object in a structured format. Keys can be a maximum of 64 characters long and values can be a maxium of 512 characters long.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub metadata: Option<HashMap<String, String>>,

    /// What sampling temperature to use, between 0 and 2. Higher values like 0.8 will make the output more random, while lower values like 0.2 will make it more focused and deterministic.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub temperature: Option<f32>,

    /// An alternative to sampling with temperature, called nucleus sampling, where the model considers the results of the tokens with top_p probability mass. So 0.1 means only the tokens comprising the top 10% probability mass are considered.
    ///
    /// We generally recommend altering this or temperature but not both.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub top_p: Option<f32>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub response_format: Option<AssistantsApiResponseFormatOption>,
}

#[derive(Clone, Serialize, Default, Debug, Deserialize, PartialEq)]
pub struct DeleteAssistantResponse {
    pub id: String,
    pub deleted: bool,
    pub object: String,
}

#[derive(Clone, Serialize, Default, Debug, Deserialize, PartialEq)]
pub struct ListAssistantsResponse {
    pub object: String,
    pub data: Vec<AssistantObject>,
    pub first_id: Option<String>,
    pub last_id: Option<String>,
    pub has_more: bool,
}

/// Controls which (if any) tool is called by the model.
/// `none` means the model will not call any tools and instead generates a message.
/// `auto` is the default value and means the model can pick between generating a message or calling one or more tools.
/// `required` means the model must call one or more tools before responding to the user.
/// Specifying a particular tool like `{"type": "file_search"}` or `{"type": "function", "function": {"name": "my_function"}}` forces the model to call that tool.
#[derive(Clone, Serialize, Default, Debug, Deserialize, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum AssistantsApiToolChoiceOption {
    #[default]
    None,
    Auto,
    Required,
    #[serde(untagged)]
    Named(AssistantsNamedToolChoice),
}

/// Specifies a tool the model should use. Use to force the model to call a specific tool.
#[derive(Clone, Serialize, Default, Debug, Deserialize, PartialEq)]
pub struct AssistantsNamedToolChoice {
    /// The type of the tool. If type is `function`, the function name must be set
    pub r#type: AssistantToolType,

    pub function: Option<FunctionName>,
}

#[derive(Clone, Serialize, Default, Debug, Deserialize, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum AssistantToolType {
    #[default]
    Function,
    CodeInterpreter,
    FileSearch,
}
