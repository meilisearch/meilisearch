//! OpenAPI schema types for the chat completions endpoint.
//!
//! These types mirror the OpenAI chat completion request/response structures
//! for the sole purpose of generating accurate OpenAPI documentation.
//! They are NOT used at runtime — the actual deserialization uses
//! `async_openai::types::CreateChatCompletionRequest`.

#![allow(dead_code)]

use std::collections::HashMap;

use serde::Serialize;
use utoipa::ToSchema;

/// A chat completion request compatible with OpenAI's API.
#[derive(Debug, Serialize, ToSchema)]
pub struct ChatCompletionRequest {
    /// A list of messages comprising the conversation so far.
    pub messages: Vec<ChatCompletionRequestMessage>,

    /// ID of the model to use.
    pub model: String,

    /// Number between -2.0 and 2.0. Positive values penalize new tokens based
    /// on their existing frequency in the text so far, decreasing the model's
    /// likelihood to repeat the same line verbatim.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub frequency_penalty: Option<f32>,

    /// Modify the likelihood of specified tokens appearing in the completion.
    /// Accepts a JSON object that maps tokens (specified by their token ID in
    /// the tokenizer) to an associated bias value from -100 to 100.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub logit_bias: Option<HashMap<String, serde_json::Value>>,

    /// Whether to return log probabilities of the output tokens or not.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub logprobs: Option<bool>,

    /// An integer between 0 and 20 specifying the number of most likely tokens
    /// to return at each token position, each with an associated log probability.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub top_logprobs: Option<u8>,

    /// An upper bound for the number of tokens that can be generated for a
    /// completion, including visible output tokens and reasoning tokens.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_completion_tokens: Option<u32>,

    /// How many chat completion choices to generate for each input message.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub n: Option<u8>,

    /// Number between -2.0 and 2.0. Positive values penalize new tokens based
    /// on whether they appear in the text so far, increasing the model's
    /// likelihood to talk about new topics.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub presence_penalty: Option<f32>,

    /// An object specifying the format that the model must output.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub response_format: Option<ChatCompletionResponseFormat>,

    /// If specified, the system will make a best effort to sample
    /// deterministically, such that repeated requests with the same seed and
    /// parameters should return the same result.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub seed: Option<i64>,

    /// Up to 4 sequences where the API will stop generating further tokens.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub stop: Option<ChatCompletionStop>,

    /// If set, partial message deltas will be sent as server-sent events as
    /// they become available, with the stream terminated by a `data: [DONE]`
    /// message.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub stream: Option<bool>,

    /// Options for streaming response. Only set this when you set `stream: true`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub stream_options: Option<ChatCompletionStreamOptions>,

    /// What sampling temperature to use, between 0 and 2. Higher values like
    /// 0.8 will make the output more random, while lower values like 0.2 will
    /// make it more focused and deterministic.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub temperature: Option<f32>,

    /// An alternative to sampling with temperature, called nucleus sampling,
    /// where the model considers the results of the tokens with top_p
    /// probability mass.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub top_p: Option<f32>,

    /// A list of tools the model may call. Currently, only functions are
    /// supported as a tool. Use this to provide a list of functions the model
    /// may generate JSON inputs for.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tools: Option<Vec<ChatCompletionTool>>,

    /// Controls which (if any) tool is called by the model.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tool_choice: Option<ChatCompletionToolChoiceOption>,

    /// Whether to enable parallel function calling during tool use.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub parallel_tool_calls: Option<bool>,

    /// A unique identifier representing your end-user.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub user: Option<String>,
}

/// A message in the chat completion conversation.
#[derive(Debug, Serialize, ToSchema)]
#[serde(tag = "role", rename_all = "lowercase")]
pub enum ChatCompletionRequestMessage {
    /// A system message setting the behavior of the assistant.
    System {
        /// The contents of the system message.
        content: String,
        /// An optional name for the participant.
        #[serde(skip_serializing_if = "Option::is_none")]
        name: Option<String>,
    },
    /// A user message.
    User {
        /// The contents of the user message (text or array of content parts).
        content: serde_json::Value,
        /// An optional name for the participant.
        #[serde(skip_serializing_if = "Option::is_none")]
        name: Option<String>,
    },
    /// An assistant message, possibly including tool calls.
    Assistant {
        /// The contents of the assistant message.
        #[serde(skip_serializing_if = "Option::is_none")]
        content: Option<String>,
        /// An optional name for the participant.
        #[serde(skip_serializing_if = "Option::is_none")]
        name: Option<String>,
        /// The tool calls generated by the model.
        #[serde(skip_serializing_if = "Option::is_none")]
        tool_calls: Option<Vec<ChatCompletionMessageToolCall>>,
    },
    /// A tool result message.
    Tool {
        /// The contents of the tool message.
        content: String,
        /// Tool call that this message is responding to.
        tool_call_id: String,
    },
    /// A developer/system message (newer models).
    Developer {
        /// The contents of the developer message.
        content: String,
        /// An optional name for the participant.
        #[serde(skip_serializing_if = "Option::is_none")]
        name: Option<String>,
    },
}

/// A tool call generated by the model.
#[derive(Debug, Serialize, ToSchema)]
pub struct ChatCompletionMessageToolCall {
    /// The ID of the tool call.
    pub id: String,
    /// The type of the tool. Currently, only `function` is supported.
    pub r#type: String,
    /// The function that the model called.
    pub function: FunctionCall,
}

/// A function call within a tool call.
#[derive(Debug, Serialize, ToSchema)]
pub struct FunctionCall {
    /// The name of the function to call.
    pub name: String,
    /// The arguments to call the function with, as a JSON string generated by
    /// the model.
    pub arguments: String,
}

/// A tool the model may call.
#[derive(Debug, Serialize, ToSchema)]
pub struct ChatCompletionTool {
    /// The type of the tool. Currently, only `function` is supported.
    pub r#type: String,
    /// The function definition.
    pub function: FunctionObject,
}

/// A function definition for tool use.
#[derive(Debug, Serialize, ToSchema)]
pub struct FunctionObject {
    /// The name of the function to be called.
    pub name: String,
    /// A description of what the function does.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    /// The parameters the function accepts, described as a JSON Schema object.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub parameters: Option<serde_json::Value>,
    /// Whether to enable strict schema adherence when generating the function call.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub strict: Option<bool>,
}

/// Controls which tool is called by the model.
#[derive(Debug, Serialize, ToSchema)]
#[serde(untagged)]
pub enum ChatCompletionToolChoiceOption {
    /// `none` means the model will not call any tool.
    /// `auto` means the model can pick between generating a message or calling
    /// one or more tools.
    /// `required` means the model must call one or more tools.
    Simple(String),
    /// Specifies a tool the model should use.
    Named(ChatCompletionNamedToolChoice),
}

/// Forces the model to call a specific tool.
#[derive(Debug, Serialize, ToSchema)]
pub struct ChatCompletionNamedToolChoice {
    /// The type of the tool. Currently, only `function` is supported.
    pub r#type: String,
    /// The function to call.
    pub function: ChatCompletionNamedToolChoiceFunction,
}

/// The function to call in a named tool choice.
#[derive(Debug, Serialize, ToSchema)]
pub struct ChatCompletionNamedToolChoiceFunction {
    /// The name of the function to call.
    pub name: String,
}

/// The format that the model must output.
#[derive(Debug, Serialize, ToSchema)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ChatCompletionResponseFormat {
    /// Standard text output.
    Text,
    /// JSON mode — the model will output valid JSON.
    JsonObject,
    /// Structured output — the model will match the supplied JSON schema.
    JsonSchema {
        /// The JSON schema the model must conform to.
        json_schema: serde_json::Value,
    },
}

/// Up to 4 sequences where the API will stop generating further tokens.
#[derive(Debug, Serialize, ToSchema)]
#[serde(untagged)]
pub enum ChatCompletionStop {
    /// A single stop string.
    String(String),
    /// An array of stop strings.
    Array(Vec<String>),
}

/// Options for streaming response.
#[derive(Debug, Serialize, ToSchema)]
pub struct ChatCompletionStreamOptions {
    /// If set, an additional chunk will be streamed before the `data: [DONE]`
    /// message containing token usage statistics for the entire request.
    pub include_usage: bool,
}

/// A chat completion response (non-streaming).
#[derive(Debug, Serialize, ToSchema)]
pub struct ChatCompletionResponse {
    /// A unique identifier for the chat completion.
    pub id: String,
    /// A list of chat completion choices.
    pub choices: Vec<ChatCompletionChoice>,
    /// The Unix timestamp (in seconds) of when the chat completion was created.
    pub created: u32,
    /// The model used for the chat completion.
    pub model: String,
    /// This fingerprint represents the backend configuration that the model
    /// runs with.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub system_fingerprint: Option<String>,
    /// The object type, which is always `chat.completion`.
    pub object: String,
    /// Usage statistics for the completion request.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub usage: Option<CompletionUsage>,
}

/// A chat completion choice.
#[derive(Debug, Serialize, ToSchema)]
pub struct ChatCompletionChoice {
    /// The index of the choice in the list.
    pub index: u32,
    /// The chat completion message generated by the model.
    pub message: ChatCompletionChoiceMessage,
    /// The reason the model stopped generating tokens.
    pub finish_reason: Option<String>,
}

/// A message generated by the model in a chat completion choice.
#[derive(Debug, Serialize, ToSchema)]
pub struct ChatCompletionChoiceMessage {
    /// The role of the author of this message (always `assistant`).
    pub role: String,
    /// The contents of the message.
    pub content: Option<String>,
    /// The refusal message generated by the model.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub refusal: Option<String>,
    /// The tool calls generated by the model.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tool_calls: Option<Vec<ChatCompletionMessageToolCall>>,
}

/// Usage statistics for a completion request.
#[derive(Debug, Serialize, ToSchema)]
pub struct CompletionUsage {
    /// Number of tokens in the prompt.
    pub prompt_tokens: u32,
    /// Number of tokens in the generated completion.
    pub completion_tokens: u32,
    /// Total number of tokens used in the request (prompt + completion).
    pub total_tokens: u32,
}
