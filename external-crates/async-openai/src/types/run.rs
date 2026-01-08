use std::collections::HashMap;

use derive_builder::Builder;
use serde::{Deserialize, Serialize};

use crate::{error::OpenAIError, types::FunctionCall};

use super::{
    AssistantTools, AssistantsApiResponseFormatOption, AssistantsApiToolChoiceOption,
    CreateMessageRequest,
};

/// Represents an execution run on a [thread](https://platform.openai.com/docs/api-reference/threads).
#[derive(Clone, Serialize, Debug, Deserialize, PartialEq)]
pub struct RunObject {
    /// The identifier, which can be referenced in API endpoints.
    pub id: String,
    /// The object type, which is always `thread.run`.
    pub object: String,
    /// The Unix timestamp (in seconds) for when the run was created.
    pub created_at: i32,
    ///The ID of the [thread](https://platform.openai.com/docs/api-reference/threads) that was executed on as a part of this run.
    pub thread_id: String,

    /// The ID of the [assistant](https://platform.openai.com/docs/api-reference/assistants) used for execution of this run.
    pub assistant_id: Option<String>,

    /// The status of the run, which can be either `queued`, `in_progress`, `requires_action`, `cancelling`, `cancelled`, `failed`, `completed`, `incomplete`, or `expired`.
    pub status: RunStatus,

    /// Details on the action required to continue the run. Will be `null` if no action is required.
    pub required_action: Option<RequiredAction>,

    /// The last error associated with this run. Will be `null` if there are no errors.
    pub last_error: Option<LastError>,

    /// The Unix timestamp (in seconds) for when the run will expire.
    pub expires_at: Option<i32>,
    ///  The Unix timestamp (in seconds) for when the run was started.
    pub started_at: Option<i32>,
    /// The Unix timestamp (in seconds) for when the run was cancelled.
    pub cancelled_at: Option<i32>,
    /// The Unix timestamp (in seconds) for when the run failed.
    pub failed_at: Option<i32>,
    ///The Unix timestamp (in seconds) for when the run was completed.
    pub completed_at: Option<i32>,

    /// Details on why the run is incomplete. Will be `null` if the run is not incomplete.
    pub incomplete_details: Option<RunObjectIncompleteDetails>,

    /// The model that the [assistant](https://platform.openai.com/docs/api-reference/assistants) used for this run.
    pub model: String,

    /// The instructions that the [assistant](https://platform.openai.com/docs/api-reference/assistants) used for this run.
    pub instructions: String,

    /// The list of tools that the [assistant](https://platform.openai.com/docs/api-reference/assistants) used for this run.
    pub tools: Vec<AssistantTools>,

    pub metadata: Option<HashMap<String, serde_json::Value>>,

    /// Usage statistics related to the run. This value will be `null` if the run is not in a terminal state (i.e. `in_progress`, `queued`, etc.).
    pub usage: Option<RunCompletionUsage>,

    /// The sampling temperature used for this run. If not set, defaults to 1.
    pub temperature: Option<f32>,

    /// The nucleus sampling value used for this run. If not set, defaults to 1.
    pub top_p: Option<f32>,

    /// The maximum number of prompt tokens specified to have been used over the course of the run.
    pub max_prompt_tokens: Option<u32>,

    /// The maximum number of completion tokens specified to have been used over the course of the run.
    pub max_completion_tokens: Option<u32>,

    /// Controls for how a thread will be truncated prior to the run. Use this to control the intial context window of the run.
    pub truncation_strategy: Option<TruncationObject>,

    pub tool_choice: Option<AssistantsApiToolChoiceOption>,

    /// Whether to enable [parallel function calling](https://platform.openai.com/docs/guides/function-calling/parallel-function-calling) during tool use.
    pub parallel_tool_calls: bool,

    pub response_format: Option<AssistantsApiResponseFormatOption>,
}

#[derive(Clone, Serialize, Debug, Deserialize, PartialEq, Default)]
#[serde(rename_all = "snake_case")]
pub enum TruncationObjectType {
    #[default]
    Auto,
    LastMessages,
}

/// Thread Truncation Controls
#[derive(Clone, Serialize, Debug, Deserialize, PartialEq)]
pub struct TruncationObject {
    /// The truncation strategy to use for the thread. The default is `auto`. If set to `last_messages`, the thread will be truncated to the n most recent messages in the thread. When set to `auto`, messages in the middle of the thread will be dropped to fit the context length of the model, `max_prompt_tokens`.
    pub r#type: TruncationObjectType,
    /// The number of most recent messages from the thread when constructing the context for the run.
    pub last_messages: Option<u32>,
}

#[derive(Clone, Serialize, Debug, Deserialize, PartialEq)]
pub struct RunObjectIncompleteDetails {
    /// The reason why the run is incomplete. This will point to which specific token limit was reached over the course of the run.
    pub reason: RunObjectIncompleteDetailsReason,
}

#[derive(Clone, Serialize, Debug, Deserialize, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum RunObjectIncompleteDetailsReason {
    MaxCompletionTokens,
    MaxPromptTokens,
}

#[derive(Clone, Serialize, Debug, Deserialize, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum RunStatus {
    Queued,
    InProgress,
    RequiresAction,
    Cancelling,
    Cancelled,
    Failed,
    Completed,
    Incomplete,
    Expired,
}

#[derive(Clone, Serialize, Debug, Deserialize, PartialEq)]
pub struct RequiredAction {
    /// For now, this is always `submit_tool_outputs`.
    pub r#type: String,

    pub submit_tool_outputs: SubmitToolOutputs,
}

#[derive(Clone, Serialize, Debug, Deserialize, PartialEq)]
pub struct SubmitToolOutputs {
    pub tool_calls: Vec<RunToolCallObject>,
}

#[derive(Clone, Serialize, Debug, Deserialize, PartialEq)]
pub struct RunToolCallObject {
    /// The ID of the tool call. This ID must be referenced when you submit the tool outputs in using the [Submit tool outputs to run](https://platform.openai.com/docs/api-reference/runs/submitToolOutputs) endpoint.
    pub id: String,
    /// The type of tool call the output is required for. For now, this is always `function`.
    pub r#type: String,
    /// The function definition.
    pub function: FunctionCall,
}

#[derive(Clone, Serialize, Debug, Deserialize, PartialEq)]
pub struct LastError {
    /// One of `server_error`, `rate_limit_exceeded`, or `invalid_prompt`.
    pub code: LastErrorCode,
    /// A human-readable description of the error.
    pub message: String,
}

#[derive(Clone, Serialize, Debug, Deserialize, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum LastErrorCode {
    ServerError,
    RateLimitExceeded,
    InvalidPrompt,
}

#[derive(Clone, Serialize, Debug, Deserialize, PartialEq)]
pub struct RunCompletionUsage {
    /// Number of completion tokens used over the course of the run.
    pub completion_tokens: u32,
    /// Number of prompt tokens used over the course of the run.
    pub prompt_tokens: u32,
    /// Total number of tokens used (prompt + completion).
    pub total_tokens: u32,
}

#[derive(Clone, Serialize, Default, Debug, Deserialize, Builder, PartialEq)]
#[builder(name = "CreateRunRequestArgs")]
#[builder(pattern = "mutable")]
#[builder(setter(into, strip_option), default)]
#[builder(derive(Debug))]
#[builder(build_fn(error = "OpenAIError"))]
pub struct CreateRunRequest {
    /// The ID of the [assistant](https://platform.openai.com/docs/api-reference/assistants) to use to execute this run.
    pub assistant_id: String,

    /// The ID of the [Model](https://platform.openai.com/docs/api-reference/models) to be used to execute this run. If a value is provided here, it will override the model associated with the assistant. If not, the model associated with the assistant will be used.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub model: Option<String>,

    /// Overrides the [instructions](https://platform.openai.com/docs/api-reference/assistants/createAssistant) of the assistant. This is useful for modifying the behavior on a per-run basis.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub instructions: Option<String>,

    /// Appends additional instructions at the end of the instructions for the run. This is useful for modifying the behavior on a per-run basis without overriding other instructions.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub additional_instructions: Option<String>,

    /// Adds additional messages to the thread before creating the run.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub additional_messages: Option<Vec<CreateMessageRequest>>,

    /// Override the tools the assistant can use for this run. This is useful for modifying the behavior on a per-run basis.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tools: Option<Vec<AssistantTools>>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub metadata: Option<HashMap<String, serde_json::Value>>,

    /// The sampling temperature used for this run. If not set, defaults to 1.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub temperature: Option<f32>,

    ///  An alternative to sampling with temperature, called nucleus sampling, where the model considers the results of the tokens with top_p probability mass. So 0.1 means only the tokens comprising the top 10% probability mass are considered.
    ///
    /// We generally recommend altering this or temperature but not both.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub top_p: Option<f32>,

    /// If `true`, returns a stream of events that happen during the Run as server-sent events, terminating when the Run enters a terminal state with a `data: [DONE]` message.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub stream: Option<bool>,

    /// The maximum number of prompt tokens that may be used over the course of the run. The run will make a best effort to use only the number of prompt tokens specified, across multiple turns of the run. If the run exceeds the number of prompt tokens specified, the run will end with status `incomplete`. See `incomplete_details` for more info.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_prompt_tokens: Option<u32>,

    /// The maximum number of completion tokens that may be used over the course of the run. The run will make a best effort to use only the number of completion tokens specified, across multiple turns of the run. If the run exceeds the number of completion tokens specified, the run will end with status `incomplete`. See `incomplete_details` for more info.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_completion_tokens: Option<u32>,

    /// Controls for how a thread will be truncated prior to the run. Use this to control the intial context window of the run.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub truncation_strategy: Option<TruncationObject>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub tool_choice: Option<AssistantsApiToolChoiceOption>,

    /// Whether to enable [parallel function calling](https://platform.openai.com/docs/guides/function-calling/parallel-function-calling) during tool use.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub parallel_tool_calls: Option<bool>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub response_format: Option<AssistantsApiResponseFormatOption>,
}

#[derive(Clone, Serialize, Default, Debug, Deserialize, PartialEq)]
pub struct ModifyRunRequest {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub metadata: Option<HashMap<String, serde_json::Value>>,
}

#[derive(Clone, Serialize, Default, Debug, Deserialize, PartialEq)]
pub struct ListRunsResponse {
    pub object: String,
    pub data: Vec<RunObject>,
    pub first_id: Option<String>,
    pub last_id: Option<String>,
    pub has_more: bool,
}

#[derive(Clone, Serialize, Default, Debug, Deserialize, PartialEq)]
pub struct SubmitToolOutputsRunRequest {
    /// A list of tools for which the outputs are being submitted.
    pub tool_outputs: Vec<ToolsOutputs>,
    /// If `true`, returns a stream of events that happen during the Run as server-sent events, terminating when the Run enters a terminal state with a `data: [DONE]` message.
    pub stream: Option<bool>,
}

#[derive(Clone, Serialize, Default, Debug, Deserialize, Builder, PartialEq)]
#[builder(name = "ToolsOutputsArgs")]
#[builder(pattern = "mutable")]
#[builder(setter(into, strip_option), default)]
#[builder(derive(Debug))]
#[builder(build_fn(error = "OpenAIError"))]
pub struct ToolsOutputs {
    /// The ID of the tool call in the `required_action` object within the run object the output is being submitted for.
    pub tool_call_id: Option<String>,
    /// The output of the tool call to be submitted to continue the run.
    pub output: Option<String>,
}
