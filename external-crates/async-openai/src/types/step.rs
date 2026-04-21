use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use super::{FileSearchRankingOptions, ImageFile, LastError, RunStatus};

#[derive(Clone, Serialize, Debug, Deserialize, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum RunStepType {
    MessageCreation,
    ToolCalls,
}

/// Represents a step in execution of a run.
#[derive(Clone, Serialize, Debug, Deserialize, PartialEq)]
pub struct RunStepObject {
    /// The identifier, which can be referenced in API endpoints.
    pub id: String,
    /// The object type, which is always `thread.run.step`.
    pub object: String,
    /// The Unix timestamp (in seconds) for when the run step was created.
    pub created_at: i32,

    /// The ID of the [assistant](https://platform.openai.com/docs/api-reference/assistants) associated with the run step.
    pub assistant_id: Option<String>,

    /// The ID of the [thread](https://platform.openai.com/docs/api-reference/threads) that was run.
    pub thread_id: String,

    ///  The ID of the [run](https://platform.openai.com/docs/api-reference/runs) that this run step is a part of.
    pub run_id: String,

    /// The type of run step, which can be either `message_creation` or `tool_calls`.
    pub r#type: RunStepType,

    /// The status of the run step, which can be either `in_progress`, `cancelled`, `failed`, `completed`, or `expired`.
    pub status: RunStatus,

    /// The details of the run step.
    pub step_details: StepDetails,

    /// The last error associated with this run. Will be `null` if there are no errors.
    pub last_error: Option<LastError>,

    ///The Unix timestamp (in seconds) for when the run step expired. A step is considered expired if the parent run is expired.
    pub expires_at: Option<i32>,

    /// The Unix timestamp (in seconds) for when the run step was cancelled.
    pub cancelled_at: Option<i32>,

    /// The Unix timestamp (in seconds) for when the run step failed.
    pub failed_at: Option<i32>,

    /// The Unix timestamp (in seconds) for when the run step completed.
    pub completed_at: Option<i32>,

    pub metadata: Option<HashMap<String, serde_json::Value>>,

    /// Usage statistics related to the run step. This value will be `null` while the run step's status is `in_progress`.
    pub usage: Option<RunStepCompletionUsage>,
}

#[derive(Clone, Serialize, Debug, Deserialize, PartialEq)]
pub struct RunStepCompletionUsage {
    /// Number of completion tokens used over the course of the run step.
    pub completion_tokens: u32,
    /// Number of prompt tokens used over the course of the run step.
    pub prompt_tokens: u32,
    /// Total number of tokens used (prompt + completion).
    pub total_tokens: u32,
}

#[derive(Clone, Serialize, Debug, Deserialize, PartialEq)]
#[serde(tag = "type")]
#[serde(rename_all = "snake_case")]
pub enum StepDetails {
    MessageCreation(RunStepDetailsMessageCreationObject),
    ToolCalls(RunStepDetailsToolCallsObject),
}

/// Details of the message creation by the run step.
#[derive(Clone, Serialize, Debug, Deserialize, PartialEq)]
pub struct RunStepDetailsMessageCreationObject {
    pub message_creation: MessageCreation,
}

#[derive(Clone, Serialize, Debug, Deserialize, PartialEq)]
pub struct MessageCreation {
    /// The ID of the message that was created by this run step.
    pub message_id: String,
}

/// Details of the tool call.
#[derive(Clone, Serialize, Debug, Deserialize, PartialEq)]
pub struct RunStepDetailsToolCallsObject {
    /// An array of tool calls the run step was involved in. These can be associated with one of three types of tools: `code_interpreter`, `file_search`, or `function`.
    pub tool_calls: Vec<RunStepDetailsToolCalls>,
}

#[derive(Clone, Serialize, Debug, Deserialize, PartialEq)]
#[serde(tag = "type")]
#[serde(rename_all = "snake_case")]
pub enum RunStepDetailsToolCalls {
    /// Details of the Code Interpreter tool call the run step was involved in.
    CodeInterpreter(RunStepDetailsToolCallsCodeObject),
    FileSearch(RunStepDetailsToolCallsFileSearchObject),
    Function(RunStepDetailsToolCallsFunctionObject),
}

/// Code interpreter tool call
#[derive(Clone, Serialize, Debug, Deserialize, PartialEq)]
pub struct RunStepDetailsToolCallsCodeObject {
    /// The ID of the tool call.
    pub id: String,

    /// The Code Interpreter tool call definition.
    pub code_interpreter: CodeInterpreter,
}

#[derive(Clone, Serialize, Debug, Deserialize, PartialEq)]
pub struct CodeInterpreter {
    /// The input to the Code Interpreter tool call.
    pub input: String,
    /// The outputs from the Code Interpreter tool call. Code Interpreter can output one or more items, including text (`logs`) or images (`image`). Each of these are represented by a different object type.
    pub outputs: Vec<CodeInterpreterOutput>,
}

#[derive(Clone, Serialize, Debug, Deserialize, PartialEq)]
#[serde(tag = "type")]
#[serde(rename_all = "lowercase")]
pub enum CodeInterpreterOutput {
    /// Code interpreter log output
    Logs(RunStepDetailsToolCallsCodeOutputLogsObject),
    /// Code interpreter image output
    Image(RunStepDetailsToolCallsCodeOutputImageObject),
}

/// Text output from the Code Interpreter tool call as part of a run step.
#[derive(Clone, Serialize, Debug, Deserialize, PartialEq)]
pub struct RunStepDetailsToolCallsCodeOutputLogsObject {
    /// The text output from the Code Interpreter tool call.
    pub logs: String,
}

#[derive(Clone, Serialize, Debug, Deserialize, PartialEq)]
pub struct RunStepDetailsToolCallsCodeOutputImageObject {
    /// The [file](https://platform.openai.com/docs/api-reference/files) ID of the image.
    pub image: ImageFile,
}

/// File search tool call
#[derive(Clone, Serialize, Debug, Deserialize, PartialEq)]
pub struct RunStepDetailsToolCallsFileSearchObject {
    /// The ID of the tool call object.
    pub id: String,
    /// For now, this is always going to be an empty object.
    pub file_search: RunStepDetailsToolCallsFileSearchObjectFileSearch,
}

#[derive(Clone, Serialize, Debug, Deserialize, PartialEq)]
pub struct RunStepDetailsToolCallsFileSearchObjectFileSearch {
    pub ranking_options: Option<FileSearchRankingOptions>,
    /// The results of the file search.
    pub results: Option<Vec<RunStepDetailsToolCallsFileSearchResultObject>>,
}

/// A result instance of the file search.
#[derive(Clone, Serialize, Debug, Deserialize, PartialEq)]
pub struct RunStepDetailsToolCallsFileSearchResultObject {
    /// The ID of the file that result was found in.
    pub file_id: String,
    /// The name of the file that result was found in.
    pub file_name: String,
    /// The score of the result. All values must be a floating point number between 0 and 1.
    pub score: f32,
    /// The content of the result that was found. The content is only included if requested via the include query parameter.
    pub content: Option<Vec<RunStepDetailsToolCallsFileSearchResultObjectContent>>,
}

#[derive(Clone, Serialize, Debug, Deserialize, PartialEq)]
pub struct RunStepDetailsToolCallsFileSearchResultObjectContent {
    // note: type is text hence omitted from struct
    /// The text content of the file.
    pub text: Option<String>,
}

#[derive(Clone, Serialize, Debug, Deserialize, PartialEq)]
pub struct RunStepDetailsToolCallsFunctionObject {
    /// The ID of the tool call object.
    pub id: String,
    /// he definition of the function that was called.
    pub function: RunStepFunctionObject,
}

#[derive(Clone, Serialize, Debug, Deserialize, PartialEq)]
pub struct RunStepFunctionObject {
    /// The name of the function.
    pub name: String,
    /// The arguments passed to the function.
    pub arguments: String,
    /// The output of the function. This will be `null` if the outputs have not been [submitted](https://platform.openai.com/docs/api-reference/runs/submitToolOutputs) yet.
    pub output: Option<String>,
}

#[derive(Clone, Serialize, Debug, Deserialize, PartialEq)]
pub struct RunStepFunctionObjectDelta {
    /// The name of the function.
    pub name: Option<String>,
    /// The arguments passed to the function.
    pub arguments: Option<String>,
    /// The output of the function. This will be `null` if the outputs have not been [submitted](https://platform.openai.com/docs/api-reference/runs/submitToolOutputs) yet.
    pub output: Option<String>,
}

#[derive(Clone, Serialize, Debug, Deserialize, PartialEq)]
pub struct ListRunStepsResponse {
    pub object: String,
    pub data: Vec<RunStepObject>,
    pub first_id: Option<String>,
    pub last_id: Option<String>,
    pub has_more: bool,
}

/// Represents a run step delta i.e. any changed fields on a run step during streaming.
#[derive(Clone, Serialize, Debug, Deserialize, PartialEq)]
pub struct RunStepDeltaObject {
    /// The identifier of the run step, which can be referenced in API endpoints.
    pub id: String,
    /// The object type, which is always `thread.run.step.delta`.
    pub object: String,
    /// The delta containing the fields that have changed on the run step.
    pub delta: RunStepDelta,
}

#[derive(Clone, Serialize, Debug, Deserialize, PartialEq)]
pub struct RunStepDelta {
    pub step_details: DeltaStepDetails,
}

#[derive(Clone, Serialize, Debug, Deserialize, PartialEq)]
#[serde(tag = "type")]
#[serde(rename_all = "snake_case")]
pub enum DeltaStepDetails {
    MessageCreation(RunStepDeltaStepDetailsMessageCreationObject),
    ToolCalls(RunStepDeltaStepDetailsToolCallsObject),
}

/// Details of the message creation by the run step.
#[derive(Clone, Serialize, Debug, Deserialize, PartialEq)]
pub struct RunStepDeltaStepDetailsMessageCreationObject {
    pub message_creation: Option<MessageCreation>,
}

/// Details of the tool call.
#[derive(Clone, Serialize, Debug, Deserialize, PartialEq)]
pub struct RunStepDeltaStepDetailsToolCallsObject {
    /// An array of tool calls the run step was involved in. These can be associated with one of three types of tools: `code_interpreter`, `file_search`, or `function`.
    pub tool_calls: Option<Vec<RunStepDeltaStepDetailsToolCalls>>,
}

#[derive(Clone, Serialize, Debug, Deserialize, PartialEq)]
#[serde(tag = "type")]
#[serde(rename_all = "snake_case")]
pub enum RunStepDeltaStepDetailsToolCalls {
    CodeInterpreter(RunStepDeltaStepDetailsToolCallsCodeObject),
    FileSearch(RunStepDeltaStepDetailsToolCallsFileSearchObject),
    Function(RunStepDeltaStepDetailsToolCallsFunctionObject),
}

/// Details of the Code Interpreter tool call the run step was involved in.
#[derive(Clone, Serialize, Debug, Deserialize, PartialEq)]
pub struct RunStepDeltaStepDetailsToolCallsCodeObject {
    /// The index of the tool call in the tool calls array.
    pub index: u32,
    /// The ID of the tool call.
    pub id: Option<String>,
    /// The Code Interpreter tool call definition.
    pub code_interpreter: Option<DeltaCodeInterpreter>,
}

#[derive(Clone, Serialize, Debug, Deserialize, PartialEq)]
pub struct DeltaCodeInterpreter {
    /// The input to the Code Interpreter tool call.
    pub input: Option<String>,
    /// The outputs from the Code Interpreter tool call. Code Interpreter can output one or more items, including text (`logs`) or images (`image`). Each of these are represented by a different object type.
    pub outputs: Option<Vec<DeltaCodeInterpreterOutput>>,
}

#[derive(Clone, Serialize, Debug, Deserialize, PartialEq)]
#[serde(tag = "type")]
#[serde(rename_all = "lowercase")]
pub enum DeltaCodeInterpreterOutput {
    Logs(RunStepDeltaStepDetailsToolCallsCodeOutputLogsObject),
    Image(RunStepDeltaStepDetailsToolCallsCodeOutputImageObject),
}

/// Text output from the Code Interpreter tool call as part of a run step.
#[derive(Clone, Serialize, Debug, Deserialize, PartialEq)]
pub struct RunStepDeltaStepDetailsToolCallsCodeOutputLogsObject {
    /// The index of the output in the outputs array.
    pub index: u32,
    /// The text output from the Code Interpreter tool call.
    pub logs: Option<String>,
}

/// Code interpreter image output
#[derive(Clone, Serialize, Debug, Deserialize, PartialEq)]
pub struct RunStepDeltaStepDetailsToolCallsCodeOutputImageObject {
    /// The index of the output in the outputs array.
    pub index: u32,

    pub image: Option<ImageFile>,
}

#[derive(Clone, Serialize, Debug, Deserialize, PartialEq)]
pub struct RunStepDeltaStepDetailsToolCallsFileSearchObject {
    /// The index of the tool call in the tool calls array.
    pub index: u32,
    /// The ID of the tool call object.
    pub id: Option<String>,
    /// For now, this is always going to be an empty object.
    pub file_search: Option<serde_json::Value>,
}

/// Function tool call
#[derive(Clone, Serialize, Debug, Deserialize, PartialEq)]
pub struct RunStepDeltaStepDetailsToolCallsFunctionObject {
    /// The index of the tool call in the tool calls array.
    pub index: u32,
    /// The ID of the tool call object.
    pub id: Option<String>,
    /// The definition of the function that was called.
    pub function: Option<RunStepFunctionObjectDelta>,
}
