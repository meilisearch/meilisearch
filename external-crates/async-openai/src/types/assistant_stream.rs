use std::{pin::Pin, time::Duration};

use futures::Stream;
use serde::Deserialize;

use crate::error::{map_deserialization_error, ApiError, OpenAIError};

use super::{
    MessageDeltaObject, MessageObject, RunObject, RunStepDeltaObject, RunStepObject, ThreadObject,
};

/// Represents an event emitted when streaming a Run.
///
/// Each event in a server-sent events stream has an `event` and `data` property:
///
/// ```text
/// event: thread.created
/// data: {"id": "thread_123", "object": "thread", ...}
/// ```
///
/// We emit events whenever a new object is created, transitions to a new state, or is being
/// streamed in parts (deltas). For example, we emit `thread.run.created` when a new run
/// is created, `thread.run.completed` when a run completes, and so on. When an Assistant chooses
/// to create a message during a run, we emit a `thread.message.created event`, a
/// `thread.message.in_progress` event, many `thread.message.delta` events, and finally a
/// `thread.message.completed` event.
///
/// We may add additional events over time, so we recommend handling unknown events gracefully
/// in your code. See the [Assistants API quickstart](https://platform.openai.com/docs/assistants/overview) to learn how to
/// integrate the Assistants API with streaming.

#[derive(Debug, Deserialize, Clone)]
#[serde(tag = "event", content = "data")]
#[non_exhaustive]
pub enum AssistantStreamEvent {
    /// Occurs when a new [thread](https://platform.openai.com/docs/api-reference/threads/object) is created.
    #[serde(rename = "thread.created")]
    TreadCreated(ThreadObject),
    /// Occurs when a new [run](https://platform.openai.com/docs/api-reference/runs/object) is created.
    #[serde(rename = "thread.run.created")]
    ThreadRunCreated(RunObject),
    /// Occurs when a [run](https://platform.openai.com/docs/api-reference/runs/object) moves to a `queued` status.
    #[serde(rename = "thread.run.queued")]
    ThreadRunQueued(RunObject),
    /// Occurs when a [run](https://platform.openai.com/docs/api-reference/runs/object) moves to an `in_progress` status.
    #[serde(rename = "thread.run.in_progress")]
    ThreadRunInProgress(RunObject),
    /// Occurs when a [run](https://platform.openai.com/docs/api-reference/runs/object) moves to a `requires_action` status.
    #[serde(rename = "thread.run.requires_action")]
    ThreadRunRequiresAction(RunObject),
    /// Occurs when a [run](https://platform.openai.com/docs/api-reference/runs/object) is completed.
    #[serde(rename = "thread.run.completed")]
    ThreadRunCompleted(RunObject),
    /// Occurs when a [run](https://platform.openai.com/docs/api-reference/runs/object) ends with status `incomplete`.
    #[serde(rename = "thread.run.incomplete")]
    ThreadRunIncomplete(RunObject),
    /// Occurs when a [run](https://platform.openai.com/docs/api-reference/runs/object) fails.
    #[serde(rename = "thread.run.failed")]
    ThreadRunFailed(RunObject),
    /// Occurs when a [run](https://platform.openai.com/docs/api-reference/runs/object) moves to a `cancelling` status.
    #[serde(rename = "thread.run.cancelling")]
    ThreadRunCancelling(RunObject),
    /// Occurs when a [run](https://platform.openai.com/docs/api-reference/runs/object) is cancelled.
    #[serde(rename = "thread.run.cancelled")]
    ThreadRunCancelled(RunObject),
    /// Occurs when a [run](https://platform.openai.com/docs/api-reference/runs/object) expires.
    #[serde(rename = "thread.run.expired")]
    ThreadRunExpired(RunObject),
    /// Occurs when a [run step](https://platform.openai.com/docs/api-reference/run-steps/step-object) is created.
    #[serde(rename = "thread.run.step.created")]
    ThreadRunStepCreated(RunStepObject),
    /// Occurs when a [run step](https://platform.openai.com/docs/api-reference/run-steps/step-object) moves to an `in_progress` state.
    #[serde(rename = "thread.run.step.in_progress")]
    ThreadRunStepInProgress(RunStepObject),
    /// Occurs when parts of a [run step](https://platform.openai.com/docs/api-reference/run-steps/step-object) are being streamed.
    #[serde(rename = "thread.run.step.delta")]
    ThreadRunStepDelta(RunStepDeltaObject),
    ///  Occurs when a [run step](https://platform.openai.com/docs/api-reference/run-steps/step-object) is completed.
    #[serde(rename = "thread.run.step.completed")]
    ThreadRunStepCompleted(RunStepObject),
    /// Occurs when a [run step](https://platform.openai.com/docs/api-reference/run-steps/step-object) fails.
    #[serde(rename = "thread.run.step.failed")]
    ThreadRunStepFailed(RunStepObject),
    /// Occurs when a [run step](https://platform.openai.com/docs/api-reference/run-steps/step-object) is cancelled.
    #[serde(rename = "thread.run.step.cancelled")]
    ThreadRunStepCancelled(RunStepObject),
    /// Occurs when a [run step](https://platform.openai.com/docs/api-reference/run-steps/step-object) expires.
    #[serde(rename = "thread.run.step.expired")]
    ThreadRunStepExpired(RunStepObject),
    /// Occurs when a [message](https://platform.openai.com/docs/api-reference/messages/object) is created.
    #[serde(rename = "thread.message.created")]
    ThreadMessageCreated(MessageObject),
    /// Occurs when a [message](https://platform.openai.com/docs/api-reference/messages/object) moves to an `in_progress` state.
    #[serde(rename = "thread.message.in_progress")]
    ThreadMessageInProgress(MessageObject),
    /// Occurs when parts of a [Message](https://platform.openai.com/docs/api-reference/messages/object) are being streamed.
    #[serde(rename = "thread.message.delta")]
    ThreadMessageDelta(MessageDeltaObject),
    /// Occurs when a [message](https://platform.openai.com/docs/api-reference/messages/object) is completed.
    #[serde(rename = "thread.message.completed")]
    ThreadMessageCompleted(MessageObject),
    /// Occurs when a [message](https://platform.openai.com/docs/api-reference/messages/object) ends before it is completed.
    #[serde(rename = "thread.message.incomplete")]
    ThreadMessageIncomplete(MessageObject),
    /// Occurs when an [error](https://platform.openai.com/docs/guides/error-codes/api-errors) occurs. This can happen due to an internal server error or a timeout.
    #[serde(rename = "error")]
    ErrorEvent(ApiError),
    /// Occurs when a stream ends.
    #[serde(rename = "done")]
    Done(String),
    /// Unknown event type.
    #[serde(rename = "unknown")]
    Unknown {
        event: String,
        data: String,
        id: String,
        retry: Option<Duration>,
    },
}

pub type AssistantEventStream =
    Pin<Box<dyn Stream<Item = Result<AssistantStreamEvent, OpenAIError>> + Send>>;

impl TryFrom<eventsource_stream::Event> for AssistantStreamEvent {
    type Error = OpenAIError;

    fn try_from(value: eventsource_stream::Event) -> Result<Self, Self::Error> {
        match value.event.as_str() {
            "thread.created" => serde_json::from_str::<ThreadObject>(value.data.as_str())
                .map_err(|e| map_deserialization_error(e, value.data.as_bytes()))
                .map(AssistantStreamEvent::TreadCreated),
            "thread.run.created" => serde_json::from_str::<RunObject>(value.data.as_str())
                .map_err(|e| map_deserialization_error(e, value.data.as_bytes()))
                .map(AssistantStreamEvent::ThreadRunCreated),
            "thread.run.queued" => serde_json::from_str::<RunObject>(value.data.as_str())
                .map_err(|e| map_deserialization_error(e, value.data.as_bytes()))
                .map(AssistantStreamEvent::ThreadRunQueued),
            "thread.run.in_progress" => serde_json::from_str::<RunObject>(value.data.as_str())
                .map_err(|e| map_deserialization_error(e, value.data.as_bytes()))
                .map(AssistantStreamEvent::ThreadRunInProgress),
            "thread.run.requires_action" => serde_json::from_str::<RunObject>(value.data.as_str())
                .map_err(|e| map_deserialization_error(e, value.data.as_bytes()))
                .map(AssistantStreamEvent::ThreadRunRequiresAction),
            "thread.run.completed" => serde_json::from_str::<RunObject>(value.data.as_str())
                .map_err(|e| map_deserialization_error(e, value.data.as_bytes()))
                .map(AssistantStreamEvent::ThreadRunCompleted),
            "thread.run.incomplete" => serde_json::from_str::<RunObject>(value.data.as_str())
                .map_err(|e| map_deserialization_error(e, value.data.as_bytes()))
                .map(AssistantStreamEvent::ThreadRunIncomplete),
            "thread.run.failed" => serde_json::from_str::<RunObject>(value.data.as_str())
                .map_err(|e| map_deserialization_error(e, value.data.as_bytes()))
                .map(AssistantStreamEvent::ThreadRunFailed),
            "thread.run.cancelling" => serde_json::from_str::<RunObject>(value.data.as_str())
                .map_err(|e| map_deserialization_error(e, value.data.as_bytes()))
                .map(AssistantStreamEvent::ThreadRunCancelling),
            "thread.run.cancelled" => serde_json::from_str::<RunObject>(value.data.as_str())
                .map_err(|e| map_deserialization_error(e, value.data.as_bytes()))
                .map(AssistantStreamEvent::ThreadRunCancelled),
            "thread.run.expired" => serde_json::from_str::<RunObject>(value.data.as_str())
                .map_err(|e| map_deserialization_error(e, value.data.as_bytes()))
                .map(AssistantStreamEvent::ThreadRunExpired),
            "thread.run.step.created" => serde_json::from_str::<RunStepObject>(value.data.as_str())
                .map_err(|e| map_deserialization_error(e, value.data.as_bytes()))
                .map(AssistantStreamEvent::ThreadRunStepCreated),
            "thread.run.step.in_progress" => {
                serde_json::from_str::<RunStepObject>(value.data.as_str())
                    .map_err(|e| map_deserialization_error(e, value.data.as_bytes()))
                    .map(AssistantStreamEvent::ThreadRunStepInProgress)
            }
            "thread.run.step.delta" => {
                serde_json::from_str::<RunStepDeltaObject>(value.data.as_str())
                    .map_err(|e| map_deserialization_error(e, value.data.as_bytes()))
                    .map(AssistantStreamEvent::ThreadRunStepDelta)
            }
            "thread.run.step.completed" => {
                serde_json::from_str::<RunStepObject>(value.data.as_str())
                    .map_err(|e| map_deserialization_error(e, value.data.as_bytes()))
                    .map(AssistantStreamEvent::ThreadRunStepCompleted)
            }
            "thread.run.step.failed" => serde_json::from_str::<RunStepObject>(value.data.as_str())
                .map_err(|e| map_deserialization_error(e, value.data.as_bytes()))
                .map(AssistantStreamEvent::ThreadRunStepFailed),
            "thread.run.step.cancelled" => {
                serde_json::from_str::<RunStepObject>(value.data.as_str())
                    .map_err(|e| map_deserialization_error(e, value.data.as_bytes()))
                    .map(AssistantStreamEvent::ThreadRunStepCancelled)
            }
            "thread.run.step.expired" => serde_json::from_str::<RunStepObject>(value.data.as_str())
                .map_err(|e| map_deserialization_error(e, value.data.as_bytes()))
                .map(AssistantStreamEvent::ThreadRunStepExpired),
            "thread.message.created" => serde_json::from_str::<MessageObject>(value.data.as_str())
                .map_err(|e| map_deserialization_error(e, value.data.as_bytes()))
                .map(AssistantStreamEvent::ThreadMessageCreated),
            "thread.message.in_progress" => {
                serde_json::from_str::<MessageObject>(value.data.as_str())
                    .map_err(|e| map_deserialization_error(e, value.data.as_bytes()))
                    .map(AssistantStreamEvent::ThreadMessageInProgress)
            }
            "thread.message.delta" => {
                serde_json::from_str::<MessageDeltaObject>(value.data.as_str())
                    .map_err(|e| map_deserialization_error(e, value.data.as_bytes()))
                    .map(AssistantStreamEvent::ThreadMessageDelta)
            }
            "thread.message.completed" => {
                serde_json::from_str::<MessageObject>(value.data.as_str())
                    .map_err(|e| map_deserialization_error(e, value.data.as_bytes()))
                    .map(AssistantStreamEvent::ThreadMessageCompleted)
            }
            "thread.message.incomplete" => {
                serde_json::from_str::<MessageObject>(value.data.as_str())
                    .map_err(|e| map_deserialization_error(e, value.data.as_bytes()))
                    .map(AssistantStreamEvent::ThreadMessageIncomplete)
            }
            "error" => serde_json::from_str::<ApiError>(value.data.as_str())
                .map_err(|e| map_deserialization_error(e, value.data.as_bytes()))
                .map(AssistantStreamEvent::ErrorEvent),
            "done" => Ok(AssistantStreamEvent::Done(value.data)),
            _ => {
                let eventsource_stream::Event {
                    id,
                    event,
                    data,
                    retry,
                } = value;
                Ok(AssistantStreamEvent::Unknown {
                    event,
                    data,
                    id,
                    retry,
                })
            }
        }
    }
}
