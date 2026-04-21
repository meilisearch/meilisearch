use serde::{Deserialize, Serialize};

use super::{
    content_part::ContentPart, conversation::Conversation, error::RealtimeAPIError, item::Item,
    rate_limit::RateLimit, response_resource::ResponseResource, session_resource::SessionResource,
};

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ErrorEvent {
    /// The unique ID of the server event.
    pub event_id: String,
    /// Details of the error.
    pub error: RealtimeAPIError,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct SessionCreatedEvent {
    /// The unique ID of the server event.
    pub event_id: String,
    /// The session resource.
    pub session: SessionResource,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct SessionUpdatedEvent {
    /// The unique ID of the server event.
    pub event_id: String,
    /// The updated session resource.
    pub session: SessionResource,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ConversationCreatedEvent {
    /// The unique ID of the server event.
    pub event_id: String,
    /// The conversation resource.
    pub conversation: Conversation,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct InputAudioBufferCommitedEvent {
    /// The unique ID of the server event.
    pub event_id: String,
    /// The ID of the preceding item after which the new item will be inserted.
    pub previous_item_id: String,
    /// The ID of the user message item that will be created.
    pub item_id: String,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct InputAudioBufferClearedEvent {
    /// The unique ID of the server event.
    pub event_id: String,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct InputAudioBufferSpeechStartedEvent {
    /// The unique ID of the server event.
    pub event_id: String,
    /// Milliseconds since the session started when speech was detected.
    pub audio_start_ms: u32,
    /// The ID of the user message item that will be created when speech stops.
    pub item_id: String,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct InputAudioBufferSpeechStoppedEvent {
    /// The unique ID of the server event.
    pub event_id: String,
    /// Milliseconds since the session started when speech stopped.
    pub audio_end_ms: u32,
    /// The ID of the user message item that will be created.
    pub item_id: String,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ConversationItemCreatedEvent {
    /// The unique ID of the server event.
    pub event_id: String,
    /// The ID of the preceding item.
    pub previous_item_id: Option<String>,
    /// The item that was created.
    pub item: Item,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ConversationItemInputAudioTranscriptionCompletedEvent {
    /// The unique ID of the server event.
    pub event_id: String,
    /// The ID of the user message item.
    pub item_id: String,
    /// The index of the content part containing the audio.
    pub content_index: u32,
    /// The transcribed text.
    pub transcript: String,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ConversationItemInputAudioTranscriptionFailedEvent {
    /// The unique ID of the server event.
    pub event_id: String,
    /// The ID of the user message item.
    pub item_id: String,
    /// The index of the content part containing the audio.
    pub content_index: u32,
    /// Details of the transcription error.
    pub error: RealtimeAPIError,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ConversationItemTruncatedEvent {
    /// The unique ID of the server event.
    pub event_id: String,
    /// The ID of the assistant message item that was truncated.
    pub item_id: String,
    /// The index of the content part that was truncated.
    pub content_index: u32,
    /// The duration up to which the audio was truncated, in milliseconds.
    pub audio_end_ms: u32,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ConversationItemDeletedEvent {
    /// The unique ID of the server event.
    pub event_id: String,
    /// The ID of the item that was deleted.
    pub item_id: String,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ResponseCreatedEvent {
    /// The unique ID of the server event.
    pub event_id: String,
    /// The response resource.
    pub response: ResponseResource,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ResponseDoneEvent {
    /// The unique ID of the server event.
    pub event_id: String,
    /// The response resource.
    pub response: ResponseResource,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ResponseOutputItemAddedEvent {
    /// The unique ID of the server event.
    pub event_id: String,
    /// The ID of the response to which the item belongs.
    pub response_id: String,
    /// The index of the output item in the response.
    pub output_index: u32,
    /// The item that was added.
    pub item: Item,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ResponseOutputItemDoneEvent {
    /// The unique ID of the server event.
    pub event_id: String,
    /// The ID of the response to which the item belongs.
    pub response_id: String,
    /// The index of the output item in the response.
    pub output_index: u32,
    /// The completed item.
    pub item: Item,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ResponseContentPartAddedEvent {
    /// The unique ID of the server event.
    pub event_id: String,
    /// The ID of the response.
    pub response_id: String,
    /// The ID of the item to which the content part was added.
    pub item_id: String,
    /// The index of the output item in the response.
    pub output_index: u32,
    /// The index of the content part in the item's content array.
    pub content_index: u32,
    /// The content part that was added.
    pub part: ContentPart,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ResponseContentPartDoneEvent {
    /// The unique ID of the server event.
    pub event_id: String,
    /// The ID of the response.
    pub response_id: String,
    /// The ID of the item to which the content part was added.
    pub item_id: String,
    /// The index of the output item in the response.
    pub output_index: u32,
    /// The index of the content part in the item's content array.
    pub content_index: u32,
    /// The content part that is done.
    pub part: ContentPart,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ResponseTextDeltaEvent {
    /// The unique ID of the server event.
    pub event_id: String,
    /// The ID of the response.
    pub response_id: String,
    /// The ID of the item.
    pub item_id: String,
    /// The index of the output item in the response.
    pub output_index: u32,
    /// The index of the content part in the item's content array.
    pub content_index: u32,
    /// The text delta.
    pub delta: String,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ResponseTextDoneEvent {
    /// The unique ID of the server event.
    pub event_id: String,
    /// The ID of the response.
    pub response_id: String,
    /// The ID of the item.
    pub item_id: String,
    /// The index of the output item in the response.
    pub output_index: u32,
    /// The index of the content part in the item's content array.
    pub content_index: u32,
    /// The final text content.
    pub text: String,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ResponseAudioTranscriptDeltaEvent {
    /// The unique ID of the server event.
    pub event_id: String,
    /// The ID of the response.
    pub response_id: String,
    /// The ID of the item.
    pub item_id: String,
    /// The index of the output item in the response.
    pub output_index: u32,
    /// The index of the content part in the item's content array.
    pub content_index: u32,
    /// The text delta.
    pub delta: String,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ResponseAudioTranscriptDoneEvent {
    /// The unique ID of the server event.
    pub event_id: String,
    /// The ID of the response.
    pub response_id: String,
    /// The ID of the item.
    pub item_id: String,
    /// The index of the output item in the response.
    pub output_index: u32,
    /// The index of the content part in the item's content array.
    pub content_index: u32,
    ///The final transcript of the audio.
    pub transcript: String,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ResponseAudioDeltaEvent {
    /// The unique ID of the server event.
    pub event_id: String,
    /// The ID of the response.
    pub response_id: String,
    /// The ID of the item.
    pub item_id: String,
    /// The index of the output item in the response.
    pub output_index: u32,
    /// The index of the content part in the item's content array.
    pub content_index: u32,
    /// Base64-encoded audio data delta.
    pub delta: String,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ResponseAudioDoneEvent {
    /// The unique ID of the server event.
    pub event_id: String,
    /// The ID of the response.
    pub response_id: String,
    /// The ID of the item.
    pub item_id: String,
    /// The index of the output item in the response.
    pub output_index: u32,
    /// The index of the content part in the item's content array.
    pub content_index: u32,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ResponseFunctionCallArgumentsDeltaEvent {
    /// The unique ID of the server event.
    pub event_id: String,
    /// The ID of the response.
    pub response_id: String,
    /// The ID of the function call item.
    pub item_id: String,
    /// The index of the output item in the response.
    pub output_index: u32,
    /// The ID of the function call.
    pub call_id: String,
    /// The arguments delta as a JSON string.
    pub delta: String,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ResponseFunctionCallArgumentsDoneEvent {
    /// The unique ID of the server event.
    pub event_id: String,
    /// The ID of the response.
    pub response_id: String,
    /// The ID of the function call item.
    pub item_id: String,
    /// The index of the output item in the response.
    pub output_index: u32,
    /// The ID of the function call.
    pub call_id: String,
    /// The final arguments as a JSON string.
    pub arguments: String,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct RateLimitsUpdatedEvent {
    /// The unique ID of the server event.
    pub event_id: String,
    pub rate_limits: Vec<RateLimit>,
}

/// These are events emitted from the OpenAI Realtime WebSocket server to the client.
#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(tag = "type")]
pub enum ServerEvent {
    /// Returned when an error occurs.
    #[serde(rename = "error")]
    Error(ErrorEvent),

    /// Returned when a session is created. Emitted automatically when a new connection is established.
    #[serde(rename = "session.created")]
    SessionCreated(SessionCreatedEvent),

    /// Returned when a session is updated.
    #[serde(rename = "session.updated")]
    SessionUpdated(SessionUpdatedEvent),

    /// Returned when a conversation is created. Emitted right after session creation.
    #[serde(rename = "conversation.created")]
    ConversationCreated(ConversationCreatedEvent),

    /// Returned when an input audio buffer is committed, either by the client or automatically in server VAD mode.
    #[serde(rename = "input_audio_buffer.committed")]
    InputAudioBufferCommited(InputAudioBufferCommitedEvent),

    /// Returned when the input audio buffer is cleared by the client.
    #[serde(rename = "input_audio_buffer.cleared")]
    InputAudioBufferCleared(InputAudioBufferClearedEvent),

    /// Returned in server turn detection mode when speech is detected.
    #[serde(rename = "input_audio_buffer.speech_started")]
    InputAudioBufferSpeechStarted(InputAudioBufferSpeechStartedEvent),

    /// Returned in server turn detection mode when speech stops.
    #[serde(rename = "input_audio_buffer.speech_stopped")]
    InputAudioBufferSpeechStopped(InputAudioBufferSpeechStoppedEvent),

    /// Returned when a conversation item is created.
    #[serde(rename = "conversation.item.created")]
    ConversationItemCreated(ConversationItemCreatedEvent),

    /// Returned when input audio transcription is enabled and a transcription succeeds.
    #[serde(rename = "conversation.item.input_audio_transcription.completed")]
    ConversationItemInputAudioTranscriptionCompleted(
        ConversationItemInputAudioTranscriptionCompletedEvent,
    ),

    /// Returned when input audio transcription is configured, and a transcription request for a user message failed.
    #[serde(rename = "conversation.item.input_audio_transcription.failed")]
    ConversationItemInputAudioTranscriptionFailed(
        ConversationItemInputAudioTranscriptionFailedEvent,
    ),

    /// Returned when an earlier assistant audio message item is truncated by the client.
    #[serde(rename = "conversation.item.truncated")]
    ConversationItemTruncated(ConversationItemTruncatedEvent),

    /// Returned when an item in the conversation is deleted.
    #[serde(rename = "conversation.item.deleted")]
    ConversationItemDeleted(ConversationItemDeletedEvent),

    /// Returned when a new Response is created. The first event of response creation, where the response is in an initial state of "in_progress".
    #[serde(rename = "response.created")]
    ResponseCreated(ResponseCreatedEvent),

    /// Returned when a Response is done streaming. Always emitted, no matter the final state.
    #[serde(rename = "response.done")]
    ResponseDone(ResponseDoneEvent),

    /// Returned when a new Item is created during response generation.
    #[serde(rename = "response.output_item.added")]
    ResponseOutputItemAdded(ResponseOutputItemAddedEvent),

    /// Returned when an Item is done streaming. Also emitted when a Response is interrupted, incomplete, or cancelled.
    #[serde(rename = "response.output_item.done")]
    ResponseOutputItemDone(ResponseOutputItemDoneEvent),

    /// Returned when a new content part is added to an assistant message item during response generation.
    #[serde(rename = "response.content_part.added")]
    ResponseContentPartAdded(ResponseContentPartAddedEvent),

    /// Returned when a content part is done streaming in an assistant message item.
    /// Also emitted when a Response is interrupted, incomplete, or cancelled.
    #[serde(rename = "response.content_part.done")]
    ResponseContentPartDone(ResponseContentPartDoneEvent),

    /// Returned when the text value of a "text" content part is updated.
    #[serde(rename = "response.text.delta")]
    ResponseTextDelta(ResponseTextDeltaEvent),

    /// Returned when the text value of a "text" content part is done streaming.
    /// Also emitted when a Response is interrupted, incomplete, or cancelled.
    #[serde(rename = "response.text.done")]
    ResponseTextDone(ResponseTextDoneEvent),

    /// Returned when the model-generated transcription of audio output is updated.
    #[serde(rename = "response.audio_transcript.delta")]
    ResponseAudioTranscriptDelta(ResponseAudioTranscriptDeltaEvent),

    /// Returned when the model-generated transcription of audio output is done streaming.
    /// Also emitted when a Response is interrupted, incomplete, or cancelled.
    #[serde(rename = "response.audio_transcript.done")]
    ResponseAudioTranscriptDone(ResponseAudioTranscriptDoneEvent),

    /// Returned when the model-generated audio is updated.
    #[serde(rename = "response.audio.delta")]
    ResponseAudioDelta(ResponseAudioDeltaEvent),

    /// Returned when the model-generated audio is done.
    /// Also emitted when a Response is interrupted, incomplete, or cancelled.
    #[serde(rename = "response.audio.done")]
    ResponseAudioDone(ResponseAudioDoneEvent),

    /// Returned when the model-generated function call arguments are updated.
    #[serde(rename = "response.function_call_arguments.delta")]
    ResponseFunctionCallArgumentsDelta(ResponseFunctionCallArgumentsDeltaEvent),

    /// Returned when the model-generated function call arguments are done streaming.
    /// Also emitted when a Response is interrupted, incomplete, or cancelled.
    #[serde(rename = "response.function_call_arguments.done")]
    ResponseFunctionCallArgumentsDone(ResponseFunctionCallArgumentsDoneEvent),

    /// Emitted after every "response.done" event to indicate the updated rate limits.
    #[serde(rename = "rate_limits.updated")]
    RateLimitsUpdated(RateLimitsUpdatedEvent),
}
