use std::cell::RefCell;
use std::sync::RwLock;

use actix_web_lab::sse::{self, Event};
use async_openai::types::{
    ChatChoiceStream, ChatCompletionMessageToolCall, ChatCompletionMessageToolCallChunk,
    ChatCompletionRequestAssistantMessage, ChatCompletionRequestMessage,
    ChatCompletionStreamResponseDelta, ChatCompletionToolType, CreateChatCompletionStreamResponse,
    FunctionCall, FunctionCallStream, Role,
};
use bumpalo::Bump;
use meilisearch_types::error::{Code, ResponseError};
use meilisearch_types::heed::RoTxn;
use meilisearch_types::milli::index::ChatConfig;
use meilisearch_types::milli::prompt::{Prompt, PromptData};
use meilisearch_types::milli::update::new::document::DocumentFromDb;
use meilisearch_types::milli::{
    DocumentId, FieldIdMapWithMetadata, GlobalFieldsIdsMap, MetadataBuilder,
};
use meilisearch_types::{Document, Index};
use serde::Serialize;
use tokio::sync::mpsc::error::SendError;
use tokio::sync::mpsc::Sender;

use super::errors::StreamErrorEvent;
use super::MEILI_APPEND_CONVERSATION_MESSAGE_NAME;
use crate::routes::chats::{MEILI_SEARCH_PROGRESS_NAME, MEILI_SEARCH_SOURCES_NAME};

pub struct SseEventSender(Sender<Event>);

impl SseEventSender {
    pub fn new(sender: Sender<Event>) -> Self {
        Self(sender)
    }

    /// Ask the front-end user to append this tool *call* to the conversation
    pub async fn append_tool_call_conversation_message(
        &self,
        resp: CreateChatCompletionStreamResponse,
        call_id: String,
        function_name: String,
        function_arguments: String,
    ) -> Result<(), SendError<Event>> {
        #[allow(deprecated)] // function_call
        let message =
            ChatCompletionRequestMessage::Assistant(ChatCompletionRequestAssistantMessage {
                content: None,
                refusal: None,
                name: None,
                audio: None,
                tool_calls: Some(vec![ChatCompletionMessageToolCall {
                    id: call_id,
                    r#type: Some(ChatCompletionToolType::Function),
                    function: FunctionCall { name: function_name, arguments: function_arguments },
                }]),
                function_call: None,
            });

        self.append_conversation_message(resp, &message).await
    }

    /// Ask the front-end user to append this tool to the conversation
    pub async fn append_conversation_message(
        &self,
        mut resp: CreateChatCompletionStreamResponse,
        message: &ChatCompletionRequestMessage,
    ) -> Result<(), SendError<Event>> {
        let call_text = serde_json::to_string(message).unwrap();
        let tool_call = ChatCompletionMessageToolCallChunk {
            index: 0,
            id: Some(uuid::Uuid::new_v4().to_string()),
            r#type: Some(ChatCompletionToolType::Function),
            function: Some(FunctionCallStream {
                name: Some(MEILI_APPEND_CONVERSATION_MESSAGE_NAME.to_string()),
                arguments: Some(call_text),
            }),
        };

        resp.choices[0] = ChatChoiceStream {
            index: 0,
            #[allow(deprecated)] // function_call
            delta: ChatCompletionStreamResponseDelta {
                content: None,
                function_call: None,
                tool_calls: Some(vec![tool_call]),
                role: Some(Role::Assistant),
                refusal: None,
            },
            finish_reason: None,
            logprobs: None,
        };

        self.send_json(&resp).await
    }

    pub async fn report_search_progress(
        &self,
        mut resp: CreateChatCompletionStreamResponse,
        call_id: &str,
        function_name: &str,
        function_arguments: &str,
    ) -> Result<(), SendError<Event>> {
        #[derive(Debug, Clone, Serialize)]
        /// Provides information about the current Meilisearch search operation.
        struct MeiliSearchProgress<'a> {
            /// The call ID to track the sources of the search.
            call_id: &'a str,
            /// The name of the function we are executing.
            function_name: &'a str,
            /// The arguments of the function we are executing, encoded in JSON.
            function_arguments: &'a str,
        }

        let progress = MeiliSearchProgress { call_id, function_name, function_arguments };
        let call_text = serde_json::to_string(&progress).unwrap();
        let tool_call = ChatCompletionMessageToolCallChunk {
            index: 0,
            id: Some(uuid::Uuid::new_v4().to_string()),
            r#type: Some(ChatCompletionToolType::Function),
            function: Some(FunctionCallStream {
                name: Some(MEILI_SEARCH_PROGRESS_NAME.to_string()),
                arguments: Some(call_text),
            }),
        };

        resp.choices[0] = ChatChoiceStream {
            index: 0,
            #[allow(deprecated)] // function_call
            delta: ChatCompletionStreamResponseDelta {
                content: None,
                function_call: None,
                tool_calls: Some(vec![tool_call]),
                role: Some(Role::Assistant),
                refusal: None,
            },
            finish_reason: None,
            logprobs: None,
        };

        self.send_json(&resp).await
    }

    pub async fn report_sources(
        &self,
        mut resp: CreateChatCompletionStreamResponse,
        call_id: &str,
        documents: &[Document],
    ) -> Result<(), SendError<Event>> {
        #[derive(Debug, Clone, Serialize)]
        /// Provides sources of the search.
        struct MeiliSearchSources<'a> {
            /// The call ID to track the original search associated to those sources.
            call_id: &'a str,
            /// The documents associated with the search (call_id).
            /// Only the displayed attributes of the documents are returned.
            sources: &'a [Document],
        }

        let sources = MeiliSearchSources { call_id, sources: documents };
        let call_text = serde_json::to_string(&sources).unwrap();
        let tool_call = ChatCompletionMessageToolCallChunk {
            index: 0,
            id: Some(uuid::Uuid::new_v4().to_string()),
            r#type: Some(ChatCompletionToolType::Function),
            function: Some(FunctionCallStream {
                name: Some(MEILI_SEARCH_SOURCES_NAME.to_string()),
                arguments: Some(call_text),
            }),
        };

        resp.choices[0] = ChatChoiceStream {
            index: 0,
            #[allow(deprecated)] // function_call
            delta: ChatCompletionStreamResponseDelta {
                content: None,
                function_call: None,
                tool_calls: Some(vec![tool_call]),
                role: Some(Role::Assistant),
                refusal: None,
            },
            finish_reason: None,
            logprobs: None,
        };

        self.send_json(&resp).await
    }

    pub async fn forward_response(
        &self,
        resp: &CreateChatCompletionStreamResponse,
    ) -> Result<(), SendError<Event>> {
        self.send_json(resp).await
    }

    pub async fn send_error(&self, error: &StreamErrorEvent) -> Result<(), SendError<Event>> {
        self.send_json(error).await
    }

    pub async fn stop(self) -> Result<(), SendError<Event>> {
        // It is the way OpenAI sends a correct end of stream
        // <https://platform.openai.com/docs/api-reference/assistants-streaming/events>
        const DONE_DATA: &str = "[DONE]";
        self.0.send(Event::Data(sse::Data::new(DONE_DATA))).await
    }

    async fn send_json<S: Serialize>(&self, data: &S) -> Result<(), SendError<Event>> {
        self.0.send(Event::Data(sse::Data::new_json(data).unwrap())).await
    }
}

/// Format documents based on the provided template and maximum bytes.
///
/// This formatting function is usually used to generate a summary of the documents for LLMs.
pub fn format_documents<'doc>(
    rtxn: &RoTxn<'_>,
    index: &Index,
    doc_alloc: &'doc Bump,
    internal_docids: Vec<DocumentId>,
) -> Result<Vec<&'doc str>, ResponseError> {
    let ChatConfig { prompt: PromptData { template, max_bytes }, .. } = index.chat_config(rtxn)?;

    let prompt = Prompt::new(template, max_bytes).unwrap();
    let fid_map = index.fields_ids_map(rtxn)?;
    let metadata_builder = MetadataBuilder::from_index(index, rtxn)?;
    let fid_map_with_meta = FieldIdMapWithMetadata::new(fid_map.clone(), metadata_builder);
    let global = RwLock::new(fid_map_with_meta);
    let gfid_map = RefCell::new(GlobalFieldsIdsMap::new(&global));

    let external_ids: Vec<String> = index
        .external_id_of(rtxn, internal_docids.iter().copied())?
        .into_iter()
        .collect::<Result<_, _>>()?;

    let mut renders = Vec::new();
    for (docid, external_docid) in internal_docids.into_iter().zip(external_ids) {
        let document = match DocumentFromDb::new(docid, rtxn, index, &fid_map)? {
            Some(doc) => doc,
            None => unreachable!("Document with internal ID {docid} not found"),
        };
        let text = match prompt.render_document(&external_docid, document, &gfid_map, doc_alloc) {
            Ok(text) => text,
            Err(err) => {
                return Err(ResponseError::from_msg(
                    err.to_string(),
                    Code::InvalidChatSettingDocumentTemplate,
                ))
            }
        };
        renders.push(text);
    }

    Ok(renders)
}
