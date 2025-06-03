use std::cell::RefCell;
use std::collections::HashMap;
use std::fmt::Write as _;
use std::mem;
use std::ops::ControlFlow;
use std::sync::RwLock;
use std::time::Duration;

use actix_web::web::{self, Data};
use actix_web::{Either, HttpRequest, HttpResponse, Responder};
use actix_web_lab::sse::{self, Event, Sse};
use async_openai::config::{Config, OpenAIConfig};
use async_openai::error::{ApiError, OpenAIError};
use async_openai::reqwest_eventsource::Error as EventSourceError;
use async_openai::types::{
    ChatChoiceStream, ChatCompletionMessageToolCall, ChatCompletionMessageToolCallChunk,
    ChatCompletionRequestAssistantMessage, ChatCompletionRequestAssistantMessageArgs,
    ChatCompletionRequestMessage, ChatCompletionRequestSystemMessage,
    ChatCompletionRequestSystemMessageContent, ChatCompletionRequestToolMessage,
    ChatCompletionRequestToolMessageContent, ChatCompletionStreamResponseDelta,
    ChatCompletionToolArgs, ChatCompletionToolType, CreateChatCompletionRequest,
    CreateChatCompletionStreamResponse, FinishReason, FunctionCall, FunctionCallStream,
    FunctionObjectArgs, Role,
};
use async_openai::Client;
use bumpalo::Bump;
use futures::StreamExt;
use index_scheduler::IndexScheduler;
use meilisearch_auth::AuthController;
use meilisearch_types::error::{Code, ResponseError};
use meilisearch_types::features::{
    ChatCompletionPrompts as DbChatCompletionPrompts, ChatCompletionSettings as DbChatSettings,
};
use meilisearch_types::heed::RoTxn;
use meilisearch_types::keys::actions;
use meilisearch_types::milli::index::ChatConfig;
use meilisearch_types::milli::prompt::{Prompt, PromptData};
use meilisearch_types::milli::update::new::document::DocumentFromDb;
use meilisearch_types::milli::{
    all_obkv_to_json, obkv_to_json, DocumentId, FieldIdMapWithMetadata, GlobalFieldsIdsMap,
    MetadataBuilder, TimeBudget,
};
use meilisearch_types::{Document, Index};
use serde::{Deserialize, Serialize};
use serde_json::json;
use tokio::runtime::Handle;
use tokio::sync::mpsc::error::SendError;
use tokio::sync::mpsc::Sender;
use uuid::Uuid;

use super::ChatsParam;
use crate::error::MeilisearchHttpError;
use crate::extractors::authentication::policies::ActionPolicy;
use crate::extractors::authentication::{extract_token_from_request, GuardedData, Policy as _};
use crate::metrics::MEILISEARCH_DEGRADED_SEARCH_REQUESTS;
use crate::routes::indexes::search::search_kind;
use crate::search::{add_search_rules, prepare_search, search_from_kind, SearchQuery};
use crate::search_queue::SearchQueue;

const MEILI_SEARCH_PROGRESS_NAME: &str = "_meiliSearchProgress";
const MEILI_APPEND_CONVERSATION_MESSAGE_NAME: &str = "_meiliAppendConversationMessage";
const MEILI_SEARCH_SOURCES_NAME: &str = "_meiliSearchSources";
const MEILI_REPORT_ERRORS_NAME: &str = "_meiliReportErrors";
const MEILI_SEARCH_IN_INDEX_FUNCTION_NAME: &str = "_meiliSearchInIndex";

pub fn configure(cfg: &mut web::ServiceConfig) {
    cfg.service(web::resource("").route(web::post().to(chat)));
}

/// Get a chat completion
async fn chat(
    index_scheduler: GuardedData<ActionPolicy<{ actions::CHAT }>, Data<IndexScheduler>>,
    auth_ctrl: web::Data<AuthController>,
    chats_param: web::Path<ChatsParam>,
    req: HttpRequest,
    search_queue: web::Data<SearchQueue>,
    web::Json(chat_completion): web::Json<CreateChatCompletionRequest>,
) -> impl Responder {
    let ChatsParam { workspace_uid } = chats_param.into_inner();

    assert_eq!(
        chat_completion.n.unwrap_or(1),
        1,
        "Meilisearch /chat only support one completion at a time (n = 1, n = null)"
    );

    if chat_completion.stream.unwrap_or(false) {
        Either::Right(
            streamed_chat(
                index_scheduler,
                auth_ctrl,
                search_queue,
                &workspace_uid,
                req,
                chat_completion,
            )
            .await,
        )
    } else {
        Either::Left(
            non_streamed_chat(
                index_scheduler,
                auth_ctrl,
                search_queue,
                &workspace_uid,
                req,
                chat_completion,
            )
            .await,
        )
    }
}

#[derive(Default, Debug, Clone, Copy)]
pub struct FunctionSupport {
    /// Defines if we can call the _meiliSearchProgress function
    /// to inform the front-end about what we are searching for.
    report_progress: bool,
    /// Defines if we can call the _meiliSearchSources function
    /// to inform the front-end about the sources of the search.
    report_sources: bool,
    /// Defines if we can call the _meiliAppendConversationMessage
    /// function to provide the messages to append into the conversation.
    append_to_conversation: bool,
}

/// Setup search tool in chat completion request
fn setup_search_tool(
    index_scheduler: &Data<IndexScheduler>,
    filters: &meilisearch_auth::AuthFilter,
    chat_completion: &mut CreateChatCompletionRequest,
    prompts: &DbChatCompletionPrompts,
) -> Result<FunctionSupport, ResponseError> {
    let tools = chat_completion.tools.get_or_insert_default();
    if tools.iter().find(|t| t.function.name == MEILI_SEARCH_IN_INDEX_FUNCTION_NAME).is_some() {
        panic!("{MEILI_SEARCH_IN_INDEX_FUNCTION_NAME} function already set");
    }

    // Remove internal tools used for front-end notifications as they should be hidden from the LLM.
    let mut report_progress = false;
    let mut report_sources = false;
    let mut append_to_conversation = false;
    let mut report_errors = false;
    tools.retain(|tool| {
        match tool.function.name.as_str() {
            MEILI_SEARCH_PROGRESS_NAME => {
                report_progress = true;
                false
            }
            MEILI_SEARCH_SOURCES_NAME => {
                report_sources = true;
                false
            }
            MEILI_APPEND_CONVERSATION_MESSAGE_NAME => {
                append_to_conversation = true;
                false
            }
            MEILI_REPORT_ERRORS_NAME => {
                report_errors = true;
                false
            }
            _ => true, // keep other tools
        }
    });

    let mut index_uids = Vec::new();
    let mut function_description = prompts.search_description.clone();
    index_scheduler.try_for_each_index::<_, ()>(|name, index| {
        // Make sure to skip unauthorized indexes
        if !filters.is_index_authorized(&name) {
            return Ok(());
        }

        let rtxn = index.read_txn()?;
        let chat_config = index.chat_config(&rtxn)?;
        let index_description = chat_config.description;
        let _ = writeln!(&mut function_description, "\n\n - {name}: {index_description}\n");
        index_uids.push(name.to_string());

        Ok(())
    })?;

    let tool = ChatCompletionToolArgs::default()
        .r#type(ChatCompletionToolType::Function)
        .function(
            FunctionObjectArgs::default()
                .name(MEILI_SEARCH_IN_INDEX_FUNCTION_NAME)
                .description(&function_description)
                .parameters(json!({
                    "type": "object",
                    "properties": {
                        "index_uid": {
                            "type": "string",
                            "enum": index_uids,
                            "description": prompts.search_index_uid_param,
                        },
                        "q": {
                            // Unfortunately, Mistral does not support an array of types, here.
                            // "type": ["string", "null"],
                            "type": "string",
                            "description": prompts.search_q_param,
                        }
                    },
                    "required": ["index_uid", "q"],
                    "additionalProperties": false,
                }))
                .strict(true)
                .build()
                .unwrap(),
        )
        .build()
        .unwrap();

    tools.push(tool);

    chat_completion.messages.insert(
        0,
        ChatCompletionRequestMessage::System(ChatCompletionRequestSystemMessage {
            content: ChatCompletionRequestSystemMessageContent::Text(prompts.system.clone()),
            name: None,
        }),
    );

    Ok(FunctionSupport { report_progress, report_sources, append_to_conversation })
}

/// Process search request and return formatted results
async fn process_search_request(
    index_scheduler: &GuardedData<ActionPolicy<{ actions::CHAT }>, Data<IndexScheduler>>,
    auth_ctrl: web::Data<AuthController>,
    search_queue: &web::Data<SearchQueue>,
    auth_token: &str,
    index_uid: String,
    q: Option<String>,
) -> Result<(Index, Vec<Document>, String), ResponseError> {
    // TBD
    // let mut aggregate = SearchAggregator::<SearchPOST>::from_query(&query);

    let index = index_scheduler.index(&index_uid)?;
    let rtxn = index.static_read_txn()?;
    let ChatConfig { description: _, prompt: _, search_parameters } = index.chat_config(&rtxn)?;
    let mut query = SearchQuery { q, ..SearchQuery::from(search_parameters) };
    let auth_filter = ActionPolicy::<{ actions::SEARCH }>::authenticate(
        auth_ctrl,
        auth_token,
        Some(index_uid.as_str()),
    )?;

    // Tenant token search_rules.
    if let Some(search_rules) = auth_filter.get_index_search_rules(&index_uid) {
        add_search_rules(&mut query.filter, search_rules);
    }
    let search_kind =
        search_kind(&query, index_scheduler.get_ref(), index_uid.to_string(), &index)?;

    let permit = search_queue.try_get_search_permit().await?;
    let features = index_scheduler.features();
    let index_cloned = index.clone();
    let output = tokio::task::spawn_blocking(move || -> Result<_, ResponseError> {
        let time_budget = match index_cloned
            .search_cutoff(&rtxn)
            .map_err(|e| MeilisearchHttpError::from_milli(e, Some(index_uid.clone())))?
        {
            Some(cutoff) => TimeBudget::new(Duration::from_millis(cutoff)),
            None => TimeBudget::default(),
        };

        let (search, _is_finite_pagination, _max_total_hits, _offset) =
            prepare_search(&index_cloned, &rtxn, &query, &search_kind, time_budget, features)?;

        search_from_kind(index_uid, search_kind, search)
            .map(|(search_results, _)| (rtxn, search_results))
            .map_err(ResponseError::from)
    })
    .await;
    permit.drop().await;

    let output = output?;
    let mut documents = Vec::new();
    if let Ok((ref rtxn, ref search_result)) = output {
        // aggregate.succeed(search_result);
        if search_result.degraded {
            MEILISEARCH_DEGRADED_SEARCH_REQUESTS.inc();
        }

        let fields_ids_map = index.fields_ids_map(rtxn)?;
        let displayed_fields = index.displayed_fields_ids(rtxn)?;
        for &document_id in &search_result.documents_ids {
            let obkv = index.document(rtxn, document_id)?;
            let document = match displayed_fields {
                Some(ref fields) => obkv_to_json(fields, &fields_ids_map, obkv)?,
                None => all_obkv_to_json(obkv, &fields_ids_map)?,
            };
            documents.push(document);
        }
    }
    // analytics.publish(aggregate, &req);

    let (rtxn, search_result) = output?;
    let render_alloc = Bump::new();
    let formatted = format_documents(&rtxn, &index, &render_alloc, search_result.documents_ids)?;
    let text = formatted.join("\n");
    drop(rtxn);

    Ok((index, documents, text))
}

async fn non_streamed_chat(
    index_scheduler: GuardedData<ActionPolicy<{ actions::CHAT }>, Data<IndexScheduler>>,
    auth_ctrl: web::Data<AuthController>,
    search_queue: web::Data<SearchQueue>,
    workspace_uid: &str,
    req: HttpRequest,
    mut chat_completion: CreateChatCompletionRequest,
) -> Result<HttpResponse, ResponseError> {
    index_scheduler.features().check_chat_completions("Using the /chats chat completions route")?;
    let filters = index_scheduler.filters();

    let rtxn = index_scheduler.read_txn()?;
    let chat_settings = match index_scheduler.chat_settings(&rtxn, workspace_uid).unwrap() {
        Some(settings) => settings,
        None => {
            return Err(ResponseError::from_msg(
                format!("Chat `{workspace_uid}` not found"),
                Code::ChatWorkspaceNotFound,
            ))
        }
    };

    let mut config = OpenAIConfig::default();
    if let Some(api_key) = chat_settings.api_key.as_ref() {
        config = config.with_api_key(api_key);
    }
    if let Some(base_api) = chat_settings.base_api.as_ref() {
        config = config.with_api_base(base_api);
    }
    let client = Client::with_config(config);

    let auth_token = extract_token_from_request(&req)?.unwrap();
    // TODO do function support later
    let _function_support =
        setup_search_tool(&index_scheduler, filters, &mut chat_completion, &chat_settings.prompts)?;

    let mut response;
    loop {
        response = client.chat().create(chat_completion.clone()).await.unwrap();

        let choice = &mut response.choices[0];
        match choice.finish_reason {
            Some(FinishReason::ToolCalls) => {
                let tool_calls = mem::take(&mut choice.message.tool_calls).unwrap_or_default();

                let (meili_calls, other_calls): (Vec<_>, Vec<_>) = tool_calls
                    .into_iter()
                    .partition(|call| call.function.name == MEILI_SEARCH_IN_INDEX_FUNCTION_NAME);

                chat_completion.messages.push(
                    ChatCompletionRequestAssistantMessageArgs::default()
                        .tool_calls(meili_calls.clone())
                        .build()
                        .unwrap()
                        .into(),
                );

                for call in meili_calls {
                    let result = match serde_json::from_str(&call.function.arguments) {
                        Ok(SearchInIndexParameters { index_uid, q }) => process_search_request(
                            &index_scheduler,
                            auth_ctrl.clone(),
                            &search_queue,
                            &auth_token,
                            index_uid,
                            q,
                        )
                        .await
                        .map_err(|e| e.to_string()),
                        Err(err) => Err(err.to_string()),
                    };

                    // TODO report documents sources later
                    let text = match result {
                        Ok((_, _documents, text)) => text,
                        Err(err) => err,
                    };

                    let answer = format!("{}\n\n{text}", chat_settings.prompts.pre_query);
                    chat_completion.messages.push(ChatCompletionRequestMessage::Tool(
                        ChatCompletionRequestToolMessage {
                            tool_call_id: call.id.clone(),
                            content: ChatCompletionRequestToolMessageContent::Text(answer),
                        },
                    ));
                }

                // Let the client call other tools by themselves
                if !other_calls.is_empty() {
                    response.choices[0].message.tool_calls = Some(other_calls);
                    break;
                }
            }
            _ => break,
        }
    }

    Ok(HttpResponse::Ok().json(response))
}

async fn streamed_chat(
    index_scheduler: GuardedData<ActionPolicy<{ actions::CHAT }>, Data<IndexScheduler>>,
    auth_ctrl: web::Data<AuthController>,
    search_queue: web::Data<SearchQueue>,
    workspace_uid: &str,
    req: HttpRequest,
    mut chat_completion: CreateChatCompletionRequest,
) -> Result<impl Responder, ResponseError> {
    index_scheduler.features().check_chat_completions("Using the /chats chat completions route")?;
    let filters = index_scheduler.filters();

    let rtxn = index_scheduler.read_txn()?;
    let chat_settings = match index_scheduler.chat_settings(&rtxn, workspace_uid)? {
        Some(settings) => settings,
        None => {
            return Err(ResponseError::from_msg(
                format!("Chat `{workspace_uid}` not found"),
                Code::ChatWorkspaceNotFound,
            ))
        }
    };
    drop(rtxn);

    let mut config = OpenAIConfig::default();
    if let Some(api_key) = chat_settings.api_key.as_ref() {
        config = config.with_api_key(api_key);
    }
    if let Some(base_api) = chat_settings.base_api.as_ref() {
        config = config.with_api_base(base_api);
    }

    let auth_token = extract_token_from_request(&req)?.unwrap().to_string();
    let function_support =
        setup_search_tool(&index_scheduler, filters, &mut chat_completion, &chat_settings.prompts)?;

    tracing::debug!("Conversation function support: {function_support:?}");

    let (tx, rx) = tokio::sync::mpsc::channel(10);
    let tx = SseEventSender(tx);
    let _join_handle = Handle::current().spawn(async move {
        let client = Client::with_config(config.clone());
        let mut global_tool_calls = HashMap::<u32, Call>::new();

        // Limit the number of internal calls to satisfy the search requests of the LLM
        for _ in 0..20 {
            let output = run_conversation(
                &index_scheduler,
                &auth_ctrl,
                &search_queue,
                &auth_token,
                &client,
                &chat_settings,
                &mut chat_completion,
                &tx,
                &mut global_tool_calls,
                function_support,
            );

            match output.await {
                Ok(ControlFlow::Continue(())) => (),
                Ok(ControlFlow::Break(_finish_reason)) => break,
                // If the connection is closed we must stop
                Err(SendError(_)) => return,
            }
        }

        let _ = tx.stop().await;
    });

    Ok(Sse::from_infallible_receiver(rx).with_retry_duration(Duration::from_secs(10)))
}

/// Updates the chat completion with the new messages, streams the LLM tokens,
/// and report progress and errors.
async fn run_conversation<C: Config>(
    index_scheduler: &GuardedData<ActionPolicy<{ actions::CHAT }>, Data<IndexScheduler>>,
    auth_ctrl: &web::Data<AuthController>,
    search_queue: &web::Data<SearchQueue>,
    auth_token: &str,
    client: &Client<C>,
    chat_settings: &DbChatSettings,
    chat_completion: &mut CreateChatCompletionRequest,
    tx: &SseEventSender,
    global_tool_calls: &mut HashMap<u32, Call>,
    function_support: FunctionSupport,
) -> Result<ControlFlow<Option<FinishReason>, ()>, SendError<Event>> {
    let mut finish_reason = None;
    // safety: The unwrap can only happen if the stream is not correctly configured.
    let mut response = client.chat().create_stream(chat_completion.clone()).await.unwrap();
    while let Some(result) = response.next().await {
        match result {
            Ok(resp) => {
                let choice = &resp.choices[0];
                finish_reason = choice.finish_reason;

                let ChatCompletionStreamResponseDelta { ref tool_calls, .. } = &choice.delta;

                match tool_calls {
                    Some(tool_calls) => {
                        for chunk in tool_calls {
                            let ChatCompletionMessageToolCallChunk {
                                index,
                                id,
                                r#type: _,
                                function,
                            } = chunk;
                            let FunctionCallStream { name, arguments } = function.as_ref().unwrap();

                            global_tool_calls
                                .entry(*index)
                                .and_modify(|call| {
                                    if call.is_internal() {
                                        call.append(arguments.as_ref().unwrap())
                                    }
                                })
                                .or_insert_with(|| {
                                    if name
                                        .as_ref()
                                        .map_or(false, |n| n == MEILI_SEARCH_IN_INDEX_FUNCTION_NAME)
                                    {
                                        Call::Internal {
                                            id: id.as_ref().unwrap().clone(),
                                            function_name: name.as_ref().unwrap().clone(),
                                            arguments: arguments.as_ref().unwrap().clone(),
                                        }
                                    } else {
                                        Call::External
                                    }
                                });

                            if global_tool_calls.get(index).map_or(false, Call::is_external) {
                                todo!("Support forwarding external tool calls");
                            }
                        }
                    }
                    None => {
                        if !global_tool_calls.is_empty() {
                            let (meili_calls, other_calls): (Vec<_>, Vec<_>) =
                                mem::take(global_tool_calls)
                                    .into_values()
                                    .flat_map(|call| match call {
                                        Call::Internal { id, function_name: name, arguments } => {
                                            Some(ChatCompletionMessageToolCall {
                                                id,
                                                r#type: Some(ChatCompletionToolType::Function),
                                                function: FunctionCall { name, arguments },
                                            })
                                        }
                                        Call::External => None,
                                    })
                                    .partition(|call| {
                                        call.function.name == MEILI_SEARCH_IN_INDEX_FUNCTION_NAME
                                    });

                            chat_completion.messages.push(
                                ChatCompletionRequestAssistantMessageArgs::default()
                                    .tool_calls(meili_calls.clone())
                                    .build()
                                    .unwrap()
                                    .into(),
                            );

                            assert!(
                                other_calls.is_empty(),
                                "We do not support external tool forwarding for now"
                            );

                            handle_meili_tools(
                                &index_scheduler,
                                &auth_ctrl,
                                &search_queue,
                                &auth_token,
                                chat_settings,
                                tx,
                                meili_calls,
                                chat_completion,
                                &resp,
                                function_support,
                            )
                            .await?;
                        } else {
                            tx.forward_response(&resp).await?;
                        }
                    }
                }
            }
            Err(error) => {
                let error = StreamErrorEvent::from_openai_error(error).await.unwrap();
                tx.send_error(&error).await?;
                return Ok(ControlFlow::Break(None));
            }
        }
    }

    // We must stop if the finish reason is not something we can solve with Meilisearch
    match finish_reason {
        Some(FinishReason::ToolCalls) => Ok(ControlFlow::Continue(())),
        otherwise => Ok(ControlFlow::Break(otherwise)),
    }
}

async fn handle_meili_tools(
    index_scheduler: &GuardedData<ActionPolicy<{ actions::CHAT }>, Data<IndexScheduler>>,
    auth_ctrl: &web::Data<AuthController>,
    search_queue: &web::Data<SearchQueue>,
    auth_token: &str,
    chat_settings: &DbChatSettings,
    tx: &SseEventSender,
    meili_calls: Vec<ChatCompletionMessageToolCall>,
    chat_completion: &mut CreateChatCompletionRequest,
    resp: &CreateChatCompletionStreamResponse,
    FunctionSupport { report_progress, report_sources, append_to_conversation, .. }: FunctionSupport,
) -> Result<(), SendError<Event>> {
    for call in meili_calls {
        if report_progress {
            tx.report_search_progress(
                resp.clone(),
                &call.id,
                &call.function.name,
                &call.function.arguments,
            )
            .await?;
        }

        if append_to_conversation {
            tx.append_tool_call_conversation_message(
                resp.clone(),
                call.id.clone(),
                call.function.name.clone(),
                call.function.arguments.clone(),
            )
            .await?;
        }

        let result = match serde_json::from_str(&call.function.arguments) {
            Ok(SearchInIndexParameters { index_uid, q }) => process_search_request(
                &index_scheduler,
                auth_ctrl.clone(),
                &search_queue,
                &auth_token,
                index_uid,
                q,
            )
            .await
            .map_err(|e| e.to_string()),
            Err(err) => Err(err.to_string()),
        };

        let text = match result {
            Ok((_index, documents, text)) => {
                if report_sources {
                    tx.report_sources(resp.clone(), &call.id, &documents).await?;
                }

                text
            }
            Err(err) => err,
        };

        let answer = format!("{}\n\n{text}", chat_settings.prompts.pre_query);
        let tool = ChatCompletionRequestMessage::Tool(ChatCompletionRequestToolMessage {
            tool_call_id: call.id.clone(),
            content: ChatCompletionRequestToolMessageContent::Text(answer),
        });

        if append_to_conversation {
            tx.append_conversation_message(resp.clone(), &tool).await?;
        }

        chat_completion.messages.push(tool);
    }

    Ok(())
}

pub struct SseEventSender(Sender<Event>);

impl SseEventSender {
    /// Ask the front-end user to append this tool *call* to the conversation
    pub async fn append_tool_call_conversation_message(
        &self,
        resp: CreateChatCompletionStreamResponse,
        call_id: String,
        function_name: String,
        function_arguments: String,
    ) -> Result<(), SendError<Event>> {
        #[allow(deprecated)]
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
            #[allow(deprecated)]
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
            #[allow(deprecated)]
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
            #[allow(deprecated)]
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
        self.0.send(Event::Data(sse::Data::new("[DONE]"))).await
    }

    async fn send_json<S: Serialize>(&self, data: &S) -> Result<(), SendError<Event>> {
        self.0.send(Event::Data(sse::Data::new_json(data).unwrap())).await
    }
}

/// The structure used to aggregate the function calls to make.
#[derive(Debug)]
enum Call {
    /// Tool calls to tools that must be managed by Meilisearch internally.
    /// Typically the search functions.
    Internal { id: String, function_name: String, arguments: String },
    /// Tool calls that we track but only to know that its not our functions.
    /// We return the function calls as-is to the end-user.
    External,
}

impl Call {
    fn is_internal(&self) -> bool {
        matches!(self, Call::Internal { .. })
    }

    fn is_external(&self) -> bool {
        matches!(self, Call::External { .. })
    }

    fn append(&mut self, more: &str) {
        match self {
            Call::Internal { arguments, .. } => arguments.push_str(more),
            Call::External { .. } => {
                panic!("Cannot append argument chunks to an external function")
            }
        }
    }
}

#[derive(Deserialize)]
struct SearchInIndexParameters {
    /// The index uid to search in.
    index_uid: String,
    /// The query parameter to use.
    q: Option<String>,
}

fn format_documents<'t, 'doc>(
    rtxn: &RoTxn<'t>,
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
            None => continue,
        };

        let text = prompt.render_document(&external_docid, document, &gfid_map, doc_alloc).unwrap();
        renders.push(text);
    }

    Ok(renders)
}

/// An error that occurs during the streaming process.
///
/// It directly comes from the OpenAI API and you can
/// read more about error events on their website:
/// <https://platform.openai.com/docs/api-reference/realtime-server-events/error>
#[derive(Debug, Serialize, Deserialize)]
pub struct StreamErrorEvent {
    /// The unique ID of the server event.
    event_id: String,
    /// The event type, must be error.
    r#type: String,
    /// Details of the error.
    error: StreamError,
}

/// Details of the error.
#[derive(Debug, Serialize, Deserialize)]
pub struct StreamError {
    /// The type of error (e.g., "invalid_request_error", "server_error").
    r#type: String,
    /// Error code, if any.
    code: Option<String>,
    /// A human-readable error message.
    message: String,
    /// Parameter related to the error, if any.
    param: Option<String>,
    /// The event_id of the client event that caused the error, if applicable.
    event_id: Option<String>,
}

impl StreamErrorEvent {
    pub async fn from_openai_error(error: OpenAIError) -> Result<Self, reqwest::Error> {
        let error_type = "error".to_string();
        match error {
            OpenAIError::Reqwest(e) => Ok(StreamErrorEvent {
                event_id: Uuid::new_v4().to_string(),
                r#type: error_type,
                error: StreamError {
                    r#type: "internal_reqwest_error".to_string(),
                    code: Some("internal".to_string()),
                    message: e.to_string(),
                    param: None,
                    event_id: None,
                },
            }),
            OpenAIError::ApiError(ApiError { message, r#type, param, code }) => {
                Ok(StreamErrorEvent {
                    r#type: error_type,
                    event_id: Uuid::new_v4().to_string(),
                    error: StreamError {
                        r#type: r#type.unwrap_or_else(|| "unknown".to_string()),
                        code,
                        message,
                        param,
                        event_id: None,
                    },
                })
            }
            OpenAIError::JSONDeserialize(error) => Ok(StreamErrorEvent {
                event_id: Uuid::new_v4().to_string(),
                r#type: error_type,
                error: StreamError {
                    r#type: "json_deserialize_error".to_string(),
                    code: Some("internal".to_string()),
                    message: error.to_string(),
                    param: None,
                    event_id: None,
                },
            }),
            OpenAIError::FileSaveError(_) | OpenAIError::FileReadError(_) => unreachable!(),
            OpenAIError::StreamError(error) => match error {
                EventSourceError::InvalidStatusCode(_status_code, response) => {
                    let OpenAiOutsideError {
                        error: OpenAiInnerError { code, message, param, r#type },
                    } = response.json().await?;

                    Ok(StreamErrorEvent {
                        event_id: Uuid::new_v4().to_string(),
                        r#type: error_type,
                        error: StreamError { r#type, code, message, param, event_id: None },
                    })
                }
                EventSourceError::InvalidContentType(_header_value, response) => {
                    let OpenAiOutsideError {
                        error: OpenAiInnerError { code, message, param, r#type },
                    } = response.json().await?;

                    Ok(StreamErrorEvent {
                        event_id: Uuid::new_v4().to_string(),
                        r#type: error_type,
                        error: StreamError { r#type, code, message, param, event_id: None },
                    })
                }
                EventSourceError::Utf8(error) => Ok(StreamErrorEvent {
                    event_id: Uuid::new_v4().to_string(),
                    r#type: error_type,
                    error: StreamError {
                        r#type: "invalid_utf8_error".to_string(),
                        code: None,
                        message: error.to_string(),
                        param: None,
                        event_id: None,
                    },
                }),
                EventSourceError::Parser(error) => Ok(StreamErrorEvent {
                    event_id: Uuid::new_v4().to_string(),
                    r#type: error_type,
                    error: StreamError {
                        r#type: "parser_error".to_string(),
                        code: None,
                        message: error.to_string(),
                        param: None,
                        event_id: None,
                    },
                }),
                EventSourceError::Transport(error) => Ok(StreamErrorEvent {
                    event_id: Uuid::new_v4().to_string(),
                    r#type: error_type,
                    error: StreamError {
                        r#type: "transport_error".to_string(),
                        code: None,
                        message: error.to_string(),
                        param: None,
                        event_id: None,
                    },
                }),
                EventSourceError::InvalidLastEventId(message) => Ok(StreamErrorEvent {
                    event_id: Uuid::new_v4().to_string(),
                    r#type: error_type,
                    error: StreamError {
                        r#type: "invalid_last_event_id".to_string(),
                        code: None,
                        message,
                        param: None,
                        event_id: None,
                    },
                }),
                EventSourceError::StreamEnded => Ok(StreamErrorEvent {
                    event_id: Uuid::new_v4().to_string(),
                    r#type: error_type,
                    error: StreamError {
                        r#type: "stream_ended".to_string(),
                        code: None,
                        message: "Stream ended".to_string(),
                        param: None,
                        event_id: None,
                    },
                }),
            },
            OpenAIError::InvalidArgument(message) => Ok(StreamErrorEvent {
                event_id: Uuid::new_v4().to_string(),
                r#type: error_type,
                error: StreamError {
                    r#type: "invalid_argument".to_string(),
                    code: None,
                    message,
                    param: None,
                    event_id: None,
                },
            }),
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct OpenAiOutsideError {
    /// Emitted when an error occurs.
    error: OpenAiInnerError,
}

/// Emitted when an error occurs.
#[derive(Debug, Clone, Deserialize)]
pub struct OpenAiInnerError {
    /// The error code.
    code: Option<String>,
    /// The error message.
    message: String,
    /// The error parameter.
    param: Option<String>,
    /// The type of the event. Always `error`.
    r#type: String,
}
