use std::collections::HashMap;
use std::fmt::Write as _;
use std::mem;
use std::ops::ControlFlow;
use std::time::Duration;

use actix_web::web::{self, Data};
use actix_web::{Either, HttpRequest, HttpResponse, Responder};
use actix_web_lab::sse::{Event, Sse};
use async_openai::types::{
    ChatCompletionMessageToolCall, ChatCompletionMessageToolCallChunk,
    ChatCompletionRequestAssistantMessageArgs, ChatCompletionRequestDeveloperMessage,
    ChatCompletionRequestDeveloperMessageContent, ChatCompletionRequestMessage,
    ChatCompletionRequestSystemMessage, ChatCompletionRequestSystemMessageContent,
    ChatCompletionRequestToolMessage, ChatCompletionRequestToolMessageContent,
    ChatCompletionStreamOptions, ChatCompletionStreamResponseDelta, ChatCompletionToolArgs,
    ChatCompletionToolType, CreateChatCompletionRequest, CreateChatCompletionStreamResponse,
    FinishReason, FunctionCall, FunctionCallStream, FunctionObjectArgs,
};
use async_openai::Client;
use bumpalo::Bump;
use futures::StreamExt;
use index_scheduler::IndexScheduler;
use meilisearch_auth::AuthController;
use meilisearch_types::error::{Code, ResponseError};
use meilisearch_types::features::{
    ChatCompletionPrompts as DbChatCompletionPrompts,
    ChatCompletionSource as DbChatCompletionSource, SystemRole,
};
use meilisearch_types::heed::RoTxn;
use meilisearch_types::keys::actions;
use meilisearch_types::milli::index::ChatConfig;
use meilisearch_types::milli::{all_obkv_to_json, obkv_to_json, OrderBy, PatternMatch, TimeBudget};
use meilisearch_types::{Document, Index};
use serde::Deserialize;
use serde_json::json;
use tokio::runtime::Handle;
use tokio::sync::mpsc::error::SendError;

use super::chat_completion_analytics::ChatCompletionAggregator;
use super::config::Config;
use super::errors::{MistralError, OpenAiOutsideError, StreamErrorEvent};
use super::utils::format_documents;
use super::{
    ChatsParam, MEILI_APPEND_CONVERSATION_MESSAGE_NAME, MEILI_SEARCH_IN_INDEX_FUNCTION_NAME,
    MEILI_SEARCH_PROGRESS_NAME, MEILI_SEARCH_SOURCES_NAME,
};
use crate::analytics::Analytics;
use crate::error::MeilisearchHttpError;
use crate::extractors::authentication::policies::ActionPolicy;
use crate::extractors::authentication::{extract_token_from_request, GuardedData, Policy as _};
use crate::metrics::{
    MEILISEARCH_CHAT_COMPLETION_TOKENS_TOTAL, MEILISEARCH_CHAT_PROMPT_TOKENS_TOTAL,
    MEILISEARCH_CHAT_SEARCHES_TOTAL, MEILISEARCH_CHAT_TOKENS_TOTAL,
    MEILISEARCH_DEGRADED_SEARCH_REQUESTS,
};
use crate::routes::chats::utils::SseEventSender;
use crate::routes::indexes::search::search_kind;
use crate::search::{add_search_rules, prepare_search, search_from_kind, SearchQuery};
use crate::search_queue::SearchQueue;

pub fn configure(cfg: &mut web::ServiceConfig) {
    cfg.service(web::resource("").route(web::post().to(chat)));
}

/// Get a chat completion
async fn chat(
    index_scheduler: GuardedData<ActionPolicy<{ actions::CHAT_COMPLETIONS }>, Data<IndexScheduler>>,
    auth_ctrl: web::Data<AuthController>,
    chats_param: web::Path<ChatsParam>,
    req: HttpRequest,
    search_queue: web::Data<SearchQueue>,
    web::Json(chat_completion): web::Json<CreateChatCompletionRequest>,
    analytics: web::Data<Analytics>,
) -> impl Responder {
    let ChatsParam { workspace_uid } = chats_param.into_inner();

    if chat_completion.stream.unwrap_or(false) {
        Either::Right(
            streamed_chat(
                index_scheduler,
                auth_ctrl,
                search_queue,
                &workspace_uid,
                req,
                chat_completion,
                analytics,
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
                analytics,
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
    system_role: SystemRole,
) -> Result<FunctionSupport, ResponseError> {
    let tools = chat_completion.tools.get_or_insert_default();
    for tool in &tools[..] {
        match tool.function.name.as_str() {
            MEILI_SEARCH_IN_INDEX_FUNCTION_NAME => {
                return Err(ResponseError::from_msg(
                    format!("{MEILI_SEARCH_IN_INDEX_FUNCTION_NAME} function is already defined."),
                    Code::BadRequest,
                ));
            }
            MEILI_SEARCH_PROGRESS_NAME
            | MEILI_SEARCH_SOURCES_NAME
            | MEILI_APPEND_CONVERSATION_MESSAGE_NAME => (),
            external_function_name => {
                return Err(ResponseError::from_msg(
                    format!("{external_function_name}: External functions are not supported yet."),
                    Code::UnimplementedExternalFunctionCalling,
                ));
            }
        }
    }

    // Remove internal tools used for front-end notifications as they should be hidden from the LLM.
    let mut report_progress = false;
    let mut report_sources = false;
    let mut append_to_conversation = false;
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
            _ => true, // keep other tools
        }
    });

    let mut index_uids = Vec::new();
    let mut function_description = prompts.search_description.clone();
    let mut filter_description = prompts.search_filter_param.clone();
    index_scheduler.try_for_each_index::<_, ()>(|name, index| {
        // Make sure to skip unauthorized indexes
        if !filters.is_index_authorized(name) {
            return Ok(());
        }

        let rtxn = index.read_txn()?;
        let chat_config = index.chat_config(&rtxn)?;
        let index_description = chat_config.description;
        let _ = writeln!(&mut function_description, "\n\n - {name}: {index_description}\n");
        index_uids.push(name.to_string());
        let facet_distributions = format_facet_distributions(index, &rtxn, 10).unwrap(); // TODO do not unwrap
        let _ = writeln!(&mut filter_description, "\n## Facet distributions of the {name} index");
        let _ = writeln!(&mut filter_description, "{facet_distributions}");

        Ok(())
    })?;

    tracing::debug!("LLM function description: {function_description}");
    tracing::debug!("LLM filter description: {filter_description}");

    let tool = ChatCompletionToolArgs::default()
        .r#type(ChatCompletionToolType::Function)
        .function(
            FunctionObjectArgs::default()
                .name(MEILI_SEARCH_IN_INDEX_FUNCTION_NAME)
                .description(function_description)
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
                        },
                        "filter": {
                            "type": "string",
                            "description": filter_description,
                        }
                    },
                    "required": ["index_uid", "q", "filter"],
                    "additionalProperties": false,
                }))
                .strict(true)
                .build()
                .unwrap(),
        )
        .build()
        .unwrap();

    tools.push(tool);

    let system_message = match system_role {
        SystemRole::System => {
            ChatCompletionRequestMessage::System(ChatCompletionRequestSystemMessage {
                content: ChatCompletionRequestSystemMessageContent::Text(prompts.system.clone()),
                name: None,
            })
        }
        SystemRole::Developer => {
            ChatCompletionRequestMessage::Developer(ChatCompletionRequestDeveloperMessage {
                content: ChatCompletionRequestDeveloperMessageContent::Text(prompts.system.clone()),
                name: None,
            })
        }
    };
    chat_completion.messages.insert(0, system_message);

    Ok(FunctionSupport { report_progress, report_sources, append_to_conversation })
}

/// Process search request and return formatted results
async fn process_search_request(
    index_scheduler: &GuardedData<
        ActionPolicy<{ actions::CHAT_COMPLETIONS }>,
        Data<IndexScheduler>,
    >,
    auth_ctrl: web::Data<AuthController>,
    search_queue: &web::Data<SearchQueue>,
    auth_token: &str,
    index_uid: String,
    q: Option<String>,
    filter: Option<String>,
) -> Result<(Index, Vec<Document>, String), ResponseError> {
    let index = index_scheduler.index(&index_uid)?;
    let rtxn = index.static_read_txn()?;
    let ChatConfig { description: _, prompt: _, search_parameters } = index.chat_config(&rtxn)?;
    let mut query = SearchQuery {
        q,
        filter: filter.map(serde_json::Value::from),
        ..SearchQuery::from(search_parameters)
    };

    tracing::debug!("LLM query: {:?}", query);

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

        match search_from_kind(index_uid, search_kind, search) {
            Ok((search_results, _)) => Ok((rtxn, Ok(search_results))),
            Err(MeilisearchHttpError::Milli {
                error: meilisearch_types::milli::Error::UserError(user_error),
                index_name: _,
            }) => Ok((rtxn, Err(user_error))),
            Err(err) => Err(ResponseError::from(err)),
        }
    })
    .await;
    permit.drop().await;

    let output = match output? {
        Ok((rtxn, Ok(search_results))) => Ok((rtxn, search_results)),
        Ok((_rtxn, Err(error))) => return Ok((index, Vec::new(), error.to_string())),
        Err(err) => Err(err),
    };
    let mut documents = Vec::new();
    if let Ok((ref rtxn, ref search_result)) = output {
        MEILISEARCH_CHAT_SEARCHES_TOTAL.with_label_values(&["internal"]).inc();
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

    let (rtxn, search_result) = output?;
    let render_alloc = Bump::new();
    let formatted = format_documents(&rtxn, &index, &render_alloc, search_result.documents_ids)?;
    let text = formatted.join("\n");
    drop(rtxn);

    Ok((index, documents, text))
}

#[allow(unreachable_code, unused_variables)] // will be correctly implemented in the future
async fn non_streamed_chat(
    index_scheduler: GuardedData<ActionPolicy<{ actions::CHAT_COMPLETIONS }>, Data<IndexScheduler>>,
    auth_ctrl: web::Data<AuthController>,
    search_queue: web::Data<SearchQueue>,
    workspace_uid: &str,
    req: HttpRequest,
    chat_completion: CreateChatCompletionRequest,
    analytics: web::Data<Analytics>,
) -> Result<HttpResponse, ResponseError> {
    index_scheduler.features().check_chat_completions("using the /chats chat completions route")?;

    // Create analytics aggregator
    let aggregate = ChatCompletionAggregator::from_request(
        &chat_completion.model,
        chat_completion.messages.len(),
        false, // non_streamed_chat is not streaming
    );
    let start_time = std::time::Instant::now();

    if let Some(n) = chat_completion.n.filter(|&n| n != 1) {
        return Err(ResponseError::from_msg(
            format!("You tried to specify n = {n} but only single choices are supported (n = 1)."),
            Code::UnimplementedMultiChoiceChatCompletions,
        ));
    }

    return Err(ResponseError::from_msg(
        "Non-streamed chat completions is not implemented".to_string(),
        Code::UnimplementedNonStreamingChatCompletions,
    ));

    let filters = index_scheduler.filters();
    let chat_settings = match index_scheduler.chat_settings(workspace_uid).unwrap() {
        Some(settings) => settings,
        None => {
            return Err(ResponseError::from_msg(
                format!("Chat `{workspace_uid}` not found"),
                Code::ChatNotFound,
            ))
        }
    };

    let config = Config::new(&chat_settings);
    let client = Client::with_config(config);
    let auth_token = extract_token_from_request(&req)?.unwrap();
    let system_role = chat_settings.source.system_role(&chat_completion.model);
    // TODO do function support later
    let _function_support = setup_search_tool(
        &index_scheduler,
        filters,
        &mut chat_completion,
        &chat_settings.prompts,
        system_role,
    )?;

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
                        Ok(SearchInIndexParameters { index_uid, q, filter }) => {
                            process_search_request(
                                &index_scheduler,
                                auth_ctrl.clone(),
                                &search_queue,
                                auth_token,
                                index_uid,
                                q,
                                filter,
                            )
                            .await
                            .map_err(|e| e.to_string())
                        }
                        Err(err) => Err(err.to_string()),
                    };

                    // TODO report documents sources later
                    let answer = match result {
                        Ok((_, _documents, text)) => text,
                        Err(err) => err,
                    };

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

    // Record success in analytics
    let mut aggregate = aggregate;
    aggregate.succeed(start_time.elapsed());
    analytics.publish(aggregate, &req);

    Ok(HttpResponse::Ok().json(response))
}

async fn streamed_chat(
    index_scheduler: GuardedData<ActionPolicy<{ actions::CHAT_COMPLETIONS }>, Data<IndexScheduler>>,
    auth_ctrl: web::Data<AuthController>,
    search_queue: web::Data<SearchQueue>,
    workspace_uid: &str,
    req: HttpRequest,
    mut chat_completion: CreateChatCompletionRequest,
    analytics: web::Data<Analytics>,
) -> Result<impl Responder, ResponseError> {
    index_scheduler.features().check_chat_completions("using the /chats chat completions route")?;
    let filters = index_scheduler.filters();

    if let Some(n) = chat_completion.n.filter(|&n| n != 1) {
        return Err(ResponseError::from_msg(
            format!("You tried to specify n = {n} but only single choices are supported (n = 1)."),
            Code::UnimplementedMultiChoiceChatCompletions,
        ));
    }

    let chat_settings = match index_scheduler.chat_settings(workspace_uid)? {
        Some(settings) => settings,
        None => {
            return Err(ResponseError::from_msg(
                format!("Chat `{workspace_uid}` not found"),
                Code::ChatNotFound,
            ))
        }
    };

    // Create analytics aggregator
    let mut aggregate = ChatCompletionAggregator::from_request(
        &chat_completion.model,
        chat_completion.messages.len(),
        true, // streamed_chat is always streaming
    );
    let start_time = std::time::Instant::now();

    let config = Config::new(&chat_settings);
    let auth_token = extract_token_from_request(&req)?.unwrap().to_string();
    let system_role = chat_settings.source.system_role(&chat_completion.model);
    let function_support = setup_search_tool(
        &index_scheduler,
        filters,
        &mut chat_completion,
        &chat_settings.prompts,
        system_role,
    )?;

    tracing::debug!("Conversation function support: {function_support:?}");

    let (tx, rx) = tokio::sync::mpsc::channel(10);
    let tx = SseEventSender::new(tx);
    let workspace_uid = workspace_uid.to_string();
    let _join_handle = Handle::current().spawn(async move {
        let client = Client::with_config(config.clone());
        let mut global_tool_calls = HashMap::<u32, Call>::new();

        // Limit the number of internal calls to satisfy the search requests of the LLM
        for _ in 0..20 {
            let output = run_conversation(
                &index_scheduler,
                &auth_ctrl,
                &workspace_uid,
                &search_queue,
                &auth_token,
                &client,
                chat_settings.source,
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

    // Record success in analytics after the stream is set up
    aggregate.succeed(start_time.elapsed());
    analytics.publish(aggregate, &req);

    Ok(Sse::from_infallible_receiver(rx).with_retry_duration(Duration::from_secs(10)))
}

/// Updates the chat completion with the new messages, streams the LLM tokens,
/// and report progress and errors.
#[allow(clippy::too_many_arguments)]
async fn run_conversation<C: async_openai::config::Config>(
    index_scheduler: &GuardedData<
        ActionPolicy<{ actions::CHAT_COMPLETIONS }>,
        Data<IndexScheduler>,
    >,
    auth_ctrl: &web::Data<AuthController>,
    workspace_uid: &str,
    search_queue: &web::Data<SearchQueue>,
    auth_token: &str,
    client: &Client<C>,
    source: DbChatCompletionSource,
    chat_completion: &mut CreateChatCompletionRequest,
    tx: &SseEventSender,
    global_tool_calls: &mut HashMap<u32, Call>,
    function_support: FunctionSupport,
) -> Result<ControlFlow<Option<FinishReason>, ()>, SendError<Event>> {
    use DbChatCompletionSource::*;

    let mut finish_reason = None;
    chat_completion.stream_options = match source {
        OpenAi | AzureOpenAi => Some(ChatCompletionStreamOptions { include_usage: true }),
        Mistral | VLlm => None,
    };

    // safety: unwrap: can only happens if `stream` was set to `false`
    let mut response = client.chat().create_stream(chat_completion.clone()).await.unwrap();
    while let Some(result) = response.next().await {
        match result {
            Ok(resp) => {
                if let Some(usage) = resp.usage.as_ref() {
                    MEILISEARCH_CHAT_PROMPT_TOKENS_TOTAL
                        .with_label_values(&[workspace_uid, &chat_completion.model])
                        .inc_by(usage.prompt_tokens as u64);
                    MEILISEARCH_CHAT_COMPLETION_TOKENS_TOTAL
                        .with_label_values(&[workspace_uid, &chat_completion.model])
                        .inc_by(usage.completion_tokens as u64);
                    MEILISEARCH_CHAT_TOKENS_TOTAL
                        .with_label_values(&[workspace_uid, &chat_completion.model])
                        .inc_by(usage.total_tokens as u64);
                }
                let choice = match resp.choices.first() {
                    Some(choice) => choice,
                    None => break,
                };
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
                                    if name.as_deref() == Some(MEILI_SEARCH_IN_INDEX_FUNCTION_NAME)
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
                        }
                    }
                    None => {
                        if !global_tool_calls.is_empty() {
                            let (meili_calls, _other_calls): (Vec<_>, Vec<_>) =
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

                            handle_meili_tools(
                                index_scheduler,
                                auth_ctrl,
                                search_queue,
                                auth_token,
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
                let result = match source {
                    DbChatCompletionSource::Mistral => {
                        StreamErrorEvent::from_openai_error::<MistralError>(error).await
                    }
                    _ => StreamErrorEvent::from_openai_error::<OpenAiOutsideError>(error).await,
                };
                let error = result.unwrap_or_else(StreamErrorEvent::from_reqwest_error);
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

#[allow(clippy::too_many_arguments)]
async fn handle_meili_tools(
    index_scheduler: &GuardedData<
        ActionPolicy<{ actions::CHAT_COMPLETIONS }>,
        Data<IndexScheduler>,
    >,
    auth_ctrl: &web::Data<AuthController>,
    search_queue: &web::Data<SearchQueue>,
    auth_token: &str,
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

        let mut error = None;

        let result = match serde_json::from_str(&call.function.arguments) {
            Ok(SearchInIndexParameters { index_uid, q, filter }) => match process_search_request(
                index_scheduler,
                auth_ctrl.clone(),
                search_queue,
                auth_token,
                index_uid,
                q,
                filter,
            )
            .await
            {
                Ok(output) => Ok(output),
                Err(err) => {
                    let error_text = format!("the search tool call failed with {err}");
                    error = Some(err);
                    Err(error_text)
                }
            },
            Err(err) => Err(err.to_string()),
        };

        let answer = match result {
            Ok((_index, documents, text)) => {
                if report_sources {
                    tx.report_sources(resp.clone(), &call.id, &documents).await?;
                }
                text
            }
            Err(err) => err,
        };

        let tool = ChatCompletionRequestMessage::Tool(ChatCompletionRequestToolMessage {
            tool_call_id: call.id.clone(),
            content: ChatCompletionRequestToolMessageContent::Text(answer),
        });

        if append_to_conversation {
            tx.append_conversation_message(resp.clone(), &tool).await?;
        }

        chat_completion.messages.push(tool);

        if let Some(error) = error {
            tx.send_error(&StreamErrorEvent::from_response_error(error)).await?;
        }
    }

    Ok(())
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

    /// # Panics
    ///
    /// - if called on external calls
    fn append(&mut self, more: &str) {
        match self {
            Call::Internal { arguments, .. } => arguments.push_str(more),
            Call::External => panic!("Cannot append argument chunks to an external function"),
        }
    }
}

#[derive(Deserialize)]
struct SearchInIndexParameters {
    /// The index uid to search in.
    index_uid: String,
    /// The query parameter to use.
    q: Option<String>,
    /// The filter parameter to use.
    filter: Option<String>,
}

fn format_facet_distributions(
    index: &Index,
    rtxn: &RoTxn,
    max_values_per_facet: usize,
) -> meilisearch_types::milli::Result<String> {
    let universe = index.documents_ids(rtxn)?;
    let rules = index.filterable_attributes_rules(rtxn)?;
    let fields_ids_map = index.fields_ids_map(rtxn)?;
    let filterable_attributes = fields_ids_map
        .names()
        .filter(|name| rules.iter().any(|rule| matches!(rule.match_str(name), PatternMatch::Match)))
        .map(|name| (name, OrderBy::Count));
    let facets_distribution = index
        .facets_distribution(rtxn)
        .max_values_per_facet(max_values_per_facet)
        .candidates(universe)
        .facets(filterable_attributes)
        .execute()?;

    let mut output = String::new();
    for (facet_name, entries) in facets_distribution {
        let _ = write!(&mut output, "{}: ", facet_name);
        let total_entries = entries.len();
        for (i, (value, _count)) in entries.into_iter().enumerate() {
            let _ = if total_entries.saturating_sub(1) == i {
                write!(&mut output, "{value}.")
            } else {
                write!(&mut output, "{value}, ")
            };
        }
        let _ = writeln!(&mut output);
    }

    Ok(output)
}
