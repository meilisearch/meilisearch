use std::cell::RefCell;
use std::collections::HashMap;
use std::fmt::Write as _;
use std::mem;
use std::sync::RwLock;
use std::time::Duration;

use actix_web::web::{self, Data};
use actix_web::{Either, HttpRequest, HttpResponse, Responder};
use actix_web_lab::sse::{self, Event, Sse};
use async_openai::config::OpenAIConfig;
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
use meilisearch_types::error::ResponseError;
use meilisearch_types::heed::RoTxn;
use meilisearch_types::keys::actions;
use meilisearch_types::milli::index::{self, ChatConfig, SearchParameters};
use meilisearch_types::milli::prompt::{Prompt, PromptData};
use meilisearch_types::milli::update::new::document::DocumentFromDb;
use meilisearch_types::milli::update::Setting;
use meilisearch_types::milli::{
    DocumentId, FieldIdMapWithMetadata, GlobalFieldsIdsMap, MetadataBuilder, TimeBudget,
};
use meilisearch_types::Index;
use serde::{Deserialize, Serialize};
use serde_json::json;
use tokio::runtime::Handle;
use tokio::sync::mpsc::error::SendError;

use super::settings::chat::{ChatPrompts, GlobalChatSettings};
use crate::error::MeilisearchHttpError;
use crate::extractors::authentication::policies::ActionPolicy;
use crate::extractors::authentication::{extract_token_from_request, GuardedData, Policy as _};
use crate::metrics::MEILISEARCH_DEGRADED_SEARCH_REQUESTS;
use crate::routes::indexes::search::search_kind;
use crate::search::{
    add_search_rules, prepare_search, search_from_kind, HybridQuery, MatchingStrategy,
    RankingScoreThreshold, SearchQuery, SemanticRatio, DEFAULT_SEARCH_LIMIT,
    DEFAULT_SEMANTIC_RATIO,
};
use crate::search_queue::SearchQueue;

const MEILI_SEARCH_PROGRESS_NAME: &str = "_meiliSearchProgress";
const MEILI_APPEND_CONVERSATION_MESSAGE_NAME: &str = "_meiliAppendConversationMessage";
const MEILI_SEARCH_IN_INDEX_FUNCTION_NAME: &str = "_meiliSearchInIndex";

pub fn configure(cfg: &mut web::ServiceConfig) {
    cfg.service(web::resource("/completions").route(web::post().to(chat)));
}

/// Get a chat completion
async fn chat(
    index_scheduler: GuardedData<ActionPolicy<{ actions::CHAT }>, Data<IndexScheduler>>,
    auth_ctrl: web::Data<AuthController>,
    req: HttpRequest,
    search_queue: web::Data<SearchQueue>,
    web::Json(chat_completion): web::Json<CreateChatCompletionRequest>,
) -> impl Responder {
    // To enable later on, when the feature will be experimental
    // index_scheduler.features().check_chat("Using the /chat route")?;

    assert_eq!(
        chat_completion.n.unwrap_or(1),
        1,
        "Meilisearch /chat only support one completion at a time (n = 1, n = null)"
    );

    if chat_completion.stream.unwrap_or(false) {
        Either::Right(
            streamed_chat(index_scheduler, auth_ctrl, req, search_queue, chat_completion).await,
        )
    } else {
        Either::Left(
            non_streamed_chat(index_scheduler, auth_ctrl, req, search_queue, chat_completion).await,
        )
    }
}

#[derive(Default, Debug, Clone, Copy)]
pub struct FunctionSupport {
    /// Defines if we can call the _meiliSearchProgress function
    /// to inform the front-end about what we are searching for.
    progress: bool,
    /// Defines if we can call the _meiliAppendConversationMessage
    /// function to provide the messages to append into the conversation.
    append_to_conversation: bool,
}

/// Setup search tool in chat completion request
fn setup_search_tool(
    index_scheduler: &Data<IndexScheduler>,
    filters: &meilisearch_auth::AuthFilter,
    chat_completion: &mut CreateChatCompletionRequest,
    prompts: &ChatPrompts,
) -> Result<FunctionSupport, ResponseError> {
    let tools = chat_completion.tools.get_or_insert_default();
    if tools.iter().find(|t| t.function.name == MEILI_SEARCH_IN_INDEX_FUNCTION_NAME).is_some() {
        panic!("{MEILI_SEARCH_IN_INDEX_FUNCTION_NAME} function already set");
    }

    // Remove internal tools used for front-end notifications as they should be hidden from the LLM.
    let mut progress = false;
    let mut append_to_conversation = false;
    tools.retain(|tool| {
        match tool.function.name.as_str() {
            MEILI_SEARCH_PROGRESS_NAME => {
                progress = true;
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
    let mut function_description = prompts.search_description.clone().unwrap();
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
                            "description": prompts.search_index_uid_param.clone().unwrap(),
                        },
                        "q": {
                            // Unfortunately, Mistral does not support an array of types, here.
                            // "type": ["string", "null"],
                            "type": "string",
                            "description": prompts.search_q_param.clone().unwrap(),
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
            content: ChatCompletionRequestSystemMessageContent::Text(
                prompts.system.as_ref().unwrap().clone(),
            ),
            name: None,
        }),
    );

    Ok(FunctionSupport { progress, append_to_conversation })
}

/// Process search request and return formatted results
async fn process_search_request(
    index_scheduler: &GuardedData<ActionPolicy<{ actions::CHAT }>, Data<IndexScheduler>>,
    auth_ctrl: web::Data<AuthController>,
    search_queue: &web::Data<SearchQueue>,
    auth_token: &str,
    index_uid: String,
    q: Option<String>,
) -> Result<(Index, String), ResponseError> {
    // TBD
    // let mut aggregate = SearchAggregator::<SearchPOST>::from_query(&query);

    let index = index_scheduler.index(&index_uid)?;
    let rtxn = index.static_read_txn()?;
    let ChatConfig { description: _, prompt: _, search_parameters } = index.chat_config(&rtxn)?;
    let SearchParameters {
        hybrid,
        limit,
        sort,
        distinct,
        matching_strategy,
        attributes_to_search_on,
        ranking_score_threshold,
    } = search_parameters;

    let mut query = SearchQuery {
        q,
        hybrid: hybrid.map(|index::HybridQuery { semantic_ratio, embedder }| HybridQuery {
            semantic_ratio: SemanticRatio::try_from(semantic_ratio)
                .ok()
                .unwrap_or_else(DEFAULT_SEMANTIC_RATIO),
            embedder,
        }),
        limit: limit.unwrap_or_else(DEFAULT_SEARCH_LIMIT),
        sort: sort,
        distinct: distinct,
        matching_strategy: matching_strategy
            .map(|ms| match ms {
                index::MatchingStrategy::Last => MatchingStrategy::Last,
                index::MatchingStrategy::All => MatchingStrategy::All,
                index::MatchingStrategy::Frequency => MatchingStrategy::Frequency,
            })
            .unwrap_or(MatchingStrategy::Frequency),
        attributes_to_search_on: attributes_to_search_on,
        ranking_score_threshold: ranking_score_threshold
            .and_then(|rst| RankingScoreThreshold::try_from(rst).ok()),
        ..Default::default()
    };

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
    if let Ok((_, ref search_result)) = output {
        // aggregate.succeed(search_result);
        if search_result.degraded {
            MEILISEARCH_DEGRADED_SEARCH_REQUESTS.inc();
        }
    }
    // analytics.publish(aggregate, &req);

    let (rtxn, search_result) = output?;
    // let rtxn = index.read_txn()?;
    let render_alloc = Bump::new();
    let formatted = format_documents(&rtxn, &index, &render_alloc, search_result.documents_ids)?;
    let text = formatted.join("\n");
    drop(rtxn);

    Ok((index, text))
}

async fn non_streamed_chat(
    index_scheduler: GuardedData<ActionPolicy<{ actions::CHAT }>, Data<IndexScheduler>>,
    auth_ctrl: web::Data<AuthController>,
    req: HttpRequest,
    search_queue: web::Data<SearchQueue>,
    mut chat_completion: CreateChatCompletionRequest,
) -> Result<HttpResponse, ResponseError> {
    let filters = index_scheduler.filters();

    let chat_settings = match index_scheduler.chat_settings().unwrap() {
        Some(value) => serde_json::from_value(value).unwrap(),
        None => GlobalChatSettings::default(),
    };

    let mut config = OpenAIConfig::default();
    if let Setting::Set(api_key) = chat_settings.api_key.as_ref() {
        config = config.with_api_key(api_key);
    }
    if let Setting::Set(base_api) = chat_settings.base_api.as_ref() {
        config = config.with_api_base(base_api);
    }
    let client = Client::with_config(config);

    let auth_token = extract_token_from_request(&req)?.unwrap();
    let prompts = chat_settings.prompts.clone().or(Setting::Set(ChatPrompts::default())).unwrap();
    let FunctionSupport { progress, append_to_conversation } =
        setup_search_tool(&index_scheduler, filters, &mut chat_completion, &prompts)?;

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

                    let text = match result {
                        Ok((_, text)) => text,
                        Err(err) => err,
                    };

                    chat_completion.messages.push(ChatCompletionRequestMessage::Tool(
                        ChatCompletionRequestToolMessage {
                            tool_call_id: call.id.clone(),
                            content: ChatCompletionRequestToolMessageContent::Text(format!(
                                "{}\n\n{text}",
                                chat_settings.prompts.clone().unwrap().pre_query.unwrap()
                            )),
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
    req: HttpRequest,
    search_queue: web::Data<SearchQueue>,
    mut chat_completion: CreateChatCompletionRequest,
) -> Result<impl Responder, ResponseError> {
    let filters = index_scheduler.filters();

    let chat_settings = match index_scheduler.chat_settings().unwrap() {
        Some(value) => serde_json::from_value(value.clone()).unwrap(),
        None => GlobalChatSettings::default(),
    };

    let mut config = OpenAIConfig::default();
    if let Setting::Set(api_key) = chat_settings.api_key.as_ref() {
        config = config.with_api_key(api_key);
    }
    if let Setting::Set(base_api) = chat_settings.base_api.as_ref() {
        config = config.with_api_base(base_api);
    }

    let auth_token = extract_token_from_request(&req)?.unwrap().to_string();
    let prompts = chat_settings.prompts.clone().or(Setting::Set(ChatPrompts::default())).unwrap();
    let FunctionSupport { progress, append_to_conversation } =
        setup_search_tool(&index_scheduler, filters, &mut chat_completion, &prompts)?;

    let (tx, rx) = tokio::sync::mpsc::channel(10);
    let _join_handle = Handle::current().spawn(async move {
        let client = Client::with_config(config.clone());
        let mut global_tool_calls = HashMap::<u32, Call>::new();
        let mut finish_reason = None;

        // Limit the number of internal calls to satisfy the search requests of the LLM
        'main: for _ in 0..20 {
            let mut response = client.chat().create_stream(chat_completion.clone()).await.unwrap();
            while let Some(result) = response.next().await {
                match result {
                    Ok(resp) => {
                        let choice = &resp.choices[0];
                        finish_reason = choice.finish_reason;

                        let ChatCompletionStreamResponseDelta { ref tool_calls, .. } =
                            &choice.delta;

                        match tool_calls {
                            Some(tool_calls) => {
                                for chunk in tool_calls {
                                    let ChatCompletionMessageToolCallChunk {
                                        index,
                                        id,
                                        r#type: _,
                                        function,
                                    } = chunk;
                                    let FunctionCallStream { name, arguments } =
                                        function.as_ref().unwrap();

                                    global_tool_calls
                                        .entry(*index)
                                        .and_modify(|call| {
                                            if call.is_internal() {
                                                call.append(arguments.as_ref().unwrap())
                                            }
                                        })
                                        .or_insert_with(|| {
                                            if name.as_ref().map_or(false, |n| {
                                                n == MEILI_SEARCH_IN_INDEX_FUNCTION_NAME
                                            }) {
                                                Call::Internal {
                                                    id: id.as_ref().unwrap().clone(),
                                                    function_name: name.as_ref().unwrap().clone(),
                                                    arguments: arguments.as_ref().unwrap().clone(),
                                                }
                                            } else {
                                                Call::External { _id: id.as_ref().unwrap().clone() }
                                            }
                                        });

                                    if global_tool_calls.get(index).map_or(false, Call::is_external)
                                    {
                                        todo!("Support forwarding external tool calls");
                                    }
                                }
                            }
                            None => {
                                if !global_tool_calls.is_empty() {
                                    let (meili_calls, other_calls): (Vec<_>, Vec<_>) =
                                        mem::take(&mut global_tool_calls)
                                            .into_values()
                                            .flat_map(|call| match call {
                                                Call::Internal {
                                                    id,
                                                    function_name: name,
                                                    arguments,
                                                } => Some(ChatCompletionMessageToolCall {
                                                    id,
                                                    r#type: Some(ChatCompletionToolType::Function),
                                                    function: FunctionCall { name, arguments },
                                                }),
                                                Call::External { _id: _ } => None,
                                            })
                                            .partition(|call| {
                                                call.function.name
                                                    == MEILI_SEARCH_IN_INDEX_FUNCTION_NAME
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

                                    for call in meili_calls {
                                        if progress {
                                            let call = MeiliSearchProgress {
                                                function_name: call.function.name.clone(),
                                                function_arguments: call
                                                    .function
                                                    .arguments
                                                    .clone(),
                                            };
                                            let resp = call.create_response(resp.clone());
                                            // Send the event of "we are doing a search"
                                            if let Err(SendError(_)) = tx
                                                .send(Event::Data(sse::Data::new_json(&resp).unwrap()))
                                                .await
                                            {
                                                return;
                                            }
                                        }

                                        if append_to_conversation {
                                            // Ask the front-end user to append this tool *call* to the conversation
                                            let call = MeiliAppendConversationMessage(ChatCompletionRequestMessage::Assistant(
                                                ChatCompletionRequestAssistantMessage {
                                                    content: None,
                                                    refusal: None,
                                                    name: None,
                                                    audio: None,
                                                    tool_calls: Some(vec![
                                                        ChatCompletionMessageToolCall {
                                                            id: call.id.clone(),
                                                            r#type: Some(ChatCompletionToolType::Function),
                                                            function: FunctionCall {
                                                                name: call.function.name.clone(),
                                                                arguments: call.function.arguments.clone(),
                                                            },
                                                        },
                                                    ]),
                                                    function_call: None,
                                                }
                                            ));
                                            let resp = call.create_response(resp.clone());
                                            if let Err(SendError(_)) = tx
                                                .send(Event::Data(sse::Data::new_json(&resp).unwrap()))
                                                .await
                                            {
                                                return;
                                            }
                                        }

                                        let result =
                                            match serde_json::from_str(&call.function.arguments) {
                                                Ok(SearchInIndexParameters { index_uid, q }) => {
                                                    process_search_request(
                                                        &index_scheduler,
                                                        auth_ctrl.clone(),
                                                        &search_queue,
                                                        &auth_token,
                                                        index_uid,
                                                        q,
                                                    )
                                                    .await
                                                    .map_err(|e| e.to_string())
                                                }
                                                Err(err) => Err(err.to_string()),
                                            };

                                        let text = match result {
                                            Ok((_, text)) => text,
                                            Err(err) => err,
                                        };

                                        let tool = ChatCompletionRequestMessage::Tool(ChatCompletionRequestToolMessage {
                                            tool_call_id: call.id.clone(),
                                            content: ChatCompletionRequestToolMessageContent::Text(
                                                format!(
                                                    "{}\n\n{text}",
                                                    chat_settings
                                                        .prompts
                                                        .as_ref()
                                                        .unwrap()
                                                        .pre_query
                                                        .as_ref()
                                                        .unwrap()
                                                ),
                                            ),
                                        });

                                        if append_to_conversation {
                                            // Ask the front-end user to append this tool *output* to the conversation
                                            let tool = MeiliAppendConversationMessage(tool.clone());
                                            let resp = tool.create_response(resp.clone());
                                            if let Err(SendError(_)) = tx
                                                .send(Event::Data(sse::Data::new_json(&resp).unwrap()))
                                                .await
                                            {
                                                return;
                                            }
                                        }

                                        chat_completion.messages.push(tool);
                                    }
                                } else {
                                    if let Err(SendError(_)) = tx
                                        .send(Event::Data(sse::Data::new_json(&resp).unwrap()))
                                        .await
                                    {
                                        return;
                                    }
                                }
                            }
                        }
                    }
                    Err(err) => {
                        // tracing::error!("{err:?}");
                        // if let Err(SendError(_)) = tx
                        //     .send(Event::Data(
                        //         sse::Data::new_json(&json!({
                        //             "object": "chat.completion.error",
                        //             "tool": err.to_string(),
                        //         }))
                        //         .unwrap(),
                        //     ))
                        //     .await
                        // {
                        //     return;
                        // }

                        break 'main;
                    }
                }
            }

            // We must stop if the finish reason is not something we can solve with Meilisearch
            if finish_reason.map_or(true, |fr| fr != FinishReason::ToolCalls) {
                break;
            }
        }

        let _ = tx.send(Event::Data(sse::Data::new("[DONE]")));
    });

    Ok(Sse::from_infallible_receiver(rx).with_retry_duration(Duration::from_secs(10)))
}

#[derive(Debug, Clone, Serialize)]
/// Give context about what Meilisearch is doing.
struct MeiliSearchProgress {
    /// The name of the function we are executing.
    pub function_name: String,
    /// The arguments of the function we are executing, encoded in JSON.
    pub function_arguments: String,
}

impl MeiliSearchProgress {
    fn create_response(
        &self,
        mut resp: CreateChatCompletionStreamResponse,
    ) -> CreateChatCompletionStreamResponse {
        let call_text = serde_json::to_string(self).unwrap();
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
        resp
    }
}

struct MeiliAppendConversationMessage(pub ChatCompletionRequestMessage);

impl MeiliAppendConversationMessage {
    fn create_response(
        &self,
        mut resp: CreateChatCompletionStreamResponse,
    ) -> CreateChatCompletionStreamResponse {
        let call_text = serde_json::to_string(&self.0).unwrap();
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
        resp
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
    External { _id: String },
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
