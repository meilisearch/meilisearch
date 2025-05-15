use std::collections::HashMap;
use std::mem;
use std::time::Duration;

use actix_web::web::{self, Data};
use actix_web::{Either, HttpResponse, Responder};
use actix_web_lab::sse::{self, Event, Sse};
use async_openai::config::OpenAIConfig;
use async_openai::types::{
    ChatCompletionMessageToolCall, ChatCompletionMessageToolCallChunk,
    ChatCompletionRequestAssistantMessageArgs, ChatCompletionRequestMessage,
    ChatCompletionRequestToolMessage, ChatCompletionRequestToolMessageContent,
    ChatCompletionStreamResponseDelta, ChatCompletionToolArgs, ChatCompletionToolType,
    CreateChatCompletionRequest, FinishReason, FunctionCall, FunctionCallStream,
    FunctionObjectArgs,
};
use async_openai::Client;
use futures::StreamExt;
use index_scheduler::IndexScheduler;
use meilisearch_types::error::ResponseError;
use meilisearch_types::keys::actions;
use meilisearch_types::milli::index::IndexEmbeddingConfig;
use meilisearch_types::milli::prompt::PromptData;
use meilisearch_types::milli::vector::EmbeddingConfig;
use meilisearch_types::{Document, Index};
use serde::{Deserialize, Serialize};
use serde_json::json;
use tokio::runtime::Handle;

use crate::extractors::authentication::policies::ActionPolicy;
use crate::extractors::authentication::GuardedData;
use crate::metrics::MEILISEARCH_DEGRADED_SEARCH_REQUESTS;
use crate::routes::indexes::search::search_kind;
use crate::search::{
    add_search_rules, perform_search, HybridQuery, RetrieveVectors, SearchQuery, SemanticRatio,
};
use crate::search_queue::SearchQueue;

/// The default description of the searchInIndex tool provided to OpenAI.
const DEFAULT_SEARCH_IN_INDEX_TOOL_DESCRIPTION: &str =
    "Search the database for relevant JSON documents using an optional query.";
/// The default description of the searchInIndex `q` parameter tool provided to OpenAI.
const DEFAULT_SEARCH_IN_INDEX_Q_PARAMETER_TOOL_DESCRIPTION: &str =
    "The search query string used to find relevant documents in the index. \
This should contain keywords or phrases that best represent what the user is looking for. \
More specific queries will yield more precise results.";
/// The default description of the searchInIndex `index` parameter tool provided to OpenAI.
const DEFAULT_SEARCH_IN_INDEX_INDEX_PARAMETER_TOOL_DESCRIPTION: &str =
"The name of the index to search within. An index is a collection of documents organized for search. \
Selecting the right index ensures the most relevant results for the user query";

const EMBEDDER_NAME: &str = "openai";

pub fn configure(cfg: &mut web::ServiceConfig) {
    cfg.service(web::resource("").route(web::post().to(chat)));
}

/// Creates OpenAI client with API key
fn create_openai_client() -> Client<OpenAIConfig> {
    let api_key = std::env::var("MEILI_OPENAI_API_KEY")
        .expect("cannot find OpenAI API Key (MEILI_OPENAI_API_KEY)");
    let config = OpenAIConfig::default().with_api_key(&api_key);
    Client::with_config(config)
}

/// Get a chat completion
async fn chat(
    index_scheduler: GuardedData<ActionPolicy<{ actions::CHAT_GET }>, Data<IndexScheduler>>,
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
        Either::Right(streamed_chat(index_scheduler, search_queue, chat_completion).await)
    } else {
        Either::Left(non_streamed_chat(index_scheduler, search_queue, chat_completion).await)
    }
}

/// Setup search tool in chat completion request
fn setup_search_tool(
    chat_completion: &mut CreateChatCompletionRequest,
    search_in_index_description: &str,
    search_in_index_q_param_description: &str,
    search_in_index_index_description: &str,
) {
    let tools = chat_completion.tools.get_or_insert_default();
    tools.push(
        ChatCompletionToolArgs::default()
            .r#type(ChatCompletionToolType::Function)
            .function(
                FunctionObjectArgs::default()
                    .name("searchInIndex")
                    .description(search_in_index_description)
                    .parameters(json!({
                        "type": "object",
                        "properties": {
                            "index_uid": {
                                "type": "string",
                                "enum": ["main"],
                                "description": search_in_index_index_description,
                            },
                            "q": {
                                "type": ["string", "null"],
                                "description": search_in_index_q_param_description,
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
            .unwrap(),
    );
}

/// Process search request and return formatted results
async fn process_search_request(
    index_scheduler: &GuardedData<ActionPolicy<{ actions::CHAT_GET }>, Data<IndexScheduler>>,
    search_queue: &web::Data<SearchQueue>,
    index_uid: String,
    q: Option<String>,
) -> Result<(Index, String), ResponseError> {
    let mut query = SearchQuery {
        q,
        hybrid: Some(HybridQuery {
            semantic_ratio: SemanticRatio::default(),
            embedder: EMBEDDER_NAME.to_string(),
        }),
        limit: 20,
        ..Default::default()
    };

    // Tenant token search_rules.
    if let Some(search_rules) = index_scheduler.filters().get_index_search_rules(&index_uid) {
        add_search_rules(&mut query.filter, search_rules);
    }

    // TBD
    // let mut aggregate = SearchAggregator::<SearchPOST>::from_query(&query);

    let index = index_scheduler.index(&index_uid)?;
    let search_kind =
        search_kind(&query, index_scheduler.get_ref(), index_uid.to_string(), &index)?;

    let permit = search_queue.try_get_search_permit().await?;
    let features = index_scheduler.features();
    let index_cloned = index.clone();
    let search_result = tokio::task::spawn_blocking(move || {
        perform_search(
            index_uid.to_string(),
            &index_cloned,
            query,
            search_kind,
            RetrieveVectors::new(false),
            features,
        )
    })
    .await;
    permit.drop().await;

    let search_result = search_result?;
    if let Ok(ref search_result) = search_result {
        // aggregate.succeed(search_result);
        if search_result.degraded {
            MEILISEARCH_DEGRADED_SEARCH_REQUESTS.inc();
        }
    }
    // analytics.publish(aggregate, &req);

    let search_result = search_result?;
    let formatted =
        format_documents(&index, search_result.hits.into_iter().map(|doc| doc.document));
    let text = formatted.join("\n");

    Ok((index, text))
}

/// Get prompt descriptions from index scheduler
fn get_prompt_descriptions(
    index_scheduler: &GuardedData<ActionPolicy<{ actions::CHAT_GET }>, Data<IndexScheduler>>,
) -> (String, String, String) {
    let rtxn = index_scheduler.read_txn().unwrap();
    let search_in_index_description = index_scheduler
        .chat_prompts(&rtxn, "searchInIndex-description")
        .unwrap()
        .unwrap_or(DEFAULT_SEARCH_IN_INDEX_TOOL_DESCRIPTION)
        .to_string();
    let search_in_index_q_param_description = index_scheduler
        .chat_prompts(&rtxn, "searchInIndex-q-param-description")
        .unwrap()
        .unwrap_or(DEFAULT_SEARCH_IN_INDEX_Q_PARAMETER_TOOL_DESCRIPTION)
        .to_string();
    let search_in_index_index_description = index_scheduler
        .chat_prompts(&rtxn, "searchInIndex-index-param-description")
        .unwrap()
        .unwrap_or(DEFAULT_SEARCH_IN_INDEX_INDEX_PARAMETER_TOOL_DESCRIPTION)
        .to_string();
    drop(rtxn);

    (
        search_in_index_description,
        search_in_index_q_param_description,
        search_in_index_index_description,
    )
}

async fn non_streamed_chat(
    index_scheduler: GuardedData<ActionPolicy<{ actions::CHAT_GET }>, Data<IndexScheduler>>,
    search_queue: web::Data<SearchQueue>,
    mut chat_completion: CreateChatCompletionRequest,
) -> Result<HttpResponse, ResponseError> {
    let client = create_openai_client();

    let (
        search_in_index_description,
        search_in_index_q_param_description,
        search_in_index_index_description,
    ) = get_prompt_descriptions(&index_scheduler);

    let mut response;
    loop {
        setup_search_tool(
            &mut chat_completion,
            &search_in_index_description,
            &search_in_index_q_param_description,
            &search_in_index_index_description,
        );

        response = client.chat().create(chat_completion.clone()).await.unwrap();

        let choice = &mut response.choices[0];
        match choice.finish_reason {
            Some(FinishReason::ToolCalls) => {
                let tool_calls = mem::take(&mut choice.message.tool_calls).unwrap_or_default();

                let (meili_calls, other_calls): (Vec<_>, Vec<_>) =
                    tool_calls.into_iter().partition(|call| call.function.name == "searchInIndex");

                chat_completion.messages.push(
                    ChatCompletionRequestAssistantMessageArgs::default()
                        .tool_calls(meili_calls.clone())
                        .build()
                        .unwrap()
                        .into(),
                );

                for call in meili_calls {
                    let SearchInIndexParameters { index_uid, q } =
                        serde_json::from_str(&call.function.arguments).unwrap();

                    let (_, text) =
                        process_search_request(&index_scheduler, &search_queue, index_uid, q)
                            .await?;

                    chat_completion.messages.push(ChatCompletionRequestMessage::Tool(
                        ChatCompletionRequestToolMessage {
                            tool_call_id: call.id,
                            content: ChatCompletionRequestToolMessageContent::Text(text),
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
    index_scheduler: GuardedData<ActionPolicy<{ actions::CHAT_GET }>, Data<IndexScheduler>>,
    search_queue: web::Data<SearchQueue>,
    mut chat_completion: CreateChatCompletionRequest,
) -> impl Responder {
    let (
        search_in_index_description,
        search_in_index_q_param_description,
        search_in_index_index_description,
    ) = get_prompt_descriptions(&index_scheduler);

    setup_search_tool(
        &mut chat_completion,
        &search_in_index_description,
        &search_in_index_q_param_description,
        &search_in_index_index_description,
    );

    let (tx, rx) = tokio::sync::mpsc::channel(10);
    let _join_handle = Handle::current().spawn(async move {
        let client = create_openai_client();
        let mut global_tool_calls = HashMap::<u32, Call>::new();

        'main: loop {
            let mut response = client.chat().create_stream(chat_completion.clone()).await.unwrap();

            while let Some(result) = response.next().await {
                match result {
                    Ok(resp) => {
                        let delta = &resp.choices[0].delta;
                        let ChatCompletionStreamResponseDelta {
                            content,
                            // Using deprecated field but keeping for compatibility
                            #[allow(deprecated)]
                                function_call: _,
                            ref tool_calls,
                            role: _,
                            refusal: _,
                        } = delta;

                        if content.is_none() && tool_calls.is_none() && global_tool_calls.is_empty()
                        {
                            break 'main;
                        }

                        if let Some(_) = content {
                            tx.send(Event::Data(sse::Data::new_json(&resp).unwrap())).await.unwrap()
                        }

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
                                        .or_insert_with(|| Call {
                                            id: id.as_ref().unwrap().clone(),
                                            function_name: name.as_ref().unwrap().clone(),
                                            arguments: arguments.as_ref().unwrap().clone(),
                                        })
                                        .append(arguments.as_ref().unwrap());
                                }
                            }
                            None if !global_tool_calls.is_empty() => {
                                // dbg!(&global_tool_calls);

                                let (meili_calls, _other_calls): (Vec<_>, Vec<_>) =
                                    mem::take(&mut global_tool_calls)
                                        .into_iter()
                                        .map(|(_, call)| ChatCompletionMessageToolCall {
                                            id: call.id,
                                            r#type: ChatCompletionToolType::Function,
                                            function: FunctionCall {
                                                name: call.function_name,
                                                arguments: call.arguments,
                                            },
                                        })
                                        .partition(|call| call.function.name == "searchInIndex");

                                chat_completion.messages.push(
                                    ChatCompletionRequestAssistantMessageArgs::default()
                                        .tool_calls(meili_calls.clone())
                                        .build()
                                        .unwrap()
                                        .into(),
                                );

                                for call in meili_calls {
                                    tx.send(Event::Data(
                                        sse::Data::new_json(&json!({
                                            "object": "chat.completion.tool.call",
                                            "tool": call,
                                        }))
                                        .unwrap(),
                                    ))
                                    .await
                                    .unwrap();

                                    let SearchInIndexParameters { index_uid, q } =
                                        serde_json::from_str(&call.function.arguments).unwrap();

                                    let result = process_search_request(
                                        &index_scheduler,
                                        &search_queue,
                                        index_uid,
                                        q,
                                    )
                                    .await;

                                    // Handle potential errors more explicitly
                                    if let Err(err) = &result {
                                        // Log the error or handle it as needed
                                        eprintln!("Error processing search request: {:?}", err);
                                        continue;
                                    }

                                    let (_, text) = result.unwrap();

                                    let tool = ChatCompletionRequestMessage::Tool(
                                        ChatCompletionRequestToolMessage {
                                            tool_call_id: call.id,
                                            content: ChatCompletionRequestToolMessageContent::Text(
                                                text,
                                            ),
                                        },
                                    );
                                    tx.send(Event::Data(
                                        sse::Data::new_json(&json!({
                                            "object": "chat.completion.tool.output",
                                            "tool": tool,
                                        }))
                                        .unwrap(),
                                    ))
                                    .await
                                    .unwrap();
                                    chat_completion.messages.push(tool);
                                }
                            }
                            None => (),
                        }
                    }
                    Err(_err) => {
                        // writeln!(lock, "error: {err}").unwrap();
                    }
                }
            }
        }
    });

    Sse::from_infallible_receiver(rx).with_retry_duration(Duration::from_secs(10))
}

/// The structure used to aggregate the function calls to make.
#[derive(Debug)]
struct Call {
    id: String,
    function_name: String,
    arguments: String,
}

impl Call {
    fn append(&mut self, arguments: &str) {
        self.arguments.push_str(arguments);
    }
}

#[derive(Deserialize)]
struct SearchInIndexParameters {
    /// The index uid to search in.
    index_uid: String,
    /// The query parameter to use.
    q: Option<String>,
}

fn format_documents(index: &Index, documents: impl Iterator<Item = Document>) -> Vec<String> {
    let rtxn = index.read_txn().unwrap();
    let IndexEmbeddingConfig { name: _, config, user_provided: _ } = index
        .embedding_configs(&rtxn)
        .unwrap()
        .into_iter()
        .find(|conf| conf.name == EMBEDDER_NAME)
        .unwrap();

    let EmbeddingConfig {
        embedder_options: _,
        prompt: PromptData { template, max_bytes: _ },
        quantized: _,
    } = config;

    #[derive(Serialize)]
    struct Doc<T: Serialize> {
        doc: T,
    }

    let template = liquid::ParserBuilder::with_stdlib().build().unwrap().parse(&template).unwrap();
    documents
        .map(|doc| {
            let object = liquid::to_object(&Doc { doc }).unwrap();
            template.render(&object).unwrap()
        })
        .collect()
}
