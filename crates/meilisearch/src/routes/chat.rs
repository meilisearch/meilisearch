use std::mem;

use actix_web::web::{self, Data};
use actix_web::{Either, HttpResponse, Responder};
use actix_web_lab::sse::{self, Event};
use async_openai::config::OpenAIConfig;
use async_openai::types::{
    ChatCompletionRequestAssistantMessageArgs, ChatCompletionRequestMessage,
    ChatCompletionRequestToolMessage, ChatCompletionRequestToolMessageContent,
    ChatCompletionToolArgs, ChatCompletionToolType, CreateChatCompletionRequest, FinishReason,
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

/// Get a chat completion
async fn chat(
    index_scheduler: GuardedData<ActionPolicy<{ actions::CHAT_GET }>, Data<IndexScheduler>>,
    search_queue: web::Data<SearchQueue>,
    web::Json(mut chat_completion): web::Json<CreateChatCompletionRequest>,
) -> impl Responder {
    // To enable later on, when the feature will be experimental
    // index_scheduler.features().check_chat("Using the /chat route")?;

    if chat_completion.stream.unwrap_or(false) {
        Either::Right(streamed_chat(index_scheduler, search_queue, chat_completion).await)
    } else {
        Either::Left(non_streamed_chat(index_scheduler, search_queue, chat_completion).await)
    }
}

async fn non_streamed_chat(
    index_scheduler: GuardedData<ActionPolicy<{ actions::CHAT_GET }>, Data<IndexScheduler>>,
    search_queue: web::Data<SearchQueue>,
    mut chat_completion: CreateChatCompletionRequest,
) -> Result<HttpResponse, ResponseError> {
    let api_key = std::env::var("MEILI_OPENAI_API_KEY")
        .expect("cannot find OpenAI API Key (MEILI_OPENAI_API_KEY)");
    let config = OpenAIConfig::default().with_api_key(&api_key); // we can also change the API base
    let client = Client::with_config(config);

    assert_eq!(
        chat_completion.n.unwrap_or(1),
        1,
        "Meilisearch /chat only support one completion at a time (n = 1, n = null)"
    );

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

    let mut response;
    loop {
        let tools = chat_completion.tools.get_or_insert_default();
        tools.push(
            ChatCompletionToolArgs::default()
                .r#type(ChatCompletionToolType::Function)
                .function(
                    FunctionObjectArgs::default()
                        .name("searchInIndex")
                        .description(&search_in_index_description)
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
                    if let Some(search_rules) =
                        index_scheduler.filters().get_index_search_rules(&index_uid)
                    {
                        add_search_rules(&mut query.filter, search_rules);
                    }

                    // TBD
                    // let mut aggregate = SearchAggregator::<SearchPOST>::from_query(&query);

                    let index = index_scheduler.index(&index_uid)?;
                    let search_kind = search_kind(
                        &query,
                        index_scheduler.get_ref(),
                        index_uid.to_string(),
                        &index,
                    )?;

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
                    let formatted = format_documents(
                        &index,
                        search_result.hits.into_iter().map(|doc| doc.document),
                    );
                    let text = formatted.join("\n");
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
    assert!(chat_completion.stream.unwrap_or(false));

    let api_key = std::env::var("MEILI_OPENAI_API_KEY")
        .expect("cannot find OpenAI API Key (MEILI_OPENAI_API_KEY)");
    let config = OpenAIConfig::default().with_api_key(&api_key); // we can also change the API base
    let client = Client::with_config(config);
    let response = client.chat().create_stream(chat_completion).await.unwrap();
    actix_web_lab::sse::Sse::from_stream(response.map(|response| {
        response
            .map(|mut r| Event::Data(sse::Data::new_json(r.choices.pop().unwrap().delta).unwrap()))
    }))
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
        prompt: PromptData { template, max_bytes },
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
