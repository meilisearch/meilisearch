use actix_web::web::{self, Data};
use actix_web::HttpResponse;
use async_openai::config::OpenAIConfig;
use async_openai::types::CreateChatCompletionRequest;
use async_openai::Client;
use index_scheduler::IndexScheduler;
use meilisearch_types::error::ResponseError;
use meilisearch_types::keys::actions;

use crate::extractors::authentication::policies::ActionPolicy;
use crate::extractors::authentication::GuardedData;

pub fn configure(cfg: &mut web::ServiceConfig) {
    cfg.service(web::resource("").route(web::post().to(chat)));
}

/// Get a chat completion
async fn chat(
    _index_scheduler: GuardedData<ActionPolicy<{ actions::CHAT_GET }>, Data<IndexScheduler>>,
    web::Json(chat_completion): web::Json<CreateChatCompletionRequest>,
) -> Result<HttpResponse, ResponseError> {
    // To enable later on, when the feature will be experimental
    // index_scheduler.features().check_chat("Using the /chat route")?;

    let api_key = std::env::var("MEILI_OPENAI_API_KEY")
        .expect("cannot find OpenAI API Key (MEILI_OPENAI_API_KEY)");
    let config = OpenAIConfig::default().with_api_key(&api_key); // we can also change the API base
    let client = Client::with_config(config);
    let response = client.chat().create(chat_completion).await.unwrap();

    Ok(HttpResponse::Ok().json(response))
}
