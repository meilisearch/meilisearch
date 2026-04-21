use crate::{
    client::Client,
    config::Config,
    error::OpenAIError,
    types::{CompletionResponseStream, CreateCompletionRequest, CreateCompletionResponse},
};

/// Given a prompt, the model will return one or more predicted completions,
/// and can also return the probabilities of alternative tokens at each position.
/// We recommend most users use our Chat completions API.
/// [Learn more](https://platform.openai.com/docs/deprecations/2023-07-06-gpt-and-embeddings)
///
/// Related guide: [Legacy Completions](https://platform.openai.com/docs/guides/gpt/completions-api)
pub struct Completions<'c, C: Config> {
    client: &'c Client<C>,
}

impl<'c, C: Config> Completions<'c, C> {
    pub fn new(client: &'c Client<C>) -> Self {
        Self { client }
    }

    /// Creates a completion for the provided prompt and parameters
    ///
    /// You must ensure that "stream: false" in serialized `request`
    #[crate::byot(
        T0 = serde::Serialize,
        R = serde::de::DeserializeOwned
    )]
    pub async fn create(
        &self,
        request: CreateCompletionRequest,
    ) -> Result<CreateCompletionResponse, OpenAIError> {
        #[cfg(not(feature = "byot"))]
        {
            if request.stream.is_some() && request.stream.unwrap() {
                return Err(OpenAIError::InvalidArgument(
                    "When stream is true, use Completion::create_stream".into(),
                ));
            }
        }
        self.client.post("/completions", request).await
    }

    /// Creates a completion request for the provided prompt and parameters
    ///
    /// Stream back partial progress. Tokens will be sent as data-only
    /// [server-sent events](https://developer.mozilla.org/en-US/docs/Web/API/Server-sent_events/Using_server-sent_events#event_stream_format)
    /// as they become available, with the stream terminated by a data: \[DONE\] message.
    ///
    /// [CompletionResponseStream] is a parsed SSE stream until a \[DONE\] is received from server.
    ///
    /// You must ensure that "stream: true" in serialized `request`
    #[crate::byot(
        T0 = serde::Serialize,
        R = serde::de::DeserializeOwned,
        stream = "true",
        where_clause = "R: std::marker::Send + 'static"
    )]
    #[allow(unused_mut)]
    pub async fn create_stream(
        &self,
        mut request: CreateCompletionRequest,
    ) -> Result<CompletionResponseStream, OpenAIError> {
        #[cfg(not(feature = "byot"))]
        {
            if request.stream.is_some() && !request.stream.unwrap() {
                return Err(OpenAIError::InvalidArgument(
                    "When stream is false, use Completion::create".into(),
                ));
            }

            request.stream = Some(true);
        }
        Ok(self.client.post_stream("/completions", request).await)
    }
}
