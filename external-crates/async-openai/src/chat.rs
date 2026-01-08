use crate::{
    config::Config,
    error::OpenAIError,
    types::{
        ChatCompletionResponseStream, CreateChatCompletionRequest, CreateChatCompletionResponse,
    },
    Client,
};

/// Given a list of messages comprising a conversation, the model will return a response.
///
/// Related guide: [Chat completions](https://platform.openai.com//docs/guides/text-generation)
pub struct Chat<'c, C: Config> {
    client: &'c Client<C>,
}

impl<'c, C: Config> Chat<'c, C> {
    pub fn new(client: &'c Client<C>) -> Self {
        Self { client }
    }

    /// Creates a model response for the given chat conversation. Learn more in
    /// the
    ///
    /// [text generation](https://platform.openai.com/docs/guides/text-generation),
    /// [vision](https://platform.openai.com/docs/guides/vision),
    ///
    /// and [audio](https://platform.openai.com/docs/guides/audio) guides.
    ///
    ///
    /// Parameter support can differ depending on the model used to generate the
    /// response, particularly for newer reasoning models. Parameters that are
    /// only supported for reasoning models are noted below. For the current state
    /// of unsupported parameters in reasoning models,
    ///
    /// [refer to the reasoning guide](https://platform.openai.com/docs/guides/reasoning).
    ///
    /// byot: You must ensure "stream: false" in serialized `request`
    #[crate::byot(
        T0 = serde::Serialize,
        R = serde::de::DeserializeOwned
    )]
    pub async fn create(
        &self,
        request: CreateChatCompletionRequest,
    ) -> Result<CreateChatCompletionResponse, OpenAIError> {
        #[cfg(not(feature = "byot"))]
        {
            if request.stream.is_some() && request.stream.unwrap() {
                return Err(OpenAIError::InvalidArgument(
                    "When stream is true, use Chat::create_stream".into(),
                ));
            }
        }
        self.client.post("/chat/completions", request).await
    }

    /// Creates a completion for the chat message
    ///
    /// partial message deltas will be sent, like in ChatGPT. Tokens will be sent as data-only [server-sent events](https://developer.mozilla.org/en-US/docs/Web/API/Server-sent_events/Using_server-sent_events#Event_stream_format) as they become available, with the stream terminated by a `data: [DONE]` message.
    ///
    /// [ChatCompletionResponseStream] is a parsed SSE stream until a \[DONE\] is received from server.
    ///
    /// byot: You must ensure "stream: true" in serialized `request`
    #[crate::byot(
        T0 = serde::Serialize,
        R = serde::de::DeserializeOwned,
        stream = "true",
        where_clause = "R: std::marker::Send + 'static"
    )]
    #[allow(unused_mut)]
    pub async fn create_stream(
        &self,
        mut request: CreateChatCompletionRequest,
    ) -> Result<ChatCompletionResponseStream, OpenAIError> {
        #[cfg(not(feature = "byot"))]
        {
            if request.stream.is_some() && !request.stream.unwrap() {
                return Err(OpenAIError::InvalidArgument(
                    "When stream is false, use Chat::create".into(),
                ));
            }

            request.stream = Some(true);
        }
        Ok(self.client.post_stream("/chat/completions", request).await)
    }
}
