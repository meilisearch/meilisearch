use crate::{
    config::Config,
    error::OpenAIError,
    types::{
        AssistantEventStream, CreateThreadAndRunRequest, CreateThreadRequest, DeleteThreadResponse,
        ModifyThreadRequest, RunObject, ThreadObject,
    },
    Client, Messages, Runs,
};

/// Create threads that assistants can interact with.
///
/// Related guide: [Assistants](https://platform.openai.com/docs/assistants/overview)
pub struct Threads<'c, C: Config> {
    client: &'c Client<C>,
}

impl<'c, C: Config> Threads<'c, C> {
    pub fn new(client: &'c Client<C>) -> Self {
        Self { client }
    }

    /// Call [Messages] group API to manage message in [thread_id] thread.
    pub fn messages(&self, thread_id: &str) -> Messages<C> {
        Messages::new(self.client, thread_id)
    }

    /// Call [Runs] group API to manage runs in [thread_id] thread.
    pub fn runs(&self, thread_id: &str) -> Runs<C> {
        Runs::new(self.client, thread_id)
    }

    /// Create a thread and run it in one request.
    #[crate::byot(T0 = serde::Serialize, R = serde::de::DeserializeOwned)]
    pub async fn create_and_run(
        &self,
        request: CreateThreadAndRunRequest,
    ) -> Result<RunObject, OpenAIError> {
        self.client.post("/threads/runs", request).await
    }

    /// Create a thread and run it in one request (streaming).
    ///
    /// byot: You must ensure "stream: true" in serialized `request`
    #[crate::byot(
        T0 = serde::Serialize,
        R = serde::de::DeserializeOwned,
        stream = "true",
        where_clause = "R: std::marker::Send + 'static + TryFrom<eventsource_stream::Event, Error = OpenAIError>"
    )]
    #[allow(unused_mut)]
    pub async fn create_and_run_stream(
        &self,
        mut request: CreateThreadAndRunRequest,
    ) -> Result<AssistantEventStream, OpenAIError> {
        #[cfg(not(feature = "byot"))]
        {
            if request.stream.is_some() && !request.stream.unwrap() {
                return Err(OpenAIError::InvalidArgument(
                    "When stream is false, use Threads::create_and_run".into(),
                ));
            }

            request.stream = Some(true);
        }
        Ok(self
            .client
            .post_stream_mapped_raw_events("/threads/runs", request, TryFrom::try_from)
            .await)
    }

    /// Create a thread.
    #[crate::byot(T0 = serde::Serialize, R = serde::de::DeserializeOwned)]
    pub async fn create(&self, request: CreateThreadRequest) -> Result<ThreadObject, OpenAIError> {
        self.client.post("/threads", request).await
    }

    /// Retrieves a thread.
    #[crate::byot(T0 = std::fmt::Display, R = serde::de::DeserializeOwned)]
    pub async fn retrieve(&self, thread_id: &str) -> Result<ThreadObject, OpenAIError> {
        self.client.get(&format!("/threads/{thread_id}")).await
    }

    /// Modifies a thread.
    #[crate::byot(T0 = std::fmt::Display, T1 = serde::Serialize, R = serde::de::DeserializeOwned)]
    pub async fn update(
        &self,
        thread_id: &str,
        request: ModifyThreadRequest,
    ) -> Result<ThreadObject, OpenAIError> {
        self.client
            .post(&format!("/threads/{thread_id}"), request)
            .await
    }

    /// Delete a thread.
    #[crate::byot(T0 = std::fmt::Display, R = serde::de::DeserializeOwned)]
    pub async fn delete(&self, thread_id: &str) -> Result<DeleteThreadResponse, OpenAIError> {
        self.client.delete(&format!("/threads/{thread_id}")).await
    }
}
