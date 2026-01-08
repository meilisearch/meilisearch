use serde::Serialize;

use crate::{
    config::Config,
    error::OpenAIError,
    steps::Steps,
    types::{
        AssistantEventStream, CreateRunRequest, ListRunsResponse, ModifyRunRequest, RunObject,
        SubmitToolOutputsRunRequest,
    },
    Client,
};

/// Represents an execution run on a thread.
///
/// Related guide: [Assistants](https://platform.openai.com/docs/assistants/overview)
pub struct Runs<'c, C: Config> {
    pub thread_id: String,
    client: &'c Client<C>,
}

impl<'c, C: Config> Runs<'c, C> {
    pub fn new(client: &'c Client<C>, thread_id: &str) -> Self {
        Self {
            client,
            thread_id: thread_id.into(),
        }
    }

    ///  [Steps] API group
    pub fn steps(&self, run_id: &str) -> Steps<C> {
        Steps::new(self.client, &self.thread_id, run_id)
    }

    /// Create a run.
    #[crate::byot(T0 = serde::Serialize, R = serde::de::DeserializeOwned)]
    pub async fn create(&self, request: CreateRunRequest) -> Result<RunObject, OpenAIError> {
        self.client
            .post(&format!("/threads/{}/runs", self.thread_id), request)
            .await
    }

    /// Create a run.
    ///
    /// byot: You must ensure "stream: true" in serialized `request`
    #[crate::byot(
        T0 = serde::Serialize,
        R = serde::de::DeserializeOwned,
        stream = "true",
        where_clause = "R: std::marker::Send + 'static + TryFrom<eventsource_stream::Event, Error = OpenAIError>"
    )]
    #[allow(unused_mut)]
    pub async fn create_stream(
        &self,
        mut request: CreateRunRequest,
    ) -> Result<AssistantEventStream, OpenAIError> {
        #[cfg(not(feature = "byot"))]
        {
            if request.stream.is_some() && !request.stream.unwrap() {
                return Err(OpenAIError::InvalidArgument(
                    "When stream is false, use Runs::create".into(),
                ));
            }

            request.stream = Some(true);
        }

        Ok(self
            .client
            .post_stream_mapped_raw_events(
                &format!("/threads/{}/runs", self.thread_id),
                request,
                TryFrom::try_from,
            )
            .await)
    }

    /// Retrieves a run.
    #[crate::byot(T0 = std::fmt::Display, R = serde::de::DeserializeOwned)]
    pub async fn retrieve(&self, run_id: &str) -> Result<RunObject, OpenAIError> {
        self.client
            .get(&format!("/threads/{}/runs/{run_id}", self.thread_id))
            .await
    }

    /// Modifies a run.
    #[crate::byot(T0 = std::fmt::Display, T1 = serde::Serialize, R = serde::de::DeserializeOwned)]
    pub async fn update(
        &self,
        run_id: &str,
        request: ModifyRunRequest,
    ) -> Result<RunObject, OpenAIError> {
        self.client
            .post(
                &format!("/threads/{}/runs/{run_id}", self.thread_id),
                request,
            )
            .await
    }

    /// Returns a list of runs belonging to a thread.
    #[crate::byot(T0 = serde::Serialize, R = serde::de::DeserializeOwned)]
    pub async fn list<Q>(&self, query: &Q) -> Result<ListRunsResponse, OpenAIError>
    where
        Q: Serialize + ?Sized,
    {
        self.client
            .get_with_query(&format!("/threads/{}/runs", self.thread_id), &query)
            .await
    }

    /// When a run has the status: "requires_action" and required_action.type is submit_tool_outputs, this endpoint can be used to submit the outputs from the tool calls once they're all completed. All outputs must be submitted in a single request.
    #[crate::byot(T0 = std::fmt::Display, T1 = serde::Serialize, R = serde::de::DeserializeOwned)]
    pub async fn submit_tool_outputs(
        &self,
        run_id: &str,
        request: SubmitToolOutputsRunRequest,
    ) -> Result<RunObject, OpenAIError> {
        self.client
            .post(
                &format!(
                    "/threads/{}/runs/{run_id}/submit_tool_outputs",
                    self.thread_id
                ),
                request,
            )
            .await
    }

    /// byot: You must ensure "stream: true" in serialized `request`
    #[crate::byot(
        T0 = std::fmt::Display,
        T1 = serde::Serialize,
        R = serde::de::DeserializeOwned,
        stream = "true",
        where_clause = "R: std::marker::Send + 'static + TryFrom<eventsource_stream::Event, Error = OpenAIError>"
    )]
    #[allow(unused_mut)]
    pub async fn submit_tool_outputs_stream(
        &self,
        run_id: &str,
        mut request: SubmitToolOutputsRunRequest,
    ) -> Result<AssistantEventStream, OpenAIError> {
        #[cfg(not(feature = "byot"))]
        {
            if request.stream.is_some() && !request.stream.unwrap() {
                return Err(OpenAIError::InvalidArgument(
                    "When stream is false, use Runs::submit_tool_outputs".into(),
                ));
            }

            request.stream = Some(true);
        }

        Ok(self
            .client
            .post_stream_mapped_raw_events(
                &format!(
                    "/threads/{}/runs/{run_id}/submit_tool_outputs",
                    self.thread_id
                ),
                request,
                TryFrom::try_from,
            )
            .await)
    }

    /// Cancels a run that is `in_progress`
    #[crate::byot(T0 = std::fmt::Display, R = serde::de::DeserializeOwned)]
    pub async fn cancel(&self, run_id: &str) -> Result<RunObject, OpenAIError> {
        self.client
            .post(
                &format!("/threads/{}/runs/{run_id}/cancel", self.thread_id),
                (),
            )
            .await
    }
}
