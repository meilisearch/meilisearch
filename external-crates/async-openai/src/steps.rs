use serde::Serialize;

use crate::{
    config::Config,
    error::OpenAIError,
    types::{ListRunStepsResponse, RunStepObject},
    Client,
};

/// Represents a step in execution of a run.
pub struct Steps<'c, C: Config> {
    pub thread_id: String,
    pub run_id: String,
    client: &'c Client<C>,
}

impl<'c, C: Config> Steps<'c, C> {
    pub fn new(client: &'c Client<C>, thread_id: &str, run_id: &str) -> Self {
        Self {
            client,
            thread_id: thread_id.into(),
            run_id: run_id.into(),
        }
    }

    /// Retrieves a run step.
    #[crate::byot(T0 = std::fmt::Display, R = serde::de::DeserializeOwned)]
    pub async fn retrieve(&self, step_id: &str) -> Result<RunStepObject, OpenAIError> {
        self.client
            .get(&format!(
                "/threads/{}/runs/{}/steps/{step_id}",
                self.thread_id, self.run_id
            ))
            .await
    }

    /// Returns a list of run steps belonging to a run.
    #[crate::byot(T0 = serde::Serialize, R = serde::de::DeserializeOwned)]
    pub async fn list<Q>(&self, query: &Q) -> Result<ListRunStepsResponse, OpenAIError>
    where
        Q: Serialize + ?Sized,
    {
        self.client
            .get_with_query(
                &format!("/threads/{}/runs/{}/steps", self.thread_id, self.run_id),
                &query,
            )
            .await
    }
}
