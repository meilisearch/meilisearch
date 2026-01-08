use serde::Serialize;

use crate::{
    config::Config,
    error::OpenAIError,
    types::{
        CreateVectorStoreFileBatchRequest, ListVectorStoreFilesResponse, VectorStoreFileBatchObject,
    },
    Client,
};

/// Vector store file batches represent operations to add multiple files to a vector store.
///
/// Related guide: [File Search](https://platform.openai.com/docs/assistants/tools/file-search)
pub struct VectorStoreFileBatches<'c, C: Config> {
    client: &'c Client<C>,
    pub vector_store_id: String,
}

impl<'c, C: Config> VectorStoreFileBatches<'c, C> {
    pub fn new(client: &'c Client<C>, vector_store_id: &str) -> Self {
        Self {
            client,
            vector_store_id: vector_store_id.into(),
        }
    }

    /// Create vector store file batch
    #[crate::byot(T0 = serde::Serialize, R = serde::de::DeserializeOwned)]
    pub async fn create(
        &self,
        request: CreateVectorStoreFileBatchRequest,
    ) -> Result<VectorStoreFileBatchObject, OpenAIError> {
        self.client
            .post(
                &format!("/vector_stores/{}/file_batches", &self.vector_store_id),
                request,
            )
            .await
    }

    /// Retrieves a vector store file batch.
    #[crate::byot(T0 = std::fmt::Display, R = serde::de::DeserializeOwned)]
    pub async fn retrieve(
        &self,
        batch_id: &str,
    ) -> Result<VectorStoreFileBatchObject, OpenAIError> {
        self.client
            .get(&format!(
                "/vector_stores/{}/file_batches/{batch_id}",
                &self.vector_store_id
            ))
            .await
    }

    /// Cancel a vector store file batch. This attempts to cancel the processing of files in this batch as soon as possible.
    #[crate::byot(T0 = std::fmt::Display, R = serde::de::DeserializeOwned)]
    pub async fn cancel(&self, batch_id: &str) -> Result<VectorStoreFileBatchObject, OpenAIError> {
        self.client
            .post(
                &format!(
                    "/vector_stores/{}/file_batches/{batch_id}/cancel",
                    &self.vector_store_id
                ),
                serde_json::json!({}),
            )
            .await
    }

    /// Returns a list of vector store files in a batch.
    #[crate::byot(T0 = std::fmt::Display, T1 = serde::Serialize, R = serde::de::DeserializeOwned)]
    pub async fn list<Q>(
        &self,
        batch_id: &str,
        query: &Q,
    ) -> Result<ListVectorStoreFilesResponse, OpenAIError>
    where
        Q: Serialize + ?Sized,
    {
        self.client
            .get_with_query(
                &format!(
                    "/vector_stores/{}/file_batches/{batch_id}/files",
                    &self.vector_store_id
                ),
                &query,
            )
            .await
    }
}
