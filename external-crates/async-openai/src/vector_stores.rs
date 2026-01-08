use serde::Serialize;

use crate::{
    config::Config,
    error::OpenAIError,
    types::{
        CreateVectorStoreRequest, DeleteVectorStoreResponse, ListVectorStoresResponse,
        UpdateVectorStoreRequest, VectorStoreObject,
    },
    vector_store_file_batches::VectorStoreFileBatches,
    Client, VectorStoreFiles,
};

pub struct VectorStores<'c, C: Config> {
    client: &'c Client<C>,
}

impl<'c, C: Config> VectorStores<'c, C> {
    pub fn new(client: &'c Client<C>) -> Self {
        Self { client }
    }

    /// [VectorStoreFiles] API group
    pub fn files(&self, vector_store_id: &str) -> VectorStoreFiles<C> {
        VectorStoreFiles::new(self.client, vector_store_id)
    }

    /// [VectorStoreFileBatches] API group
    pub fn file_batches(&self, vector_store_id: &str) -> VectorStoreFileBatches<C> {
        VectorStoreFileBatches::new(self.client, vector_store_id)
    }

    /// Create a vector store.
    #[crate::byot(T0 = serde::Serialize, R = serde::de::DeserializeOwned)]
    pub async fn create(
        &self,
        request: CreateVectorStoreRequest,
    ) -> Result<VectorStoreObject, OpenAIError> {
        self.client.post("/vector_stores", request).await
    }

    /// Retrieves a vector store.
    #[crate::byot(T0 = std::fmt::Display, R = serde::de::DeserializeOwned)]
    pub async fn retrieve(&self, vector_store_id: &str) -> Result<VectorStoreObject, OpenAIError> {
        self.client
            .get(&format!("/vector_stores/{vector_store_id}"))
            .await
    }

    /// Returns a list of vector stores.
    #[crate::byot(T0 = serde::Serialize, R = serde::de::DeserializeOwned)]
    pub async fn list<Q>(&self, query: &Q) -> Result<ListVectorStoresResponse, OpenAIError>
    where
        Q: Serialize + ?Sized,
    {
        self.client.get_with_query("/vector_stores", &query).await
    }

    /// Delete a vector store.
    #[crate::byot(T0 = std::fmt::Display, R = serde::de::DeserializeOwned)]
    pub async fn delete(
        &self,
        vector_store_id: &str,
    ) -> Result<DeleteVectorStoreResponse, OpenAIError> {
        self.client
            .delete(&format!("/vector_stores/{vector_store_id}"))
            .await
    }

    /// Modifies a vector store.
    #[crate::byot(T0 = std::fmt::Display, T1 = serde::Serialize, R = serde::de::DeserializeOwned)]
    pub async fn update(
        &self,
        vector_store_id: &str,
        request: UpdateVectorStoreRequest,
    ) -> Result<VectorStoreObject, OpenAIError> {
        self.client
            .post(&format!("/vector_stores/{vector_store_id}"), request)
            .await
    }
}
