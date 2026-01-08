use serde::Serialize;

use crate::{
    config::Config,
    error::OpenAIError,
    types::{
        CreateVectorStoreFileRequest, DeleteVectorStoreFileResponse, ListVectorStoreFilesResponse,
        VectorStoreFileObject,
    },
    Client,
};

/// Vector store files represent files inside a vector store.
///
/// Related guide: [File Search](https://platform.openai.com/docs/assistants/tools/file-search)
pub struct VectorStoreFiles<'c, C: Config> {
    client: &'c Client<C>,
    pub vector_store_id: String,
}

impl<'c, C: Config> VectorStoreFiles<'c, C> {
    pub fn new(client: &'c Client<C>, vector_store_id: &str) -> Self {
        Self {
            client,
            vector_store_id: vector_store_id.into(),
        }
    }

    /// Create a vector store file by attaching a [File](https://platform.openai.com/docs/api-reference/files) to a [vector store](https://platform.openai.com/docs/api-reference/vector-stores/object).
    #[crate::byot(T0 = serde::Serialize, R = serde::de::DeserializeOwned)]
    pub async fn create(
        &self,
        request: CreateVectorStoreFileRequest,
    ) -> Result<VectorStoreFileObject, OpenAIError> {
        self.client
            .post(
                &format!("/vector_stores/{}/files", &self.vector_store_id),
                request,
            )
            .await
    }

    /// Retrieves a vector store file.
    #[crate::byot(T0 = std::fmt::Display, R = serde::de::DeserializeOwned)]
    pub async fn retrieve(&self, file_id: &str) -> Result<VectorStoreFileObject, OpenAIError> {
        self.client
            .get(&format!(
                "/vector_stores/{}/files/{file_id}",
                &self.vector_store_id
            ))
            .await
    }

    /// Delete a vector store file. This will remove the file from the vector store but the file itself will not be deleted. To delete the file, use the [delete file](https://platform.openai.com/docs/api-reference/files/delete) endpoint.
    #[crate::byot(T0 = std::fmt::Display, R = serde::de::DeserializeOwned)]
    pub async fn delete(
        &self,
        file_id: &str,
    ) -> Result<DeleteVectorStoreFileResponse, OpenAIError> {
        self.client
            .delete(&format!(
                "/vector_stores/{}/files/{file_id}",
                &self.vector_store_id
            ))
            .await
    }

    /// Returns a list of vector store files.
    #[crate::byot(T0 = serde::Serialize, R = serde::de::DeserializeOwned)]
    pub async fn list<Q>(&self, query: &Q) -> Result<ListVectorStoreFilesResponse, OpenAIError>
    where
        Q: Serialize + ?Sized,
    {
        self.client
            .get_with_query(
                &format!("/vector_stores/{}/files", &self.vector_store_id),
                &query,
            )
            .await
    }
}

#[cfg(test)]
mod tests {
    use crate::types::{CreateFileRequest, CreateVectorStoreRequest, FileInput, FilePurpose};
    use crate::Client;

    #[tokio::test]
    async fn vector_store_file_creation_and_deletion(
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let client = Client::new();

        // Create a file
        let file_handle = client
            .files()
            .create(CreateFileRequest {
                file: FileInput::from_vec_u8(
                    String::from("meow.txt"),
                    String::from(":3").into_bytes(),
                ),
                purpose: FilePurpose::Assistants,
            })
            .await?;

        // Create a vector store
        let vector_store_handle = client
            .vector_stores()
            .create(CreateVectorStoreRequest {
                file_ids: Some(vec![file_handle.id.clone()]),
                name: None,
                expires_after: None,
                chunking_strategy: None,
                metadata: None,
            })
            .await?;
        let vector_store_file = client
            .vector_stores()
            .files(&vector_store_handle.id)
            .retrieve(&file_handle.id)
            .await?;

        assert_eq!(vector_store_file.id, file_handle.id);
        // Delete the vector store
        client
            .vector_stores()
            .delete(&vector_store_handle.id)
            .await?;

        // Delete the file
        client.files().delete(&file_handle.id).await?;

        Ok(())
    }
}
