use crate::{
    config::Config,
    error::OpenAIError,
    types::{AddUploadPartRequest, CompleteUploadRequest, CreateUploadRequest, Upload, UploadPart},
    Client,
};

/// Allows you to upload large files in multiple parts.
pub struct Uploads<'c, C: Config> {
    client: &'c Client<C>,
}

impl<'c, C: Config> Uploads<'c, C> {
    pub fn new(client: &'c Client<C>) -> Self {
        Self { client }
    }

    /// Creates an intermediate [Upload](https://platform.openai.com/docs/api-reference/uploads/object) object that
    /// you can add [Parts](https://platform.openai.com/docs/api-reference/uploads/part-object) to. Currently,
    /// an Upload can accept at most 8 GB in total and expires after an hour after you create it.
    ///            
    /// Once you complete the Upload, we will create a [File](https://platform.openai.com/docs/api-reference/files/object)
    /// object that contains all the parts you uploaded. This File is usable in the rest of our platform as a regular File object.
    ///            
    /// For certain `purpose`s, the correct `mime_type` must be specified. Please refer to documentation for the
    /// supported MIME types for your use case:
    /// - [Assistants](https://platform.openai.com/docs/assistants/tools/file-search/supported-files)
    ///
    /// For guidance on the proper filename extensions for each purpose, please follow the documentation on
    /// [creating a File](https://platform.openai.com/docs/api-reference/files/create).
    #[crate::byot(T0 = serde::Serialize, R = serde::de::DeserializeOwned)]
    pub async fn create(&self, request: CreateUploadRequest) -> Result<Upload, OpenAIError> {
        self.client.post("/uploads", request).await
    }

    /// Adds a [Part](https://platform.openai.com/docs/api-reference/uploads/part-object) to an
    /// [Upload](https://platform.openai.com/docs/api-reference/uploads/object) object.
    /// A Part represents a chunk of bytes from the file you are trying to upload.
    ///
    /// Each Part can be at most 64 MB, and you can add Parts until you hit the Upload maximum of 8 GB.
    ///
    /// It is possible to add multiple Parts in parallel. You can decide the intended order of the Parts
    /// when you [complete the Upload](https://platform.openai.com/docs/api-reference/uploads/complete).
    #[crate::byot(
        T0 = std::fmt::Display,
        T1 = Clone,
        R = serde::de::DeserializeOwned,
        where_clause =  "reqwest::multipart::Form: crate::traits::AsyncTryFrom<T1, Error = OpenAIError>")]
    pub async fn add_part(
        &self,
        upload_id: &str,
        request: AddUploadPartRequest,
    ) -> Result<UploadPart, OpenAIError> {
        self.client
            .post_form(&format!("/uploads/{upload_id}/parts"), request)
            .await
    }

    /// Completes the [Upload](https://platform.openai.com/docs/api-reference/uploads/object).
    ///
    /// Within the returned Upload object, there is a nested [File](https://platform.openai.com/docs/api-reference/files/object)
    /// object that is ready to use in the rest of the platform.
    ///
    /// You can specify the order of the Parts by passing in an ordered list of the Part IDs.
    ///
    /// The number of bytes uploaded upon completion must match the number of bytes initially specified
    /// when creating the Upload object. No Parts may be added after an Upload is completed.

    #[crate::byot(T0 = std::fmt::Display, T1 = serde::Serialize, R = serde::de::DeserializeOwned)]
    pub async fn complete(
        &self,
        upload_id: &str,
        request: CompleteUploadRequest,
    ) -> Result<Upload, OpenAIError> {
        self.client
            .post(&format!("/uploads/{upload_id}/complete"), request)
            .await
    }

    /// Cancels the Upload. No Parts may be added after an Upload is cancelled.
    #[crate::byot(T0 = std::fmt::Display, R = serde::de::DeserializeOwned)]
    pub async fn cancel(&self, upload_id: &str) -> Result<Upload, OpenAIError> {
        self.client
            .post(
                &format!("/uploads/{upload_id}/cancel"),
                serde_json::json!({}),
            )
            .await
    }
}
