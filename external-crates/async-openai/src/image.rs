use crate::{
    config::Config,
    error::OpenAIError,
    types::{
        CreateImageEditRequest, CreateImageRequest, CreateImageVariationRequest, ImagesResponse,
    },
    Client,
};

/// Given a prompt and/or an input image, the model will generate a new image.
///
/// Related guide: [Image generation](https://platform.openai.com/docs/guides/images)
pub struct Images<'c, C: Config> {
    client: &'c Client<C>,
}

impl<'c, C: Config> Images<'c, C> {
    pub fn new(client: &'c Client<C>) -> Self {
        Self { client }
    }

    /// Creates an image given a prompt.
    #[crate::byot(T0 = serde::Serialize, R = serde::de::DeserializeOwned)]
    pub async fn create(&self, request: CreateImageRequest) -> Result<ImagesResponse, OpenAIError> {
        self.client.post("/images/generations", request).await
    }

    /// Creates an edited or extended image given an original image and a prompt.
    #[crate::byot(
        T0 = Clone,
        R = serde::de::DeserializeOwned,
        where_clause =  "reqwest::multipart::Form: crate::traits::AsyncTryFrom<T0, Error = OpenAIError>",
    )]
    pub async fn create_edit(
        &self,
        request: CreateImageEditRequest,
    ) -> Result<ImagesResponse, OpenAIError> {
        self.client.post_form("/images/edits", request).await
    }

    /// Creates a variation of a given image.
    #[crate::byot(
        T0 = Clone,
        R = serde::de::DeserializeOwned,
        where_clause =  "reqwest::multipart::Form: crate::traits::AsyncTryFrom<T0, Error = OpenAIError>",
    )]
    pub async fn create_variation(
        &self,
        request: CreateImageVariationRequest,
    ) -> Result<ImagesResponse, OpenAIError> {
        self.client.post_form("/images/variations", request).await
    }
}
