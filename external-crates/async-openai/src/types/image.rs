use derive_builder::Builder;
use serde::{Deserialize, Serialize};

use crate::error::OpenAIError;

use super::InputSource;

#[derive(Default, Debug, Serialize, Deserialize, Clone, Copy, PartialEq)]
pub enum ImageSize {
    #[serde(rename = "256x256")]
    S256x256,
    #[serde(rename = "512x512")]
    S512x512,
    #[default]
    #[serde(rename = "1024x1024")]
    S1024x1024,
    #[serde(rename = "1792x1024")]
    S1792x1024,
    #[serde(rename = "1024x1792")]
    S1024x1792,
}

#[derive(Default, Debug, Serialize, Deserialize, Clone, Copy, PartialEq)]
pub enum DallE2ImageSize {
    #[serde(rename = "256x256")]
    S256x256,
    #[serde(rename = "512x512")]
    S512x512,
    #[default]
    #[serde(rename = "1024x1024")]
    S1024x1024,
}

#[derive(Debug, Serialize, Deserialize, Default, Clone, Copy, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum ImageResponseFormat {
    #[default]
    Url,
    #[serde(rename = "b64_json")]
    B64Json,
}

#[derive(Debug, Serialize, Deserialize, Default, Clone, PartialEq)]
pub enum ImageModel {
    #[default]
    #[serde(rename = "dall-e-2")]
    DallE2,
    #[serde(rename = "dall-e-3")]
    DallE3,
    #[serde(untagged)]
    Other(String),
}

#[derive(Debug, Serialize, Deserialize, Default, Clone, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum ImageQuality {
    #[default]
    Standard,
    HD,
}

#[derive(Debug, Serialize, Deserialize, Default, Clone, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum ImageStyle {
    #[default]
    Vivid,
    Natural,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default, Builder, PartialEq)]
#[builder(name = "CreateImageRequestArgs")]
#[builder(pattern = "mutable")]
#[builder(setter(into, strip_option), default)]
#[builder(derive(Debug))]
#[builder(build_fn(error = "OpenAIError"))]
pub struct CreateImageRequest {
    /// A text description of the desired image(s). The maximum length is 1000 characters for `dall-e-2`
    /// and 4000 characters for `dall-e-3`.
    pub prompt: String,

    /// The model to use for image generation.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub model: Option<ImageModel>,

    /// The number of images to generate. Must be between 1 and 10. For `dall-e-3`, only `n=1` is supported.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub n: Option<u8>, // min:1 max:10 default:1

    /// The quality of the image that will be generated. `hd` creates images with finer details and greater
    /// consistency across the image. This param is only supported for `dall-e-3`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub quality: Option<ImageQuality>,

    /// The format in which the generated images are returned. Must be one of `url` or `b64_json`. URLs are only valid for 60 minutes after the image has been generated.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub response_format: Option<ImageResponseFormat>,

    /// The size of the generated images. Must be one of `256x256`, `512x512`, or `1024x1024` for `dall-e-2`.
    /// Must be one of `1024x1024`, `1792x1024`, or `1024x1792` for `dall-e-3` models.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub size: Option<ImageSize>,

    /// The style of the generated images. Must be one of `vivid` or `natural`.
    /// Vivid causes the model to lean towards generating hyper-real and dramatic images.
    /// Natural causes the model to produce more natural, less hyper-real looking images.
    /// This param is only supported for `dall-e-3`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub style: Option<ImageStyle>,

    /// A unique identifier representing your end-user, which will help OpenAI to monitor and detect abuse. [Learn more](https://platform.openai.com/docs/usage-policies/end-user-ids).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub user: Option<String>,
}

#[derive(Debug, Deserialize, Serialize, Clone, PartialEq)]
#[serde(untagged)]
pub enum Image {
    /// The URL of the generated image, if `response_format` is `url` (default).
    Url {
        url: String,
        revised_prompt: Option<String>,
    },
    /// The base64-encoded JSON of the generated image, if `response_format` is `b64_json`.
    B64Json {
        b64_json: std::sync::Arc<String>,
        revised_prompt: Option<String>,
    },
}

#[derive(Debug, Deserialize, Serialize, Clone, PartialEq)]
pub struct ImagesResponse {
    pub created: u32,
    pub data: Vec<std::sync::Arc<Image>>,
}

#[derive(Debug, Default, Clone, PartialEq)]
pub struct ImageInput {
    pub source: InputSource,
}

#[derive(Debug, Clone, Default, Builder, PartialEq)]
#[builder(name = "CreateImageEditRequestArgs")]
#[builder(pattern = "mutable")]
#[builder(setter(into, strip_option), default)]
#[builder(derive(Debug))]
#[builder(build_fn(error = "OpenAIError"))]
pub struct CreateImageEditRequest {
    /// The image to edit. Must be a valid PNG file, less than 4MB, and square. If mask is not provided, image must have transparency, which will be used as the mask.
    pub image: ImageInput,

    /// A text description of the desired image(s). The maximum length is 1000 characters.
    pub prompt: String,

    /// An additional image whose fully transparent areas (e.g. where alpha is zero) indicate where `image` should be edited. Must be a valid PNG file, less than 4MB, and have the same dimensions as `image`.
    pub mask: Option<ImageInput>,

    /// The model to use for image generation. Only `dall-e-2` is supported at this time.
    pub model: Option<ImageModel>,

    /// The number of images to generate. Must be between 1 and 10.
    pub n: Option<u8>, // min:1 max:10 default:1

    /// The size of the generated images. Must be one of `256x256`, `512x512`, or `1024x1024`.
    pub size: Option<DallE2ImageSize>,

    /// The format in which the generated images are returned. Must be one of `url` or `b64_json`.
    pub response_format: Option<ImageResponseFormat>,

    /// A unique identifier representing your end-user, which will help OpenAI to monitor and detect abuse. [Learn more](https://platform.openai.com/docs/usage-policies/end-user-ids).
    pub user: Option<String>,
}

#[derive(Debug, Default, Clone, Builder, PartialEq)]
#[builder(name = "CreateImageVariationRequestArgs")]
#[builder(pattern = "mutable")]
#[builder(setter(into, strip_option), default)]
#[builder(derive(Debug))]
#[builder(build_fn(error = "OpenAIError"))]
pub struct CreateImageVariationRequest {
    /// The image to use as the basis for the variation(s). Must be a valid PNG file, less than 4MB, and square.
    pub image: ImageInput,

    /// The model to use for image generation. Only `dall-e-2` is supported at this time.
    pub model: Option<ImageModel>,

    /// The number of images to generate. Must be between 1 and 10.
    pub n: Option<u8>, // min:1 max:10 default:1

    /// The size of the generated images. Must be one of `256x256`, `512x512`, or `1024x1024`.
    pub size: Option<DallE2ImageSize>,

    /// The format in which the generated images are returned. Must be one of `url` or `b64_json`.
    pub response_format: Option<ImageResponseFormat>,

    /// A unique identifier representing your end-user, which will help OpenAI to monitor and detect abuse. [Learn more](https://platform.openai.com/docs/usage-policies/end-user-ids).
    pub user: Option<String>,
}
