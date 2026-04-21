use derive_builder::Builder;
use serde::{Deserialize, Serialize};

use crate::error::OpenAIError;

#[derive(Debug, Serialize, Clone, PartialEq, Deserialize)]
#[serde(untagged)]
pub enum ModerationInput {
    /// A single string of text to classify for moderation
    String(String),

    /// An array of strings to classify for moderation
    StringArray(Vec<String>),

    /// An array of multi-modal inputs to the moderation model
    MultiModal(Vec<ModerationContentPart>),
}

/// Content part for multi-modal moderation input
#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
#[serde(tag = "type")]
pub enum ModerationContentPart {
    /// An object describing text to classify
    #[serde(rename = "text")]
    Text {
        /// A string of text to classify
        text: String,
    },

    /// An object describing an image to classify
    #[serde(rename = "image_url")]
    ImageUrl {
        /// Contains either an image URL or a data URL for a base64 encoded image
        image_url: ModerationImageUrl,
    },
}

/// Image URL configuration for image moderation
#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
pub struct ModerationImageUrl {
    /// Either a URL of the image or the base64 encoded image data
    pub url: String,
}

#[derive(Debug, Default, Clone, Serialize, Builder, PartialEq, Deserialize)]
#[builder(name = "CreateModerationRequestArgs")]
#[builder(pattern = "mutable")]
#[builder(setter(into, strip_option), default)]
#[builder(derive(Debug))]
#[builder(build_fn(error = "OpenAIError"))]
pub struct CreateModerationRequest {
    /// Input (or inputs) to classify. Can be a single string, an array of strings, or
    /// an array of multi-modal input objects similar to other models.
    pub input: ModerationInput,

    /// The content moderation model you would like to use. Learn more in the
    /// [moderation guide](https://platform.openai.com/docs/guides/moderation), and learn about
    /// available models [here](https://platform.openai.com/docs/models/moderation).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub model: Option<String>,
}

#[derive(Debug, Deserialize, Serialize, Clone, PartialEq)]
pub struct Category {
    /// Content that expresses, incites, or promotes hate based on race, gender,
    /// ethnicity, religion, nationality, sexual orientation, disability status, or
    /// caste. Hateful content aimed at non-protected groups (e.g., chess players)
    /// is harrassment.
    pub hate: bool,
    #[serde(rename = "hate/threatening")]
    /// Hateful content that also includes violence or serious harm towards the
    /// targeted group based on race, gender, ethnicity, religion, nationality,
    /// sexual orientation, disability status, or caste.
    pub hate_threatening: bool,
    /// Content that expresses, incites, or promotes harassing language towards any target.
    pub harassment: bool,
    /// Harassment content that also includes violence or serious harm towards any target.
    #[serde(rename = "harassment/threatening")]
    pub harassment_threatening: bool,
    /// Content that includes instructions or advice that facilitate the planning or execution of wrongdoing, or that gives advice or instruction on how to commit illicit acts. For example, "how to shoplift" would fit this category.
    pub illicit: bool,
    /// Content that includes instructions or advice that facilitate the planning or execution of wrongdoing that also includes violence, or that gives advice or instruction on the procurement of any weapon.
    #[serde(rename = "illicit/violent")]
    pub illicit_violent: bool,
    /// Content that promotes, encourages, or depicts acts of self-harm, such as suicide, cutting, and eating disorders.
    #[serde(rename = "self-harm")]
    pub self_harm: bool,
    /// Content where the speaker expresses that they are engaging or intend to engage in acts of self-harm, such as suicide, cutting, and eating disorders.
    #[serde(rename = "self-harm/intent")]
    pub self_harm_intent: bool,
    /// Content that encourages performing acts of self-harm, such as suicide, cutting, and eating disorders, or that gives instructions or advice on how to commit such acts.
    #[serde(rename = "self-harm/instructions")]
    pub self_harm_instructions: bool,
    /// Content meant to arouse sexual excitement, such as the description of sexual activity, or that promotes sexual services (excluding sex education and wellness).
    pub sexual: bool,
    /// Sexual content that includes an individual who is under 18 years old.
    #[serde(rename = "sexual/minors")]
    pub sexual_minors: bool,
    /// Content that depicts death, violence, or physical injury.
    pub violence: bool,
    /// Content that depicts death, violence, or physical injury in graphic detail.
    #[serde(rename = "violence/graphic")]
    pub violence_graphic: bool,
}

/// A list of the categories along with their scores as predicted by model.
#[derive(Debug, Deserialize, Serialize, Clone, PartialEq)]
pub struct CategoryScore {
    /// The score for the category 'hate'.
    pub hate: f32,
    /// The score for the category 'hate/threatening'.
    #[serde(rename = "hate/threatening")]
    pub hate_threatening: f32,
    /// The score for the category 'harassment'.
    pub harassment: f32,
    /// The score for the category 'harassment/threatening'.
    #[serde(rename = "harassment/threatening")]
    pub harassment_threatening: f32,
    /// The score for the category 'illicit'.
    pub illicit: f32,
    /// The score for the category 'illicit/violent'.
    #[serde(rename = "illicit/violent")]
    pub illicit_violent: f32,
    /// The score for the category 'self-harm'.
    #[serde(rename = "self-harm")]
    pub self_harm: f32,
    /// The score for the category 'self-harm/intent'.
    #[serde(rename = "self-harm/intent")]
    pub self_harm_intent: f32,
    /// The score for the category 'self-harm/instructions'.
    #[serde(rename = "self-harm/instructions")]
    pub self_harm_instructions: f32,
    /// The score for the category 'sexual'.
    pub sexual: f32,
    /// The score for the category 'sexual/minors'.
    #[serde(rename = "sexual/minors")]
    pub sexual_minors: f32,
    /// The score for the category 'violence'.
    pub violence: f32,
    /// The score for the category 'violence/graphic'.
    #[serde(rename = "violence/graphic")]
    pub violence_graphic: f32,
}

#[derive(Debug, Deserialize, Serialize, Clone, PartialEq)]
pub struct ContentModerationResult {
    /// Whether any of the below categories are flagged.
    pub flagged: bool,
    /// A list of the categories, and whether they are flagged or not.
    pub categories: Category,
    /// A list of the categories along with their scores as predicted by model.
    pub category_scores: CategoryScore,
    /// A list of the categories along with the input type(s) that the score applies to.
    pub category_applied_input_types: CategoryAppliedInputTypes,
}

/// Represents if a given text input is potentially harmful.
#[derive(Debug, Deserialize, Serialize, Clone, PartialEq)]
pub struct CreateModerationResponse {
    /// The unique identifier for the moderation request.
    pub id: String,
    /// The model used to generate the moderation results.
    pub model: String,
    /// A list of moderation objects.
    pub results: Vec<ContentModerationResult>,
}

/// A list of the categories along with the input type(s) that the score applies to.
#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
pub struct CategoryAppliedInputTypes {
    /// The applied input type(s) for the category 'hate'.
    pub hate: Vec<ModInputType>,

    /// The applied input type(s) for the category 'hate/threatening'.
    #[serde(rename = "hate/threatening")]
    pub hate_threatening: Vec<ModInputType>,

    /// The applied input type(s) for the category 'harassment'.
    pub harassment: Vec<ModInputType>,

    /// The applied input type(s) for the category 'harassment/threatening'.
    #[serde(rename = "harassment/threatening")]
    pub harassment_threatening: Vec<ModInputType>,

    /// The applied input type(s) for the category 'illicit'.
    pub illicit: Vec<ModInputType>,

    /// The applied input type(s) for the category 'illicit/violent'.
    #[serde(rename = "illicit/violent")]
    pub illicit_violent: Vec<ModInputType>,

    /// The applied input type(s) for the category 'self-harm'.
    #[serde(rename = "self-harm")]
    pub self_harm: Vec<ModInputType>,

    /// The applied input type(s) for the category 'self-harm/intent'.
    #[serde(rename = "self-harm/intent")]
    pub self_harm_intent: Vec<ModInputType>,

    /// The applied input type(s) for the category 'self-harm/instructions'.
    #[serde(rename = "self-harm/instructions")]
    pub self_harm_instructions: Vec<ModInputType>,

    /// The applied input type(s) for the category 'sexual'.
    pub sexual: Vec<ModInputType>,

    /// The applied input type(s) for the category 'sexual/minors'.
    #[serde(rename = "sexual/minors")]
    pub sexual_minors: Vec<ModInputType>,

    /// The applied input type(s) for the category 'violence'.
    pub violence: Vec<ModInputType>,

    /// The applied input type(s) for the category 'violence/graphic'.
    #[serde(rename = "violence/graphic")]
    pub violence_graphic: Vec<ModInputType>,
}

/// The type of input that was moderated
#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum ModInputType {
    /// Text content that was moderated
    Text,
    /// Image content that was moderated
    Image,
}
