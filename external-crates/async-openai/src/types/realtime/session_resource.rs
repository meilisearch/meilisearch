use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize, Clone)]
pub enum AudioFormat {
    #[serde(rename = "pcm16")]
    PCM16,
    #[serde(rename = "g711-ulaw")]
    G711ULAW,
    #[serde(rename = "g711-alaw")]
    G711ALAW,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct AudioTranscription {
    /// Whether to enable input audio transcription.
    pub enabled: bool,
    /// The model to use for transcription (e.g., "whisper-1").
    pub model: String,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(tag = "type")]
pub enum TurnDetection {
    /// Type of turn detection, only "server_vad" is currently supported.
    #[serde(rename = "server_vad")]
    ServerVAD {
        /// Activation threshold for VAD (0.0 to 1.0).
        threshold: f32,
        /// Amount of audio to include before speech starts (in milliseconds).
        prefix_padding_ms: u32,
        /// Duration of silence to detect speech stop (in milliseconds).
        silence_duration_ms: u32,
    },
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub enum MaxResponseOutputTokens {
    #[serde(rename = "inf")]
    Inf,
    #[serde(untagged)]
    Num(u16),
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(tag = "type")]
pub enum ToolDefinition {
    #[serde(rename = "function")]
    Function {
        /// The name of the function.
        name: String,
        /// The description of the function.
        description: String,
        /// Parameters of the function in JSON Schema.
        parameters: serde_json::Value,
    },
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "lowercase")]
pub enum FunctionType {
    Function,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "lowercase")]
pub enum ToolChoice {
    Auto,
    None,
    Required,
    #[serde(untagged)]
    Function {
        r#type: FunctionType,
        name: String,
    },
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "lowercase")]
pub enum RealtimeVoice {
    Alloy,
    Shimmer,
    Echo,
}

#[derive(Debug, Serialize, Deserialize, Clone, Default)]
pub struct SessionResource {
    /// The default model used for this session.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub model: Option<String>,

    /// The set of modalities the model can respond with. To disable audio, set this to ["text"].
    #[serde(skip_serializing_if = "Option::is_none")]
    pub modalities: Option<Vec<String>>,

    //// The default system instructions prepended to model calls.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub instructions: Option<String>,

    /// The voice the model uses to respond. Cannot be changed once the model has responded with audio at least once.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub voice: Option<RealtimeVoice>,

    /// The format of input audio. Options are "pcm16", "g711_ulaw", or "g711_alaw".
    #[serde(skip_serializing_if = "Option::is_none")]
    pub input_audio_format: Option<AudioFormat>,

    /// The format of output audio. Options are "pcm16", "g711_ulaw", or "g711_alaw".
    #[serde(skip_serializing_if = "Option::is_none")]
    pub output_audio_format: Option<AudioFormat>,

    /// Configuration for input audio transcription. Can be set to null to turn off.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub input_audio_transcription: Option<AudioTranscription>,

    /// Configuration for turn detection. Can be set to null to turn off.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub turn_detection: Option<TurnDetection>,

    /// Tools (functions) available to the model.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tools: Option<Vec<ToolDefinition>>,

    #[serde(skip_serializing_if = "Option::is_none")]
    /// How the model chooses tools.
    pub tool_choice: Option<ToolChoice>,

    #[serde(skip_serializing_if = "Option::is_none")]
    /// Sampling temperature for the model.
    pub temperature: Option<f32>,

    /// Maximum number of output tokens for a single assistant response, inclusive of tool calls.
    /// Provide an integer between 1 and 4096 to limit output tokens, or "inf" for the maximum available tokens for a given model.
    /// Defaults to "inf".
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_response_output_tokens: Option<MaxResponseOutputTokens>,
}
