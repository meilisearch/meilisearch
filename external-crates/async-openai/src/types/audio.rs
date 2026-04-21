use bytes::Bytes;
use derive_builder::Builder;
use serde::{Deserialize, Serialize};

use super::InputSource;
use crate::error::OpenAIError;

#[derive(Debug, Default, Clone, PartialEq)]
pub struct AudioInput {
    pub source: InputSource,
}

#[derive(Debug, Serialize, Deserialize, Default, Clone, Copy, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum AudioResponseFormat {
    #[default]
    Json,
    Text,
    Srt,
    VerboseJson,
    Vtt,
}

#[derive(Debug, Serialize, Deserialize, Default, Clone, Copy, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum SpeechResponseFormat {
    #[default]
    Mp3,
    Opus,
    Aac,
    Flac,
    Pcm,
    Wav,
}

#[derive(Debug, Default, Serialize, Deserialize, Clone, PartialEq)]
#[serde(rename_all = "lowercase")]
#[non_exhaustive]
pub enum Voice {
    #[default]
    Alloy,
    Ash,
    Coral,
    Echo,
    Fable,
    Onyx,
    Nova,
    Sage,
    Shimmer,
}

#[derive(Debug, Default, Serialize, Deserialize, Clone, PartialEq)]
pub enum SpeechModel {
    #[default]
    #[serde(rename = "tts-1")]
    Tts1,
    #[serde(rename = "tts-1-hd")]
    Tts1Hd,
    #[serde(untagged)]
    Other(String),
}

#[derive(Debug, Default, Serialize, Deserialize, Clone, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum TimestampGranularity {
    Word,
    #[default]
    Segment,
}

#[derive(Clone, Default, Debug, Builder, PartialEq)]
#[builder(name = "CreateTranscriptionRequestArgs")]
#[builder(pattern = "mutable")]
#[builder(setter(into, strip_option), default)]
#[builder(derive(Debug))]
#[builder(build_fn(error = "OpenAIError"))]
pub struct CreateTranscriptionRequest {
    /// The audio file to transcribe, in one of these formats: mp3, mp4, mpeg, mpga, m4a, wav, or webm.
    pub file: AudioInput,

    /// ID of the model to use. Only `whisper-1` (which is powered by our open source Whisper V2 model) is currently available.
    pub model: String,

    /// An optional text to guide the model's style or continue a previous audio segment. The [prompt](https://platform.openai.com/docs/guides/speech-to-text#prompting) should match the audio language.
    pub prompt: Option<String>,

    /// The format of the transcript output, in one of these options: json, text, srt, verbose_json, or vtt.
    pub response_format: Option<AudioResponseFormat>,

    /// The sampling temperature, between 0 and 1. Higher values like 0.8 will make the output more random, while lower values like 0.2 will make it more focused and deterministic. If set to 0, the model will use [log probability](https://en.wikipedia.org/wiki/Log_probability) to automatically increase the temperature until certain thresholds are hit.
    pub temperature: Option<f32>, // default: 0

    /// The language of the input audio. Supplying the input language in [ISO-639-1](https://en.wikipedia.org/wiki/List_of_ISO_639-1_codes) format will improve accuracy and latency.
    pub language: Option<String>,

    /// The timestamp granularities to populate for this transcription. `response_format` must be set `verbose_json` to use timestamp granularities. Either or both of these options are supported: `word`, or `segment`. Note: There is no additional latency for segment timestamps, but generating word timestamps incurs additional latency.
    pub timestamp_granularities: Option<Vec<TimestampGranularity>>,
}

/// Represents a transcription response returned by model, based on the provided
/// input.
#[derive(Debug, Deserialize, Clone, Serialize)]
pub struct CreateTranscriptionResponseJson {
    /// The transcribed text.
    pub text: String,
}

/// Represents a verbose json transcription response returned by model, based on
/// the provided input.
#[derive(Debug, Deserialize, Clone, Serialize)]
pub struct CreateTranscriptionResponseVerboseJson {
    /// The language of the input audio.
    pub language: String,

    /// The duration of the input audio.
    pub duration: f32,

    /// The transcribed text.
    pub text: String,

    /// Extracted words and their corresponding timestamps.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub words: Option<Vec<TranscriptionWord>>,

    /// Segments of the transcribed text and their corresponding details.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub segments: Option<Vec<TranscriptionSegment>>,
}

#[derive(Debug, Deserialize, Clone, Serialize)]
pub struct TranscriptionWord {
    /// The text content of the word.
    pub word: String,

    /// Start time of the word in seconds.
    pub start: f32,

    /// End time of the word in seconds.
    pub end: f32,
}

#[derive(Debug, Deserialize, Clone, Serialize)]
pub struct TranscriptionSegment {
    /// Unique identifier of the segment.
    pub id: i32,

    // Seek offset of the segment.
    pub seek: i32,

    /// Start time of the segment in seconds.
    pub start: f32,

    /// End time of the segment in seconds.
    pub end: f32,

    /// Text content of the segment.
    pub text: String,

    /// Array of token IDs for the text content.
    pub tokens: Vec<i32>,

    /// Temperature parameter used for generating the segment.
    pub temperature: f32,

    /// Average logprob of the segment. If the value is lower than -1, consider
    /// the logprobs failed.
    pub avg_logprob: f32,

    /// Compression ratio of the segment. If the value is greater than 2.4,
    /// consider the compression failed.
    pub compression_ratio: f32,

    /// Probability of no speech in the segment. If the value is higher than 1.0
    /// and the `avg_logprob` is below -1, consider this segment silent.
    pub no_speech_prob: f32,
}

#[derive(Clone, Default, Debug, Builder, PartialEq, Serialize, Deserialize)]
#[builder(name = "CreateSpeechRequestArgs")]
#[builder(pattern = "mutable")]
#[builder(setter(into, strip_option), default)]
#[builder(derive(Debug))]
#[builder(build_fn(error = "OpenAIError"))]
pub struct CreateSpeechRequest {
    /// The text to generate audio for. The maximum length is 4096 characters.
    pub input: String,

    /// One of the available [TTS models](https://platform.openai.com/docs/models/tts): `tts-1` or `tts-1-hd`
    pub model: SpeechModel,

    /// The voice to use when generating the audio. Supported voices are `alloy`, `ash`, `coral`, `echo`, `fable`, `onyx`, `nova`, `sage` and `shimmer`.
    /// Previews of the voices are available in the [Text to speech guide](https://platform.openai.com/docs/guides/text-to-speech#voice-options).
    pub voice: Voice,

    /// The format to audio in. Supported formats are `mp3`, `opus`, `aac`, `flac`, `wav`, and `pcm`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub response_format: Option<SpeechResponseFormat>,

    /// The speed of the generated audio. Select a value from 0.25 to 4.0. 1.0 is the default.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub speed: Option<f32>, // default: 1.0
}

#[derive(Clone, Default, Debug, Builder, PartialEq)]
#[builder(name = "CreateTranslationRequestArgs")]
#[builder(pattern = "mutable")]
#[builder(setter(into, strip_option), default)]
#[builder(derive(Debug))]
#[builder(build_fn(error = "OpenAIError"))]
pub struct CreateTranslationRequest {
    /// The audio file object (not file name) translate, in one of these
    ///formats: flac, mp3, mp4, mpeg, mpga, m4a, ogg, wav, or webm.
    pub file: AudioInput,

    /// ID of the model to use. Only `whisper-1` (which is powered by our open source Whisper V2 model) is currently available.
    pub model: String,

    /// An optional text to guide the model's style or continue a previous audio segment. The [prompt](https://platform.openai.com/docs/guides/speech-to-text#prompting) should be in English.
    pub prompt: Option<String>,

    /// The format of the transcript output, in one of these options: json, text, srt, verbose_json, or vtt.
    pub response_format: Option<AudioResponseFormat>,

    /// The sampling temperature, between 0 and 1. Higher values like 0.8 will make the output more random, while lower values like 0.2 will make it more focused and deterministic. If set to 0, the model will use [log probability](https://en.wikipedia.org/wiki/Log_probability) to automatically increase the temperature until certain thresholds are hit.
    pub temperature: Option<f32>, // default: 0
}

#[derive(Debug, Deserialize, Clone, PartialEq, Serialize)]
pub struct CreateTranslationResponseJson {
    pub text: String,
}

#[derive(Debug, Deserialize, Clone, Serialize)]
pub struct CreateTranslationResponseVerboseJson {
    /// The language of the output translation (always `english`).
    pub language: String,
    /// The duration of the input audio.
    pub duration: String,
    /// The translated text.
    pub text: String,
    /// Segments of the translated text and their corresponding details.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub segments: Option<Vec<TranscriptionSegment>>,
}

#[derive(Debug, Clone)]
pub struct CreateSpeechResponse {
    pub bytes: Bytes,
}
