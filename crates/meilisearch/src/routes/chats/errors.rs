use async_openai::error::{ApiError, OpenAIError};
use async_openai::reqwest_eventsource::Error as EventSourceError;
use meilisearch_types::error::ResponseError;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// The error type which is always `error`.
const ERROR_TYPE: &str = "error";

/// The error struct returned by the Mistral API.
///
/// ```json
/// {
///   "object": "error",
///   "message": "Service tier capacity exceeded for this model.",
///   "type": "invalid_request_error",
///   "param": null,
///   "code": null
/// }
/// ```
#[derive(Debug, Clone, Deserialize)]
pub struct MistralError {
    message: String,
    r#type: String,
    param: Option<String>,
    code: Option<String>,
}

impl From<MistralError> for StreamErrorEvent {
    fn from(error: MistralError) -> Self {
        let MistralError { message, r#type, param, code } = error;
        StreamErrorEvent {
            event_id: Uuid::new_v4().to_string(),
            r#type: ERROR_TYPE.to_owned(),
            error: StreamError { r#type, code, message, param, event_id: None },
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct OpenAiOutsideError {
    /// Emitted when an error occurs.
    error: OpenAiInnerError,
}

/// Emitted when an error occurs.
#[derive(Debug, Clone, Deserialize)]
pub struct OpenAiInnerError {
    /// The error code.
    code: Option<String>,
    /// The error message.
    message: String,
    /// The error parameter.
    param: Option<String>,
    /// The type of the event. Always `error`.
    r#type: String,
}

impl From<OpenAiOutsideError> for StreamErrorEvent {
    fn from(error: OpenAiOutsideError) -> Self {
        let OpenAiOutsideError { error: OpenAiInnerError { code, message, param, r#type } } = error;
        StreamErrorEvent {
            event_id: Uuid::new_v4().to_string(),
            r#type: ERROR_TYPE.to_string(),
            error: StreamError { r#type, code, message, param, event_id: None },
        }
    }
}

/// An error that occurs during the streaming process.
///
/// It directly comes from the OpenAI API and you can
/// read more about error events on their website:
/// <https://platform.openai.com/docs/api-reference/realtime-server-events/error>
#[derive(Debug, Serialize, Deserialize)]
pub struct StreamErrorEvent {
    /// The unique ID of the server event.
    pub event_id: String,
    /// The event type, must be error.
    pub r#type: String,
    /// Details of the error.
    pub error: StreamError,
}

/// Details of the error.
#[derive(Debug, Serialize, Deserialize)]
pub struct StreamError {
    /// The type of error (e.g., "invalid_request_error", "server_error").
    pub r#type: String,
    /// Error code, if any.
    pub code: Option<String>,
    /// A human-readable error message.
    pub message: String,
    /// Parameter related to the error, if any.
    pub param: Option<String>,
    /// The event_id of the client event that caused the error, if applicable.
    pub event_id: Option<String>,
}

impl StreamErrorEvent {
    pub async fn from_openai_error<E>(error: OpenAIError) -> Result<Self, reqwest::Error>
    where
        E: serde::de::DeserializeOwned,
        Self: From<E>,
    {
        match error {
            OpenAIError::Reqwest(e) => Ok(StreamErrorEvent {
                event_id: Uuid::new_v4().to_string(),
                r#type: ERROR_TYPE.to_string(),
                error: StreamError {
                    r#type: "internal_reqwest_error".to_string(),
                    code: Some("internal".to_string()),
                    message: e.to_string(),
                    param: None,
                    event_id: None,
                },
            }),
            OpenAIError::ApiError(ApiError { message, r#type, param, code }) => {
                Ok(StreamErrorEvent {
                    r#type: ERROR_TYPE.to_string(),
                    event_id: Uuid::new_v4().to_string(),
                    error: StreamError {
                        r#type: r#type.unwrap_or_else(|| "unknown".to_string()),
                        code,
                        message,
                        param,
                        event_id: None,
                    },
                })
            }
            OpenAIError::JSONDeserialize(error) => Ok(StreamErrorEvent {
                event_id: Uuid::new_v4().to_string(),
                r#type: ERROR_TYPE.to_string(),
                error: StreamError {
                    r#type: "json_deserialize_error".to_string(),
                    code: Some("internal".to_string()),
                    message: error.to_string(),
                    param: None,
                    event_id: None,
                },
            }),
            OpenAIError::FileSaveError(_) | OpenAIError::FileReadError(_) => unreachable!(),
            OpenAIError::StreamError(error) => match error {
                EventSourceError::InvalidStatusCode(_status_code, response) => {
                    let error = response.json::<E>().await?;
                    Ok(StreamErrorEvent::from(error))
                }
                EventSourceError::InvalidContentType(_header_value, response) => {
                    let error = response.json::<E>().await?;
                    Ok(StreamErrorEvent::from(error))
                }
                EventSourceError::Utf8(error) => Ok(StreamErrorEvent {
                    event_id: Uuid::new_v4().to_string(),
                    r#type: ERROR_TYPE.to_string(),
                    error: StreamError {
                        r#type: "invalid_utf8_error".to_string(),
                        code: None,
                        message: error.to_string(),
                        param: None,
                        event_id: None,
                    },
                }),
                EventSourceError::Parser(error) => Ok(StreamErrorEvent {
                    event_id: Uuid::new_v4().to_string(),
                    r#type: ERROR_TYPE.to_string(),
                    error: StreamError {
                        r#type: "parser_error".to_string(),
                        code: None,
                        message: error.to_string(),
                        param: None,
                        event_id: None,
                    },
                }),
                EventSourceError::Transport(error) => Ok(StreamErrorEvent {
                    event_id: Uuid::new_v4().to_string(),
                    r#type: ERROR_TYPE.to_string(),
                    error: StreamError {
                        r#type: "transport_error".to_string(),
                        code: None,
                        message: error.to_string(),
                        param: None,
                        event_id: None,
                    },
                }),
                EventSourceError::InvalidLastEventId(message) => Ok(StreamErrorEvent {
                    event_id: Uuid::new_v4().to_string(),
                    r#type: ERROR_TYPE.to_string(),
                    error: StreamError {
                        r#type: "invalid_last_event_id".to_string(),
                        code: None,
                        message,
                        param: None,
                        event_id: None,
                    },
                }),
                EventSourceError::StreamEnded => Ok(StreamErrorEvent {
                    event_id: Uuid::new_v4().to_string(),
                    r#type: ERROR_TYPE.to_string(),
                    error: StreamError {
                        r#type: "stream_ended".to_string(),
                        code: None,
                        message: "Stream ended".to_string(),
                        param: None,
                        event_id: None,
                    },
                }),
            },
            OpenAIError::InvalidArgument(message) => Ok(StreamErrorEvent {
                event_id: Uuid::new_v4().to_string(),
                r#type: ERROR_TYPE.to_string(),
                error: StreamError {
                    r#type: "invalid_argument".to_string(),
                    code: None,
                    message,
                    param: None,
                    event_id: None,
                },
            }),
        }
    }

    pub fn from_response_error(error: ResponseError) -> Self {
        let ResponseError { code, message, .. } = error;
        StreamErrorEvent {
            event_id: Uuid::new_v4().to_string(),
            r#type: ERROR_TYPE.to_string(),
            error: StreamError {
                r#type: "response_error".to_string(),
                code: Some(code.as_str().to_string()),
                message,
                param: None,
                event_id: None,
            },
        }
    }

    pub fn from_reqwest_error(error: reqwest::Error) -> Self {
        StreamErrorEvent {
            event_id: Uuid::new_v4().to_string(),
            r#type: ERROR_TYPE.to_string(),
            error: StreamError {
                r#type: "reqwest_error".to_string(),
                code: None,
                message: error.to_string(),
                param: None,
                event_id: None,
            },
        }
    }
}
