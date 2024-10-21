use http::StatusCode;
use serde::Deserialize;

#[derive(Debug, Deserialize, Clone)]
#[cfg_attr(test, derive(serde::Serialize))]
#[serde(rename_all = "camelCase")]
pub struct ResponseError {
    #[serde(skip)]
    pub code: StatusCode,
    pub message: String,
    pub error_code: String,
    pub error_type: String,
    pub error_link: String,
}
