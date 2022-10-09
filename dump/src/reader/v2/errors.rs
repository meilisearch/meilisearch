use http::StatusCode;
use serde::Deserialize;

#[derive(Debug, Deserialize, Clone)]
#[cfg_attr(test, derive(serde::Serialize))]
#[serde(rename_all = "camelCase")]
pub struct ResponseError {
    #[serde(skip)]
    code: StatusCode,
    message: String,
    error_code: String,
    error_type: String,
    error_link: String,
}
