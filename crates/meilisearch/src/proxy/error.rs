use meilisearch_types::error::{ErrorCode as _, ResponseError};
use reqwest::StatusCode;

#[derive(Debug, thiserror::Error)]
pub enum ProxyError {
    #[error("{0}")]
    CouldNotSendRequest(ReqwestErrorWithoutUrl),
    #[error("could not authenticate against the remote host\n  - hint: check that the remote instance was registered with a valid API key having the `documents.add` action")]
    AuthenticationError,
    #[error(
            "could not parse response from the remote host as a document addition response{}\n  - hint: check that the remote instance is a Meilisearch instance running the same version",
            response_from_remote(response)
        )]
    CouldNotParseResponse { response: Result<String, ReqwestErrorWithoutUrl> },
    #[error("remote host responded with code {}{}\n  - hint: check that the remote instance has the correct index configuration for that request\n  - hint: check that the `network` experimental feature is enabled on the remote instance", status_code.as_u16(), response_from_remote(response))]
    BadRequest { status_code: StatusCode, response: Result<String, ReqwestErrorWithoutUrl> },
    #[error("remote host did not answer before the deadline")]
    Timeout,
    #[error("remote host responded with code {}{}", status_code.as_u16(), response_from_remote(response))]
    RemoteError { status_code: StatusCode, response: Result<String, ReqwestErrorWithoutUrl> },
    #[error("error while preparing the request: {error}")]
    Milli {
        #[from]
        error: Box<meilisearch_types::milli::Error>,
    },
}

impl ProxyError {
    pub fn as_response_error(&self) -> ResponseError {
        use meilisearch_types::error::Code;
        let message = self.to_string();
        let code = match self {
            ProxyError::CouldNotSendRequest(_) => Code::RemoteCouldNotSendRequest,
            ProxyError::AuthenticationError => Code::RemoteInvalidApiKey,
            ProxyError::BadRequest { .. } => Code::RemoteBadRequest,
            ProxyError::Timeout => Code::RemoteTimeout,
            ProxyError::RemoteError { .. } => Code::RemoteRemoteError,
            ProxyError::CouldNotParseResponse { .. } => Code::RemoteBadResponse,
            ProxyError::Milli { error } => error.error_code(),
        };
        ResponseError::from_msg(message, code)
    }
}

#[derive(Debug, thiserror::Error)]
#[error(transparent)]
pub struct ReqwestErrorWithoutUrl(reqwest::Error);
impl ReqwestErrorWithoutUrl {
    pub fn new(inner: reqwest::Error) -> Self {
        Self(inner.without_url())
    }
}

fn response_from_remote(response: &Result<String, ReqwestErrorWithoutUrl>) -> String {
    match response {
        Ok(response) => {
            format!(":\n  - response from remote: {}", response)
        }
        Err(error) => {
            format!(":\n  - additionally, could not retrieve response from remote: {error}")
        }
    }
}
