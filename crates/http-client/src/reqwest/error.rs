use std::fmt::Display;

pub use ::reqwest::{Error as ReqwestError, Result as ReqwestResult};

use crate::policy;

#[derive(Debug)]
pub enum Error {
    Reqwest(ReqwestError),
    Policy(policy::Error),
}

impl Error {
    pub fn is_timeout(&self) -> bool {
        match self {
            Error::Reqwest(error) => error.is_timeout(),
            Error::Policy(_) => false,
        }
    }

    pub fn without_url(self) -> Error {
        match self {
            Error::Reqwest(error) => Error::Reqwest(error.without_url()),
            Error::Policy(error) => Error::Policy(error),
        }
    }
}

impl From<ReqwestError> for Error {
    fn from(value: ReqwestError) -> Self {
        Self::Reqwest(value)
    }
}

impl From<policy::Error> for Error {
    fn from(value: policy::Error) -> Self {
        Self::Policy(value)
    }
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

impl std::error::Error for Error {}

impl Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Error::Reqwest(error) => write!(f, "{error}"),
            Error::Policy(error) => write!(f, "{error}"),
        }
    }
}
