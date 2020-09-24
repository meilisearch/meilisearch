use std::{error, fmt};

use meilisearch_error::{ErrorCode, Code};

pub type SResult<T> = Result<T, Error>;

#[derive(Debug)]
pub enum Error {
    FieldNameNotFound(String),
    PrimaryKeyAlreadyPresent,
    MaxFieldsLimitExceeded,
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        use self::Error::*;
        match self {
            FieldNameNotFound(field) => write!(f, "The field {:?} doesn't exist", field),
            PrimaryKeyAlreadyPresent => write!(f, "A primary key is already present. It's impossible to update it"),
            MaxFieldsLimitExceeded => write!(f, "The maximum of possible reattributed field id has been reached"),
        }
    }
}

impl error::Error for Error {}

impl ErrorCode for Error {
    fn error_code(&self) -> Code {
        use Error::*;

        match self {
            FieldNameNotFound(_) => Code::Internal,
            MaxFieldsLimitExceeded => Code::MaxFieldsLimitExceeded,
            PrimaryKeyAlreadyPresent => Code::PrimaryKeyAlreadyPresent,
        }
    }
}
