
use std::{error, fmt};

pub type SResult<T> = Result<T, Error>;

#[derive(Debug)]
pub enum Error {
    FieldNameNotFound(String),
    IdentifierAlreadyPresent,
    MaxFieldsLimitExceeded,
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        use self::Error::*;
        match self {
            FieldNameNotFound(field) => write!(f, "The field {:?} doesn't exist", field),
            IdentifierAlreadyPresent => write!(f, "The schema already have an identifier. It's impossible to update it"),
            MaxFieldsLimitExceeded => write!(f, "The maximum of possible reattributed field id has been reached"),
        }
    }
}

impl error::Error for Error {}
