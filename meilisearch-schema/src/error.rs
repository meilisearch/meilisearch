
use std::{error, fmt};

pub type SResult<T> = Result<T, Error>;

#[derive(Debug)]
pub enum Error {
    MaxFieldsLimitExceeded,
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        use self::Error::*;
        match self {
            MaxFieldsLimitExceeded => write!(f, "The maximum of possible reatributed field id has been reached"),
        }
    }
}

impl error::Error for Error {}
