use std::{error, fmt};
use crate::serde::SerializerError;

pub type MResult<T> = Result<T, Error>;

#[derive(Debug)]
pub enum Error {
    SchemaDiffer,
    SchemaMissing,
    WordIndexMissing,
    MissingDocumentId,
    RkvError(rkv::StoreError),
    FstError(fst::Error),
    RmpDecodeError(rmp_serde::decode::Error),
    RmpEncodeError(rmp_serde::encode::Error),
    BincodeError(bincode::Error),
    SerializerError(SerializerError),
}

impl From<rkv::StoreError> for Error {
    fn from(error: rkv::StoreError) -> Error {
        Error::RkvError(error)
    }
}

impl From<fst::Error> for Error {
    fn from(error: fst::Error) -> Error {
        Error::FstError(error)
    }
}

impl From<rmp_serde::decode::Error> for Error {
    fn from(error: rmp_serde::decode::Error) -> Error {
        Error::RmpDecodeError(error)
    }
}

impl From<rmp_serde::encode::Error> for Error {
    fn from(error: rmp_serde::encode::Error) -> Error {
        Error::RmpEncodeError(error)
    }
}

impl From<bincode::Error> for Error {
    fn from(error: bincode::Error) -> Error {
        Error::BincodeError(error)
    }
}

impl From<SerializerError> for Error {
    fn from(error: SerializerError) -> Error {
        Error::SerializerError(error)
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        use self::Error::*;
        match self {
            SchemaDiffer => write!(f, "schemas differ"),
            SchemaMissing => write!(f, "this index does not have a schema"),
            WordIndexMissing => write!(f, "this index does not have a word index"),
            MissingDocumentId => write!(f, "document id is missing"),
            RkvError(e) => write!(f, "rkv error; {}", e),
            FstError(e) => write!(f, "fst error; {}", e),
            RmpDecodeError(e) => write!(f, "rmp decode error; {}", e),
            RmpEncodeError(e) => write!(f, "rmp encode error; {}", e),
            BincodeError(e) => write!(f, "bincode error; {}", e),
            SerializerError(e) => write!(f, "serializer error; {}", e),
        }
    }
}

impl error::Error for Error { }

