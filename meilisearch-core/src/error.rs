use crate::serde::{DeserializerError, SerializerError};
use serde_json::Error as SerdeJsonError;
use pest::error::Error as PestError;
use crate::filters::Rule;
use std::{error, fmt, io};

pub use bincode::Error as BincodeError;
pub use fst::Error as FstError;
pub use heed::Error as HeedError;
pub use pest::error as pest_error;

use meilisearch_error::{ErrorCode, Code};

pub type MResult<T> = Result<T, Error>;

#[derive(Debug)]
pub enum Error {
    Io(io::Error),
    IndexAlreadyExists,
    MissingPrimaryKey,
    SchemaMissing,
    WordIndexMissing,
    MissingDocumentId,
    MaxFieldsLimitExceeded,
    Schema(meilisearch_schema::Error),
    Heed(heed::Error),
    Fst(fst::Error),
    SerdeJson(SerdeJsonError),
    Bincode(bincode::Error),
    Serializer(SerializerError),
    Deserializer(DeserializerError),
    UnsupportedOperation(UnsupportedOperation),
    FilterParseError(PestError<Rule>),
    FacetError(FacetError),
}

impl ErrorCode for Error {
    fn error_code(&self) -> Code {
        use Error::*;

        match self {
            FacetError(_) => Code::Facet,
            FilterParseError(_) => Code::Filter,
            UnsupportedOperation(_) => Code::BadRequest,
            IndexAlreadyExists => Code::IndexAlreadyExists,
            MissingPrimaryKey => Code::InvalidState,
            MissingDocumentId => Code::MissingDocumentId,
            MaxFieldsLimitExceeded => Code::MaxFieldsLimitExceeded,
            Schema(s) =>  s.error_code(),
            WordIndexMissing
            | SchemaMissing => Code::InvalidState,
            Heed(_)
            | Fst(_)
            | SerdeJson(_)
            | Bincode(_)
            | Serializer(_)
            | Deserializer(_)
            | Io(_) => Code::Internal,
        }
    }
}

impl From<io::Error> for Error {
    fn from(error: io::Error) -> Error {
        Error::Io(error)
    }
}

impl From<PestError<Rule>> for Error {
    fn from(error: PestError<Rule>) -> Error {
        Error::FilterParseError(error.renamed_rules(|r| {
            let s  = match r {
                Rule::or => "OR",
                Rule::and => "AND",
                Rule::not => "NOT",
                Rule::string => "string",
                Rule::word => "word",
                Rule::greater => "field > value",
                Rule::less => "field < value",
                Rule::eq => "field = value",
                Rule::leq => "field <= value",
                Rule::geq => "field >= value",
                Rule::key => "key",
                _ => "other",
            };
            s.to_string()
        }))
    }
}

impl From<FacetError> for Error {
    fn from(error: FacetError) -> Error {
        Error::FacetError(error)
    }
}

impl From<meilisearch_schema::Error> for Error {
    fn from(error: meilisearch_schema::Error) -> Error {
        Error::Schema(error)
    }
}

impl From<HeedError> for Error {
    fn from(error: HeedError) -> Error {
        Error::Heed(error)
    }
}

impl From<FstError> for Error {
    fn from(error: FstError) -> Error {
        Error::Fst(error)
    }
}

impl From<SerdeJsonError> for Error {
    fn from(error: SerdeJsonError) -> Error {
        Error::SerdeJson(error)
    }
}

impl From<BincodeError> for Error {
    fn from(error: BincodeError) -> Error {
        Error::Bincode(error)
    }
}

impl From<SerializerError> for Error {
    fn from(error: SerializerError) -> Error {
        Error::Serializer(error)
    }
}

impl From<DeserializerError> for Error {
    fn from(error: DeserializerError) -> Error {
        Error::Deserializer(error)
    }
}

impl From<UnsupportedOperation> for Error {
    fn from(op: UnsupportedOperation) -> Error {
        Error::UnsupportedOperation(op)
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        use self::Error::*;
        match self {
            Io(e) => write!(f, "{}", e),
            IndexAlreadyExists => write!(f, "index already exists"),
            MissingPrimaryKey => write!(f, "schema cannot be built without a primary key"),
            SchemaMissing => write!(f, "this index does not have a schema"),
            WordIndexMissing => write!(f, "this index does not have a word index"),
            MissingDocumentId => write!(f, "document id is missing"),
            MaxFieldsLimitExceeded => write!(f, "maximum number of fields in a document exceeded"),
            Schema(e) => write!(f, "schema error; {}", e),
            Heed(e) => write!(f, "heed error; {}", e),
            Fst(e) => write!(f, "fst error; {}", e),
            SerdeJson(e) => write!(f, "serde json error; {}", e),
            Bincode(e) => write!(f, "bincode error; {}", e),
            Serializer(e) => write!(f, "serializer error; {}", e),
            Deserializer(e) => write!(f, "deserializer error; {}", e),
            UnsupportedOperation(op) => write!(f, "unsupported operation; {}", op),
            FilterParseError(e) => write!(f, "error parsing filter; {}", e),
            FacetError(e) => write!(f, "error processing facet filter: {}", e),
        }
    }
}

impl error::Error for Error {}

#[derive(Debug)]
pub enum UnsupportedOperation {
    SchemaAlreadyExists,
    CannotUpdateSchemaPrimaryKey,
    CannotReorderSchemaAttribute,
    CanOnlyIntroduceNewSchemaAttributesAtEnd,
    CannotRemoveSchemaAttribute,
}

impl fmt::Display for UnsupportedOperation {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        use self::UnsupportedOperation::*;
        match self {
            SchemaAlreadyExists => write!(f, "Cannot update index which already have a schema"),
            CannotUpdateSchemaPrimaryKey => write!(f, "Cannot update the primary key of a schema"),
            CannotReorderSchemaAttribute => write!(f, "Cannot reorder the attributes of a schema"),
            CanOnlyIntroduceNewSchemaAttributesAtEnd => {
                write!(f, "Can only introduce new attributes at end of a schema")
            }
            CannotRemoveSchemaAttribute => write!(f, "Cannot remove attributes from a schema"),
        }
    }
}

#[derive(Debug)]
pub enum FacetError {
    EmptyArray,
    ParsingError(String),
    UnexpectedToken { expected: &'static [&'static str], found: String },
    InvalidFormat(String),
    AttributeNotFound(String),
    AttributeNotSet { expected: Vec<String>, found: String },
    InvalidDocumentAttribute(String),
    NoFacetAttributes,
}

impl FacetError {
    pub fn unexpected_token(expected: &'static [&'static str], found: impl ToString) -> FacetError {
        FacetError::UnexpectedToken{ expected, found: found.to_string() }
    }

    pub fn attribute_not_set(expected: Vec<String>, found: impl ToString) -> FacetError {
        FacetError::AttributeNotSet{ expected, found: found.to_string() }
    }
}

impl fmt::Display for FacetError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        use FacetError::*;

        match self {
            EmptyArray => write!(f, "empty array in facet filter is unspecified behavior"),
            ParsingError(msg) => write!(f, "parsing error: {}", msg),
            UnexpectedToken { expected, found } => write!(f, "unexpected token {}, expected {}", found, expected.join("or")),
            InvalidFormat(found) => write!(f, "invalid facet: {}, facets should be \"facetName:facetValue\"", found),
            AttributeNotFound(attr) => write!(f, "unknown {:?} attribute", attr),
            AttributeNotSet { found, expected } => write!(f, "`{}` is not set as a faceted attribute. available facet attributes: {}", found, expected.join(", ")),
            InvalidDocumentAttribute(attr) => write!(f, "invalid document attribute {}, accepted types: String and [String]", attr),
            NoFacetAttributes => write!(f, "No attributes are set for faceting"),
        }
    }
}
