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
    Bincode(bincode::Error),
    Deserializer(DeserializerError),
    FacetError(FacetError),
    FilterParseError(PestError<Rule>),
    Fst(fst::Error),
    Heed(heed::Error),
    IndexAlreadyExists,
    Io(io::Error),
    MaxFieldsLimitExceeded,
    MissingDocumentId,
    MissingPrimaryKey,
    Schema(meilisearch_schema::Error),
    SchemaMissing,
    SerdeJson(SerdeJsonError),
    Serializer(SerializerError),
    VersionMismatch(String),
    WordIndexMissing,
}

impl ErrorCode for Error {
    fn error_code(&self) -> Code {
        use Error::*;

        match self {
            FacetError(_) => Code::Facet,
            FilterParseError(_) => Code::Filter,
            IndexAlreadyExists => Code::IndexAlreadyExists,
            MissingPrimaryKey => Code::MissingPrimaryKey,
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
            | VersionMismatch(_)
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
        match error {
            SerializerError::DocumentIdNotFound => Error::MissingDocumentId,
            e => Error::Serializer(e),
        }
    }
}

impl From<DeserializerError> for Error {
    fn from(error: DeserializerError) -> Error {
        Error::Deserializer(error)
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        use self::Error::*;
        match self {
            Bincode(e) => write!(f, "bincode error; {}", e),
            Deserializer(e) => write!(f, "deserializer error; {}", e),
            FacetError(e) => write!(f, "error processing facet filter: {}", e),
            FilterParseError(e) => write!(f, "error parsing filter; {}", e),
            Fst(e) => write!(f, "fst error; {}", e),
            Heed(e) => write!(f, "heed error; {}", e),
            IndexAlreadyExists => write!(f, "index already exists"),
            Io(e) => write!(f, "{}", e),
            MaxFieldsLimitExceeded => write!(f, "maximum number of fields in a document exceeded"),
            MissingDocumentId => write!(f, "document id is missing"),
            MissingPrimaryKey => write!(f, "schema cannot be built without a primary key"),
            Schema(e) => write!(f, "schema error; {}", e),
            SchemaMissing => write!(f, "this index does not have a schema"),
            SerdeJson(e) => write!(f, "serde json error; {}", e),
            Serializer(e) => write!(f, "serializer error; {}", e),
            VersionMismatch(version) => write!(f, "Cannot open database, expected MeiliSearch engine version: {}, current engine version: {}.{}.{}",
                version,
                env!("CARGO_PKG_VERSION_MAJOR"),
                env!("CARGO_PKG_VERSION_MINOR"),
                env!("CARGO_PKG_VERSION_PATCH")),
            WordIndexMissing => write!(f, "this index does not have a word index"),
        }
    }
}

impl error::Error for Error {}

struct FilterParseError(PestError<Rule>);

impl fmt::Display for FilterParseError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        use crate::pest_error::LineColLocation::*;

        let (line, column) = match self.0.line_col {
            Span((line, _), (column, _)) => (line, column),
            Pos((line, column)) => (line, column),
        };
        write!(f, "parsing error on line {} at column {}: {}", line, column, self.0.variant.message())
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
    NoAttributesForFaceting,
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
            NoAttributesForFaceting => write!(f, "impossible to perform faceted search, no attributes for faceting are set"),
        }
    }
}
