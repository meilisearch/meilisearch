use serde::{Deserialize, Serialize};
use time::OffsetDateTime;

mod error;
mod reader;
mod writer;

pub use error::Error;
pub use writer::DumpWriter;

const CURRENT_DUMP_VERSION: Version = Version::V6;

type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct Metadata {
    pub dump_version: Version,
    pub db_version: String,
    #[serde(with = "time::serde::rfc3339")]
    pub dump_date: OffsetDateTime,
}

#[derive(Debug, PartialEq, Eq, Deserialize, Serialize)]
pub enum Version {
    V1,
    V2,
    V3,
    V4,
    V5,
    V6,
}
