use serde::{Deserialize, Serialize};
use time::OffsetDateTime;

// mod dump;
mod error;
mod writer;

pub use error::Error;
pub use writer::DumpWriter;

const CURRENT_DUMP_VERSION: &str = "V6";

pub struct DumpReader;

type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct Metadata {
    pub dump_version: String,
    pub db_version: String,
    pub dump_date: OffsetDateTime,
}
