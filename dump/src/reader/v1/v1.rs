use serde::Deserialize;
use time::OffsetDateTime;

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Index {
    pub name: String,
    pub uid: String,
    #[serde(with = "time::serde::rfc3339")]
    created_at: OffsetDateTime,
    #[serde(with = "time::serde::rfc3339")]
    updated_at: OffsetDateTime,
    pub primary_key: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Metadata {
    indexes: Vec<Index>,
    db_version: String,
    dump_version: crate::Version,
}
