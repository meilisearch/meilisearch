use std::{
    fs::{self},
    path::Path,
};

use time::OffsetDateTime;

use crate::Result;

type Metadata = crate::Metadata;

pub fn date(dump: &Path) -> Result<OffsetDateTime> {
    let metadata = fs::read(dump.join("metadata.json"))?;
    let metadata: Metadata = serde_json::from_reader(metadata)?;
    Ok(metadata.dump_date)
}
