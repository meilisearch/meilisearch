use anyhow::Result;
use meilisearch_types::error::ResponseError;
use meilisearch_types::milli::update::IndexDocumentsMethod;
use meilisearch_types::settings::{Settings, Unchecked};

use meilisearch_types::tasks::{Details, Kind, Status};
use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use time::OffsetDateTime;
use uuid::Uuid;

use crate::TaskId;
