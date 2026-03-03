use meilisearch_types::heed::{Database, Env, RwTxn, WithoutTls};
use meilisearch_types::milli::{CboRoaringBitmapCodec, BEU32};
use tracing::info;

use super::UpgradeIndexScheduler;
use crate::queue::db_name::BATCH_TO_TASKS_MAPPING;
use crate::queue::BatchQueue;
