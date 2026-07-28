use std::collections::BTreeSet;
use std::convert::Infallible;
use std::fmt::Write;
use std::{io, str};

use bstr::BString;
use heed::{Error as HeedError, MdbError};
use rayon::ThreadPoolBuildError;
use rhai::EvalAltResult;
use serde_json::Value;
use thiserror::Error;

use crate::constants::{RESERVED_GEOJSON_FIELD_NAME, RESERVED_GEO_FIELD_NAME};
use crate::documents::{self, DocumentsBatchCursorError};
use crate::thread_pool_no_abort::CaughtPanic;
use crate::vector::settings::EmbeddingSettings;
use crate::{CriterionError, DocumentId, FieldId, Object, SortError};

// ... existing content preserved above

#[allow(clippy::large_enum_variant)]
#[derive(Error, Debug)]
pub enum UserError {
    // ... existing variants ...
    #[error("Vector or filter evaluation exceeded configured memory budget")] 
    VectorOrFilterBudgetExceeded,
}
