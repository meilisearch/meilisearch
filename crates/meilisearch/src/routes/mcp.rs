use std::collections::BTreeMap;
use std::io::Cursor;
use std::sync::LazyLock;
use std::{fmt, mem};

use actix_web::web::{self, Data};
use actix_web::{FromRequest, HttpRequest, HttpResponse};
use anyhow::Context as _;
use deserr::actix_web::{AwebJson, AwebQueryParameter};
use deserr::{Deserr, IntoValue, Value, ValuePointerRef};
use either::Either;
use index_scheduler::IndexScheduler;
use meilisearch_types::batch_view::BatchView;
use meilisearch_types::deserr::{DeserrError, DeserrJson, DeserrJsonError};
use meilisearch_types::error::deserr_codes::BadRequest;
use meilisearch_types::error::Code::BadParameter;
use meilisearch_types::error::ResponseError;
use meilisearch_types::index_uid::IndexUid;
use meilisearch_types::milli;
use serde::Serialize;
use serde_json::Number;
use utoipa::openapi::path::Operation;
use utoipa::openapi::schema::{AdditionalProperties, ArrayItems, Components, Ref, Schema};
use utoipa::openapi::{ObjectBuilder, OpenApi, RefOr};
use utoipa::{OpenApi as _, PartialSchema, ToSchema};

use crate::analytics::Analytics;
use crate::extractors::authentication::GuardedData;
use crate::routes::MeilisearchApi;
use crate::search_queue::SearchQueue;
macro_rules! r#try_or_internal_error {
    ($jsonrpc:ident, $id:ident, $expr:expr $(,)?) => {
        try_or_internal_error!($jsonrpc, $id, $expr, internal_error)
    };
    ($jsonrpc:ident, $id:ident, $expr:expr, $error_type:ident $(,)?) => {
        match $expr {
            ::std::result::Result::Ok(val) => val,
            ::std::result::Result::Err(err) => {
                return Ok(::actix_web::HttpResponse::Ok().json(McpResponse {
                    $jsonrpc,
                    $id,
                    result: None,
                    error: Some(McpError::$error_type(err)),
                }));
            }
        }
    };
}
