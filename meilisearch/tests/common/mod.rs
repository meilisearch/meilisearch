pub mod encoder;
pub mod index;
pub mod server;
pub mod service;

use std::fmt::{self, Display};

#[allow(unused)]
pub use index::{GetAllDocumentsOptions, GetDocumentOptions};
use meili_snap::json_string;
use serde::{Deserialize, Serialize};
#[allow(unused)]
pub use server::{default_settings, Server};

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct Value(pub serde_json::Value);

impl Value {
    #[track_caller]
    pub fn uid(&self) -> u64 {
        if let Some(uid) = self["uid"].as_u64() {
            uid
        } else if let Some(uid) = self["taskUid"].as_u64() {
            uid
        } else {
            panic!("Didn't find any task id in: {self}");
        }
    }
}

impl From<serde_json::Value> for Value {
    fn from(value: serde_json::Value) -> Self {
        Value(value)
    }
}

impl std::ops::Deref for Value {
    type Target = serde_json::Value;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl PartialEq<serde_json::Value> for Value {
    fn eq(&self, other: &serde_json::Value) -> bool {
        &self.0 == other
    }
}

impl PartialEq<Value> for serde_json::Value {
    fn eq(&self, other: &Value) -> bool {
        self == &other.0
    }
}

impl PartialEq<&str> for Value {
    fn eq(&self, other: &&str) -> bool {
        self.0.eq(other)
    }
}

impl Display for Value {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{}",
            json_string!(self, { ".enqueuedAt" => "[date]", ".startedAt" => "[date]", ".finishedAt" => "[date]", ".duration" => "[duration]" })
        )
    }
}

impl From<Vec<Value>> for Value {
    fn from(value: Vec<Value>) -> Self {
        Self(value.into_iter().map(|value| value.0).collect::<serde_json::Value>())
    }
}

#[macro_export]
macro_rules! json {
    ($($json:tt)+) => {
        $crate::common::Value(serde_json::json!($($json)+))
    };
}

/// Performs a search test on both post and get routes
#[macro_export]
macro_rules! test_post_get_search {
    ($server:expr, $query:expr, |$response:ident, $status_code:ident | $block:expr) => {
        let post_query: meilisearch::routes::search::SearchQueryPost =
            serde_json::from_str(&$query.clone().to_string()).unwrap();
        let get_query: meilisearch::routes::search::SearchQuery = post_query.into();
        let get_query = ::serde_url_params::to_string(&get_query).unwrap();
        let ($response, $status_code) = $server.search_get(&get_query).await;
        let _ = ::std::panic::catch_unwind(|| $block)
            .map_err(|e| panic!("panic in get route: {:?}", e.downcast_ref::<&str>().unwrap()));
        let ($response, $status_code) = $server.search_post($query).await;
        let _ = ::std::panic::catch_unwind(|| $block)
            .map_err(|e| panic!("panic in post route: {:?}", e.downcast_ref::<&str>().unwrap()));
    };
}
