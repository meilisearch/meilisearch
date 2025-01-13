pub mod encoder;
pub mod index;
pub mod server;
pub mod service;

use std::fmt::{self, Display};

#[allow(unused)]
pub use index::GetAllDocumentsOptions;
use meili_snap::json_string;
use once_cell::sync::Lazy;
use serde::{Deserialize, Serialize};
#[allow(unused)]
pub use server::{default_settings, Server};
use tokio::sync::OnceCell;

use crate::common::index::Index;

pub enum Shared {}
pub enum Owned {}

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

    /// Return `true` if the `status` field is set to `succeeded`.
    /// Panic if the `status` field doesn't exists.
    #[track_caller]
    pub fn is_success(&self) -> bool {
        if !self["status"].is_string() {
            panic!("Called `is_success` on {}", serde_json::to_string_pretty(&self.0).unwrap());
        }
        self["status"] == serde_json::Value::String(String::from("succeeded"))
    }

    // Panic if the json doesn't contain the `status` field set to "succeeded"
    #[track_caller]
    pub fn succeeded(&self) -> Self {
        if !self.is_success() {
            panic!("Called succeeded on {}", serde_json::to_string_pretty(&self.0).unwrap());
        }
        self.clone()
    }

    /// Return `true` if the `status` field is set to `failed`.
    /// Panic if the `status` field doesn't exists.
    #[track_caller]
    pub fn is_fail(&self) -> bool {
        if !self["status"].is_string() {
            panic!("Called `is_fail` on {}", serde_json::to_string_pretty(&self.0).unwrap());
        }
        self["status"] == serde_json::Value::String(String::from("failed"))
    }

    // Panic if the json doesn't contain the `status` field set to "succeeded"
    #[track_caller]
    pub fn failed(&self) -> Self {
        if !self.is_fail() {
            panic!("Called failed on {}", serde_json::to_string_pretty(&self.0).unwrap());
        }
        self.clone()
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

impl std::ops::DerefMut for Value {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
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
            json_string!(self, {
                ".uid" => "[uid]",
                ".batchUid" => "[batch_uid]",
                ".enqueuedAt" => "[date]",
                ".startedAt" => "[date]",
                ".finishedAt" => "[date]",
                ".duration" => "[duration]",
                ".processingTimeMs" => "[duration]",
                ".details.embedders.*.url" => "[url]"
            })
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

pub async fn shared_does_not_exists_index() -> &'static Index<'static, Shared> {
    static INDEX: Lazy<Index<'static, Shared>> = Lazy::new(|| {
        let server = Server::new_shared();
        server._index("DOES_NOT_EXISTS").to_shared()
    });
    &INDEX
}

pub async fn shared_empty_index() -> &'static Index<'static, Shared> {
    static INDEX: OnceCell<Index<'static, Shared>> = OnceCell::const_new();

    INDEX
        .get_or_init(|| async {
            let server = Server::new_shared();
            let index = server._index("EMPTY_INDEX").to_shared();
            let (response, _code) = index._create(None).await;
            index.wait_task(response.uid()).await.succeeded();
            index
        })
        .await
}

pub static DOCUMENTS: Lazy<Value> = Lazy::new(|| {
    json!([
        {
            "title": "Shazam!",
            "id": "287947",
            "color": ["green", "blue"],
            "_vectors": { "manual": [1, 2, 3]},
        },
        {
            "title": "Captain Marvel",
            "id": "299537",
            "color": ["yellow", "blue"],
            "_vectors": { "manual": [1, 2, 54] },
        },
        {
            "title": "Escape Room",
            "id": "522681",
            "color": ["yellow", "red"],
            "_vectors": { "manual": [10, -23, 32] },
        },
        {
            "title": "How to Train Your Dragon: The Hidden World",
            "id": "166428",
            "color": ["green", "red"],
            "_vectors": { "manual": [-100, 231, 32] },
        },
        {
            "title": "Gläss",
            "id": "450465",
            "color": ["blue", "red"],
            "_vectors": { "manual": [-100, 340, 90] },
        }
    ])
});

pub async fn shared_index_with_documents() -> &'static Index<'static, Shared> {
    static INDEX: OnceCell<Index<'static, Shared>> = OnceCell::const_new();
    INDEX.get_or_init(|| async {
        let server = Server::new_shared();
        let index = server._index("SHARED_DOCUMENTS").to_shared();
        let documents = DOCUMENTS.clone();
        let (response, _code) = index._add_documents(documents, None).await;
        index.wait_task(response.uid()).await.succeeded();
        let (response, _code) = index
            ._update_settings(
                json!({"filterableAttributes": ["id", "title"], "sortableAttributes": ["id", "title"]}),
            )
            .await;
        index.wait_task(response.uid()).await.succeeded();
        index
    }).await
}

pub static SCORE_DOCUMENTS: Lazy<Value> = Lazy::new(|| {
    json!([
        {
            "title": "Batman the dark knight returns: Part 1",
            "id": "A",
        },
        {
            "title": "Batman the dark knight returns: Part 2",
            "id": "B",
        },
        {
            "title": "Batman Returns",
            "id": "C",
        },
        {
            "title": "Batman",
            "id": "D",
        },
        {
            "title": "Badman",
            "id": "E",
        }
    ])
});

pub static NESTED_DOCUMENTS: Lazy<Value> = Lazy::new(|| {
    json!([
        {
            "id": 852,
            "father": "jean",
            "mother": "michelle",
            "doggos": [
                {
                    "name": "bobby",
                    "age": 2,
                },
                {
                    "name": "buddy",
                    "age": 4,
                },
            ],
            "cattos": "pésti",
            "_vectors": { "manual": [1, 2, 3]},
        },
        {
            "id": 654,
            "father": "pierre",
            "mother": "sabine",
            "doggos": [
                {
                    "name": "gros bill",
                    "age": 8,
                },
            ],
            "cattos": ["simba", "pestiféré"],
            "_vectors": { "manual": [1, 2, 54] },
        },
        {
            "id": 750,
            "father": "romain",
            "mother": "michelle",
            "cattos": ["enigma"],
            "_vectors": { "manual": [10, 23, 32] },
        },
        {
            "id": 951,
            "father": "jean-baptiste",
            "mother": "sophie",
            "doggos": [
                {
                    "name": "turbo",
                    "age": 5,
                },
                {
                    "name": "fast",
                    "age": 6,
                },
            ],
            "cattos": ["moumoute", "gomez"],
            "_vectors": { "manual": [10, 23, 32] },
        },
    ])
});

pub async fn shared_index_with_nested_documents() -> &'static Index<'static, Shared> {
    static INDEX: OnceCell<Index<'static, Shared>> = OnceCell::const_new();
    INDEX.get_or_init(|| async {
        let server = Server::new_shared();
        let index = server._index("SHARED_NESTED_DOCUMENTS").to_shared();
        let documents = NESTED_DOCUMENTS.clone();
        let (response, _code) = index._add_documents(documents, None).await;
        index.wait_task(response.uid()).await.succeeded();
        let (response, _code) = index
            ._update_settings(
                json!({"filterableAttributes": ["father", "doggos"], "sortableAttributes": ["doggos"]}),
            )
            .await;
        index.wait_task(response.uid()).await.succeeded();
        index
    }).await
}

pub static FRUITS_DOCUMENTS: Lazy<Value> = Lazy::new(|| {
    json!([
        {
            "name": "Exclusive sale: green apple",
            "id": "green-apple-boosted",
            "BOOST": true
        },
        {
            "name": "Pear",
            "id": "pear",
        },
        {
            "name": "Red apple gala",
            "id": "red-apple-gala",
        },
        {
            "name": "Exclusive sale: Red Tomato",
            "id": "red-tomatoes-boosted",
            "BOOST": true
        },
        {
            "name": "Exclusive sale: Red delicious apple",
            "id": "red-delicious-boosted",
            "BOOST": true,
        }
    ])
});

pub static VECTOR_DOCUMENTS: Lazy<Value> = Lazy::new(|| {
    json!([
      {
        "id": "A",
        "description": "the dog barks at the cat",
        "_vectors": {
          // dimensions [canine, feline, young]
          "animal": [0.9, 0.8, 0.05],
          // dimensions [negative/positive, energy]
          "sentiment": [-0.1, 0.55]
        }
      },
      {
        "id": "B",
        "description": "the kitten scratched the beagle",
        "_vectors": {
          // dimensions [canine, feline, young]
          "animal": [0.8, 0.9, 0.5],
          // dimensions [negative/positive, energy]
          "sentiment": [-0.2, 0.65]
        }
      },
      {
        "id": "C",
        "description": "the dog had to stay alone today",
        "_vectors": {
          // dimensions [canine, feline, young]
          "animal": [0.85, 0.02, 0.1],
          // dimensions [negative/positive, energy]
          "sentiment": [-1.0, 0.1]
        }
      },
      {
        "id": "D",
        "description": "the little boy pets the puppy",
        "_vectors": {
          // dimensions [canine, feline, young]
          "animal": [0.8, 0.09, 0.8],
          // dimensions [negative/positive, energy]
          "sentiment": [0.8, 0.3]
        }
      },
    ])
});

pub async fn shared_index_with_test_set() -> &'static Index<'static, Shared> {
    static INDEX: OnceCell<Index<'static, Shared>> = OnceCell::const_new();
    INDEX
        .get_or_init(|| async {
            let server = Server::new_shared();
            let index = server._index("SHARED_TEST_SET").to_shared();
            let url = format!("/indexes/{}/documents", urlencoding::encode(index.uid.as_ref()));
            let (response, code) = index
                .service
                .post_str(
                    url,
                    include_str!("../assets/test_set.json"),
                    vec![("content-type", "application/json")],
                )
                .await;
            assert_eq!(code, 202);
            index.wait_task(response.uid()).await.succeeded();
            index
        })
        .await
}
