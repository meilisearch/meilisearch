pub mod index;
pub mod server;
pub mod service;

pub use index::{GetAllDocumentsOptions, GetDocumentOptions};
pub use server::{default_settings, Server};

/// Performs a search test on both post and get routes
#[macro_export]
macro_rules! test_post_get_search {
    ($server:expr, $query:expr, |$response:ident, $status_code:ident | $block:expr) => {
        let post_query: meilisearch_http::routes::search::SearchQueryPost =
            serde_json::from_str(&$query.clone().to_string()).unwrap();
        let get_query: meilisearch_http::routes::search::SearchQuery = post_query.into();
        let get_query = ::serde_url_params::to_string(&get_query).unwrap();
        let ($response, $status_code) = $server.search_get(&get_query).await;
        let _ = ::std::panic::catch_unwind(|| $block).map_err(|e| {
            panic!(
                "panic in get route: {:?}",
                e.downcast_ref::<&str>().unwrap()
            )
        });
        let ($response, $status_code) = $server.search_post($query).await;
        let _ = ::std::panic::catch_unwind(|| $block).map_err(|e| {
            panic!(
                "panic in post route: {:?}",
                e.downcast_ref::<&str>().unwrap()
            )
        });
    };
}
