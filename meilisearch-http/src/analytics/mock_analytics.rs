use std::fmt::Display;

use actix_web::HttpRequest;
use meilisearch_lib::index::SearchQuery;
use serde_json::Value;

use crate::{routes::indexes::documents::UpdateDocumentsQuery, Opt};

use super::{find_user_id, Analytics};

pub struct MockAnalytics {
    user: String,
}

impl MockAnalytics {
    pub fn new(opt: &Opt) -> &'static Self {
        let user = find_user_id(&opt.db_path).unwrap_or_default();
        let analytics = Box::new(Self { user });
        Box::leak(analytics)
    }
}

impl Analytics for MockAnalytics {
    // These methods are noop and should be optimized out
    fn publish(&'static self, _event_name: String, _send: Value, _request: Option<&HttpRequest>) {}
    fn start_get_search(&'static self, _query: &SearchQuery, _request: &HttpRequest) {}
    fn end_get_search(&'static self, _process_time: usize) {}
    fn start_post_search(&'static self, _query: &SearchQuery, _request: &HttpRequest) {}
    fn end_post_search(&'static self, _process_time: usize) {}
    fn add_documents(
        &'static self,
        _documents_query: &UpdateDocumentsQuery,
        _index_creation: bool,
        _request: &HttpRequest,
    ) {
    }
    fn update_documents(
        &'static self,
        _documents_query: &UpdateDocumentsQuery,
        _index_creation: bool,
        _request: &HttpRequest,
    ) {
    }
}

impl Display for MockAnalytics {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.user)
    }
}
