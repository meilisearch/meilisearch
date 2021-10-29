use std::{any::Any, sync::Arc};

use actix_web::HttpRequest;
use serde_json::Value;

use crate::{routes::indexes::documents::UpdateDocumentsQuery, Opt};

use super::{find_user_id, Analytics};

pub struct MockAnalytics;

#[derive(Default)]
pub struct SearchAggregator {}

#[allow(dead_code)]
impl SearchAggregator {
    pub fn from_query(_: &dyn Any, _: &dyn Any) -> Self {
        Self::default()
    }

    pub fn finish(&mut self, _: &dyn Any) {}
}

impl MockAnalytics {
    #[allow(clippy::new_ret_no_self)]
    pub fn new(opt: &Opt) -> (Arc<dyn Analytics>, String) {
        let user = find_user_id(&opt.db_path).unwrap_or_default();
        (Arc::new(Self), user)
    }
}

impl Analytics for MockAnalytics {
    // These methods are noop and should be optimized out
    fn publish(&self, _event_name: String, _send: Value, _request: Option<&HttpRequest>) {}
    fn get_search(&self, _aggregate: super::SearchAggregator) {}
    fn post_search(&self, _aggregate: super::SearchAggregator) {}
    fn add_documents(
        &self,
        _documents_query: &UpdateDocumentsQuery,
        _index_creation: bool,
        _request: &HttpRequest,
    ) {
    }
    fn update_documents(
        &self,
        _documents_query: &UpdateDocumentsQuery,
        _index_creation: bool,
        _request: &HttpRequest,
    ) {
    }
}
