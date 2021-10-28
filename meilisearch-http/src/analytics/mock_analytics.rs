use std::{any::Any, fmt::Display};

use actix_web::HttpRequest;
use serde_json::Value;

use crate::{routes::indexes::documents::UpdateDocumentsQuery, Opt};

use super::{find_user_id, Analytics};

pub struct MockAnalytics {
    user: String,
}

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
    pub fn new(opt: &Opt) -> &'static Self {
        let user = find_user_id(&opt.db_path).unwrap_or_default();
        let analytics = Box::new(Self { user });
        Box::leak(analytics)
    }
}

impl Analytics for MockAnalytics {
    // These methods are noop and should be optimized out
    fn publish(&'static self, _event_name: String, _send: Value, _request: Option<&HttpRequest>) {}
    fn get_search(&'static self, _aggregate: super::SearchAggregator) {}
    fn post_search(&'static self, _aggregate: super::SearchAggregator) {}
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
