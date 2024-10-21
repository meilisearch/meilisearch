use std::any::Any;
use std::sync::Arc;

use actix_web::HttpRequest;
use meilisearch_types::InstanceUid;
use serde_json::Value;

use super::{find_user_id, Analytics, DocumentDeletionKind, DocumentFetchKind};
use crate::routes::indexes::documents::{DocumentEditionByFunction, UpdateDocumentsQuery};
use crate::Opt;

pub struct MockAnalytics {
    instance_uid: Option<InstanceUid>,
}

#[derive(Default)]
pub struct SearchAggregator;

#[allow(dead_code)]
impl SearchAggregator {
    pub fn from_query(_: &dyn Any, _: &dyn Any) -> Self {
        Self
    }

    pub fn succeed(&mut self, _: &dyn Any) {}
}

#[derive(Default)]
pub struct SimilarAggregator;

#[allow(dead_code)]
impl SimilarAggregator {
    pub fn from_query(_: &dyn Any, _: &dyn Any) -> Self {
        Self
    }

    pub fn succeed(&mut self, _: &dyn Any) {}
}

#[derive(Default)]
pub struct MultiSearchAggregator;

#[allow(dead_code)]
impl MultiSearchAggregator {
    pub fn from_federated_search(_: &dyn Any, _: &dyn Any) -> Self {
        Self
    }

    pub fn succeed(&mut self) {}
}

#[derive(Default)]
pub struct FacetSearchAggregator;

#[allow(dead_code)]
impl FacetSearchAggregator {
    pub fn from_query(_: &dyn Any, _: &dyn Any) -> Self {
        Self
    }

    pub fn succeed(&mut self, _: &dyn Any) {}
}

impl MockAnalytics {
    #[allow(clippy::new_ret_no_self)]
    pub fn new(opt: &Opt) -> Arc<dyn Analytics> {
        let instance_uid = find_user_id(&opt.db_path);
        Arc::new(Self { instance_uid })
    }
}

impl Analytics for MockAnalytics {
    fn instance_uid(&self) -> Option<&meilisearch_types::InstanceUid> {
        self.instance_uid.as_ref()
    }

    // These methods are noop and should be optimized out
    fn publish(&self, _event_name: String, _send: Value, _request: Option<&HttpRequest>) {}
    fn get_search(&self, _aggregate: super::SearchAggregator) {}
    fn post_search(&self, _aggregate: super::SearchAggregator) {}
    fn get_similar(&self, _aggregate: super::SimilarAggregator) {}
    fn post_similar(&self, _aggregate: super::SimilarAggregator) {}
    fn post_multi_search(&self, _aggregate: super::MultiSearchAggregator) {}
    fn post_facet_search(&self, _aggregate: super::FacetSearchAggregator) {}
    fn add_documents(
        &self,
        _documents_query: &UpdateDocumentsQuery,
        _index_creation: bool,
        _request: &HttpRequest,
    ) {
    }
    fn delete_documents(&self, _kind: DocumentDeletionKind, _request: &HttpRequest) {}
    fn update_documents(
        &self,
        _documents_query: &UpdateDocumentsQuery,
        _index_creation: bool,
        _request: &HttpRequest,
    ) {
    }
    fn update_documents_by_function(
        &self,
        _documents_query: &DocumentEditionByFunction,
        _index_creation: bool,
        _request: &HttpRequest,
    ) {
    }
    fn get_fetch_documents(&self, _documents_query: &DocumentFetchKind, _request: &HttpRequest) {}
    fn post_fetch_documents(&self, _documents_query: &DocumentFetchKind, _request: &HttpRequest) {}
}
