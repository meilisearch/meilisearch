use std::any::Any;
use std::sync::Arc;

use actix_web::HttpRequest;
use meilisearch_types::InstanceUid;
use serde_json::Value;

use super::{find_user_id, Analytics, DocumentDeletionKind};
use crate::routes::indexes::documents::UpdateDocumentsQuery;
use crate::routes::tasks::TasksFilterQuery;
use crate::Opt;

pub struct MockAnalytics {
    instance_uid: Option<InstanceUid>,
}

#[derive(Default)]
pub struct SearchAggregator;

#[allow(dead_code)]
impl SearchAggregator {
    pub fn from_query(_: &dyn Any, _: &dyn Any) -> Self {
        Self::default()
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
    fn get_tasks(&self, _query: &TasksFilterQuery, _request: &HttpRequest) {}
    fn health_seen(&self, _request: &HttpRequest) {}
}
