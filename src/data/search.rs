use serde_json::{Map, Value};

use crate::index::{SearchQuery, SearchResult};
use super::Data;

impl Data {
    pub async fn search<S: AsRef<str>>(
        &self,
        index: S,
        search_query: SearchQuery,
    ) -> anyhow::Result<SearchResult> {
        self.index_controller.search(index.as_ref().to_string(), search_query).await
    }

    pub async fn retrieve_documents(
        &self,
        index: String,
        offset: usize,
        limit: usize,
        attributes_to_retrieve: Option<Vec<String>>,
    ) -> anyhow::Result<Vec<Map<String, Value>>> {
        self.index_controller.documents(index, offset, limit, attributes_to_retrieve).await
    }

    pub async fn retrieve_document(
        &self,
        index: impl AsRef<str> + Sync + Send + 'static,
        document_id: impl AsRef<str> + Sync + Send + 'static,
        attributes_to_retrieve: Option<Vec<String>>,
    ) -> anyhow::Result<Map<String, Value>>
    {
        self.index_controller.document(index.as_ref().to_string(), document_id.as_ref().to_string(), attributes_to_retrieve).await
    }
}
