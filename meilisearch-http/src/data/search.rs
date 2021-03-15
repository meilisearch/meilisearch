use serde_json::{Map, Value};

use crate::index::{SearchQuery, SearchResult};
use super::Data;

impl Data {
    pub async fn search(
        &self,
        index: String,
        search_query: SearchQuery,
    ) -> anyhow::Result<SearchResult> {
        self.index_controller.search(index, search_query).await
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
        index: String,
        document_id: String,
        attributes_to_retrieve: Option<Vec<String>>,
    ) -> anyhow::Result<Map<String, Value>>
    {
        self.index_controller.document(index, document_id, attributes_to_retrieve).await
    }
}
