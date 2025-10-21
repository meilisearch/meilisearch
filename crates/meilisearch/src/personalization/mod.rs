use crate::search::{Personalize, SearchResult};
use cohere_rust::{
    api::rerank::{ReRankModel, ReRankRequest},
    Cohere,
};
use meilisearch_types::error::ResponseError;
use tracing::{debug, error, info};

pub struct CohereService {
    cohere: Cohere,
}

impl CohereService {
    pub fn new(api_key: String) -> Self {
        info!("Personalization service initialized with Cohere API");
        Self { cohere: Cohere::new("https://api.cohere.ai", api_key) }
    }

    pub async fn rerank_search_results(
        &self,
        search_result: SearchResult,
        personalize: Option<&Personalize>,
        query: Option<&str>,
    ) -> Result<SearchResult, ResponseError> {
        // Extract user context from personalization
        let Some(user_context) = personalize.and_then(|p| p.user_context.as_deref()) else {
            return Ok(search_result);
        };

        // Build the prompt by merging query and user context
        let prompt = match query {
            Some(q) => format!("User Context: {user_context}\nQuery: {q}"),
            None => format!("User Context: {user_context}"),
        };

        // Extract documents for reranking
        let documents: Vec<String> = search_result
            .hits
            .iter()
            .map(|hit| {
                // Convert the document to a string representation for reranking
                serde_json::to_string(&hit.document).unwrap_or_else(|_| "{}".to_string())
            })
            .collect();

        if documents.is_empty() {
            return Ok(search_result);
        }

        // Prepare the rerank request
        let rerank_request = ReRankRequest {
            query: &prompt,
            documents: &documents,
            model: ReRankModel::EnglishV3, // Use the default and more recent model
            top_n: None,
            max_chunks_per_doc: None,
        };

        // Call Cohere's rerank API
        match self.cohere.rerank(&rerank_request).await {
            Ok(rerank_response) => {
                debug!("Cohere rerank successful, reordering {} results", search_result.hits.len());

                // Create a mapping from original index to new rank
                let reranked_indices: Vec<usize> =
                    rerank_response.iter().map(|result| result.index as usize).collect();

                // Reorder the hits based on Cohere's reranking
                let mut reranked_hits = Vec::new();
                for index in reranked_indices.iter() {
                    reranked_hits.push(search_result.hits[*index].clone());
                }

                Ok(SearchResult { hits: reranked_hits, ..search_result })
            }
            Err(e) => {
                error!("Cohere rerank failed with model EnglishV3: {}", e);
                // Return original results on error
                Ok(search_result)
            }
        }
    }
}

pub enum PersonalizationService {
    Cohere(CohereService),
    Uninitialized,
}

impl PersonalizationService {
    pub fn cohere(api_key: String) -> Self {
        Self::Cohere(CohereService::new(api_key))
    }

    pub fn uninitialized() -> Self {
        debug!("Personalization service uninitialized");
        Self::Uninitialized
    }

    pub async fn rerank_search_results(
        &self,
        search_result: SearchResult,
        personalize: Option<&Personalize>,
        query: Option<&str>,
    ) -> Result<SearchResult, ResponseError> {
        match self {
            Self::Cohere(cohere_service) => {
                cohere_service.rerank_search_results(search_result, personalize, query).await
            }
            Self::Uninitialized => Ok(search_result),
        }
    }
}
